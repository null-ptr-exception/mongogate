#!/usr/bin/env bash
# Brings up the mongogate E2E test environment: a kind cluster with two
# namespaces (mongo-source, mongo-target), each a 3-node MongoDB replica
# set with auth enabled, plus a monitor Mongo instance.
#
# Mongo version per side is a variable, not something to hand-edit in the
# checked-in YAML: override via env var, e.g.
#   SOURCE_MONGO_VERSION=7.0 bash setup.sh
# Defaults match what's checked in (source 4.4, target 8.0) so plain
# `bash setup.sh` reproduces docs/TESTING.md section 8 unchanged. Neither
# variable ever touches the YAML on disk - substituted on the fly and piped
# straight into `kubectl apply -f -`.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
KIND_DIR="kind"
CLUSTER_NAME="mongogate-e2e"
SOURCE_MONGO_VERSION="${SOURCE_MONGO_VERSION:-4.4}"
TARGET_MONGO_VERSION="${TARGET_MONGO_VERSION:-8.0}"
# Test-only credential for an ephemeral kind cluster, never a real secret -
# still a single variable, not copy-pasted, so there's no way for a typo in
# one of the many places that need it to silently diverge from the rest.
MONGO_ROOT_USER="${MONGO_ROOT_USER:-root}"
MONGO_ROOT_PASSWORD="${MONGO_ROOT_PASSWORD:-rootpass123}"

echo "==> Creating kind cluster ($CLUSTER_NAME)..."
if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  echo "    cluster already exists, skipping"
else
  kind create cluster --config "$KIND_DIR/cluster-config.yaml"
fi

echo "==> Waiting for node to be Ready..."
kubectl wait --for=condition=Ready node --all --timeout=120s

echo "==> Creating namespaces..."
kubectl apply -f "$KIND_DIR/namespaces.yaml"

setup_replica_set() {
  local ns="$1"
  echo "==> [$ns] Generating keyfile secret..."
  if ! kubectl -n "$ns" get secret mongo-keyfile >/dev/null 2>&1; then
    openssl rand -base64 756 > /tmp/mongo-keyfile-$ns
    kubectl -n "$ns" create secret generic mongo-keyfile \
      --from-file=keyfile=/tmp/mongo-keyfile-$ns
    rm -f /tmp/mongo-keyfile-$ns
  fi

  echo "==> [$ns] Applying StatefulSet..."
  local side version
  if [ "$ns" = mongo-source ]; then side="source"; version="$SOURCE_MONGO_VERSION"
  else side="target"; version="$TARGET_MONGO_VERSION"
  fi
  sed "s|image: mongo:[^[:space:]]*|image: mongo:${version}|g" "$KIND_DIR/$side/statefulset.yaml" \
    | kubectl apply -f -

  echo "==> [$ns] Waiting for all 3 replicas to be ready..."
  kubectl -n "$ns" wait --for=jsonpath='{.status.readyReplicas}'=3 statefulset/mongo --timeout=240s

  # Detect the shell binary actually present in this image rather than
  # assuming by namespace: mongo:4.4 ships only the legacy `mongo` shell,
  # mongo:6.0+ ships only `mongosh`, and mongo:5.0 ships both - so which
  # namespace has which depends entirely on which image tag is currently
  # set in that namespace's statefulset.yaml, not on source-vs-target.
  local shell="mongosh"
  if ! kubectl -n "$ns" exec mongo-0 -- which mongosh >/dev/null 2>&1; then
    shell="mongo"
  fi
  echo "    [$ns] using shell: $shell"

  echo "==> [$ns] Initiating replica set..."
  # Both the throw-on-error path (mongosh) and the return-ok:0 path (legacy
  # mongo shell, pre-6.0 images) need handling here - the two shells disagree
  # on whether replSetGetStatus failing throws a JS exception or just
  # returns {ok:0,...}, so check both.
  kubectl -n "$ns" exec mongo-0 -- "$shell" --quiet --eval "
    function doInitiate() {
      rs.initiate({
        _id: 'rs0',
        members: [
          { _id: 0, host: 'mongo-0.mongo-headless.$ns.svc.cluster.local:27017' },
          { _id: 1, host: 'mongo-1.mongo-headless.$ns.svc.cluster.local:27017' },
          { _id: 2, host: 'mongo-2.mongo-headless.$ns.svc.cluster.local:27017' }
        ]
      });
    }
    try {
      var st = db.adminCommand({replSetGetStatus: 1});
      if (st.ok === 1) {
        print('replica set already initiated');
      } else {
        doInitiate();
      }
    } catch (e) {
      doInitiate();
    }
  "

  # rs.status() can be read from any member regardless of who's primary, but
  # createUser is a write - it must land on whichever pod actually holds the
  # PRIMARY role. mongo-0 isn't guaranteed to be that pod (election doesn't
  # always favor the initiator, especially under the CPU contention noted
  # above). Hardcoding mongo-0 here caused a real failure: createUser threw
  # "not primary", but the catch below swallowed it as "may already exist"
  # and moved on, only to fail confusingly 120s later when no member could
  # authenticate. Resolve the actual primary pod and target it directly.
  echo "==> [$ns] Waiting for PRIMARY election..."
  PRIMARY_POD=""
  for i in $(seq 1 30); do
    PRIMARY_HOST=$(kubectl -n "$ns" exec mongo-0 -- "$shell" --quiet --eval "
      var p = rs.status().members.find(m => m.stateStr === 'PRIMARY');
      print(p ? p.name : '');
    " 2>/dev/null | tail -1)
    if [ -n "$PRIMARY_HOST" ]; then
      PRIMARY_POD="${PRIMARY_HOST%%.*}"
      echo "    PRIMARY elected: $PRIMARY_POD"
      break
    fi
    sleep 2
  done
  if [ -z "$PRIMARY_POD" ]; then
    echo "::error::[$ns] no PRIMARY elected after 60s"
    exit 1
  fi

  echo "==> [$ns] Creating root user on $PRIMARY_POD (via localhost exception)..."
  kubectl -n "$ns" exec "$PRIMARY_POD" -- "$shell" --quiet --eval "
    db.getSiblingDB('admin').runCommand({ping:1});
    try {
      db.getSiblingDB('admin').createUser({
        user: '$MONGO_ROOT_USER', pwd: '$MONGO_ROOT_PASSWORD',
        roles: [{ role: 'root', db: 'admin' }]
      });
      print('root user created');
    } catch (e) {
      if (/already exists/i.test(e.message)) {
        print('root user already exists');
      } else {
        print('ERROR creating root user: ' + e.message);
        quit(1);
      }
    }
  "

  # createUser only waits for the configured write concern (majority on
  # 8.0's implicit default, but not necessarily all 3 members, and not
  # guaranteed at all on older defaults) - a client connecting with the
  # full mongo-0,1,2 seed list (as loadgen does) authenticates against
  # every discovered member, including any secondary that hasn't replayed
  # the new user yet. Caught this for real: a CI run failed with
  # "AuthenticationFailed" from loadgen hitting exactly that race. Close
  # the window by confirming the user is independently authenticatable on
  # every member before declaring the replica set ready.
  # Both checks below previously just retried-then-fell-through: if every
  # attempt failed, the for loop simply ended and the function returned
  # "success" anyway, since reaching the end of a bash for loop isn't a
  # failure. Confirmed live with a diagnostic dump (real rs.status() output
  # in a failing CI run): mongo-target stayed perfectly healthy
  # (PRIMARY+2 SECONDARY, health:1) for the entire 2+ minute window while
  # mongo-source's root user never became authenticatable at all - even a
  # fresh, direct mongosh session couldn't, not just loadgen. That's a real
  # stuck-replica-set case the old check silently waited out and then lied
  # about. Now both checks actually fail the script (set -e) if the budget
  # is exhausted, with the same rs.status() dump used in e2e.yml so a stuck
  # cluster is diagnosable instead of surfacing as a confusing downstream
  # AuthenticationFailed three steps later.
  dump_rs_status() {
    echo "    [$ns] rs.status() member states:"
    kubectl -n "$ns" exec mongo-0 -- "$shell" --quiet -u "$MONGO_ROOT_USER" -p "$MONGO_ROOT_PASSWORD" \
      --authenticationDatabase admin --eval \
      "JSON.stringify(rs.status().members.map(m=>({host:m.name,state:m.stateStr,health:m.health})))" \
      2>&1 || echo "    [$ns] (couldn't even query rs.status() - root user itself isn't authenticating)"
  }

  echo "==> [$ns] Waiting for root user to replicate to every member..."
  for member in mongo-0 mongo-1 mongo-2; do
    ok=false
    for i in $(seq 1 60); do
      if kubectl -n "$ns" exec "$member" -- "$shell" --quiet \
           -u "$MONGO_ROOT_USER" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
           --eval "1" >/dev/null 2>&1; then
        ok=true
        break
      fi
      sleep 2
    done
    if [ "$ok" != true ]; then
      echo "::error::[$ns] $member never became authenticatable after 120s"
      dump_rs_status
      exit 1
    fi
  done

  # The per-member check above wasn't sufficient on its own - confirmed
  # live, a CI run still hit AuthenticationFailed afterward. A direct
  # `kubectl exec <pod> -- mongosh -u ... ` always happens to land on that
  # exact pod; it doesn't exercise the same multi-host discovery/mechanism
  # negotiation loadgen's driver does when given all three hosts in one
  # connection string. Check that too, since that's what actually failed.
  SEEDLIST="mongo-0.mongo-headless.$ns.svc.cluster.local:27017,mongo-1.mongo-headless.$ns.svc.cluster.local:27017,mongo-2.mongo-headless.$ns.svc.cluster.local:27017"
  ok=false
  for i in $(seq 1 60); do
    if kubectl -n "$ns" exec mongo-0 -- "$shell" --quiet \
         "mongodb://${MONGO_ROOT_USER}:${MONGO_ROOT_PASSWORD}@${SEEDLIST}/admin?replicaSet=rs0" \
         --eval "1" >/dev/null 2>&1; then
      ok=true
      break
    fi
    sleep 2
  done
  if [ "$ok" != true ]; then
    echo "::error::[$ns] multi-host connection string never became authenticatable after 120s"
    dump_rs_status
    exit 1
  fi

  echo "==> [$ns] Replica set ready."
}

setup_replica_set mongo-source
setup_replica_set mongo-target

echo "==> Deploying monitor Mongo..."
kubectl apply -f "$KIND_DIR/monitor/deployment.yaml"
kubectl -n mongogate-test wait --for=condition=Ready pod -l app=monitor-mongo --timeout=120s

echo "==> Environment is up."
echo "    Source: kubectl -n mongo-source exec mongo-0 -- mongosh (or mongo, depending on image)"
echo "    Target: kubectl -n mongo-target exec mongo-0 -- mongosh (or mongo, depending on image)"
echo "    Monitor: kubectl -n mongogate-test exec deploy/monitor-mongo -- mongosh"

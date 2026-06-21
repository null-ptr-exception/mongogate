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

  echo "==> [$ns] Waiting for PRIMARY election..."
  for i in $(seq 1 30); do
    STATE=$(kubectl -n "$ns" exec mongo-0 -- "$shell" --quiet --eval "rs.status().members.some(m => m.stateStr === 'PRIMARY')" 2>/dev/null | tail -1)
    if [ "$STATE" = "true" ]; then
      echo "    PRIMARY elected"
      break
    fi
    sleep 2
  done

  echo "==> [$ns] Creating root user (via localhost exception)..."
  kubectl -n "$ns" exec mongo-0 -- "$shell" --quiet --eval "
    db.getSiblingDB('admin').runCommand({ping:1});
    try {
      db.getSiblingDB('admin').createUser({
        user: 'root', pwd: 'rootpass123',
        roles: [{ role: 'root', db: 'admin' }]
      });
      print('root user created');
    } catch (e) {
      print('root user may already exist: ' + e.message);
    }
  " || true

  # createUser only waits for the configured write concern (majority on
  # 8.0's implicit default, but not necessarily all 3 members, and not
  # guaranteed at all on older defaults) - a client connecting with the
  # full mongo-0,1,2 seed list (as loadgen does) authenticates against
  # every discovered member, including any secondary that hasn't replayed
  # the new user yet. Caught this for real: a CI run failed with
  # "AuthenticationFailed" from loadgen hitting exactly that race. Close
  # the window by confirming the user is independently authenticatable on
  # every member before declaring the replica set ready.
  echo "==> [$ns] Waiting for root user to replicate to every member..."
  for member in mongo-0 mongo-1 mongo-2; do
    for i in $(seq 1 30); do
      if kubectl -n "$ns" exec "$member" -- "$shell" --quiet \
           -u root -p rootpass123 --authenticationDatabase admin \
           --eval "1" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
  done

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

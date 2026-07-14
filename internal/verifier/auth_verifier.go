package verifier

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

func VerifyAuth(ctx context.Context, src, tgt *mongo.Client, rpt *report.Report) {
	fmt.Println("\n🔐 [Phase1] Verifying accounts & permissions")
	var errors []string

	// ── Users ──
	// A fetch failure on either side must not fall through to the
	// comparison loop below: an empty/nil map from a swallowed error looks
	// identical to "this side genuinely has no users," which would report
	// every real user as "❌ Missing" - masking a connectivity/permissions
	// problem as a complete user-migration failure.
	srcUsers, srcErr := getUsers(ctx, src)
	tgtUsers, tgtErr := getUsers(ctx, tgt)
	if srcErr != nil {
		errors = append(errors, fmt.Sprintf("failed to get source users: %v", srcErr))
	}
	if tgtErr != nil {
		errors = append(errors, fmt.Sprintf("failed to get target users: %v", tgtErr))
	}
	if srcErr != nil || tgtErr != nil {
		srcUsers, tgtUsers = nil, nil
	}
	for username, srcUser := range srcUsers {
		tgtUser, ok := tgtUsers[username]
		if !ok {
			errors = append(errors, fmt.Sprintf("❌ Missing user: %s", username))
			continue
		}
		if !slices.Equal(srcUser.Roles, tgtUser.Roles) {
			errors = append(errors,
				fmt.Sprintf("⚠️  User [%s] has different roles\n     src=%v\n     tgt=%v",
					username, srcUser.Roles, tgtUser.Roles))
		}
		// Per-user auth mechanism (e.g. SCRAM vs x.509 on the same username
		// with identical roles) - distinct from the server-wide enabled
		// mechanism list checked below, and confirmed not version-driven
		// (a fresh user gets the same default mechanism set on 4.4 through
		// 8.0) - a mismatch here means deliberate config drift, not a
		// version artifact, so this is a blocking check.
		if !slices.Equal(srcUser.Mechanisms, tgtUser.Mechanisms) {
			errors = append(errors,
				fmt.Sprintf("⚠️  User [%s] has different auth mechanisms\n     src=%v\n     tgt=%v",
					username, srcUser.Mechanisms, tgtUser.Mechanisms))
		}
	}
	for username := range tgtUsers {
		if _, ok := srcUsers[username]; !ok {
			errors = append(errors, fmt.Sprintf("⚠️  Extra user in target: %s", username))
		}
	}

	// ── Custom Roles (name + actual privilege content + inherited roles, not just existence) ──
	// Same reasoning as Users above: don't compare past a fetch failure.
	srcRoles, srcRoleErr := getRoles(ctx, src)
	tgtRoles, tgtRoleErr := getRoles(ctx, tgt)
	if srcRoleErr != nil {
		errors = append(errors, fmt.Sprintf("failed to get source roles: %v", srcRoleErr))
	}
	if tgtRoleErr != nil {
		errors = append(errors, fmt.Sprintf("failed to get target roles: %v", tgtRoleErr))
	}
	if srcRoleErr != nil || tgtRoleErr != nil {
		srcRoles, tgtRoles = nil, nil
	}
	for role, srcInfo := range srcRoles {
		tgtInfo, ok := tgtRoles[role]
		if !ok {
			errors = append(errors, fmt.Sprintf("❌ Missing role: %s", role))
			continue
		}
		if srcInfo.Privileges != tgtInfo.Privileges {
			errors = append(errors,
				fmt.Sprintf("⚠️  Role [%s] has different privileges\n     src=%s\n     tgt=%s",
					role, srcInfo.Privileges, tgtInfo.Privileges))
		}
		if srcInfo.InheritedRoles != tgtInfo.InheritedRoles {
			errors = append(errors,
				fmt.Sprintf("⚠️  Role [%s] has different inherited roles\n     src=%s\n     tgt=%s",
					role, srcInfo.InheritedRoles, tgtInfo.InheritedRoles))
		}
	}
	for role := range tgtRoles {
		if _, ok := srcRoles[role]; !ok {
			errors = append(errors, fmt.Sprintf("⚠️  Extra role in target: %s", role))
		}
	}

	// ── Auth Mechanism ──
	srcMech := getAuthMechanism(ctx, src)
	tgtMech := getAuthMechanism(ctx, tgt)
	if srcMech != tgtMech {
		errors = append(errors,
			fmt.Sprintf("⚠️  Auth mechanism differs: src=%s tgt=%s", srcMech, tgtMech))
	}

	// ── LDAP (only verified when configured) ──
	srcLDAP := getLDAPServers(ctx, src)
	tgtLDAP := getLDAPServers(ctx, tgt)
	if srcLDAP != "" && srcLDAP != tgtLDAP {
		errors = append(errors,
			fmt.Sprintf("⚠️  LDAP settings differ: src=%s tgt=%s", srcLDAP, tgtLDAP))
	}

	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetAuth(res)
	printStatus("Account permissions", res.Passed, fmt.Sprintf("%d users", len(srcUsers)))
}

// userInfo holds one user's role list and auth mechanism list, both
// sorted for stable equality comparison.
type userInfo struct {
	Roles      []string
	Mechanisms []string
}

func getUsers(ctx context.Context, client *mongo.Client) (map[string]userInfo, error) {
	result := client.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return nil, err
	}
	users := make(map[string]userInfo)
	arr, _ := res["users"].(bson.A)
	for _, u := range arr {
		um, _ := u.(bson.M)
		username, _ := um["user"].(string)
		var roles []string
		if rarr, ok := um["roles"].(bson.A); ok {
			for _, r := range rarr {
				rm, ok := r.(bson.M)
				if !ok {
					continue
				}
				roles = append(roles, fmt.Sprintf("%s@%s", rm["role"], rm["db"]))
			}
		}
		sort.Strings(roles)
		var mechs []string
		if marr, ok := um["mechanisms"].(bson.A); ok {
			for _, m := range marr {
				mechs = append(mechs, fmt.Sprintf("%v", m))
			}
			sort.Strings(mechs)
		}
		users[username] = userInfo{Roles: roles, Mechanisms: mechs}
	}
	return users, nil
}

// roleInfo holds a custom role's directly-granted privileges and its
// inherited sub-role list, normalized for equality comparison.
type roleInfo struct {
	Privileges     string
	InheritedRoles string
}

// getRoles returns each custom role mapped to its normalized privilege and
// inherited-role content, so two roles sharing a name can be compared for
// actual permission content (including what they inherit), not just
// existence.
func getRoles(ctx context.Context, client *mongo.Client) (map[string]roleInfo, error) {
	result := client.Database("admin").RunCommand(ctx,
		bson.D{{Key: "rolesInfo", Value: 1}, {Key: "showPrivileges", Value: true}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return nil, err
	}
	roles := make(map[string]roleInfo)
	arr, _ := res["roles"].(bson.A)
	for _, r := range arr {
		rm, _ := r.(bson.M)
		name, _ := rm["role"].(string)
		roles[name] = roleInfo{
			Privileges:     normalizePrivileges(rm["privileges"]),
			InheritedRoles: normalizeInheritedRoles(rm["roles"]),
		}
	}
	return roles, nil
}

// normalizeInheritedRoles renders a role's sub-role (inheritance) list as a
// sorted, stable string for simple equality comparison - same idea as
// normalizePrivileges, but for the "roles: [...]" field rolesInfo also
// returns, which getRoles previously never looked at.
func normalizeInheritedRoles(rolesField interface{}) string {
	arr, ok := rolesField.(bson.A)
	if !ok {
		return ""
	}
	var entries []string
	for _, r := range arr {
		rm, ok := r.(bson.M)
		if !ok {
			continue
		}
		entries = append(entries, fmt.Sprintf("%v@%v", rm["role"], rm["db"]))
	}
	sort.Strings(entries)
	return strings.Join(entries, ",")
}

// normalizePrivileges renders a role's privilege list as a sorted, stable
// string (resource + actions per entry) for simple equality comparison.
func normalizePrivileges(privileges interface{}) string {
	arr, ok := privileges.(bson.A)
	if !ok {
		return ""
	}
	var entries []string
	for _, p := range arr {
		pm, ok := p.(bson.M)
		if !ok {
			continue
		}
		resource := fmt.Sprintf("%v", pm["resource"])
		var actions []string
		if acts, ok := pm["actions"].(bson.A); ok {
			for _, a := range acts {
				actions = append(actions, fmt.Sprintf("%v", a))
			}
		}
		sort.Strings(actions)
		entries = append(entries, fmt.Sprintf("%s:[%s]", resource, strings.Join(actions, ",")))
	}
	sort.Strings(entries)
	return strings.Join(entries, "|")
}

func getAuthMechanism(ctx context.Context, client *mongo.Client) string {
	result := client.Database("admin").RunCommand(ctx,
		bson.D{{Key: "getParameter", Value: 1}, {Key: "authenticationMechanisms", Value: 1}})
	var res bson.M
	_ = result.Decode(&res)
	if arr, ok := res["authenticationMechanisms"].(bson.A); ok {
		var mechs []string
		for _, m := range arr {
			mechs = append(mechs, fmt.Sprintf("%v", m))
		}
		sort.Strings(mechs)
		return strings.Join(mechs, ",")
	}
	return ""
}

func getLDAPServers(ctx context.Context, client *mongo.Client) string {
	result := client.Database("admin").RunCommand(ctx,
		bson.D{{Key: "getParameter", Value: 1}, {Key: "ldapServers", Value: 1}})
	var res bson.M
	_ = result.Decode(&res)
	if v, ok := res["ldapServers"].(string); ok {
		return v
	}
	return ""
}

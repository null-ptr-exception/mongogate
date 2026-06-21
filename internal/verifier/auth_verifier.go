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
	srcUsers, err := getUsers(ctx, src)
	if err != nil {
		errors = append(errors, fmt.Sprintf("failed to get source users: %v", err))
	}
	tgtUsers, _ := getUsers(ctx, tgt)

	for username, srcRoles := range srcUsers {
		tgtRoles, ok := tgtUsers[username]
		if !ok {
			errors = append(errors, fmt.Sprintf("❌ Missing user: %s", username))
			continue
		}
		if !slices.Equal(srcRoles, tgtRoles) {
			errors = append(errors,
				fmt.Sprintf("⚠️  User [%s] has different roles\n     src=%v\n     tgt=%v",
					username, srcRoles, tgtRoles))
		}
	}
	for username := range tgtUsers {
		if _, ok := srcUsers[username]; !ok {
			errors = append(errors, fmt.Sprintf("⚠️  Extra user in target: %s", username))
		}
	}

	// ── Custom Roles (name + actual privilege content + inherited roles, not just existence) ──
	srcRoles, _ := getRoles(ctx, src)
	tgtRoles, _ := getRoles(ctx, tgt)
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

func getUsers(ctx context.Context, client *mongo.Client) (map[string][]string, error) {
	result := client.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return nil, err
	}
	users := make(map[string][]string)
	arr, _ := res["users"].(bson.A)
	for _, u := range arr {
		um, _ := u.(bson.M)
		username, _ := um["user"].(string)
		var roles []string
		for _, r := range um["roles"].(bson.A) {
			rm := r.(bson.M)
			roles = append(roles, fmt.Sprintf("%s@%s", rm["role"], rm["db"]))
		}
		sort.Strings(roles)
		users[username] = roles
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

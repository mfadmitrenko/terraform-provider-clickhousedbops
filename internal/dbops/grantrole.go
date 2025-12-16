package dbops

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/querybuilder"
)

type GrantRole struct {
	RoleName        string  `json:"granted_role_name"`
	GranteeUserName *string `json:"user_name"`
	GranteeRoleName *string `json:"role_name"`
	AdminOption     bool    `json:"with_admin_option"`
}

func (i *impl) GrantRole(ctx context.Context, grantRole GrantRole, clusterName *string) (*GrantRole, error) {
	var to string
	{
		if grantRole.GranteeUserName != nil {
			to = *grantRole.GranteeUserName
		} else if grantRole.GranteeRoleName != nil {
			to = *grantRole.GranteeRoleName
		} else {
			return nil, errors.New("either GranteeUserName or GranteeRoleName must be set")
		}
	}

	sql, err := querybuilder.GrantRole(grantRole.RoleName, to).WithCluster(clusterName).WithAdminOption(grantRole.AdminOption).Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	// Activate role as DEFAULT ROLE if grantee is a user (not a role)
	if grantRole.GranteeUserName != nil {
		// Try to activate as default role, but don't fail if it doesn't work
		// The role is still granted successfully even if activation fails
		_ = i.activateDefaultRole(ctx, *grantRole.GranteeUserName, grantRole.RoleName, clusterName)
	}

	return i.GetGrantRole(ctx, grantRole.RoleName, grantRole.GranteeUserName, grantRole.GranteeRoleName, clusterName)
}

func (i *impl) GetGrantRole(ctx context.Context, grantedRoleName string, granteeUserName *string, granteeRoleName *string, clusterName *string) (*GrantRole, error) {
	var granteeWhere querybuilder.Where
	{
		if granteeUserName != nil {
			granteeWhere = querybuilder.WhereEquals("user_name", *granteeUserName)
		} else if granteeRoleName != nil {
			granteeWhere = querybuilder.WhereEquals("role_name", *granteeRoleName)
		} else {
			return nil, errors.New("either GranteeUserName or GranteeRoleName must be set")
		}
	}

	sql, err := querybuilder.NewSelect(
		[]querybuilder.Field{
			querybuilder.NewField("granted_role_name"),
			querybuilder.NewField("user_name"),
			querybuilder.NewField("role_name"),
			querybuilder.NewField("with_admin_option"),
		},
		"system.role_grants").
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("granted_role_name", grantedRoleName), granteeWhere).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	var grantRole *GrantRole

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		roleName, err := data.GetString("granted_role_name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'name' field")
		}
		granteeUserName, err := data.GetNullableString("user_name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'user_name' field")
		}
		granteeRoleName, err := data.GetNullableString("role_name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'role_name' field")
		}
		adminOption, err := data.GetBool("with_admin_option")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'with_admin_option' field")
		}
		grantRole = &GrantRole{
			RoleName:        roleName,
			GranteeUserName: granteeUserName,
			GranteeRoleName: granteeRoleName,
			AdminOption:     adminOption,
		}
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	if grantRole == nil {
		// Grant not found
		return nil, nil
	}

	return grantRole, nil
}

func (i *impl) RevokeGrantRole(ctx context.Context, grantedRoleName string, granteeUserName *string, granteeRoleName *string, clusterName *string) error {
	var grantee string
	{
		if granteeUserName != nil {
			grantee = *granteeUserName
		} else if granteeRoleName != nil {
			grantee = *granteeRoleName
		} else {
			return errors.New("either GranteeUserName or GranteeRoleName must be set")
		}
	}
	sql, err := querybuilder.RevokeRole(grantedRoleName, grantee).WithCluster(clusterName).Build()
	if err != nil {
		return errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return errors.WithMessage(err, "error running query")
	}

	// Deactivate role from DEFAULT ROLE if grantee is a user (not a role)
	if granteeUserName != nil {
		// Try to deactivate from default role, but don't fail if it doesn't work
		// The role is still revoked successfully even if deactivation fails
		_ = i.deactivateDefaultRole(ctx, *granteeUserName, grantedRoleName, clusterName)
	}

	return nil
}

// activateDefaultRole adds the role to user's default roles using ALTER USER DEFAULT ROLE
func (i *impl) activateDefaultRole(ctx context.Context, userName string, roleName string, clusterName *string) error {
	// Get current default roles
	currentRoles, err := i.getDefaultRoles(ctx, userName, clusterName)
	if err != nil {
		// If we can't get default roles (e.g., user doesn't exist yet), skip activation
		// The role is still granted, just not activated as default
		return nil
	}

	// Check if role is already in default roles
	for _, role := range currentRoles {
		if role == roleName {
			// Role is already a default role, nothing to do
			return nil
		}
	}

	// Add the new role to the list
	currentRoles = append(currentRoles, roleName)

	// Build ALTER USER DEFAULT ROLE query
	sql := buildAlterUserDefaultRoleSQL(userName, currentRoles, clusterName)

	// Execute the query
	if err := i.clickhouseClient.Exec(ctx, sql); err != nil {
		// If ALTER USER fails, return error but don't fail the entire grant operation
		// The role is still granted, just not activated as default
		return errors.WithMessage(err, "error executing ALTER USER DEFAULT ROLE")
	}

	return nil
}

// deactivateDefaultRole removes the role from user's default roles using ALTER USER DEFAULT ROLE
func (i *impl) deactivateDefaultRole(ctx context.Context, userName string, roleName string, clusterName *string) error {
	// Get current default roles
	currentRoles, err := i.getDefaultRoles(ctx, userName, clusterName)
	if err != nil {
		// If we can't get default roles, skip deactivation
		// The role is still revoked, just not deactivated from default
		return nil
	}

	// Check if role is in default roles
	found := false
	newRoles := make([]string, 0, len(currentRoles))
	for _, role := range currentRoles {
		if role == roleName {
			found = true
			// Skip this role - remove it from the list
			continue
		}
		newRoles = append(newRoles, role)
	}

	// If role was not in default roles, nothing to do
	if !found {
		return nil
	}

	// Build ALTER USER DEFAULT ROLE query with updated list
	sql := buildAlterUserDefaultRoleSQL(userName, newRoles, clusterName)

	// Execute the query
	if err := i.clickhouseClient.Exec(ctx, sql); err != nil {
		// If ALTER USER fails, return error but don't fail the entire revoke operation
		// The role is still revoked, just not deactivated from default
		return errors.WithMessage(err, "error executing ALTER USER DEFAULT ROLE")
	}

	return nil
}

// getDefaultRoles retrieves current default roles for a user from system.users
func (i *impl) getDefaultRoles(ctx context.Context, userName string, clusterName *string) ([]string, error) {
	// Use toString() to convert Array(String) to string representation
	sql, err := querybuilder.
		NewSelect(
			[]querybuilder.Field{querybuilder.NewField("default_roles_list").ToString()},
			"system.users",
		).
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("name", userName)).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building SELECT query")
	}

	var roles []string
	found := false
	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		found = true
		// default_roles_list is an Array(String) in ClickHouse, converted to string via toString()
		// toString() always returns a string, even for empty arrays (returns "[]")
		rolesValue, err := data.GetString("default_roles_list")
		if err != nil {
			// Try nullable string as fallback
			rolesValuePtr, err2 := data.GetNullableString("default_roles_list")
			if err2 != nil {
				return errors.WithMessage(err, "error scanning default_roles_list field")
			}
			if rolesValuePtr == nil || *rolesValuePtr == "" {
				return nil // No default roles
			}
			rolesValue = *rolesValuePtr
		}

		if rolesValue == "" || rolesValue == "[]" {
			return nil // No default roles
		}

		// Parse the array string format from ClickHouse toString()
		// ClickHouse toString() returns arrays as ['role1','role2'] or [] for empty
		rolesStr := strings.Trim(rolesValue, "[]")
		if rolesStr == "" {
			return nil
		}

		// Split by comma and clean up quotes
		parts := strings.Split(rolesStr, ",")
		for _, part := range parts {
			role := strings.Trim(strings.TrimSpace(part), "'\"")
			if role != "" {
				roles = append(roles, role)
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running SELECT query")
	}

	if !found {
		// User not found, return empty roles list
		return []string{}, nil
	}

	return roles, nil
}

// buildAlterUserDefaultRoleSQL builds ALTER USER ... DEFAULT ROLE SQL query
func buildAlterUserDefaultRoleSQL(userName string, roles []string, clusterName *string) string {
	var roleClause string
	if len(roles) == 0 {
		// If no roles, use NONE to remove all default roles
		roleClause = "NONE"
	} else {
		// Quote role names
		quotedRoles := make([]string, 0, len(roles))
		for _, role := range roles {
			quotedRoles = append(quotedRoles, fmt.Sprintf("`%s`", role))
		}
		roleClause = strings.Join(quotedRoles, ", ")
	}

	sql := fmt.Sprintf(
		"ALTER USER `%s` DEFAULT ROLE %s",
		userName,
		roleClause,
	)

	if clusterName != nil {
		sql += fmt.Sprintf(" ON CLUSTER %s", *clusterName)
	}

	return sql + ";"
}

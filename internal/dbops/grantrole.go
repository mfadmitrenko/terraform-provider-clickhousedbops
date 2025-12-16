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
		if err := i.activateDefaultRole(ctx, *grantRole.GranteeUserName, grantRole.RoleName, clusterName); err != nil {
			return nil, errors.WithMessage(err, "error activating default role")
		}
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

	return nil
}

// activateDefaultRole adds the role to user's default roles using ALTER USER DEFAULT ROLE
func (i *impl) activateDefaultRole(ctx context.Context, userName string, roleName string, clusterName *string) error {
	// Get current default roles
	currentRoles, err := i.getDefaultRoles(ctx, userName, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error getting current default roles")
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
		return errors.WithMessage(err, "error executing ALTER USER DEFAULT ROLE")
	}

	return nil
}

// getDefaultRoles retrieves current default roles for a user from system.users
func (i *impl) getDefaultRoles(ctx context.Context, userName string, clusterName *string) ([]string, error) {
	// Use toString() to convert Array(String) to string representation
	sql, err := querybuilder.
		NewSelect(
			[]querybuilder.Field{querybuilder.NewField("default_roles").ToString()},
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
		// default_roles is an Array(String) in ClickHouse, converted to string via toString()
		rolesValue, err := data.GetNullableString("default_roles")
		if err != nil {
			return errors.WithMessage(err, "error scanning default_roles field")
		}
		if rolesValue == nil || *rolesValue == "" {
			return nil // No default roles
		}

		// Parse the array string format from ClickHouse toString()
		// ClickHouse toString() returns arrays as ['role1','role2'] or [] for empty
		rolesStr := strings.Trim(*rolesValue, "[]")
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
	// Quote role names
	quotedRoles := make([]string, 0, len(roles))
	for _, role := range roles {
		quotedRoles = append(quotedRoles, fmt.Sprintf("`%s`", role))
	}

	sql := fmt.Sprintf(
		"ALTER USER `%s` DEFAULT ROLE %s",
		userName,
		strings.Join(quotedRoles, ", "),
	)

	if clusterName != nil {
		sql += fmt.Sprintf(" ON CLUSTER %s", *clusterName)
	}

	return sql + ";"
}

package dbops

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/querybuilder"
)

type SettingsProfile struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	InheritFrom []string `json:"-"`
}

func (i *impl) CreateSettingsProfile(ctx context.Context, profile SettingsProfile, clusterName *string) (*SettingsProfile, error) {
	sql, err := querybuilder.
		NewCreateSettingsProfile(profile.Name).
		WithCluster(clusterName).
		InheritFrom(profile.InheritFrom).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	return i.FindSettingsProfileByName(ctx, profile.Name, clusterName)
}

func (i *impl) GetSettingsProfile(ctx context.Context, id string, clusterName *string) (*SettingsProfile, error) {
	var profile *SettingsProfile

	sql, err := querybuilder.
		NewSelect(
			[]querybuilder.Field{
				querybuilder.NewField("name"),
			},
			"system.settings_profiles",
		).
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("id", id)).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		name, err := data.GetString("name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'name' field")
		}

		if profile == nil {
			profile = &SettingsProfile{
				ID:   id,
				Name: name,
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	if profile == nil {
		// SettingsProfile not found
		return nil, nil
	}

	// Check roles this profile is inheriting from.
	{
		sql, err := querybuilder.
			NewSelect([]querybuilder.Field{querybuilder.NewField("inherit_profile")}, "system.settings_profile_elements").
			Where(querybuilder.WhereEquals("profile_name", profile.Name)).
			OrderBy(querybuilder.NewField("index"), querybuilder.ASC).
			Build()
		if err != nil {
			return nil, errors.WithMessage(err, "error building query")
		}
		err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
			inheritedProfileName, err := data.GetNullableString("inherit_profile")
			if err != nil {
				return errors.WithMessage(err, "error scanning query result, missing 'profile_name' field")
			}

			if inheritedProfileName != nil {
				profile.InheritFrom = append(profile.InheritFrom, *inheritedProfileName)
			}

			return nil
		})
		if err != nil {
			return nil, errors.WithMessage(err, "error running query")
		}
	}

	return profile, nil
}

func (i *impl) DeleteSettingsProfile(ctx context.Context, id string, clusterName *string) error {
	profile, err := i.GetSettingsProfile(ctx, id, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error looking up settings profile name")
	}

	if profile == nil {
		// Desired status
		return nil
	}

	sql, err := querybuilder.NewDropSettingsProfile(profile.Name).WithCluster(clusterName).Build()
	if err != nil {
		return errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return errors.WithMessage(err, "error running query")
	}

	return nil
}

func (i *impl) UpdateSettingsProfile(ctx context.Context, settingsProfile SettingsProfile, clusterName *string) (*SettingsProfile, error) {
	// Retrieve current setting profile
	existing, err := i.GetSettingsProfile(ctx, settingsProfile.ID, clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "Unable to get existing settings profile")
	}

	if existing == nil {
		return nil, nil
	}

	sql, err := querybuilder.
		NewAlterSettingsProfile(existing.Name).
		WithCluster(clusterName).
		InheritFrom(settingsProfile.InheritFrom).
		RenameTo(&settingsProfile.Name).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	return i.GetSettingsProfile(ctx, settingsProfile.ID, clusterName)
}

func (i *impl) AssociateSettingsProfile(ctx context.Context, id string, roleId *string, userId *string, clusterName *string) error {
	profile, err := i.GetSettingsProfile(ctx, id, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error looking up settings profile name")
	}

	if profile == nil {
		return errors.New("No Settings Profile with such ID found")
	}

	if roleId != nil {
		role, err := i.GetRole(ctx, *roleId, clusterName)
		if err != nil {
			return errors.WithMessage(err, "Cannot find role")
		}

		if role == nil {
			return errors.New("role not found")
		}
		sql, err := querybuilder.
			NewAlterRole(role.Name).
			WithCluster(clusterName).
			AddSettingsProfile(&profile.Name).
			Build()
		if err != nil {
			return errors.WithMessage(err, "Error building query")
		}

		err = i.clickhouseClient.Exec(ctx, sql)
		if err != nil {
			return errors.WithMessage(err, "error running query")
		}

		return nil
	} else if userId != nil {
		user, err := i.resolveUserName(ctx, *userId, clusterName)
		if err != nil {
			return errors.WithMessage(err, "error resolving user")
		}
		if user == "" {
			return errors.New("Cannot find user")
		}

		sql, err := querybuilder.
			NewAlterUser(user).
			WithCluster(clusterName).
			SetSettingsProfile(&profile.Name).
			Build()
		if err != nil {
			return errors.WithMessage(err, "Error building query")
		}

		err = i.clickhouseClient.Exec(ctx, sql)
		if err != nil {
			return errors.WithMessage(err, "error running query")
		}

		return nil
	}

	return errors.New("Neither roleId nor userId were specified")
}

func (i *impl) DisassociateSettingsProfile(ctx context.Context, id string, roleId *string, userId *string, clusterName *string) error {
	profile, err := i.GetSettingsProfile(ctx, id, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error looking up settings profile name")
	}

	if profile == nil {
		return errors.New("No Settings Profile with such ID found")
	}

	if roleId != nil {
		role, err := i.GetRole(ctx, *roleId, clusterName)
		if err != nil {
			return errors.WithMessage(err, "Cannot find role")
		}

		if role == nil {
			return errors.New("role not found")
		}

		sql, err := querybuilder.
			NewAlterRole(role.Name).
			WithCluster(clusterName).
			DropSettingsProfile(&profile.Name).
			Build()
		if err != nil {
			return errors.WithMessage(err, "Error building query")
		}

		err = i.clickhouseClient.Exec(ctx, sql)
		if err != nil {
			return errors.WithMessage(err, "error running query")
		}

		return nil
	} else if userId != nil {
		user, err := i.resolveUserName(ctx, *userId, clusterName)
		if err != nil {
			return errors.WithMessage(err, "error resolving user")
		}
		if user == "" {
			return errors.New("Cannot find user")
		}

		empty := ""
		sql, err := querybuilder.
			NewAlterUser(user).
			WithCluster(clusterName).
			SetSettingsProfile(&empty).
			Build()
		if err != nil {
			return errors.WithMessage(err, "Error building query")
		}

		err = i.clickhouseClient.Exec(ctx, sql)
		if err != nil {
			return errors.WithMessage(err, "error running query")
		}

		return nil
	}

	return errors.New("Neither roleId nor userId were specified")
}

func (i *impl) FindSettingsProfileByName(ctx context.Context, name string, clusterName *string) (*SettingsProfile, error) {
	sql, err := querybuilder.
		NewSelect(
			[]querybuilder.Field{
				querybuilder.NewField("id").ToString(),
			},
			"system.settings_profiles",
		).
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("name", name)).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	var settingsProfileID string

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		id, err := data.GetString("id")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'id' field")
		}

		settingsProfileID = id

		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	if settingsProfileID == "" {
		return nil, errors.New(fmt.Sprintf("settings profile with name %s not found", name))
	}

	return i.GetSettingsProfile(ctx, settingsProfileID, clusterName)
}

func (i *impl) GetSettingsProfileByName(ctx context.Context, name string, clusterName *string) (*SettingsProfile, error) {
	return i.FindSettingsProfileByName(ctx, name, clusterName)
}

// ALTER USER/ROLE IF EXISTS ... SETTINGS PROFILE <name>
func (i *impl) AssociateSettingsProfileByName(
	ctx context.Context,
	profileName string,
	roleId *string,
	userId *string,
	clusterName *string,
) error {
	if profileName == "" {
		return errors.New("profile name must be provided")
	}
	if (roleId == nil || *roleId == "") && (userId == nil || *userId == "") {
		return errors.New("either role_id or user_id must be provided")
	}

	// USER path (legacy, 23.4)
	if userId != nil && *userId != "" {
		u, err := i.resolveUserName(ctx, *userId, clusterName)
		if err != nil {
			return errors.WithMessage(err, "error resolving user")
		}
		if u == "" {
			return errors.New("Cannot find user")
		}

		sqlStr, err := querybuilder.NewAlterUser(u).
			IfExists().
			WithCluster(clusterName).
			SetSettingsProfile(&profileName).
			Build()
		if err != nil {
			return errors.WithMessage(err, "Error building legacy ALTER USER ... SETTINGS PROFILE query")
		}
		return errors.WithMessage(i.clickhouseClient.Exec(ctx, sqlStr), "error running legacy ALTER USER ... SETTINGS PROFILE query")
	}

	// ROLE path (legacy, 23.4)
	if roleId != nil && *roleId != "" {
		r, err := i.GetRole(ctx, *roleId, clusterName)
		if err != nil {
			return errors.WithMessage(err, "Cannot find role")
		}
		if r == nil {
			return errors.New("role not found")
		}

		sqlStr, err := querybuilder.NewAlterRole(r.Name).
			IfExists().
			WithCluster(clusterName).
			SetSettingsProfile(&profileName).
			Build()
		if err != nil {
			return errors.WithMessage(err, "Error building legacy ALTER ROLE ... SETTINGS PROFILE query")
		}
		return errors.WithMessage(i.clickhouseClient.Exec(ctx, sqlStr), "error running legacy ALTER ROLE ... SETTINGS PROFILE query")
	}

	return errors.New("Neither roleId nor userId were specified")
}

package dbops

import (
	"context"

	"github.com/pingcap/errors"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/querybuilder"
)

type User struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	PasswordSha256Hash string   `json:"-"`
	DefaultRole        string   `json:"-"`
	SSLCertificateCN   string   `json:"-"`
	SettingsProfiles   []string `json:"-"`
}

func (u *User) HasSettingProfile(profileName string) bool {
	for _, p := range u.SettingsProfiles {
		if p == profileName {
			return true
		}
	}

	return false
}

func (i *impl) CreateUser(ctx context.Context, user User, clusterName *string) (*User, error) {
	q := querybuilder.
		NewCreateUser(user.Name).
		WithCluster(clusterName)

	// Choose identification method
	if user.SSLCertificateCN != "" {
		q = q.IdentifiedWithSSLCertCN(user.SSLCertificateCN)
	} else if user.PasswordSha256Hash != "" {
		q = q.Identified(querybuilder.IdentificationSHA256Hash, user.PasswordSha256Hash)
	}

	if user.DefaultRole != "" {
		q = q.WithDefaultRole(&user.DefaultRole)
	}

	sql, err := q.Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	return i.FindUserByName(ctx, user.Name, clusterName)
}

func (i *impl) GetUser(ctx context.Context, id string, clusterName *string) (*User, error) { // nolint:dupl
	sql, err := querybuilder.
		NewSelect([]querybuilder.Field{querybuilder.NewField("name")}, "system.users").
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("id", id)).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	var user *User

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		n, err := data.GetString("name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'name' field")
		}
		user = &User{
			ID:   id,
			Name: n,
		}
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	if user == nil {
		// User not found
		return nil, nil
	}

	// Check if user has settings profile associated.
	{
		sql, err = querybuilder.
			NewSelect([]querybuilder.Field{querybuilder.NewField("inherit_profile")}, "system.settings_profile_elements").
			WithCluster(clusterName).
			Where(querybuilder.WhereEquals("user_name", user.Name)).
			Build()
		if err != nil {
			return nil, errors.WithMessage(err, "error building query")
		}

		profiles := make([]string, 0)
		err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
			profile, err := data.GetNullableString("inherit_profile")
			if err != nil {
				return errors.WithMessage(err, "error scanning query result, missing 'inherit_profile' field")
			}

			if profile != nil {
				profiles = append(profiles, *profile)
			}
			return nil
		})
		if err != nil {
			return nil, errors.WithMessage(err, "error running query")
		}

	}

	return user, nil
}

func (i *impl) DeleteUser(ctx context.Context, id string, clusterName *string) error {
	user, err := i.GetUser(ctx, id, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error getting user")
	}

	if user == nil {
		// This is the desired state.
		return nil
	}

	sql, err := querybuilder.NewDropUser(user.Name).WithCluster(clusterName).Build()
	if err != nil {
		return errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return errors.WithMessage(err, "error running query")
	}

	return nil
}

func (i *impl) FindUserByName(ctx context.Context, name string, clusterName *string) (*User, error) {
	sql, err := querybuilder.
		NewSelect([]querybuilder.Field{querybuilder.NewField("id").ToString()}, "system.users").
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("name", name)).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	var uuid string

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		uuid, err = data.GetString("id")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'id' field")
		}

		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	return i.GetUser(ctx, uuid, clusterName)
}

func (i *impl) UpdateUser(ctx context.Context, user User, clusterName *string) (*User, error) {
	// Retrieve current user
	existing, err := i.GetUser(ctx, user.ID, clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "Unable to get existing user")
	}

	sql, err := querybuilder.
		NewAlterUser(existing.Name).
		WithCluster(clusterName).
		RenameTo(&user.Name).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	return i.GetUser(ctx, user.ID, clusterName)
}

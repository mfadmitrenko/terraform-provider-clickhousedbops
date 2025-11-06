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

	return i.GetUserByName(ctx, user.Name, clusterName)
}

func (i *impl) GetUserByName(ctx context.Context, name string, clusterName *string) (*User, error) {
	sql, err := querybuilder.
		NewSelect([]querybuilder.Field{
			querybuilder.NewField("name"),
			querybuilder.NewField("id").ToString(), // optional; for introspection only
		}, "system.users").
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("name", name)).
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
		chID, _ := data.GetNullableString("id") // may vary across nodes; do not use for identity
		u := &User{Name: n}
		if chID != nil {
			u.ID = *chID
		}
		user = u
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}
	if user == nil {
		return nil, nil // not found
	}

	// Also fetch settings profiles (unchanged)
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
		user.SettingsProfiles = profiles
	}

	return user, nil
}

func (i *impl) GetUserByUUID(ctx context.Context, uuid string, clusterName *string) (*User, error) {
	sql, err := querybuilder.
		NewSelect([]querybuilder.Field{querybuilder.NewField("name")}, "system.users").
		WithCluster(clusterName).
		Where(querybuilder.WhereEquals("id", uuid)).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}
	var name string
	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		n, err := data.GetString("name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'name' field")
		}
		name = n
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}
	if name == "" {
		return nil, nil
	}
	return i.GetUserByName(ctx, name, clusterName)
}

// Delete by name
func (i *impl) DeleteUser(ctx context.Context, name string, clusterName *string) error {
	user, err := i.GetUserByName(ctx, name, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error getting user")
	}
	if user == nil {
		return nil // desired state
	}

	sql, err := querybuilder.NewDropUser(user.Name).WithCluster(clusterName).Build()
	if err != nil {
		return errors.WithMessage(err, "error building query")
	}
	if err = i.clickhouseClient.Exec(ctx, sql); err != nil {
		return errors.WithMessage(err, "error running query")
	}
	return nil
}

func (i *impl) FindUserByName(ctx context.Context, name string, clusterName *string) (*User, error) {
	return i.GetUserByName(ctx, name, clusterName)
}

func (i *impl) UpdateUser(ctx context.Context, user User, clusterName *string) (*User, error) {
	// user.ID now carries the CURRENT NAME from state
	currentName := user.ID
	existing, err := i.GetUserByName(ctx, currentName, clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "Unable to get existing user")
	}
	if existing == nil {
		return nil, errors.Errorf("user %q not found", currentName)
	}

	q := querybuilder.NewAlterUser(existing.Name).WithCluster(clusterName)
	if user.Name != "" && user.Name != existing.Name {
		q = q.RenameTo(&user.Name)
	}
	sql, err := q.Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}
	if err = i.clickhouseClient.Exec(ctx, sql); err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	// Return by final name (either new or old)
	finalName := existing.Name
	if user.Name != "" {
		finalName = user.Name
	}
	return i.GetUserByName(ctx, finalName, clusterName)
}

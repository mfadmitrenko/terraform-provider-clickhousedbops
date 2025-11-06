package settingsprofileassociation_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/resourcebuilder"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/runner"
)

const (
	resourceType = "clickhousedbops_settings_profile_association"
	resourceName = "foo"
)

func TestSettingsProfileAssociation_acceptance(t *testing.T) {
	clusterName := "cluster1"

	settingsProfile := resourcebuilder.New("clickhousedbops_settings_profile", "profile1").
		WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum))

	role := resourcebuilder.New("clickhousedbops_role", "role").
		WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum))

	user := resourcebuilder.
		New("clickhousedbops_user", "user").
		WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
		WithFunction("password_sha256_hash_wo", "sha256", "test").
		WithIntAttribute("password_sha256_hash_wo_version", 1)

	checkNotExistsFunc := func(ctx context.Context, dbopsClient dbops.Client, clusterName *string, attrs map[string]string) (bool, error) {
		settingsProfileID := attrs["settings_profile_id"]
		if settingsProfileID == "" {
			return false, fmt.Errorf("settings_profile_id attribute was not set")
		}

		settingsProfile, err := dbopsClient.GetSettingsProfile(ctx, settingsProfileID, clusterName)
		if err != nil {
			return false, fmt.Errorf("cannot find settings profile")
		}

		if settingsProfile == nil {
			// Desired state.
			return false, nil
		}

		userID := attrs["user_id"]
		roleID := attrs["role_id"]

		if userID == "" && roleID == "" {
			return false, fmt.Errorf("both user_id and role_id are nil")
		}

		if userID != "" {
			var (
				user   *dbops.User
				getErr error
			)
			if _, parseErr := uuid.Parse(userID); parseErr == nil {
				user, getErr = dbopsClient.GetUserByUUID(ctx, userID, clusterName)
			} else {
				user, getErr = dbopsClient.GetUserByName(ctx, userID, clusterName)
			}
			if getErr != nil {
				return false, fmt.Errorf("error getting user: %w", getErr)
			}
			if user == nil {
				// Desired state
				return false, nil
			}
			return user.HasSettingProfile(settingsProfile.Name), nil
		}

		if roleID != "" {
			role, err := dbopsClient.GetRole(ctx, roleID, clusterName)
			if err != nil {
				return false, fmt.Errorf("error getting role")
			}

			if role == nil {
				// Desired state
				return false, nil
			}

			return role.HasSettingProfile(settingsProfile.Name), nil
		}

		return false, nil
	}

	checkAttributesFunc := func(ctx context.Context, dbopsClient dbops.Client, clusterName *string, attrs map[string]interface{}) error {
		settingsProfileID := attrs["settings_profile_id"]
		if settingsProfileID == nil {
			return fmt.Errorf("settings_profile_id attribute was not set")
		}

		roleID := attrs["role_id"]
		userID := attrs["user_id"]

		settingsProfile, err := dbopsClient.GetSettingsProfile(ctx, settingsProfileID.(string), clusterName)
		if err != nil {
			return fmt.Errorf("cannot find settings profile")
		}

		if settingsProfile == nil {
			return fmt.Errorf("settings profile was not found")
		}

		if roleID != nil {
			role, err := dbopsClient.GetRole(ctx, roleID.(string), clusterName)
			if err != nil {
				return err
			}

			if role == nil {
				return fmt.Errorf("role with id %q was not found", roleID.(string))
			}

			if !role.HasSettingProfile(settingsProfile.Name) {
				return fmt.Errorf("expected role with id %q to have settings profile %q but did not", roleID.(string), settingsProfile.Name)
			}
		}

		if userID != nil {
			userRef := userID.(string)

			var (
				user   *dbops.User
				getErr error
			)
			if _, parseErr := uuid.Parse(userRef); parseErr == nil {
				user, getErr = dbopsClient.GetUserByUUID(ctx, userRef, clusterName)
			} else {
				user, getErr = dbopsClient.GetUserByName(ctx, userRef, clusterName)
			}
			if getErr != nil {
				return fmt.Errorf("error getting user: %w", getErr)
			}
			if user == nil {
				return fmt.Errorf("user with ref %q was not found", userRef)
			}
			if !user.HasSettingProfile(settingsProfile.Name) {
				return fmt.Errorf("expected user %q to have settings profile %q but did not", userRef, settingsProfile.Name)
			}
		}

		return nil
	}

	tests := []runner.TestCase{
		{
			Name:     "Assign settings profile to role using Native protocol on a single replica",
			ChEnv:    map[string]string{"CONFIGFILE": "config-single.xml"},
			Protocol: "native",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithResourceFieldReference("settings_profile_id", "clickhousedbops_settings_profile", "profile1", "id").
				WithResourceFieldReference("role_id", "clickhousedbops_role", "role", "id").
				AddDependency(role.Build()).
				AddDependency(settingsProfile.Build()).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:     "Assign settings profile to user using HTTP protocol on a single replica",
			ChEnv:    map[string]string{"CONFIGFILE": "config-single.xml"},
			Protocol: "http",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithResourceFieldReference("settings_profile_id", "clickhousedbops_settings_profile", "profile1", "id").
				WithResourceFieldReference("role_id", "clickhousedbops_role", "role", "id").
				AddDependency(user.Build()).
				AddDependency(settingsProfile.Build()).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:     "Assign settings profile to role using Native protocol on a cluster using replicated storage",
			ChEnv:    map[string]string{"CONFIGFILE": "config-replicated.xml"},
			Protocol: "native",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithResourceFieldReference("settings_profile_id", "clickhousedbops_settings_profile", "profile1", "id").
				WithResourceFieldReference("role_id", "clickhousedbops_role", "role", "id").
				AddDependency(role.Build()).
				AddDependency(settingsProfile.Build()).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:     "Assign settings profile to user using HTTP protocol on a cluster using replicated storage",
			ChEnv:    map[string]string{"CONFIGFILE": "config-replicated.xml"},
			Protocol: "http",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithResourceFieldReference("settings_profile_id", "clickhousedbops_settings_profile", "profile1", "id").
				WithResourceFieldReference("role_id", "clickhousedbops_role", "role", "id").
				AddDependency(user.Build()).
				AddDependency(settingsProfile.Build()).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:        "Assign settings profile to role using Native protocol on a cluster using localfile storage",
			ChEnv:       map[string]string{"CONFIGFILE": "config-localfile.xml"},
			ClusterName: &clusterName,
			Protocol:    "native",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithResourceFieldReference("settings_profile_id", "clickhousedbops_settings_profile", "profile1", "id").
				WithResourceFieldReference("role_id", "clickhousedbops_role", "role", "id").
				AddDependency(role.WithStringAttribute("cluster_name", clusterName).Build()).
				AddDependency(settingsProfile.WithStringAttribute("cluster_name", clusterName).Build()).
				WithStringAttribute("cluster_name", clusterName).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:        "Assign settings profile to user using HTTP protocol on a cluster using localfile storage",
			ChEnv:       map[string]string{"CONFIGFILE": "config-localfile.xml"},
			ClusterName: &clusterName,
			Protocol:    "http",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithResourceFieldReference("settings_profile_id", "clickhousedbops_settings_profile", "profile1", "id").
				WithResourceFieldReference("role_id", "clickhousedbops_role", "role", "id").
				AddDependency(user.WithStringAttribute("cluster_name", clusterName).Build()).
				AddDependency(settingsProfile.WithStringAttribute("cluster_name", clusterName).Build()).
				WithStringAttribute("cluster_name", clusterName).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
	}

	runner.RunTests(t, tests)
}

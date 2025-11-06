package user_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/nilcompare"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/resourcebuilder"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/runner"
)

const (
	resourceType = "clickhousedbops_user"
	resourceName = "foo"
)

func getUserByRef(ctx context.Context, c dbops.Client, ref string, clusterName *string) (*dbops.User, error) {
	if _, parseErr := uuid.Parse(ref); parseErr == nil {
		return c.GetUserByUUID(ctx, ref, clusterName)
	}
	return c.GetUserByName(ctx, ref, clusterName)
}

func TestUser_acceptance(t *testing.T) {
	clusterName := "cluster1"

	checkNotExistsFunc := func(ctx context.Context, dbopsClient dbops.Client, clusterName *string, attrs map[string]string) (bool, error) {
		id := attrs["id"]
		if id == "" {
			return false, fmt.Errorf("id attribute was not set")
		}
		user, err := dbopsClient.GetUserByUUID(ctx, id, clusterName)
		return user != nil, err
	}

	checkAttributesFunc := func(ctx context.Context, dbopsClient dbops.Client, clusterName *string, attrs map[string]interface{}) error {
		id := attrs["id"]
		if id == nil {
			return fmt.Errorf("id was nil")
		}
		user, err := getUserByRef(ctx, dbopsClient, id.(string), clusterName)
		if err != nil {
			return err
		}
		if user == nil {
			return fmt.Errorf("user with ref %q was not found", id.(string))
		}

		if attrs["name"].(string) != user.Name {
			return fmt.Errorf("expected name to be %q, was %q", user.Name, attrs["name"].(string))
		}
		if !nilcompare.NilCompare(clusterName, attrs["cluster_name"]) {
			return fmt.Errorf("wrong value for cluster_name attribute")
		}
		return nil
	}

	tests := []runner.TestCase{
		{
			Name:        "Create User using Native protocol on a single replica",
			ChEnv:       map[string]string{"CONFIGFILE": "config-single.xml"},
			Protocol:    "native",
			ClusterName: nil,
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
				WithFunction("password_sha256_hash_wo", "sha256", "changeme").
				WithIntAttribute("password_sha256_hash_wo_version", 1).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:     "Create User using HTTP protocol on a single replica",
			ChEnv:    map[string]string{"CONFIGFILE": "config-single.xml"},
			Protocol: "http",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
				WithFunction("password_sha256_hash_wo", "sha256", "changeme").
				WithIntAttribute("password_sha256_hash_wo_version", 1).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:     "Create User using Native protocol on a cluster using replicated storage",
			ChEnv:    map[string]string{"CONFIGFILE": "config-replicated.xml"},
			Protocol: "native",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
				WithFunction("password_sha256_hash_wo", "sha256", "changeme").
				WithIntAttribute("password_sha256_hash_wo_version", 1).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:     "Create User using HTTP protocol on a cluster using replicated storage",
			ChEnv:    map[string]string{"CONFIGFILE": "config-replicated.xml"},
			Protocol: "http",
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
				WithFunction("password_sha256_hash_wo", "sha256", "changeme").
				WithIntAttribute("password_sha256_hash_wo_version", 1).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:        "Create User using Native protocol on a cluster using localfile storage",
			ChEnv:       map[string]string{"CONFIGFILE": "config-localfile.xml"},
			Protocol:    "native",
			ClusterName: &clusterName,
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithStringAttribute("cluster_name", clusterName).
				WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
				WithFunction("password_sha256_hash_wo", "sha256", "changeme").
				WithIntAttribute("password_sha256_hash_wo_version", 1).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:        "Create User using HTTP protocol on a cluster using localfile storage",
			ChEnv:       map[string]string{"CONFIGFILE": "config-localfile.xml"},
			Protocol:    "http",
			ClusterName: &clusterName,
			Resource: resourcebuilder.New(resourceType, resourceName).
				WithStringAttribute("cluster_name", clusterName).
				WithStringAttribute("name", acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).
				WithFunction("password_sha256_hash_wo", "sha256", "changeme").
				WithIntAttribute("password_sha256_hash_wo_version", 1).
				Build(),
			ResourceName:        resourceName,
			ResourceAddress:     fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc:  checkNotExistsFunc,
			CheckAttributesFunc: checkAttributesFunc,
		},
		{
			Name:        "Create User (SSL cert CN + default_role) using HTTP protocol on a cluster using localfile storage",
			ChEnv:       map[string]string{"CONFIGFILE": "config-localfile.xml"},
			Protocol:    "http",
			ClusterName: &clusterName,
			Resource: func() string {
				uname := acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
				// We set ssl_certificate_cn equal to name so we can assert equality from state attrs.
				return resourcebuilder.New(resourceType, resourceName).
					WithStringAttribute("cluster_name", clusterName).
					WithStringAttribute("name", uname).
					WithStringAttribute("ssl_certificate_cn", uname).
					WithStringAttribute("default_role", "foo").
					Build()
			}(),
			ResourceName:       resourceName,
			ResourceAddress:    fmt.Sprintf("%s.%s", resourceType, resourceName),
			CheckNotExistsFunc: checkNotExistsFunc, // unchanged
			CheckAttributesFunc: func(ctx context.Context, dbopsClient dbops.Client, clusterName *string, attrs map[string]interface{}) error {
				// Reuse the existing checks first
				if err := checkAttributesFunc(ctx, dbopsClient, clusterName, attrs); err != nil {
					return err
				}

				// Assert default_role is preserved in state (provider keeps state value; not read back)
				if v, ok := attrs["default_role"]; !ok || v == nil || v.(string) != "reader" {
					return fmt.Errorf("expected default_role to be %q, got %v", "reader", v)
				}

				// Assert ssl_certificate_cn in state equals name (we set both equal above)
				if v, ok := attrs["ssl_certificate_cn"]; !ok || v == nil {
					return fmt.Errorf("ssl_certificate_cn should be set in state")
				} else {
					want := attrs["name"].(string)
					got := v.(string)
					if got != want {
						return fmt.Errorf("expected ssl_certificate_cn to equal name %q, got %q", want, got)
					}
				}

				return nil
			},
		},
	}

	runner.RunTests(t, tests)
}

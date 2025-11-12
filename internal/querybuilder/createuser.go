package querybuilder

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

// CreateUserQueryBuilder is an interface to build CREATE USER SQL queries (already interpolated).
type CreateUserQueryBuilder interface {
	QueryBuilder
	Identified(with Identification, by string) CreateUserQueryBuilder
	IdentifiedWithSSLCertCN(cn string) CreateUserQueryBuilder
	WithDefaultRole(roleName *string) CreateUserQueryBuilder
	WithSettingsProfile(profileName *string) CreateUserQueryBuilder
	WithCluster(clusterName *string) CreateUserQueryBuilder
}

type Identification string

const (
	IdentificationSHA256Hash Identification = "sha256_hash"
)

type createUserQueryBuilder struct {
	resourceName    string
	identified      string
	defaultRole     *string
	settingsProfile *string
	clusterName     *string
}

func NewCreateUser(resourceName string) CreateUserQueryBuilder {
	return &createUserQueryBuilder{
		resourceName: resourceName,
	}
}

func (q *createUserQueryBuilder) Identified(with Identification, by string) CreateUserQueryBuilder {
	q.identified = fmt.Sprintf("IDENTIFIED WITH %s BY %s", with, quote(by))
	return q
}

func (q *createUserQueryBuilder) IdentifiedWithSSLCertCN(cn string) CreateUserQueryBuilder {
	q.identified = fmt.Sprintf("IDENTIFIED WITH ssl_certificate CN %s", quote(cn))
	return q
}

func (q *createUserQueryBuilder) WithDefaultRole(roleName *string) CreateUserQueryBuilder {
	q.defaultRole = roleName
	return q
}

func (q *createUserQueryBuilder) WithSettingsProfile(profileName *string) CreateUserQueryBuilder {
	q.settingsProfile = profileName
	return q
}

func (q *createUserQueryBuilder) WithCluster(clusterName *string) CreateUserQueryBuilder {
	q.clusterName = clusterName
	return q
}

func (q *createUserQueryBuilder) Build() (string, error) {
	if q.resourceName == "" {
		return "", errors.New("resourceName cannot be empty for CREATE USER queries")
	}

	tokens := []string{
		"CREATE",
		"USER",
		"IF",
		"NOT",
		"EXISTS",
		backtick(q.resourceName),
	}
	if q.clusterName != nil {
		tokens = append(tokens, "ON", "CLUSTER", quote(*q.clusterName))
	}
	if q.identified != "" {
		tokens = append(tokens, q.identified)
	}
	if q.settingsProfile != nil {
		tokens = append(tokens, "SETTINGS", "PROFILE", quote(*q.settingsProfile))
	}
	if q.defaultRole != nil {
		tokens = append(tokens, "DEFAULT", "ROLE", quote(*q.defaultRole))
	}

	return strings.Join(tokens, " ") + ";", nil
}

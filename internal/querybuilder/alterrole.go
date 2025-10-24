package querybuilder

import (
	"strings"

	"github.com/pingcap/errors"
)

// AlterRoleQueryBuilder is an interface to build ALTER ROLE SQL queries (already interpolated).
type AlterRoleQueryBuilder interface {
	QueryBuilder
	RenameTo(newName *string) AlterRoleQueryBuilder
	DropSettingsProfile(profileName *string) AlterRoleQueryBuilder
	AddSettingsProfile(profileName *string) AlterRoleQueryBuilder
	WithCluster(clusterName *string) AlterRoleQueryBuilder
	IfExists() AlterRoleQueryBuilder
	SetSettingsProfile(profileName *string) AlterRoleQueryBuilder
}

type alterRoleQueryBuilder struct {
	resourceName       string
	oldSettingsProfile *string
	newSettingsProfile *string
	newName            *string
	clusterName        *string
	setSettingsProfile *string
	ifExists           bool
}

func NewAlterRole(resourceName string) AlterRoleQueryBuilder {
	return &alterRoleQueryBuilder{
		resourceName: resourceName,
	}
}

func (q *alterRoleQueryBuilder) IfExists() AlterRoleQueryBuilder {
	q.ifExists = true
	return q
}

func (q *alterRoleQueryBuilder) SetSettingsProfile(profileName *string) AlterRoleQueryBuilder {
	q.setSettingsProfile = profileName
	return q
}

func (q *alterRoleQueryBuilder) RenameTo(newName *string) AlterRoleQueryBuilder {
	q.newName = newName

	return q
}

func (q *alterRoleQueryBuilder) DropSettingsProfile(profileName *string) AlterRoleQueryBuilder {
	q.oldSettingsProfile = profileName
	return q
}

func (q *alterRoleQueryBuilder) AddSettingsProfile(profileName *string) AlterRoleQueryBuilder {
	q.newSettingsProfile = profileName
	return q
}

func (q *alterRoleQueryBuilder) WithCluster(clusterName *string) AlterRoleQueryBuilder {
	q.clusterName = clusterName
	return q
}

func (q *alterRoleQueryBuilder) Build() (string, error) {
	if q.resourceName == "" {
		return "", errors.New("resourceName cannot be empty for ALTER ROLE queries")
	}

	anyChanges := false
	tokens := []string{"ALTER", "ROLE"}

	if q.ifExists {
		tokens = append(tokens, "IF", "EXISTS")
	}

	tokens = append(tokens, backtick(q.resourceName))

	if q.clusterName != nil {
		tokens = append(tokens, "ON", "CLUSTER", quote(*q.clusterName))
	}

	// ONLY legacy clause we need for 23.4:
	if q.setSettingsProfile != nil {
		anyChanges = true
		tokens = append(tokens, "SETTINGS", "PROFILE", backtick(*q.setSettingsProfile))
	}

	if !anyChanges {
		return "", errors.New("no change to be made")
	}

	return strings.Join(tokens, " ") + ";", nil
}

package querybuilder

import (
	"strings"

	"github.com/pingcap/errors"
)

// AlterUserQueryBuilder is an interface to build ALTER USER SQL queries (already interpolated).
type AlterUserQueryBuilder interface {
	QueryBuilder
	RenameTo(newName *string) AlterUserQueryBuilder
	DropSettingsProfile(profileName *string) AlterUserQueryBuilder
	AddSettingsProfile(profileName *string) AlterUserQueryBuilder
	WithCluster(clusterName *string) AlterUserQueryBuilder
	IfExists() AlterUserQueryBuilder
	SetSettingsProfile(profileName *string) AlterUserQueryBuilder
}

type alterUserQueryBuilder struct {
	resourceName       string
	oldSettingsProfile *string
	newSettingsProfile *string
	newName            *string
	clusterName        *string
	setSettingsProfile *string
	ifExists           bool
}

func NewAlterUser(resourceName string) AlterUserQueryBuilder {
	return &alterUserQueryBuilder{
		resourceName: resourceName,
	}
}

func (q *alterUserQueryBuilder) IfExists() AlterUserQueryBuilder {
	q.ifExists = true
	return q
}

func (q *alterUserQueryBuilder) SetSettingsProfile(profileName *string) AlterUserQueryBuilder {
	q.setSettingsProfile = profileName
	return q
}

func (q *alterUserQueryBuilder) RenameTo(newName *string) AlterUserQueryBuilder {
	q.newName = newName

	return q
}

func (q *alterUserQueryBuilder) DropSettingsProfile(profileName *string) AlterUserQueryBuilder {
	q.oldSettingsProfile = profileName
	return q
}

func (q *alterUserQueryBuilder) AddSettingsProfile(profileName *string) AlterUserQueryBuilder {
	q.newSettingsProfile = profileName
	return q
}

func (q *alterUserQueryBuilder) WithCluster(clusterName *string) AlterUserQueryBuilder {
	q.clusterName = clusterName
	return q
}

func (q *alterUserQueryBuilder) Build() (string, error) {
	if q.resourceName == "" {
		return "", errors.New("resourceName cannot be empty for ALTER USER queries")
	}

	anyChanges := false
	tokens := []string{"ALTER", "USER"}

	if q.ifExists {
		tokens = append(tokens, "IF", "EXISTS")
	}

	tokens = append(tokens, backtick(q.resourceName))

	// ON CLUSTER must come right after the object name
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

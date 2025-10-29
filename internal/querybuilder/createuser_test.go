package querybuilder

import (
	"testing"
)

func Test_createuser(t *testing.T) {
	tests := []struct {
		name           string
		resourceName   string
		identifiedWith Identification
		identifiedBy   string
		sslCN          string
		defaultRole    string
		clusterName    string
		want           string
		wantErr        bool
	}{
		{
			name:         "Create user no auth",
			resourceName: "john",
			want:         "CREATE USER IF NOT EXISTS `john`;",
			wantErr:      false,
		},
		{
			name:           "Create user with password",
			resourceName:   "john",
			identifiedWith: IdentificationSHA256Hash,
			identifiedBy:   "blah",
			want:           "CREATE USER IF NOT EXISTS `john` IDENTIFIED WITH sha256_hash BY 'blah';",
			wantErr:        false,
		},
		{
			name:         "Create user with SSL CN",
			resourceName: "test",
			sslCN:        "test",
			want:         "CREATE USER IF NOT EXISTS `test` IDENTIFIED WITH ssl_certificate CN 'test';",
			wantErr:      false,
		},
		{
			name:         "Create user with SSL CN and DEFAULT ROLE on cluster",
			resourceName: "test",
			clusterName:  "dev_cluster",
			sslCN:        "test",
			defaultRole:  "reader",
			want:         "CREATE USER IF NOT EXISTS `test` ON CLUSTER 'dev_cluster' IDENTIFIED WITH ssl_certificate CN 'test' DEFAULT ROLE 'reader';",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewCreateUser(tt.resourceName)
			if tt.clusterName != "" {
				q = q.WithCluster(&tt.clusterName)
			}
			if tt.sslCN != "" {
				q = q.IdentifiedWithSSLCertCN(tt.sslCN)
			} else if tt.identifiedWith != "" && tt.identifiedBy != "" {
				q = q.Identified(tt.identifiedWith, tt.identifiedBy)
			}
			if tt.defaultRole != "" {
				q = q.WithDefaultRole(&tt.defaultRole)
			}

			got, err := q.Build()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Build() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("Build() got = %q, want %q", got, tt.want)
			}
		})
	}
}

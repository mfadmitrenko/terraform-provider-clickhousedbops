package user

import (
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type User struct {
	ClusterName               types.String `tfsdk:"cluster_name"`
	ID                        types.String `tfsdk:"id"` // will hold the username
	Name                      types.String `tfsdk:"name"`
	DefaultRole               types.String `tfsdk:"default_role"`
	SSLCertificateCN          types.String `tfsdk:"ssl_certificate_cn"`
	PasswordSha256Hash        types.String `tfsdk:"password_sha256_hash_wo"`
	PasswordSha256HashVersion types.Int32  `tfsdk:"password_sha256_hash_wo_version"`
}

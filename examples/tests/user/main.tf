resource "clickhousedbops_user" "john" {
  cluster_name = var.cluster_name
  name = "john"
  # You'll want to generate the password and feed it here instead of hardcoding.
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 1
  ssl_certificate_cn = "john"
  default_role = "reader"
  # settings_profile = "profile1"
}

# resource "clickhousedbops_grant_role" "role_to_user" {
#   cluster_name = var.cluster_name
#   role_name         = "writer"
#   grantee_user_name = clickhousedbops_user.john.name
# }
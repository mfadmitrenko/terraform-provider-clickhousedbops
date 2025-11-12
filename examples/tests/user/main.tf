resource "clickhousedbops_user" "john" {
  cluster_name = var.cluster_name
  name = "john"
  # You'll want to generate the password and feed it here instead of hardcoding.
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 1
  ssl_certificate_cn = "john"
  default_role = "teleport_reader"
  settings_profile = "default"
}

resource "clickhousedbops_settings_profile_association" "userassociation" {
  settings_profile_name = "maxquery"
  user_id = clickhousedbops_user.john.id
}
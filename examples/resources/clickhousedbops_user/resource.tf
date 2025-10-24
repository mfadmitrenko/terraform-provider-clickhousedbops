resource "clickhousedbops_user" "john" {
  cluster_name = "cluster"
  name = "john"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
  ssl_certificate_cn = "john"
}
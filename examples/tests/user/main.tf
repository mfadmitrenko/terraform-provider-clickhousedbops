resource "clickhousedbops_user" "john" {
  cluster_name = "dev_cluster"
  name = "john"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
  ssl_certificate_cn = "john"
  default_role = "reader"
}

resource "clickhousedbops_user" "john2" {
  cluster_name = "dev_cluster"
  name = "john2"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
  ssl_certificate_cn = "john2"
  default_role = "reader"
}

resource "clickhousedbops_user" "john_doe" {
  cluster_name = "dev_cluster"
  name = "john_doe"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
  ssl_certificate_cn = "john_doe"
  default_role = "reader"
}


resource "clickhousedbops_grant_role" "t_writer" {
  cluster_name      = "dev_cluster"
  role_name         = "writer"
  grantee_user_name = clickhousedbops_user.john.name
}

#resource "clickhousedbops_grant_role" "lexus_t_writer" {                      
#   cluster_name      = "dev_cluster"                                    
#   role_name         = "writer"
#   grantee_user_name = "john_doe"
#}

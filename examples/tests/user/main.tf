#resource "clickhousedbops_user" "admitrenko" {
#  cluster_name = "dev_cluster"
#  name = "admitrenko"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
#  ssl_certificate_cn = "admitrenko"
#  default_role = "teleport_writer"
#}

#resource "clickhousedbops_user" "lexusrules" {
#  cluster_name = "dev_cluster"
#  name = "lexusrules"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
#  ssl_certificate_cn = "lexusrules"
#  default_role = "teleport_reader"
#}

#resource "clickhousedbops_user" "networker" {
#cluster_name = "dev_cluster"
#  name = "networker"
  # Option 1: password-based auth
  # password_sha256_hash_wo = sha256("test")
  # password_sha256_hash_wo_version = 4

  # Option 2: SSL certificate CN auth (mutually exclusive with password)
#  ssl_certificate_cn = "networker"
#  default_role = "teleport_reader"
#}


#resource "clickhousedbops_grant_role" "t_writer" {
#   cluster_name      = "dev_cluster"
#   role_name         = "teleport_writer"
#   grantee_user_name = "admitrenko"
#}

#resource "clickhousedbops_grant_role" "lexus_t_writer" {                      
#   cluster_name      = "dev_cluster"                                    
#   role_name         = "teleport_writer"                                
#   grantee_user_name = "lexusrules"                                     
#}

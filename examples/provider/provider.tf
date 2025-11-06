# This file is generated automatically please do not edit
terraform {
  required_providers {
    clickhousedbops = {
      version = "1.1.0"
      source  = "mfadmitrenko/clickhousedbops"
    }
  }
}

provider "clickhousedbops" {
  host = "localhost"

  protocol = "native"
  port = 9000

  auth_config = {
    strategy = "password"
    username = "default"
    password = "changeme"
  }
}

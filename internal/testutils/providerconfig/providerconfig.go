package providerconfig

import (
	"fmt"
)

func ProviderConfig(protocol string, host string, port uint16, username string, password string) (string, error) {
	var strategy string

	switch protocol {
	case "native":
		strategy = "password"
	case "http":
		strategy = "basicauth"
	default:
		return "", fmt.Errorf("invalid protocol %s", protocol)
	}

	return fmt.Sprintf(`provider "clickhousedbops" {
  protocol = "%s"
  host = "%s"
  port = %d
  auth_config = {
    strategy = "%s"
    username = "%s"
    password = "%s"
  }
}`, protocol, host, port, strategy, username, password), nil
}

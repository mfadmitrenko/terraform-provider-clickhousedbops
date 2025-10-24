package dbopsclient

import (
	"fmt"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
)

const (
	username = "default"
	password = "test"

	host       = "127.0.0.1"
	nativePort = 9000
	httpPort   = 8123
)

type ConnectionSettings struct {
	Host     string
	Port     uint16
	Username string
	Password string
}

func NewDbopsClient(protocol string) (dbopsClient dbops.Client, connectionSettings ConnectionSettings, err error) {
	connectionSettings = ConnectionSettings{
		Host:     host,
		Username: username,
		Password: password,
	}

	var clickhouseClient clickhouseclient.ClickhouseClient
	{
		switch protocol {
		case "native":
			connectionSettings.Port = nativePort
			config := clickhouseclient.NativeClientConfig{
				Host: host,
				Port: nativePort,
				UserPasswordAuth: &clickhouseclient.UserPasswordAuth{
					Username: username,
					Password: password,
				},
			}

			clickhouseClient, err = clickhouseclient.NewNativeClient(config)
			if err != nil {
				return
			}
		case "http":
			connectionSettings.Port = httpPort
			config := clickhouseclient.HTTPClientConfig{
				Protocol: "http",
				Host:     host,
				Port:     httpPort,
				BasicAuth: &clickhouseclient.BasicAuth{
					Username: username,
					Password: password,
				},
			}

			clickhouseClient, err = clickhouseclient.NewHTTPClient(config)
			if err != nil {
				return
			}
		default:
			err = fmt.Errorf("invalid protocol %s", protocol)
			return
		}
	}

	dbopsClient, err = dbops.NewClient(clickhouseClient)
	if err != nil {
		return
	}

	return
}

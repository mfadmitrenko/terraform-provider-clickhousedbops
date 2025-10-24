package factories

import (
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/provider"
)

func ProviderFactories() map[string]func() (tfprotov6.ProviderServer, error) {
	factories := make(map[string]func() (tfprotov6.ProviderServer, error))

	factories["clickhousedbops"] = providerserver.NewProtocol6WithError(&provider.Provider{})

	return factories
}

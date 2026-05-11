package provider

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	tfresource "github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-log/tflog"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
	settingsprofileds "github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/datasource/settingsprofile"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/project"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/database"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/grantprivilege"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/grantrole"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/role"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/setting"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/settingsprofile"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/settingsprofileassociation"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/pkg/resource/user"
)

const (
	protocolNative       = "native"
	protocolNativeSecure = "nativesecure"
	protocolHTTP         = "http"
	protocolHTTPS        = "https"

	authStrategyPassword  = "password"
	authStrategyBasicAuth = "basicauth"

	defaultInitAttempts = 4
	defaultInitBackoff  = 2 * time.Second
	maxInitRetryBackoff = 10 * time.Second
)

var (
	availableProtocols      = []string{protocolNative, protocolNativeSecure, protocolHTTP, protocolHTTPS}
	availableAuthStrategies = []string{authStrategyPassword, authStrategyBasicAuth}
)

// Ensure Provider satisfies various provider interfaces.
var _ provider.Provider = &Provider{}

// Provider defines the provider implementation.
type Provider struct{}

func (p *Provider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "clickhousedbops"
	resp.Version = project.Version()
}

func (p *Provider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"protocol": schema.StringAttribute{
				Required:    true,
				Description: fmt.Sprintf("The protocol to use to connect to clickhouse instance. Valid options are: %s", strings.Join(availableProtocols, ", ")),
				Validators: []validator.String{
					stringvalidator.OneOf(availableProtocols...),
				},
			},
			"host": schema.StringAttribute{
				Required:    true,
				Description: "The hostname to use to connect to the clickhouse instance",
			},
			"port": schema.Int32Attribute{
				Required:    true,
				Description: "The port to use to connect to the clickhouse instance",
			},
			"auth_config": schema.SingleNestedAttribute{
				Attributes: map[string]schema.Attribute{
					"strategy": schema.StringAttribute{
						Required:    true,
						Description: "The authentication method to use",
						Validators: []validator.String{
							stringvalidator.OneOf(availableAuthStrategies...),
						},
					},
					"username": schema.StringAttribute{
						Required:    true,
						Description: "The username to use to authenticate to ClickHouse",
						Validators: []validator.String{
							stringvalidator.LengthAtLeast(1),
						},
					},
					"password": schema.StringAttribute{
						Optional:    true,
						Description: "The password to use to authenticate to ClickHouse",
						Validators: []validator.String{
							stringvalidator.LengthAtLeast(1),
						},
					},
				},
				Required:    true,
				Description: "Authentication configuration",
			},
			"tls_config": schema.SingleNestedAttribute{
				Attributes: map[string]schema.Attribute{
					"insecure_skip_verify": schema.BoolAttribute{
						Optional:    true,
						Description: "Skip TLS cert verification when using the https protocol. This is insecure!",
					},
				},
				Optional:    true,
				Description: "TLS configuration options",
			},
		},
	}
}

func (p *Provider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data Model

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.Host.IsUnknown() || data.Protocol.IsUnknown() || data.Port.IsUnknown() || data.AuthConfig.Strategy.IsUnknown() || data.AuthConfig.Username.IsUnknown() {
		// We don't know the service data yet.
		return
	}

	clickhouseClient, err := p.newClickhouseClientWithRetry(ctx, data)
	if err != nil {
		resp.Diagnostics.AddError("error initializing clickhouse client", fmt.Sprintf("%+v\n", err))
		return
	}

	dbopsClient, err := dbops.NewClient(clickhouseClient)
	if err != nil {
		resp.Diagnostics.AddError("error initializing dbops client", fmt.Sprintf("%+v\n", err))
		return
	}

	resp.ResourceData = dbopsClient
	resp.DataSourceData = dbopsClient
}

func (p *Provider) newClickhouseClientWithRetry(ctx context.Context, data Model) (clickhouseclient.ClickhouseClient, error) {
	var lastErr error

	for attempt := 1; attempt <= defaultInitAttempts; attempt++ {
		client, err := p.newClickhouseClient(data)
		if err == nil {
			return client, nil
		}

		if !isRetryableInitError(err) {
			return nil, err
		}

		lastErr = err
		if attempt == defaultInitAttempts {
			break
		}

		backoff := defaultInitBackoff * time.Duration(attempt)
		if backoff > maxInitRetryBackoff {
			backoff = maxInitRetryBackoff
		}

		tflog.Warn(ctx, "clickhouse client initialization failed, retrying", map[string]any{
			"attempt":      attempt,
			"max_attempts": defaultInitAttempts,
			"retry_in":     backoff.String(),
			"error":        err.Error(),
		})

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("initialization cancelled while waiting to retry: %w", ctx.Err())
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf(
		"clickhouse client initialization failed after %d attempts: %w",
		defaultInitAttempts,
		lastErr,
	)
}

func (p *Provider) newClickhouseClient(data Model) (clickhouseclient.ClickhouseClient, error) {
	var clickhouseClient clickhouseclient.ClickhouseClient
	var err error
	{
		switch data.Protocol.ValueString() {
		case protocolNative:
			fallthrough
		case protocolNativeSecure:
			var auth *clickhouseclient.UserPasswordAuth
			switch data.AuthConfig.Strategy.ValueString() {
			case authStrategyPassword:
				auth = &clickhouseclient.UserPasswordAuth{
					Username: data.AuthConfig.Username.ValueString(),
				}

				if !data.AuthConfig.Password.IsNull() {
					auth.Password = data.AuthConfig.Password.ValueString()
				}

				valid, errorStrings := auth.ValidateConfig()
				if !valid {
					return nil, fmt.Errorf("invalid configuration: invalid authentication strategy configuration. %s", strings.Join(errorStrings, ", "))
				}
			default:
				return nil, fmt.Errorf("invalid configuration: invalid authentication strategy %q. %s protocol only supports %q", data.AuthConfig.Strategy, protocolNative, authStrategyPassword)
			}

			var port uint16
			{
				if !data.Port.IsUnknown() {
					portVal := data.Port.ValueInt32()
					if portVal <= 0 || portVal > 65535 {
						return nil, fmt.Errorf("invalid configuration: invalid port %s", data.Port.String())
					}

					port = uint16(portVal)
				}
			}

			clickhouseClient, err = clickhouseclient.NewNativeClient(clickhouseclient.NativeClientConfig{
				Host:             data.Host.ValueString(),
				Port:             port,
				UserPasswordAuth: auth,
				EnableTLS:        data.Protocol.ValueString() == protocolNativeSecure,
			})
		case protocolHTTP:
			fallthrough
		case protocolHTTPS:
			var auth *clickhouseclient.BasicAuth
			switch data.AuthConfig.Strategy.ValueString() {
			case authStrategyBasicAuth:
				auth = &clickhouseclient.BasicAuth{
					Username: data.AuthConfig.Username.ValueString(),
				}

				if !data.AuthConfig.Password.IsNull() {
					auth.Password = data.AuthConfig.Password.ValueString()
				}

				valid, errorStrings := auth.ValidateConfig()
				if !valid {
					return nil, fmt.Errorf("invalid configuration: invalid authentication strategy configuration. %s", strings.Join(errorStrings, ", "))
				}
			default:
				return nil, fmt.Errorf("invalid configuration: invalid authentication strategy %q. %s protocol only supports %q", data.AuthConfig.Strategy, protocolHTTP, authStrategyBasicAuth)
			}

			var port uint16
			{
				if !data.Port.IsUnknown() {
					portVal := data.Port.ValueInt32()
					if portVal <= 0 || portVal > 65535 {
						return nil, fmt.Errorf("invalid configuration: invalid port %s", data.Port.String())
					}

					port = uint16(portVal)
				}
			}

			var tlsConfig *tls.Config
			protocol := "http"
			if data.Protocol.ValueString() == protocolHTTPS {
				protocol = "https"
				tlsConfig = &tls.Config{} //nolint:gosec
				if data.TLSConfig != nil && !data.TLSConfig.InsecureSkipVerify.IsNull() {
					tlsConfig.InsecureSkipVerify = data.TLSConfig.InsecureSkipVerify.ValueBool()
				}
			}

			config := clickhouseclient.HTTPClientConfig{
				Protocol:  protocol,
				Host:      data.Host.ValueString(),
				Port:      port,
				BasicAuth: auth,
				TLSConfig: tlsConfig,
			}

			clickhouseClient, err = clickhouseclient.NewHTTPClient(config)
		default:
			return nil, fmt.Errorf("invalid configuration: unsupported protocol %q", data.Protocol.ValueString())
		}
	}

	return clickhouseClient, err
}

func isRetryableInitError(err error) bool {
	var netErr net.Error
	if ok := errors.As(err, &netErr); ok {
		return true
	}

	errString := strings.ToLower(err.Error())
	retryableSubstrings := []string{
		"connection refused",
		"connection reset",
		"i/o timeout",
		"timeout",
		"temporary failure",
		"no route to host",
		"broken pipe",
		"eof",
	}

	for _, candidate := range retryableSubstrings {
		if strings.Contains(errString, candidate) {
			return true
		}
	}

	return false
}

func (p *Provider) Resources(ctx context.Context) []func() tfresource.Resource {
	return []func() tfresource.Resource{
		database.NewResource,
		role.NewResource,
		user.NewResource,
		grantrole.NewResource,
		grantprivilege.NewResource,
		settingsprofile.NewResource,
		setting.NewResource,
		settingsprofileassociation.NewResource,
	}
}

func (p *Provider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		settingsprofileds.NewDataSource,
	}
}

func New() func() provider.Provider {
	return func() provider.Provider {
		return &Provider{}
	}
}

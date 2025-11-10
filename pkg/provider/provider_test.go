package provider

import (
	"context"
	"fmt"
	"testing"

	pfprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
)

type stubClickhouseClient struct{}

func (stubClickhouseClient) Select(context.Context, string, func(clickhouseclient.Row) error) error {
	return nil
}

func (stubClickhouseClient) Exec(context.Context, string) error {
	return nil
}

func configureRequest(t *testing.T, ctx context.Context, p *Provider, data Model) pfprovider.ConfigureRequest {
	t.Helper()

	schemaResp := pfprovider.SchemaResponse{}
	p.Schema(ctx, pfprovider.SchemaRequest{}, &schemaResp)

	objType, ok := schemaResp.Schema.Type().(basetypes.ObjectType)
	if !ok {
		t.Fatalf("unexpected schema type %T", schemaResp.Schema.Type())
	}

	configValue, diags := basetypes.NewObjectValueFrom(ctx, objType.AttrTypes, data)
	if diags.HasError() {
		t.Fatalf("unexpected diagnostics: %v", diags)
	}

	rawValue, err := configValue.ToTerraformValue(ctx)
	if err != nil {
		t.Fatalf("unexpected error converting configuration: %v", err)
	}

	return pfprovider.ConfigureRequest{
		Config: tfsdk.Config{
			Raw:    rawValue,
			Schema: schemaResp.Schema,
		},
	}
}

func withClientConstructors(t *testing.T, native func(clickhouseclient.NativeClientConfig) (clickhouseclient.ClickhouseClient, error), http func(clickhouseclient.HTTPClientConfig) (clickhouseclient.ClickhouseClient, error), db func(clickhouseclient.ClickhouseClient) (dbops.Client, error)) {
	t.Helper()

	origNative := newNativeClientFunc
	origHTTP := newHTTPClientFunc
	origDBOps := newDBOpsClientFunc

	newNativeClientFunc = native
	newHTTPClientFunc = http
	newDBOpsClientFunc = db

	t.Cleanup(func() {
		newNativeClientFunc = origNative
		newHTTPClientFunc = origHTTP
		newDBOpsClientFunc = origDBOps
	})
}

func TestProviderConfigureNativePassword(t *testing.T) {
	ctx := context.Background()
	p := &Provider{}

	cfg := Model{
		Protocol: types.StringValue(protocolNative),
		Host:     types.StringValue("localhost"),
		Port:     types.Int32Value(9000),
		AuthConfig: AuthConfig{
			Strategy: types.StringValue(authStrategyPassword),
			Username: types.StringValue("user"),
			Password: types.StringValue("secret"),
		},
	}

	req := configureRequest(t, ctx, p, cfg)

	fakeClient := &stubClickhouseClient{}
	var capturedNativeConfig clickhouseclient.NativeClientConfig
	var nativeCalled, dbopsCalled bool

	withClientConstructors(t,
		func(cfg clickhouseclient.NativeClientConfig) (clickhouseclient.ClickhouseClient, error) {
			nativeCalled = true
			capturedNativeConfig = cfg
			return fakeClient, nil
		},
		func(clickhouseclient.HTTPClientConfig) (clickhouseclient.ClickhouseClient, error) {
			return nil, fmt.Errorf("unexpected HTTP client invocation")
		},
		func(client clickhouseclient.ClickhouseClient) (dbops.Client, error) {
			dbopsCalled = true
			if client != fakeClient {
				t.Fatalf("expected stub clickhouse client")
			}
			return dbops.NewClient(client)
		},
	)

	resp := pfprovider.ConfigureResponse{}
	p.Configure(ctx, req, &resp)

	if resp.Diagnostics.HasError() {
		t.Fatalf("unexpected diagnostics: %v", resp.Diagnostics)
	}
	if !nativeCalled {
		t.Fatal("expected native client constructor to be called")
	}
	if !dbopsCalled {
		t.Fatal("expected dbops client constructor to be called")
	}
	if resp.ResourceData == nil || resp.DataSourceData == nil {
		t.Fatal("expected provider data to be configured")
	}
	if capturedNativeConfig.Host != "localhost" {
		t.Fatalf("unexpected host %q", capturedNativeConfig.Host)
	}
	if capturedNativeConfig.Port != 9000 {
		t.Fatalf("unexpected port %d", capturedNativeConfig.Port)
	}
	if capturedNativeConfig.EnableTLS {
		t.Fatal("expected TLS to be disabled for native protocol")
	}
	if capturedNativeConfig.UserPasswordAuth == nil {
		t.Fatal("expected authentication config")
	}
	if capturedNativeConfig.UserPasswordAuth.Username != "user" {
		t.Fatalf("unexpected username %q", capturedNativeConfig.UserPasswordAuth.Username)
	}
	if capturedNativeConfig.UserPasswordAuth.Password != "secret" {
		t.Fatalf("unexpected password %q", capturedNativeConfig.UserPasswordAuth.Password)
	}
}

func TestProviderConfigureHTTPSWithTLS(t *testing.T) {
	ctx := context.Background()
	p := &Provider{}

	cfg := Model{
		Protocol: types.StringValue(protocolHTTPS),
		Host:     types.StringValue("localhost"),
		Port:     types.Int32Value(8443),
		AuthConfig: AuthConfig{
			Strategy: types.StringValue(authStrategyBasicAuth),
			Username: types.StringValue("user"),
			Password: types.StringValue("secret"),
		},
		TLSConfig: &TLSConfig{InsecureSkipVerify: types.BoolValue(true)},
	}

	req := configureRequest(t, ctx, p, cfg)

	fakeClient := &stubClickhouseClient{}
	var capturedHTTPConfig clickhouseclient.HTTPClientConfig
	var httpCalled, dbopsCalled bool

	withClientConstructors(t,
		func(clickhouseclient.NativeClientConfig) (clickhouseclient.ClickhouseClient, error) {
			return nil, fmt.Errorf("unexpected native client invocation")
		},
		func(cfg clickhouseclient.HTTPClientConfig) (clickhouseclient.ClickhouseClient, error) {
			httpCalled = true
			capturedHTTPConfig = cfg
			return fakeClient, nil
		},
		func(client clickhouseclient.ClickhouseClient) (dbops.Client, error) {
			dbopsCalled = true
			if client != fakeClient {
				t.Fatalf("expected stub clickhouse client")
			}
			return dbops.NewClient(client)
		},
	)

	resp := pfprovider.ConfigureResponse{}
	p.Configure(ctx, req, &resp)

	if resp.Diagnostics.HasError() {
		t.Fatalf("unexpected diagnostics: %v", resp.Diagnostics)
	}
	if !httpCalled {
		t.Fatal("expected HTTP client constructor to be called")
	}
	if !dbopsCalled {
		t.Fatal("expected dbops client constructor to be called")
	}
	if capturedHTTPConfig.Protocol != "https" {
		t.Fatalf("unexpected protocol %q", capturedHTTPConfig.Protocol)
	}
	if capturedHTTPConfig.Port != 8443 {
		t.Fatalf("unexpected port %d", capturedHTTPConfig.Port)
	}
	if capturedHTTPConfig.TLSConfig == nil {
		t.Fatal("expected TLS configuration to be provided")
	}
	if !capturedHTTPConfig.TLSConfig.InsecureSkipVerify {
		t.Fatal("expected insecure skip verify to be propagated")
	}
	if capturedHTTPConfig.BasicAuth == nil {
		t.Fatal("expected basic auth configuration")
	}
	if capturedHTTPConfig.BasicAuth.Username != "user" {
		t.Fatalf("unexpected username %q", capturedHTTPConfig.BasicAuth.Username)
	}
}

func TestProviderConfigureInvalidNativeStrategy(t *testing.T) {
	ctx := context.Background()
	p := &Provider{}

	cfg := Model{
		Protocol: types.StringValue(protocolNative),
		Host:     types.StringValue("localhost"),
		Port:     types.Int32Value(9000),
		AuthConfig: AuthConfig{
			Strategy: types.StringValue(authStrategyBasicAuth),
			Username: types.StringValue("user"),
			Password: types.StringValue("secret"),
		},
	}

	req := configureRequest(t, ctx, p, cfg)

	withClientConstructors(t,
		func(clickhouseclient.NativeClientConfig) (clickhouseclient.ClickhouseClient, error) {
			t.Fatal("unexpected native client invocation")
			return nil, nil
		},
		func(clickhouseclient.HTTPClientConfig) (clickhouseclient.ClickhouseClient, error) {
			t.Fatal("unexpected HTTP client invocation")
			return nil, nil
		},
		func(clickhouseclient.ClickhouseClient) (dbops.Client, error) {
			t.Fatal("unexpected dbops client invocation")
			return nil, nil
		},
	)

	resp := pfprovider.ConfigureResponse{}
	p.Configure(ctx, req, &resp)

	if !resp.Diagnostics.HasError() {
		t.Fatal("expected diagnostics for invalid strategy")
	}
	if resp.ResourceData != nil || resp.DataSourceData != nil {
		t.Fatal("expected provider data to remain unset")
	}
}

func TestProviderConfigureInvalidNativeAuthConfig(t *testing.T) {
	ctx := context.Background()
	p := &Provider{}

	cfg := Model{
		Protocol: types.StringValue(protocolNative),
		Host:     types.StringValue("localhost"),
		Port:     types.Int32Value(9000),
		AuthConfig: AuthConfig{
			Strategy: types.StringValue(authStrategyPassword),
			Username: types.StringValue(""),
			Password: types.StringNull(),
		},
	}

	req := configureRequest(t, ctx, p, cfg)

	withClientConstructors(t,
		func(clickhouseclient.NativeClientConfig) (clickhouseclient.ClickhouseClient, error) {
			t.Fatal("unexpected native client invocation")
			return nil, nil
		},
		func(clickhouseclient.HTTPClientConfig) (clickhouseclient.ClickhouseClient, error) {
			t.Fatal("unexpected HTTP client invocation")
			return nil, nil
		},
		func(clickhouseclient.ClickhouseClient) (dbops.Client, error) {
			t.Fatal("unexpected dbops client invocation")
			return nil, nil
		},
	)

	resp := pfprovider.ConfigureResponse{}
	p.Configure(ctx, req, &resp)

	if !resp.Diagnostics.HasError() {
		t.Fatal("expected diagnostics for invalid authentication configuration")
	}
	if resp.ResourceData != nil || resp.DataSourceData != nil {
		t.Fatal("expected provider data to remain unset")
	}
}

func TestProviderConfigureInvalidHTTPPort(t *testing.T) {
	ctx := context.Background()
	p := &Provider{}

	cfg := Model{
		Protocol: types.StringValue(protocolHTTP),
		Host:     types.StringValue("localhost"),
		Port:     types.Int32Value(70000),
		AuthConfig: AuthConfig{
			Strategy: types.StringValue(authStrategyBasicAuth),
			Username: types.StringValue("user"),
			Password: types.StringValue("secret"),
		},
	}

	req := configureRequest(t, ctx, p, cfg)

	withClientConstructors(t,
		func(clickhouseclient.NativeClientConfig) (clickhouseclient.ClickhouseClient, error) {
			t.Fatal("unexpected native client invocation")
			return nil, nil
		},
		func(clickhouseclient.HTTPClientConfig) (clickhouseclient.ClickhouseClient, error) {
			t.Fatal("unexpected HTTP client invocation")
			return nil, nil
		},
		func(clickhouseclient.ClickhouseClient) (dbops.Client, error) {
			t.Fatal("unexpected dbops client invocation")
			return nil, nil
		},
	)

	resp := pfprovider.ConfigureResponse{}
	p.Configure(ctx, req, &resp)

	if !resp.Diagnostics.HasError() {
		t.Fatal("expected diagnostics for invalid HTTP port")
	}
	if resp.ResourceData != nil || resp.DataSourceData != nil {
		t.Fatal("expected provider data to remain unset")
	}
}

// pkg/datasource/settingsprofile/datasource.go
package settingsprofile

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
)

var _ datasource.DataSource = &DataSource{}

type DataSource struct {
	client dbops.Client
}

func NewDataSource() datasource.DataSource { return &DataSource{} }

func (d *DataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "clickhousedbops_settings_profile"
}

func (d *DataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Settings profile name to look up (e.g. 'maxquery').",
			},
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "UUID of the settings profile.",
			},
			"cluster_name": schema.StringAttribute{
				Optional:    true,
				Description: "Cluster name for lookups on replicated/localfile setups.",
			},
		},
	}
}

func (d *DataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	c, ok := req.ProviderData.(dbops.Client)
	if !ok || c == nil {
		resp.Diagnostics.AddError("Configuration Error", "Provider did not supply dbops client")
		return
	}
	d.client = c
}

type dsModel struct {
	Name        types.String `tfsdk:"name"`
	ClusterName types.String `tfsdk:"cluster_name"`
	ID          types.String `tfsdk:"id"`
}

func (d *DataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data dsModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddError("Invalid input", "name must not be empty")
		return
	}

	sp, err := d.client.GetSettingsProfileByName(ctx, name, valueOrNil(data.ClusterName))
	if err != nil {
		resp.Diagnostics.AddError("Query failed", fmt.Sprintf("lookup of %q failed: %v", name, err))
		return
	}
	if sp == nil {
		resp.Diagnostics.AddError("Not found", fmt.Sprintf("settings profile %q not found", name))
		return
	}

	data.ID = types.StringValue(sp.ID)
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func valueOrNil(v types.String) *string {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	s := v.ValueString()
	return &s
}

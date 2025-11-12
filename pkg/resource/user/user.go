package user

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int32planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
)

//go:embed user.md
var userResourceDescription string

var (
	_ resource.Resource               = &Resource{}
	_ resource.ResourceWithConfigure  = &Resource{}
	_ resource.ResourceWithModifyPlan = &Resource{}
)

func NewResource() resource.Resource {
	return &Resource{}
}

type Resource struct {
	client dbops.Client
}

func (r *Resource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (r *Resource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"cluster_name": schema.StringAttribute{
				Optional:    true,
				Description: "Name of the cluster to create the resource into. If omitted, resource will be created on the replica hit by the query.\nThis field must be left null when using a ClickHouse Cloud cluster.\nWhen using a self hosted ClickHouse instance, this field should only be set when there is more than one replica and you are not using 'replicated' storage for user_directory.\n",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "Stable identifier for the resource; equals the username.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Name of the user",
			},
			"ssl_certificate_cn": schema.StringAttribute{
				Optional:    true,
				Description: "CN of the SSL certificate to be used for the user (mutually exclusive with password_sha256_hash_wo).",
				PlanModifiers: []planmodifier.String{
					// preserves user-specified value across refresh when API doesn't echo it
					stringplanmodifier.UseStateForUnknown(),
				},
				Validators: []validator.String{
					// prevent setting both fields together (attribute-level)
					stringvalidator.ConflictsWith(path.MatchRoot("password_sha256_hash_wo")),
				},
			},
			"password_sha256_hash_wo": schema.StringAttribute{
				Optional:    true,
				Description: "SHA256 hash of the password to be set for the user (write-only, mutually exclusive with ssl_certificate_cn).",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
				Validators: []validator.String{
					stringvalidator.RegexMatches(regexp.MustCompile(`^[a-fA-F0-9]{64}$`), "password_sha256_hash must be a valid SHA256 hash"),
					stringvalidator.ConflictsWith(path.MatchRoot("ssl_certificate_cn")),
				},
				WriteOnly: true,
			},
			"password_sha256_hash_wo_version": schema.Int32Attribute{
				Optional:    true,
				Description: "Version of the password_sha256_hash_wo field. Bump this value to require a force update of the password on the user.",
				PlanModifiers: []planmodifier.Int32{
					int32planmodifier.RequiresReplace(),
				},
			},
			"default_role": schema.StringAttribute{
				Optional:    true,
				Description: "Default role to assign at creation time.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"settings_profile": schema.StringAttribute{
				Optional:    true,
				Description: "Settings profile to assign at creation time.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
		MarkdownDescription: userResourceDescription,
	}
}

func (r *Resource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		// If the entire plan is null, the resource is planned for destruction.
		return
	}

	var cfg User
	if diags := req.Config.Get(ctx, &cfg); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}

	passSet := !cfg.PasswordSha256Hash.IsNull() && !cfg.PasswordSha256Hash.IsUnknown()
	cnSet := !cfg.SSLCertificateCN.IsNull() && !cfg.SSLCertificateCN.IsUnknown()

	if (passSet && cnSet) || (!passSet && !cnSet) {
		resp.Diagnostics.AddAttributeError(
			path.Root("ssl_certificate_cn"),
			"Invalid Authentication Configuration",
			"Exactly one of 'ssl_certificate_cn' or 'password_sha256_hash_wo' must be specified.",
		)
		resp.Diagnostics.AddAttributeError(
			path.Root("password_sha256_hash_wo"),
			"Invalid Authentication Configuration",
			"Exactly one of 'ssl_certificate_cn' or 'password_sha256_hash_wo' must be specified.",
		)
		return
	}

	if r.client != nil {
		isReplicatedStorage, err := r.client.IsReplicatedStorage(ctx)
		if err != nil {
			resp.Diagnostics.AddError(
				"Error Checking if service is using replicated storage",
				fmt.Sprintf("%+v\n", err),
			)
			return
		}

		if isReplicatedStorage {
			var config User
			diags := req.Config.Get(ctx, &config)
			resp.Diagnostics.Append(diags...)
			if resp.Diagnostics.HasError() {
				return
			}

			// User cannot specify 'cluster_name' or apply will fail.
			if !config.ClusterName.IsNull() {
				resp.Diagnostics.AddWarning(
					"Invalid configuration",
					"Your ClickHouse cluster seems to be using Replicated storage for users, please remove the 'cluster_name' attribute from your User resource definition if you encounter any errors.",
				)
			}
		}
	}
}

func (r *Resource) Configure(_ context.Context, req resource.ConfigureRequest, _ *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	r.client = req.ProviderData.(dbops.Client)
}

func (r *Resource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan User
	var config User
	if diags := req.Plan.Get(ctx, &plan); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}
	if diags := req.Config.Get(ctx, &config); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}

	u := dbops.User{
		Name:               plan.Name.ValueString(),
		PasswordSha256Hash: config.PasswordSha256Hash.ValueString(),
		SSLCertificateCN:   plan.SSLCertificateCN.ValueString(),
	}

	if !plan.DefaultRole.IsNull() && !plan.DefaultRole.IsUnknown() {
		u.DefaultRole = plan.DefaultRole.ValueString()
	}

	if !plan.SettingsProfile.IsNull() && !plan.SettingsProfile.IsUnknown() {
		u.SettingsProfile = plan.SettingsProfile.ValueString()
	}

	createdUser, err := r.client.CreateUser(ctx, u, plan.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError("Error Creating ClickHouse User", fmt.Sprintf("%+v\n", err))
		return
	}

	state := User{
		ClusterName:               plan.ClusterName,
		ID:                        types.StringValue(createdUser.Name),
		Name:                      types.StringValue(createdUser.Name),
		DefaultRole:               plan.DefaultRole,
		SettingsProfile:           plan.SettingsProfile,
		PasswordSha256HashVersion: plan.PasswordSha256HashVersion,
	}

	state.SSLCertificateCN = types.StringNull()
	if !plan.SSLCertificateCN.IsNull() && !plan.SSLCertificateCN.IsUnknown() {
		state.SSLCertificateCN = plan.SSLCertificateCN
	}

	if diags := resp.State.Set(ctx, state); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state User
	if diags := req.State.Get(ctx, &state); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}

	user, err := r.client.GetUserByName(ctx, state.ID.ValueString(), state.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError("Error Reading ClickHouse User", fmt.Sprintf("%+v\n", err))
		return
	}

	if user == nil {
		resp.State.RemoveResource(ctx)
		return
	}

	state.Name = types.StringValue(user.Name)
	state.ID = types.StringValue(user.Name)
	if user.SSLCertificateCN != "" {
		state.SSLCertificateCN = types.StringValue(user.SSLCertificateCN)
	} else if state.SSLCertificateCN.IsUnknown() {
		// rare case on first refresh; make it explicitly null once
		state.SSLCertificateCN = types.StringNull()
	}

	if len(user.SettingsProfiles) == 0 {
		state.SettingsProfile = types.StringNull()
	} else if !state.SettingsProfile.IsNull() && !state.SettingsProfile.IsUnknown() {
		// Preserve planned value when still associated; otherwise mirror the first profile returned
		// by ClickHouse so Terraform detects the drift.
		wanted := state.SettingsProfile.ValueString()
		found := false
		for _, profile := range user.SettingsProfiles {
			if profile == wanted {
				found = true
				break
			}
		}
		if !found {
			state.SettingsProfile = types.StringValue(user.SettingsProfiles[0])
		}
	}

	if diags := resp.State.Set(ctx, &state); diags.HasError() {
		resp.Diagnostics.Append(diags...)
	}
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state User
	if diags := req.State.Get(ctx, &state); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}
	if diags := req.Plan.Get(ctx, &plan); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}

	u := dbops.User{
		ID:               state.ID.ValueString(),
		Name:             plan.Name.ValueString(),
		SSLCertificateCN: plan.SSLCertificateCN.ValueString(),
		// DefaultRole changes are not handled via ALTER; keep as is for now.
	}

	updated, err := r.client.UpdateUser(ctx, u, plan.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError("Error Updating ClickHouse User", fmt.Sprintf("%+v\n", err))
		return
	}

	state.Name = types.StringValue(updated.Name)
	state.ID = types.StringValue(updated.Name)
	// keep DefaultRole from plan in state
	state.DefaultRole = plan.DefaultRole
	state.SettingsProfile = plan.SettingsProfile
	if updated.SSLCertificateCN != "" {
		state.SSLCertificateCN = types.StringValue(updated.SSLCertificateCN)
	} else if !plan.SSLCertificateCN.IsNull() && !plan.SSLCertificateCN.IsUnknown() {
		state.SSLCertificateCN = plan.SSLCertificateCN
	}

	if diags := resp.State.Set(ctx, &state); diags.HasError() {
		resp.Diagnostics.Append(diags...)
	}
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state User
	if diags := req.State.Get(ctx, &state); diags.HasError() {
		resp.Diagnostics.Append(diags...)
		return
	}

	if err := r.client.DeleteUser(ctx, state.ID.ValueString(), state.ClusterName.ValueStringPointer()); err != nil {
		resp.Diagnostics.AddError("Error Deleting ClickHouse User", fmt.Sprintf("%+v\n", err))
		return
	}
}

func (r *Resource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// req.ID can either be in the form <cluster name>:<user ref> or just <user ref>
	// user ref can either be the name or the UUID of the user.

	// Check if cluster name is specified
	ref := req.ID
	var clusterName *string
	if strings.Contains(req.ID, ":") {
		parts := strings.SplitN(req.ID, ":", 2)
		cn := parts[0]
		ref = parts[1]
		clusterName = &cn
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("cluster_name"), clusterName)...)
	}
	// Check if ref is a UUID
	if _, err := uuid.Parse(ref); err == nil {
		user, err := r.client.GetUserByUUID(ctx, ref, clusterName)
		if err != nil || user == nil {
			if err != nil {
				resp.Diagnostics.AddError("Cannot import user by UUID", fmt.Sprintf("%+v\n", err))
			} else {
				resp.Diagnostics.AddError("Cannot import user by UUID", "User not found")
			}
			return
		}
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), user.Name)...)
		return
	}

	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), ref)...)
}

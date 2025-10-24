package settingsprofileassociation

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
)

//go:embed settingsprofileassociation.md
var settingsprofileassociationResourceDescription string

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
	resp.TypeName = req.ProviderTypeName + "_settings_profile_association"
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
			"settings_profile_id": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "UUID of the settings profile to attach.",
				Validators: []validator.String{
					// Exactly one of id or name must be set (the peer validator sits on the name field too)
					stringvalidator.ExactlyOneOf(path.MatchRoot("settings_profile_name")),
				},
				PlanModifiers: []planmodifier.String{
					// Prevents unexpected diffs if value is unknown at plan time
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"settings_profile_name": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Name of the settings profile to attach (e.g. 'readonly').",
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(path.MatchRoot("settings_profile_id")),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"role_id": schema.StringAttribute{
				Optional:    true,
				Description: "ID of the SettingsProfileAssociation to associate the Settings profile to",
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(path.MatchRoot("user_id")),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"user_id": schema.StringAttribute{
				Optional:    true,
				Description: "ID of the User to associate the Settings profile to",
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(path.MatchRoot("role_id")),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
		MarkdownDescription: settingsprofileassociationResourceDescription,
	}
}

func (r *Resource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		// If the entire plan is null, the resource is planned for destruction.
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
			var config SettingsProfileAssociation
			diags := req.Config.Get(ctx, &config)
			resp.Diagnostics.Append(diags...)
			if resp.Diagnostics.HasError() {
				return
			}

			// SettingsProfileAssociation cannot specify 'cluster_name' or apply will fail.
			if !config.ClusterName.IsNull() {
				resp.Diagnostics.AddWarning(
					"Invalid configuration",
					"Your ClickHouse cluster is using Replicated storage, please remove the 'cluster_name' attribute from your resource definition if you encounter any errors.",
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
	var plan SettingsProfileAssociation
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Resolve settings profile NAME
	var profileName string
	if !plan.SettingsProfileName.IsNull() && !plan.SettingsProfileName.IsUnknown() {
		profileName = plan.SettingsProfileName.ValueString()
	} else if !plan.SettingsProfileID.IsNull() && !plan.SettingsProfileID.IsUnknown() {
		sp, err := r.client.GetSettingsProfile(ctx, plan.SettingsProfileID.ValueString(), plan.ClusterName.ValueStringPointer())
		if err != nil {
			resp.Diagnostics.AddError("Error Getting Settings Profile", fmt.Sprintf("%+v\n", err))
			return
		}
		if sp == nil {
			resp.Diagnostics.AddError("Error Getting Settings Profile", "settings profile not found by id")
			return
		}
		profileName = sp.Name
	} else {
		resp.Diagnostics.AddError("Invalid configuration", "either settings_profile_name or settings_profile_id must be set")
		return
	}

	// Do the association (by NAME)
	err := r.client.AssociateSettingsProfileByName(ctx, profileName, plan.RoleID.ValueStringPointer(), plan.UserID.ValueStringPointer(), plan.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Associating Settings Profile to Role",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}

	// Keep ID in state if user provided it, otherwise read it
	if plan.SettingsProfileID.IsNull() || plan.SettingsProfileID.IsUnknown() {
		// backfill ID so Read/Delete continue to work
		sp, err := r.client.GetSettingsProfileByName(ctx, profileName, plan.ClusterName.ValueStringPointer())
		if err == nil && sp != nil {
			plan.SettingsProfileID = types.StringValue(sp.ID)
		}
	}

	state := SettingsProfileAssociation{
		ClusterName:         plan.ClusterName,
		SettingsProfileID:   plan.SettingsProfileID,
		SettingsProfileName: types.StringValue(profileName),
		RoleID:              plan.RoleID,
		UserID:              plan.UserID,
	}

	diags = resp.State.Set(ctx, state)
	resp.Diagnostics.Append(diags...)
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state SettingsProfileAssociation
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get settings profile.
	settingsProfile, err := r.client.GetSettingsProfile(ctx, state.SettingsProfileID.ValueString(), state.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Getting Settings Profile",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}

	if settingsProfile == nil {
		// Settings profile was deleted, so association was deleted too.
		resp.State.RemoveResource(ctx)
		return
	}

	if !state.RoleID.IsUnknown() && !state.RoleID.IsNull() {
		role, err := r.client.GetRole(ctx, state.RoleID.ValueString(), state.ClusterName.ValueStringPointer())
		if err != nil {
			resp.Diagnostics.AddError(
				"Error Getting Role",
				fmt.Sprintf("%+v\n", err),
			)

			return
		}

		if role == nil || !role.HasSettingProfile(settingsProfile.Name) {
			resp.State.RemoveResource(ctx)
			return
		}
	} else if !state.UserID.IsUnknown() && !state.UserID.IsNull() {
		user, err := r.client.GetUser(ctx, state.UserID.ValueString(), state.ClusterName.ValueStringPointer())
		if err != nil {
			resp.Diagnostics.AddError(
				"Error Getting User",
				fmt.Sprintf("%+v\n", err),
			)

			return
		}

		if user == nil || !user.HasSettingProfile(settingsProfile.Name) {
			resp.State.RemoveResource(ctx)
			return
		}
	} else {
		// This should never be the case, but in case it happens we can delete the resource as it would be pointless.
		resp.State.RemoveResource(ctx)
		return
	}
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	panic("Update operation is not supported for clickhousedbops_settings_profile_association resource")
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state SettingsProfileAssociation
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DisassociateSettingsProfile(ctx, state.SettingsProfileID.ValueString(), state.RoleID.ValueStringPointer(), state.UserID.ValueStringPointer(), state.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Deleting ClickHouse SettingsProfileAssociation",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}
}

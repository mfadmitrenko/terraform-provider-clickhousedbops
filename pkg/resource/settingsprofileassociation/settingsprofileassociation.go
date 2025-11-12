package settingsprofileassociation

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
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
				Description: "ID of the settings profile to associate",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(path.MatchRoot("settings_profile_name")),
				},
			},
			"settings_profile_name": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Name of the settings profile to associate",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(path.MatchRoot("settings_profile_id")),
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

	profile, err := r.resolveSettingsProfile(ctx, &plan, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	err = r.client.AssociateSettingsProfile(ctx, profile.ID, plan.RoleID.ValueStringPointer(), plan.UserID.ValueStringPointer(), plan.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Associating Settings Profile to Role",
			fmt.Sprintf("%+v\n", err),
		)

		return
	}

	state := SettingsProfileAssociation{
		ClusterName:         plan.ClusterName,
		SettingsProfileID:   types.StringValue(profile.ID),
		SettingsProfileName: types.StringValue(profile.Name),
		RoleID:              plan.RoleID,
		UserID:              plan.UserID,
	}

	diags = resp.State.Set(ctx, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state SettingsProfileAssociation
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get settings profile.
	settingsProfile, err := r.getSettingsProfile(ctx, &state)
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

	state.SettingsProfileID = types.StringValue(settingsProfile.ID)
	state.SettingsProfileName = types.StringValue(settingsProfile.Name)

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
		ref := state.UserID.ValueString()

		var (
			user   *dbops.User
			getErr error
		)

		if _, parseErr := uuid.Parse(ref); parseErr == nil {
			user, getErr = r.client.GetUserByUUID(ctx, ref, state.ClusterName.ValueStringPointer())
		} else {
			user, getErr = r.client.GetUserByName(ctx, ref, state.ClusterName.ValueStringPointer())
		}

		if getErr != nil {
			resp.Diagnostics.AddError("Error Getting User", fmt.Sprintf("%+v\n", getErr))
			return
		}
		if user == nil || !user.HasSettingProfile(settingsProfile.Name) {
			resp.State.RemoveResource(ctx)
			return
		}
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

	profileID := state.SettingsProfileID.ValueString()
	if profileID == "" {
		settingsProfile, err := r.getSettingsProfile(ctx, &state)
		if err != nil {
			resp.Diagnostics.AddError(
				"Error Looking Up Settings Profile",
				fmt.Sprintf("%+v\n", err),
			)
			return
		}

		if settingsProfile == nil {
			// Profile already gone, so association is gone as well.
			return
		}

		profileID = settingsProfile.ID
	}

	err := r.client.DisassociateSettingsProfile(ctx, profileID, state.RoleID.ValueStringPointer(), state.UserID.ValueStringPointer(), state.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Deleting ClickHouse SettingsProfileAssociation",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}
}

func (r *Resource) resolveSettingsProfile(ctx context.Context, plan *SettingsProfileAssociation, diags *diag.Diagnostics) (*dbops.SettingsProfile, error) {
	clusterName := plan.ClusterName.ValueStringPointer()

	if !plan.SettingsProfileID.IsNull() && !plan.SettingsProfileID.IsUnknown() {
		profile, err := r.client.GetSettingsProfile(ctx, plan.SettingsProfileID.ValueString(), clusterName)
		if err != nil {
			diags.AddError(
				"Error Looking Up Settings Profile",
				fmt.Sprintf("%+v\n", err),
			)
			return nil, err
		}

		if profile == nil {
			diags.AddAttributeError(
				path.Root("settings_profile_id"),
				"Settings Profile Not Found",
				fmt.Sprintf("Settings profile with ID %q was not found", plan.SettingsProfileID.ValueString()),
			)
			return nil, fmt.Errorf("settings profile %s not found", plan.SettingsProfileID.ValueString())
		}

		return profile, nil
	}

	if !plan.SettingsProfileName.IsNull() && !plan.SettingsProfileName.IsUnknown() {
		profile, err := r.client.GetSettingsProfileByName(ctx, plan.SettingsProfileName.ValueString(), clusterName)
		if err != nil {
			diags.AddError(
				"Error Looking Up Settings Profile",
				fmt.Sprintf("%+v\n", err),
			)
			return nil, err
		}

		if profile == nil {
			diags.AddAttributeError(
				path.Root("settings_profile_name"),
				"Settings Profile Not Found",
				fmt.Sprintf("Settings profile with name %q was not found", plan.SettingsProfileName.ValueString()),
			)
			return nil, fmt.Errorf("settings profile %s not found", plan.SettingsProfileName.ValueString())
		}

		return profile, nil
	}

	diags.AddError(
		"Missing Settings Profile Reference",
		"Either settings_profile_id or settings_profile_name must be provided.",
	)

	return nil, fmt.Errorf("settings profile reference not provided")
}

func (r *Resource) getSettingsProfile(ctx context.Context, state *SettingsProfileAssociation) (*dbops.SettingsProfile, error) {
	clusterName := state.ClusterName.ValueStringPointer()

	if !state.SettingsProfileID.IsNull() && !state.SettingsProfileID.IsUnknown() && state.SettingsProfileID.ValueString() != "" {
		return r.client.GetSettingsProfile(ctx, state.SettingsProfileID.ValueString(), clusterName)
	}

	if !state.SettingsProfileName.IsNull() && !state.SettingsProfileName.IsUnknown() && state.SettingsProfileName.ValueString() != "" {
		return r.client.GetSettingsProfileByName(ctx, state.SettingsProfileName.ValueString(), clusterName)
	}

	return nil, nil
}

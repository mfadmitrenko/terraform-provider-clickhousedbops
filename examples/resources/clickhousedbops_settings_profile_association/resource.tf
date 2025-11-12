resource "clickhousedbops_settings_profile_association" "roleassociation" {
  settings_profile_name = clickhousedbops_settings_profile.profile1.name
  role_id               = clickhousedbops_role.role1.id
}

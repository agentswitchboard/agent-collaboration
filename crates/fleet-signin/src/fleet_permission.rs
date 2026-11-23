//! The permissions Sign-in may issue in its Fleet policy.

macro_rules! fleet_permissions {
    ($($permission:ident => $value:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
        pub enum FleetPermission {
            $(#[serde(rename = $value)] $permission),+
        }

        impl FleetPermission {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$permission => $value),+ }
            }
        }
    };
}

fleet_permissions! {
    CreateWorkspace => "workspace:create",
    AllowMachine => "workspaceAdmin:allowMachine",
    WithdrawMachine => "workspaceAdmin:withdrawMachine",
    RemoveSession => "workspaceAdmin:removeSession",
    EnrollMachine => "machine:enroll",
    RevokeMachine => "machine:revoke",
    SetMachineTokenLifetime => "machine:setTokenLifetime",
    ViewOrganization => "organization:view",
    ReadOrganizationAudit => "organization:readAudit",
    DecideApproval => "approval:decide",
}

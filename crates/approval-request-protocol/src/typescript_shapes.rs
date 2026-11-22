// The runtime types remain workspace-ids types. This supplies the JS shape
// for the existing SessionRef, whose crate has no Specta implementation.
#[derive(serde::Serialize, specta::Type)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SessionRef {
    machine_id: workspace_ids::MachineId,
    session_id: String,
}

impl From<workspace_ids::SessionRef> for SessionRef {
    fn from(
        workspace_ids::SessionRef {
            machine_id,
            session_id,
        }: workspace_ids::SessionRef,
    ) -> Self {
        Self {
            machine_id,
            session_id: session_id.into(),
        }
    }
}

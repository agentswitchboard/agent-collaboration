//! Read a named session project's membership at its owning workspace.

use fleet_identity_protocol::ProjectRef;
use fleet_runtime_ports::{CentralStore, SessionProjectLookup, WorkspaceStores};

use crate::fleet_runtime::FleetRuntime;

impl<Host: CentralStore, Stores: WorkspaceStores> FleetRuntime<Host, Stores> {
    pub async fn read_session_start_project(&self, project: &ProjectRef) -> SessionProjectLookup {
        SessionProjectLookup::read_named(
            self.workspace_stores(),
            &project.workspace,
            &project.project,
        )
        .await
    }
}

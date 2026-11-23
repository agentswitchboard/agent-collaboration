//! Keep the machine link protocol's normal/build closure below Central owners.
use super::DependencyKind;
use super::{
    CENTRAL_CORE, CENTRAL_WORKER, FLEET_ENGINE, NonDevDependencyGraph, check_boundaries,
    check_job_reachability, graph,
};

pub(super) fn check_link_reachability(
    graph: &NonDevDependencyGraph,
) -> Result<(), Box<dyn std::error::Error>> {
    check_job_reachability(
        graph,
        "central-link-protocol",
        &[
            "fleet-contracts",
            "central-storage",
            "central-engine-model",
            "fleet-engine",
            "fleet-runtime",
            "fleet-runtime-ports",
            "fleet-storage-schema",
            "fleet-store-sqlx",
            CENTRAL_CORE,
            CENTRAL_WORKER,
            "collaboration-central-tokio",
        ],
    )
}

#[test]
fn the_machine_link_rejects_contract_storage_and_engine_dependencies_through_a_leaf() {
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        for forbidden in [
            "fleet-contracts",
            "central-storage",
            "central-engine-model",
            FLEET_ENGINE,
        ] {
            let dependencies = graph(&[
                ("central-link-protocol", "workspace-model", kind),
                ("workspace-model", "message-board", kind),
                ("message-board", forbidden, kind),
            ]);
            let observed =
                check_boundaries(&dependencies).expect_err("link reaches a Central owner");
            assert_eq!(
                observed.to_string(),
                format!("BD1: forbidden reachable dependency central-link-protocol -> {forbidden}")
            );
        }
    }
}

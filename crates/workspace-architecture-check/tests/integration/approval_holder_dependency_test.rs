use super::{DependencyKind, check_boundaries, graph};

#[test]
fn approval_holder_allows_protocol_types_and_rejects_harness_logic() {
    for allowed in [
        "approval-request-protocol",
        "workspace-ids",
        "state-machine-core",
    ] {
        let pure = graph(&[(
            "switchboard-approval-holder",
            allowed,
            DependencyKind::Normal,
        )]);
        assert!(check_boundaries(&pure).is_ok(), "{allowed}");
    }
    for kind in [DependencyKind::Normal, DependencyKind::Build] {
        let effectful = graph(&[("switchboard-approval-holder", "acp-client-runtime", kind)]);
        let refusal = check_boundaries(&effectful)
            .err()
            .map(|error| error.to_string());
        assert_eq!(
            refusal,
            Some(format!(
                "BD1: forbidden {kind:?} dependency switchboard-approval-holder -> acp-client-runtime"
            )),
            "the approval holder must name the forbidden harness dependency",
        );
    }
}

use super::*;

#[test]
fn a_dependency_write_can_name_either_referenced_deliverable_as_missing() {
    let blocking: workspace_ids::DeliverableId =
        "01890f2e-7b4c-7cc0-98c4-000000000021".parse().unwrap();
    let blocked: workspace_ids::DeliverableId =
        "01890f2e-7b4c-7cc0-98c4-000000000022".parse().unwrap();
    let unrelated = "01890f2e-7b4c-7cc0-98c4-000000000023".parse().unwrap();
    let misses = WorkspaceMisses::for_request(&WorkspaceRequest {
        workspace_id: Some("01890f2e-7b4c-7cc0-98c4-000000000010".parse().unwrap()),
        request_id: Some("01890f2e-7b4c-7cc0-98c4-000000000020".parse().unwrap()),
        operation: WorkspaceOperation::AddDependency {
            blocking: blocking.clone(),
            blocked: blocked.clone(),
        },
    });
    for item in [blocked, blocking] {
        assert!(misses.accepts_refusal(&RefusalReason::NotFound {
            item: ItemRef::Deliverable(item),
        }));
    }
    assert!(!misses.accepts_refusal(&RefusalReason::NotFound {
        item: ItemRef::Deliverable(unrelated),
    }));
}

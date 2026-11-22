use super::{
    ApprovalHolderTarget, ApprovalServiceTarget, AuthorityProbeReply, AuthorityProbeRequest,
    AuthorityProbeRpc, DecideApprovalRpc, DecideReply, DecideRequest, DeliverEndingReply,
    DeliverEndingRequest, DeliverEndingRpc, PassOnApprovalRpc, PassOnReply, PassOnRequest, Raise,
    RaiseApprovalRpc, RaiseReply, RegisterRunReply, RegisterRunRequest, RegisterRunRpc,
    ReportDeliveryReply, ReportDeliveryRequest, ReportDeliveryRpc, Rpc, StopApprovalRpc, StopReply,
    StopRequest,
};
use serde_json::json;

#[test]
fn the_approval_table_has_eight_typed_bindings_in_the_two_directions() {
    fn bound<
        Call: Rpc<Target = Target, Request = Request, Reply = Reply>,
        Target,
        Request,
        Reply,
    >() {
    }

    bound::<RegisterRunRpc, ApprovalServiceTarget, RegisterRunRequest, RegisterRunReply>();
    bound::<AuthorityProbeRpc, ApprovalServiceTarget, AuthorityProbeRequest, AuthorityProbeReply>();
    bound::<RaiseApprovalRpc, ApprovalServiceTarget, Raise, RaiseReply>();
    bound::<StopApprovalRpc, ApprovalServiceTarget, StopRequest, StopReply>();
    bound::<DeliverEndingRpc, ApprovalHolderTarget, DeliverEndingRequest, DeliverEndingReply>();
    bound::<ReportDeliveryRpc, ApprovalServiceTarget, ReportDeliveryRequest, ReportDeliveryReply>();
    bound::<DecideApprovalRpc, ApprovalServiceTarget, DecideRequest, DecideReply>();
    bound::<PassOnApprovalRpc, ApprovalServiceTarget, PassOnRequest, PassOnReply>();
}

#[test]
fn approval_targets_are_named_and_the_service_target_has_no_holder_claims() {
    let organization = "01934a50-1000-7000-8000-000000000001";
    let machine = "01934a50-1000-7000-8000-000000000002";
    let enrollment = "01934a50-1000-7000-8000-000000000003";
    let run = "01934a50-1000-7000-8000-000000000004";

    let central = json!({"organizationId": organization});
    let decoded: ApprovalServiceTarget = serde_json::from_value(central.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), central);
    assert!(serde_json::from_value::<ApprovalServiceTarget>(json!([organization])).is_err());
    assert!(
        serde_json::from_value::<ApprovalServiceTarget>(json!({
            "organizationId": organization, "machineId": machine
        }))
        .is_err()
    );

    let holder = json!({
        "organizationId": organization, "machineId": machine, "enrollmentId": enrollment,
        "routerRun": run
    });
    let decoded: ApprovalHolderTarget = serde_json::from_value(holder.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), holder);
    assert!(
        serde_json::from_value::<ApprovalHolderTarget>(json!([
            organization,
            machine,
            enrollment,
            run
        ]))
        .is_err()
    );
}

use super::{
    ApprovalHolderTarget, ApprovalServiceTarget, DecideApprovalRpc, DecideReply, DecideRequest,
    DeliverEndingReply, DeliverEndingRequest, DeliverEndingRpc, PassOnApprovalRpc, PassOnReply,
    PassOnRequest, Raise, RaiseApprovalRpc, RaiseReply, RegisterRunReply, RegisterRunRequest,
    RegisterRunRpc, Rpc, StopApprovalRpc, StopReply, StopRequest,
};
use serde_json::json;

#[test]
fn the_approval_table_has_six_typed_bindings_in_the_two_directions() {
    fn bound<
        Call: Rpc<Target = Target, Request = Request, Reply = Reply>,
        Target,
        Request,
        Reply,
    >() {
    }

    bound::<RegisterRunRpc, ApprovalServiceTarget, RegisterRunRequest, RegisterRunReply>();
    bound::<RaiseApprovalRpc, ApprovalServiceTarget, Raise, RaiseReply>();
    bound::<StopApprovalRpc, ApprovalServiceTarget, StopRequest, StopReply>();
    bound::<DeliverEndingRpc, ApprovalHolderTarget, DeliverEndingRequest, DeliverEndingReply>();
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

#[test]
fn register_run_request_and_reply_envelopes_use_named_fields() {
    use fleet_rpc::Envelope;
    use serde::{Serialize, de::DeserializeOwned};

    fn check_envelope<Body: DeserializeOwned + Serialize>(body: serde_json::Value) {
        let target = json!({"organizationId":"01934a50-1000-7000-8000-000000000001"});
        let named = json!({"target":target,"body":body});
        let decoded: Envelope<ApprovalServiceTarget, Body> =
            serde_json::from_value(named.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), named);
        for invalid in [
            json!([target, body]),
            json!({"target":target,"body":body,"correlation":"not an envelope field"}),
            json!({"target":target}),
        ] {
            assert!(
                serde_json::from_value::<Envelope<ApprovalServiceTarget, Body>>(invalid).is_err()
            );
        }
    }

    check_envelope::<RegisterRunRequest>(
        json!({"routerRun":"01934a50-1000-7000-8000-000000000004"}),
    );
    check_envelope::<RegisterRunReply>(json!({"kind":"registered"}));
}

#[test]
fn the_link_registers_only_the_four_machine_approval_calls() {
    use crate::{CentralLinkRpc, MacLinkRpc};
    fn to_central<Call: CentralLinkRpc>() {}
    fn to_mac<Call: MacLinkRpc>() {}

    to_central::<RegisterRunRpc>();
    to_central::<RaiseApprovalRpc>();
    to_central::<StopApprovalRpc>();
    to_mac::<DeliverEndingRpc>();
}

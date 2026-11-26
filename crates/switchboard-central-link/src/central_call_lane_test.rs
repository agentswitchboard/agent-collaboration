use std::{sync::Arc, time::Duration};

use central_link_client::{ClientAction, LinkClient};
use central_link_protocol::{
    LinkCallRefusal, MacWorkRpc, TypedCallResult, session_lifecycle_rpc::EndFleetSessionRpc,
};
use futures_util::FutureExt;
use router_clock::{ManualClock, RuntimeClock};
use serde_json::json;

use super::{CallEnding, CallSettlement, CentralCallHandle, CentralCallSubmission, WireWriteFence};

#[tokio::test]
async fn old_reader_cannot_refuse_the_same_correlation_on_a_new_connection() {
    let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()));
    let attempt = || {
        let machine = "019f0000-0000-7000-8000-000000000002".parse().unwrap();
        let mut client = LinkClient::new(machine, clock.clone());
        let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
            panic!("a new client begins with a due attempt");
        };
        attempt
    };
    let calls = CentralCallHandle::default();
    calls.attach_clock(clock.clone());
    let now = clock.monotonic_now();
    let deadline = now.after(Duration::from_secs(5));
    let offer = || {
        let request = EndFleetSessionRpc::work_request(
            serde_json::from_value(json!("019f0000-0000-7000-8000-000000000001")).unwrap(),
            serde_json::from_value(json!({
                "target":{"kind":"centralId","sessionId":"019f0000-0000-7000-8000-000000000002"},
                "cause":{"kind":"givenUp"}
            }))
            .unwrap(),
        );
        let CentralCallSubmission::Accepted(receipt) = calls.submit(request, deadline) else {
            panic!("the registered call must be accepted");
        };
        receipt
    };
    let old = attempt();
    calls.registered(old.clone(), || {});
    let fence = WireWriteFence::new(clock.clone());
    let old_receipt = offer();
    let correlation = old_receipt.key.correlation.clone();
    calls.begin_write(&old, now, &fence).unwrap().flushed();
    calls.retire(&old, CallEnding::CarrierClosed(None), || {});

    let replacement = attempt();
    assert_ne!(old, replacement);
    calls.registered(replacement.clone(), || {});
    let mut replacement_receipt = offer();
    calls.0.lock().entries.front_mut().unwrap().key.correlation = correlation.clone();
    replacement_receipt.key.correlation = correlation.clone();
    calls
        .begin_write(&replacement, now, &fence)
        .unwrap()
        .flushed();
    let result = replacement_receipt.resolve();
    tokio::pin!(result);
    let refuse = |reason| TypedCallResult::Refused { reason };
    calls.answer(
        &old,
        &correlation,
        refuse(LinkCallRefusal::WrongTarget),
        now,
    );
    assert!(result.as_mut().now_or_never().is_none());
    calls.answer(
        &replacement,
        &correlation,
        refuse(LinkCallRefusal::NotPermitted),
        now,
    );
    assert!(matches!(
        result.await,
        CallSettlement::RefusedByCentral(LinkCallRefusal::NotPermitted)
    ));
    assert!(matches!(
        old_receipt.resolve().await,
        CallSettlement::AnswerLost(CallEnding::CarrierClosed(None))
    ));
}

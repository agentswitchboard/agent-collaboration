use std::time::Duration;

use central_link_client::{
    CorrelatedStatusReply, LinkLaneDisposition, ReadyLinkLane, RequestStatusCommand,
};
use central_link_protocol::{
    CorrelationId, DataFrame, HandledMacCalls, LinkFrame, MacCapabilities, RequestScope,
    RequestStatusAnswer, RequestStatusQuery,
};
use router_clock::RuntimeClock;
use serde_json::json;

use super::{MACHINE_LINK_FIXTURE_MACHINE, SimulatedLink, run_link_case};

async fn check_advertisement_and_status(link: &SimulatedLink, previous: Option<u16>) -> u16 {
    let correlation_id: CorrelationId = "01890f2e-7b4c-7cc0-98c4-0000000000a1".parse().unwrap();
    let outcome = link
        .lane
        .request_status(RequestStatusCommand {
            connection: link.admission.connection.clone(),
            send_before: link.clock.monotonic_now().after(Duration::from_secs(5)),
            correlation_id: correlation_id.clone(),
            caller: serde_json::from_value(json!({
                "kind": "session",
                "value": {"machineId": MACHINE_LINK_FIXTURE_MACHINE, "sessionId": "fixture-lane"}
            }))
            .unwrap(),
            query: RequestStatusQuery {
                request_id: "01890f2e-7b4c-7cc0-98c4-0000000000a2".parse().unwrap(),
                scope: RequestScope::Organization,
            },
        })
        .await;
    assert!(
        matches!(
            &outcome,
            LinkLaneDisposition::Answered {
                reply: CorrelatedStatusReply { correlation_id: actual, result }, ..
            } if actual == &correlation_id && **result == RequestStatusAnswer::NotFound
        ),
        "real Central must answer the ordinary request on this connection: {outcome:?}"
    );

    let transcript = link.network.frames();
    let request = transcript
        .iter()
        .find(|entry| {
            Some(entry.connection_port) != previous
                && matches!(&entry.frame, LinkFrame::Data(DataFrame::RequestStatus {
                    correlation_id: actual, ..
                }) if actual == &correlation_id)
        })
        .unwrap();
    let ordinary: Vec<_> = transcript
        .iter()
        .filter(|entry| entry.connection_port == request.connection_port)
        .map(|entry| &entry.frame)
        .collect();
    assert!(
        matches!(
            ordinary.as_slice(),
            [LinkFrame::Data(DataFrame::Welcome { .. }),
             LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities { handled_calls })),
             LinkFrame::Data(DataFrame::RequestStatus { .. }),
             LinkFrame::Data(DataFrame::RequestStatusReply { .. })]
                if handled_calls == &HandledMacCalls::default()
        ),
        "expected Welcome, one empty advertisement, and an ordinary request/reply on the same socket: {ordinary:?}"
    );
    request.connection_port
}

#[test]
fn real_central_accepts_one_empty_advertisement_before_traffic_on_each_socket() {
    run_link_case(|mut link| async move {
        let first = check_advertisement_and_status(&link, None).await;
        link.reset_socket().await;
        link.reconnect().await;
        let second = check_advertisement_and_status(&link, Some(first)).await;
        assert_ne!(first, second);
        link.close().await;
    });
}

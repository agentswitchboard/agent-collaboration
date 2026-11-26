use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use central_link_protocol::{
    CentralCall, DataFrame, FrameDirection, LinkCallRefusal, LinkFrame, MachineLinkCloseCode,
    TypedCallResult,
    approval_rpc::{ApprovalServiceTarget, ListApprovalsForSessionRpc},
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};
use collaboration_central_conformance::session_link_scenarios::session_records;
use collaboration_central_tokio::test_support::{MachineLinkFixture, start_session_link_fixture};
use router_clock::{ManualClock, RuntimeClock};
use serde_json::{Value, json};
use switchboard_central_link::link_policy::LinkStatus;
use switchboard_message_system::{
    CallEnding, CallNotReached, CentralWorkNextMove, CentralWorkOutcome, CentralWorkRefusal,
    CentralWorkRefusalStatus, CloseReport, MessageSystemReason, MessageSystemStatus,
};

use super::message_unit_fixture::EPOCH;
use super::native_work_carrier::{NativeWorkCarrier, application};
use super::native_work_fixture::start_mac;

fn create_request(operation: u8) -> Value {
    json!({
        "operationId": format!("019f0000-0000-7000-8000-{operation:012x}"),
        "attempt": 1,
        "creator": {"kind":"routerOperator"},
        "agentProgram": "codex",
        "sessionKind": {"kind":"ordinary"}
    })
}

async fn native_fixture() -> MachineLinkFixture {
    start_session_link_fixture(Arc::new(ManualClock::new(EPOCH.parse().unwrap())))
        .await
        .unwrap()
}

#[tokio::test]
async fn typed_port_answers_session_and_approval_calls_then_refuses_an_ended_enrollment() {
    let central = native_fixture().await;
    let clock = central.clock.clone();
    let carrier = NativeWorkCarrier::default();
    carrier.hold_first_registration();
    let (system, mut driver_status) = start_mac(&central, carrier.clone()).await;
    let mut status = system.status();
    let calls = system.central_calls();
    let deadline = clock.monotonic_now().after(Duration::from_secs(10));
    carrier.to_driver.wait_for(|frames| frames.iter().any(|frame| matches!(
        application(frame, FrameDirection::ToRouter),
        Some(LinkFrame::Data(DataFrame::CentralCallReply { result, .. })) if matches!(*result, TypedCallResult::Answered {reply: central_link_protocol::CentralCallReply::RegisterRun(_)})
    ))).await;
    status
        .wait_for(|found| found.status == MessageSystemStatus::Registering)
        .await
        .unwrap();
    let mut before_registration = std::pin::pin!(calls.call::<CreateFleetSessionRpc>(
        central.organization_id.clone(),
        serde_json::from_value(create_request(1)).unwrap(),
        deadline,
    ));
    let Poll::Ready(CentralWorkOutcome::RefusedByLink(refusal)) =
        poll_fn(|context| Poll::Ready(before_registration.as_mut().poll(context))).await
    else {
        panic!("the successful acknowledgement is still held below the driver reader");
    };
    assert_eq!(
        refusal,
        CentralWorkRefusal {
            status: CentralWorkRefusalStatus::Registering,
            reason: MessageSystemReason::NoReason
        }
    );
    assert_eq!(refusal.valid_move(), CentralWorkNextMove::Wait);
    assert!(carrier.to_central.snapshot().iter().all(|frame| !matches!(
        application(frame, FrameDirection::ToCentral),
        Some(LinkFrame::Data(DataFrame::CallCentral { request, .. })) if !matches!(*request, CentralCall::RegisterRun(_))
    )));
    carrier.release();
    status
        .wait_for(|found| found.status == MessageSystemStatus::Connected)
        .await
        .unwrap();
    let owner = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    let statement_host = owner.runtime().statement_host();

    let wrong = calls
        .call::<CreateFleetSessionRpc>(
            "019f0000-0000-7000-8000-00000000ce73"
                .to_owned()
                .try_into()
                .unwrap(),
            serde_json::from_value(create_request(1)).unwrap(),
            deadline,
        )
        .await;
    assert!(matches!(
        wrong,
        CentralWorkOutcome::RefusedByCentral(LinkCallRefusal::WrongTarget)
    ));
    let late = calls
        .call::<CreateFleetSessionRpc>(
            central.organization_id.clone(),
            serde_json::from_value(create_request(2)).unwrap(),
            clock.monotonic_now(),
        )
        .await;
    assert!(matches!(
        late,
        CentralWorkOutcome::NotReached(CallNotReached::Ended(CallEnding::Deadline))
    ));
    assert!(
        statement_host
            .read(session_records())
            .await
            .unwrap()
            .rows
            .is_empty()
    );

    let created = calls
        .call::<CreateFleetSessionRpc>(
            central.organization_id.clone(),
            serde_json::from_value(create_request(3)).unwrap(),
            deadline,
        )
        .await;
    let CentralWorkOutcome::Answered(created) = created else {
        panic!("the typed Create call must get the native Central answer")
    };
    let reserved = serde_json::to_value(created).unwrap();
    assert_eq!(reserved["result"], "reserved");
    assert_eq!(
        statement_host
            .read(session_records())
            .await
            .unwrap()
            .rows
            .len(),
        1
    );

    let bound = calls.call::<BindFleetSessionRpc>(
        central.organization_id.clone(), serde_json::from_value(json!({
            "sessionId": reserved["sessionId"], "harnessSessionId": "typed-work", "secret": reserved["secret"],
        })).unwrap(), deadline,
    ).await;
    let CentralWorkOutcome::Answered(bound) = bound else {
        panic!("the typed Bind call must get the native Central answer")
    };
    assert_eq!(serde_json::to_value(bound).unwrap()["result"], "active");

    let approvals = calls.call::<ListApprovalsForSessionRpc>(
        ApprovalServiceTarget { organization_id: central.organization_id.clone() },
        serde_json::from_value(json!({
            "sessionId": reserved["sessionId"], "secret": reserved["secret"], "input": {"cursor": null},
        })).unwrap(), deadline,
    ).await;
    let CentralWorkOutcome::Answered(approvals) = approvals else {
        panic!("the typed approval list must get the native Central answer")
    };
    assert_eq!(
        serde_json::to_value(approvals).unwrap(),
        json!({"kind":"page","items":[],"next":null})
    );

    driver_status.publish.send_replace(false);
    driver_status.operator_status_hold.freeze();
    let mut held_observations = driver_status.operator_status_hold.held_observations();
    carrier.terminal_close(MachineLinkCloseCode::Revoked);
    let ending = driver_status.ending_reports.recv().await.unwrap();
    assert_eq!(ending.cause, central_link_client::EnrollmentEnd::Revoked);
    let published_before_refusal = *status.borrow_and_update();
    assert_eq!(
        published_before_refusal.status,
        MessageSystemStatus::Connected
    );
    let mut pending_call = std::pin::pin!(calls.call::<CreateFleetSessionRpc>(
        central.organization_id.clone(),
        serde_json::from_value(create_request(13)).unwrap(),
        deadline,
    ));
    let Poll::Ready(CentralWorkOutcome::RefusedByLink(pending)) =
        poll_fn(|context| Poll::Ready(pending_call.as_mut().poll(context))).await
    else {
        panic!("the terminal gate must refuse while its ending report is held");
    };
    assert_eq!(
        pending,
        CentralWorkRefusal {
            status: CentralWorkRefusalStatus::EnrollmentEnded,
            reason: MessageSystemReason::EnrollmentEndingPending,
        }
    );
    assert_eq!(
        pending.valid_move(),
        CentralWorkNextMove::EnrollThisMacAgain
    );
    assert_eq!(*status.borrow(), published_before_refusal);
    assert!(!status.has_changed().unwrap());
    ending.persist();
    held_observations
        .wait_for(|found| {
            found.is_some_and(|observed| {
                observed.status == MessageSystemStatus::EnrollmentEnded
                    && observed.reason == MessageSystemReason::EnrollmentRevoked
            })
        })
        .await
        .unwrap();
    assert_eq!(*status.borrow(), published_before_refusal);
    assert!(!status.has_changed().unwrap());
    let mut stored_call = std::pin::pin!(calls.call::<CreateFleetSessionRpc>(
        central.organization_id.clone(),
        serde_json::from_value(create_request(4)).unwrap(),
        deadline,
    ));
    let Poll::Ready(CentralWorkOutcome::RefusedByLink(refused)) =
        poll_fn(|context| Poll::Ready(stored_call.as_mut().poll(context))).await
    else {
        panic!(
            "the typed port must refuse against the recorded ending while Connected is still published"
        );
    };
    assert_eq!(
        refused,
        CentralWorkRefusal {
            status: CentralWorkRefusalStatus::EnrollmentEnded,
            reason: MessageSystemReason::EnrollmentRevoked,
        }
    );
    assert_eq!(
        refused.valid_move(),
        CentralWorkNextMove::EnrollThisMacAgain
    );
    assert_eq!(pending.reason, MessageSystemReason::EnrollmentEndingPending);
    assert_eq!(*status.borrow(), published_before_refusal);
    assert!(!status.has_changed().unwrap());
    driver_status.operator_status_hold.release();
    driver_status.publish.send_replace(true);
    assert_eq!(
        statement_host
            .read(session_records())
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(system.close().await, CloseReport::Closed);
    drop(owner);
    central.close().await.unwrap();
}

#[tokio::test]
async fn native_answers_held_at_the_reader_exhaust_slots_expire_and_do_not_replay() {
    let central = native_fixture().await;
    let clock = central.clock.clone();
    let carrier = NativeWorkCarrier::default();
    let (system, mut driver_status) = start_mac(&central, carrier.clone()).await;
    let mut status = system.status();
    status
        .wait_for(|found| found.status == MessageSystemStatus::Connected)
        .await
        .unwrap();
    let calls = system.central_calls();
    let owner = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    let rows = owner.runtime().statement_host();
    let now = clock.monotonic_now();
    let deadline = now.after(Duration::from_secs(10));
    let short = now.after(Duration::from_secs(2));
    let create = |operation, deadline| {
        calls.call::<CreateFleetSessionRpc>(
            central.organization_id.clone(),
            serde_json::from_value(create_request(operation)).unwrap(),
            deadline,
        )
    };

    let mut selected_refusal = Box::pin(
        calls.call::<CreateFleetSessionRpc>(
            "019f0000-0000-7000-8000-00000000ce73"
                .to_owned()
                .try_into()
                .unwrap(),
            serde_json::from_value(create_request(11)).unwrap(),
            deadline,
        ),
    );
    assert!(
        poll_fn(|context| Poll::Ready(selected_refusal.as_mut().poll(context)))
            .await
            .is_pending()
    );
    // A later valid reply on the same socket shows the earlier refusal was read.
    assert!(matches!(calls.call::<EndFleetSessionRpc>(
        central.organization_id.clone(),
        serde_json::from_value(json!({
            "target":{"kind":"centralId","sessionId":"019f0000-0000-7000-8000-000000000002"},
            "cause":{"kind":"givenUp"}
        })).unwrap(),
        deadline,
    ).await, CentralWorkOutcome::Answered(_)));

    carrier.hold();
    let mut waiting: Vec<_> = (1..=8)
        .map(|operation| {
            Box::pin(create(
                operation,
                if operation == 1 { short } else { deadline },
            ))
        })
        .collect();
    for call in &mut waiting {
        assert!(
            poll_fn(|context| Poll::Ready(call.as_mut().poll(context)))
                .await
                .is_pending()
        );
    }
    carrier.to_driver.wait_for(|frames| frames.iter().filter(|frame| {
        matches!(application(frame, FrameDirection::ToRouter),
            Some(LinkFrame::Data(DataFrame::CentralCallReply { result, .. })) if matches!(*result,
                TypedCallResult::Answered { reply: central_link_protocol::CentralCallReply::CreateFleetSession(_) }))
    }).count() == 8).await;
    assert_eq!(rows.read(session_records()).await.unwrap().rows.len(), 8);
    assert!(matches!(
        create(9, deadline).await,
        CentralWorkOutcome::NotReached(CallNotReached::LaneFull)
    ));

    clock.advance(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        waiting.remove(0).await,
        CentralWorkOutcome::AnswerLost(CallEnding::Deadline)
    ));
    driver_status.publish.send_replace(false);
    carrier.cut();
    driver_status
        .actual
        .wait_for(|status| matches!(status, LinkStatus::Connecting))
        .await
        .unwrap();
    let published_before_refusal = *status.borrow();
    assert_eq!(
        published_before_refusal.status,
        MessageSystemStatus::Connected
    );
    let mut stale_status_call = std::pin::pin!(create(12, deadline));
    let Poll::Ready(CentralWorkOutcome::RefusedByLink(refusal)) =
        poll_fn(|context| Poll::Ready(stale_status_call.as_mut().poll(context))).await
    else {
        panic!("the typed submission must refuse synchronously after the real gate closes");
    };
    assert_eq!(
        refusal,
        CentralWorkRefusal {
            status: CentralWorkRefusalStatus::Connecting,
            reason: MessageSystemReason::NoReason
        }
    );
    assert_eq!(refusal.valid_move(), CentralWorkNextMove::Wait);
    assert_eq!(*status.borrow(), published_before_refusal);
    driver_status.publish.send_replace(true);
    status
        .wait_for(|found| found.status == MessageSystemStatus::Connecting)
        .await
        .unwrap();
    carrier.release();
    let retry = clock.monotonic_now().after(Duration::from_secs(1));
    clock.not_due_check_now(retry).await;
    clock.advance(Duration::from_secs(1)).unwrap();
    status
        .wait_for(|found| found.status == MessageSystemStatus::Connected)
        .await
        .unwrap();
    for old_result in waiting {
        assert!(matches!(
            old_result.await,
            CentralWorkOutcome::AnswerLost(CallEnding::TransportLost)
        ));
    }
    assert!(matches!(
        selected_refusal.await,
        CentralWorkOutcome::RefusedByCentral(LinkCallRefusal::WrongTarget)
    ));
    assert!(matches!(
        create(10, deadline).await,
        CentralWorkOutcome::Answered(_)
    ));
    assert_eq!(rows.read(session_records()).await.unwrap().rows.len(), 9);
    let transcript = carrier.to_central.snapshot();
    let registrations: Vec<_> = transcript.iter().filter_map(|frame| {
        matches!(application(frame, FrameDirection::ToCentral), Some(LinkFrame::Data(DataFrame::CallCentral { request, .. })) if matches!(*request, CentralCall::RegisterRun(_))).then_some(frame.port)
    }).collect();
    let [first_port, replacement_port] = registrations.as_slice() else {
        panic!("one registration is required on each of the two sockets");
    };
    assert_ne!(first_port, replacement_port);
    let work: Vec<_> = transcript
        .iter()
        .filter_map(|frame| {
            let Some(LinkFrame::Data(DataFrame::CallCentral {
                correlation_id,
                request,
            })) = application(frame, FrameDirection::ToCentral)
            else {
                return None;
            };
            matches!(*request, CentralCall::CreateFleetSession(_))
                .then_some((frame.port, correlation_id))
        })
        .collect();
    assert_eq!(work.len(), 10);
    assert!(
        work.get(..9)
            .unwrap()
            .iter()
            .all(|(port, _)| port == first_port)
    );
    assert_eq!(&work.last().unwrap().0, replacement_port);
    assert!(work.iter().enumerate().all(|(index, (_, correlation))| {
        work.get(..index)
            .unwrap()
            .iter()
            .all(|(_, earlier)| earlier != correlation)
    }));
    assert_eq!(system.close().await, CloseReport::Closed);
    drop(owner);
    central.close().await.unwrap();
}

#[tokio::test]
async fn native_machine_carrier_refuses_a_raw_person_call() {
    use collaboration_central_conformance::{
        fixture_link_reader::read_fixture_frame,
        signin_fixtures::approval_socket::{SocketApprovalReply, exchange},
    };

    let central = native_fixture().await;
    let mut socket = central.connect().await.unwrap();
    let _welcome = read_fixture_frame(&mut socket, "welcome on machine carrier")
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let refusal = exchange(
        &mut socket,
        "019f0000-0000-7000-8000-000000000015",
        "listOrganizationWorkspaces",
        central.organization_id.as_str(),
        &json!({"operationId":"019f0000-0000-7000-8000-000000000016", "after":null, "limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(
        refusal,
        SocketApprovalReply::Refused {
            reason: "wrongTarget".to_owned()
        }
    );
    drop(socket);
    central.close().await.unwrap();
}

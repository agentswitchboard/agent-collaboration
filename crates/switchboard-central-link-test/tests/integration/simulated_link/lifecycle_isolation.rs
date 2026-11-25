use central_link_client::AdmittedLifecycleLink;
use fleet_identity_protocol::CreateFleetSessionReply;
use router_clock::ClockInstant;

use super::{
    DataFrame, Duration, FrameKind, LinkFrame, NetworkCustody, RuntimeClock, run_link_case,
};
use crate::simulated_link::lifecycle_cases::{create_request, fixture_scope};

#[test]
fn due_custody_batch_writes_at_its_mark_while_a_lifecycle_answer_is_held() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let calls = case.link.lane.lifecycle_calls();
        let scope = fixture_scope(&case.link.central);
        let connection = case.link.admission.connection.clone();
        let after = case.link.network.hold_next(FrameKind::LifecycleReply);
        let mut call = calls.create_fleet_session(
            &scope,
            &connection,
            create_request(1),
            case.link.lifecycle_send_deadline(),
        );
        let held = tokio::select! {
            biased;
            _ = &mut call => panic!("Central's lifecycle answer must be held"),
            held = case.link.network.seen(after, FrameKind::LifecycleReply) => held,
        };
        let mark = case.first_flush(&receipt).await;
        let batch = tokio::select! {
            biased;
            _ = &mut call => panic!("the batch must reach the wire while the lifecycle answer is held"),
            batch = case.link.network.seen(after, FrameKind::Batch) => batch,
        };
        assert_eq!(
            batch.sent_at, mark,
            "wrong build delays a due batch behind a lifecycle answer"
        );
        assert_eq!(batch.connection_port, held.connection_port);
        let LinkFrame::Data(DataFrame::Batch { requests }) = batch.frame else {
            panic!("expected the due mail batch")
        };
        assert!(
            matches!(requests.requests(), [member] if member.payload().write_request_id() == Some(&receipt.id))
        );
        case.link.network.release_and_wait().await;
        assert!(matches!(
            call.await.unwrap().into_wire(),
            CreateFleetSessionReply::Reserved { .. }
        ));
        case.finish_write(&receipt).await;
        case.close().await;
    });
}

#[test]
fn lifecycle_writes_while_a_real_custody_answer_is_held() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::WriteAnswer);
        let mark = case.first_flush(&receipt).await;
        let held = case.link.network.seen(after, FrameKind::WriteAnswer).await;

        let calls = case.link.lane.lifecycle_calls();
        let scope = fixture_scope(&case.link.central);
        let connection = case.link.admission.connection.clone();
        let mut call = calls.create_fleet_session(
            &scope,
            &connection,
            create_request(1),
            case.link.lifecycle_send_deadline(),
        );
        let sent = tokio::select! {
            biased;
            _ = &mut call => panic!("the actual lifecycle request must appear on the wire"),
            sent = case.link.network.seen(after, FrameKind::LifecycleCall) => sent,
        };
        assert_eq!(
            sent.sent_at, mark,
            "wrong build queues lifecycle behind a custody answer"
        );
        assert_eq!(sent.connection_port, held.connection_port);
        case.link.network.release_and_wait().await;
        assert!(matches!(
            call.await.unwrap().into_wire(),
            CreateFleetSessionReply::Reserved { .. }
        ));
        case.finish_write(&receipt).await;
        case.close().await;
    });
}

#[test]
fn due_renewal_writes_on_its_reserved_path_while_a_lifecycle_answer_is_held() {
    run_link_case(|link| async move {
        let before_renewal = ClockInstant::ZERO.after(Duration::from_secs(534));
        let renewal_at = before_renewal.after(Duration::from_secs(6));
        link.driver_clock
            .wait_until_pending(ClockInstant::ZERO.after(Duration::from_secs(20)))
            .await;
        link.clock
            .advance(before_renewal.saturating_duration_since(link.clock.monotonic_now()))
            .unwrap();
        // A heartbeat after the jump must be answered before the hold is armed.
        link.network.seen(0, FrameKind::Pong).await;
        link.driver_clock.wait_until_pending(renewal_at).await;

        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let after = link.network.hold_next(FrameKind::LifecycleReply);
        let mut call = calls.create_fleet_session(
            &scope,
            &link.admission.connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        let held = tokio::select! {
            biased;
            _ = &mut call => panic!("Central's lifecycle answer must be held"),
            held = link.network.seen(after, FrameKind::LifecycleReply) => held,
        };
        link.clock
            .advance(renewal_at.saturating_duration_since(link.clock.monotonic_now()))
            .unwrap();
        let renewal = tokio::select! {
            biased;
            _ = &mut call => panic!("renewal must write while the lifecycle answer is held"),
            renewed = link.network.seen(after, FrameKind::Renewal) => renewed,
        };
        assert_eq!(
            renewal.sent_at, renewal_at,
            "wrong build delays the reserved renewal path behind lifecycle"
        );
        assert_eq!(renewal.connection_port, held.connection_port);
        link.network.release_and_wait().await;
        assert!(matches!(
            call.await.unwrap().into_wire(),
            CreateFleetSessionReply::Reserved { .. }
        ));
        let reply = link.network.seen(after, FrameKind::RenewalReply).await;
        let LinkFrame::Data(DataFrame::RenewMachineToken { correlation_id, .. }) = renewal.frame
        else {
            panic!("expected renewal frame")
        };
        assert!(
            matches!(reply.frame, LinkFrame::Data(DataFrame::MachineTokenRenewal { correlation_id: actual,
            result: central_link_protocol::TokenRenewalResult::Renewed { .. } }) if actual == correlation_id)
        );
        link.close().await;
    });
}

use std::time::Duration;

use central_link_client::ReadyLinkLane;
use central_link_protocol::{
    CentralCall, CentralCallReply, CorrelationId, DataFrame, LinkFrame, TypedCallResult,
    approval_rpc::RegisterRunReply,
};
use central_storage::{SqlValue, Statement};
use switchboard_central_link::link_policy::{LinkStatus, RegistrationFailure};
use workspace_ids::RouterRunId;

use super::{
    SimulatedLink,
    packet_flight::{FrameKind, InFlightFrame, PacketFlight},
    run_link_case, run_link_case_with_network,
};

#[expect(
    clippy::panic,
    reason = "The wire proof requires the exact registration frame."
)]
fn request(frame: &InFlightFrame) -> (&CorrelationId, &RouterRunId) {
    let LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request,
    }) = &frame.frame
    else {
        panic!("expected a captured registration request");
    };
    let CentralCall::RegisterRun(request) = request.as_ref() else {
        panic!("expected the RegisterRun command");
    };
    (correlation_id, &request.body.router_run)
}

fn is_registered_reply(frame: &InFlightFrame, expected: &CorrelationId) -> bool {
    matches!(
        &frame.frame,
        LinkFrame::Data(DataFrame::CentralCallReply { correlation_id, result })
            if correlation_id == expected && matches!(
                result.as_ref(),
                TypedCallResult::Answered { reply: CentralCallReply::RegisterRun(reply) }
                    if reply.body == RegisterRunReply::Registered {}
            )
    )
}

async fn stored_runs(link: &SimulatedLink) -> Vec<Vec<SqlValue>> {
    let owner = link
        .central
        .host
        .central(&link.central.organization_id)
        .await
        .unwrap();
    owner.runtime().statement_host().read(Statement {
        sql: "SELECT router_run_id FROM approval_router_runs WHERE organization_id = ?1 AND machine_id = ?2 AND enrollment_id = ?3",
        args: vec![
            SqlValue::Text(link.central.organization_id.as_str().to_owned()),
            SqlValue::Text(super::MACHINE_LINK_FIXTURE_MACHINE.to_owned()),
            SqlValue::Text(link.central.enrollment_id.as_str().to_owned()),
        ],
    }).await.unwrap().rows
}

#[test]
fn losing_a_committed_acknowledgement_retries_once_on_the_same_socket_and_keeps_one_run() {
    let network = PacketFlight::new();
    network.hold_next(FrameKind::RegisterReply);
    run_link_case_with_network(network, |mut link| async move {
        let original = link.network.seen(0, FrameKind::RegisterRun).await;
        let reply = link.network.seen(0, FrameKind::RegisterReply).await;
        let (original_correlation, run) = request(&original);
        assert!(is_registered_reply(&reply, original_correlation));
        assert_eq!(
            stored_runs(&link).await,
            [vec![SqlValue::Text(run.to_string())]]
        );
        assert_eq!(*link.status.borrow(), LinkStatus::Registering);
        assert!(link.lane.watch_registered().borrow().is_none());
        let dropped = link.read_probe.discard_next_read();
        link.network.release();
        assert_eq!(
            super::packet_flight::decode_segment(&dropped.await.unwrap(), false),
            [reply.frame]
        );

        link.clock.advance(Duration::from_secs(20)).unwrap();
        link.network.seen(0, FrameKind::Pong).await;
        let answer_due = original.sent_at.after(Duration::from_secs(30));
        link.driver_clock.wait_until_pending(answer_due).await;
        link.clock.advance(Duration::from_secs(10)).unwrap();
        link.status
            .wait_for(|status| {
                *status
                    == LinkStatus::RegistrationFailed {
                        reason: RegistrationFailure::TimedOut,
                    }
            })
            .await
            .unwrap();
        let retry_due = answer_due.after(Duration::from_secs(1));
        link.driver_clock.wait_until_pending(retry_due).await;
        let before_retry = link.driver_clock.checkpoint();
        link.clock
            .advance(Duration::from_secs(1) - Duration::from_nanos(1))
            .unwrap();
        link.driver_clock
            .observe(before_retry, |poll| {
                poll.deadline == retry_due && !poll.elapsed
            })
            .await;
        assert_eq!(
            link.network
                .frames()
                .iter()
                .filter(|frame| frame.kind == FrameKind::RegisterRun)
                .count(),
            1
        );

        link.clock.advance(Duration::from_nanos(1)).unwrap();
        let retried = link.network.seen_nth(0, FrameKind::RegisterRun, 1).await;
        let (retried_correlation, retried_run) = request(&retried);
        assert_eq!(retried.sent_at, retry_due);
        assert_eq!(retried.connection_port, original.connection_port);
        assert_ne!(retried_correlation, original_correlation);
        assert_eq!(retried_run, run);
        let reply = link.network.seen_nth(0, FrameKind::RegisterReply, 1).await;
        assert!(is_registered_reply(&reply, retried_correlation));
        let registered = link
            .lane
            .watch_registered()
            .wait_for(Option::is_some)
            .await
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(registered.router_run(), run);
        assert_eq!(registered.admitted(), &link.admission);
        assert_eq!(
            stored_runs(&link).await,
            [vec![SqlValue::Text(run.to_string())]]
        );
        assert_eq!(
            link.network
                .frames()
                .iter()
                .filter(|frame| frame.kind == FrameKind::RegisterRun)
                .count(),
            2
        );
        link.close().await;
    });
}

#[test]
fn a_cut_withdraws_readiness_and_registers_the_same_run_on_the_replacement_socket() {
    run_link_case(|mut link| async move {
        let original = link.network.seen(0, FrameKind::RegisterRun).await;
        drop(
            link.lane
                .watch_registered()
                .wait_for(Option::is_some)
                .await
                .unwrap(),
        );
        let first_connection = link.admission.connection.clone();
        link.reset_socket().await;
        assert!(link.lane.watch_registered().borrow().is_none());

        link.reconnect().await;
        let replacement = link.network.seen_nth(0, FrameKind::RegisterRun, 1).await;
        let (original_correlation, run) = request(&original);
        let (next_correlation, next_run) = request(&replacement);
        assert_ne!(original.connection_port, replacement.connection_port);
        assert_ne!(original_correlation, next_correlation);
        assert_eq!(next_run, run);
        let registered = link
            .lane
            .watch_registered()
            .wait_for(Option::is_some)
            .await
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(registered.admitted(), &link.admission);
        assert_ne!(registered.admitted().connection, first_connection);
        assert_eq!(
            stored_runs(&link).await,
            [vec![SqlValue::Text(run.to_string())]]
        );
        link.close().await;
    });
}

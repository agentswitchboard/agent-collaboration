use std::{future::poll_fn, task::Poll, time::Duration};

use central_link_client::{
    AdmittedLifecycleLink, LifecycleCallError, LifecycleCallNotSent, ReadyLinkLane,
    link_frame_answer_deadline,
};
use central_link_protocol::{DataFrame, LinkFrame};
use fleet_identity_protocol::CreateFleetSessionReply;
use router_clock::{ClockInstant, RuntimeClock};

use super::{
    SimulatedLink,
    lifecycle_cases::{create_request, fixture_scope},
    packet_flight::{FrameKind, decode_segment},
    run_link_case,
};

#[test]
fn a_blocked_heartbeat_cannot_delay_lifecycle_expiry_or_hold_its_slot() {
    run_link_case(|link| async move {
        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let connection = &link.admission.connection;
        let after = link.network.hold_next(FrameKind::LifecycleReply);
        let mut first = calls.create_fleet_session(
            &scope,
            connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        let held = tokio::select! {
            biased;
            _ = &mut first => panic!("Central's lifecycle answer must stay in flight"),
            held = link.network.seen(after, FrameKind::LifecycleReply) => held,
        };
        let expires = link_frame_answer_deadline(held.sent_at, held.sent_at);
        let heartbeat = link.writer_gate.hold();
        let until_expiry = expires.saturating_duration_since(link.clock.monotonic_now());
        link.clock
            .advance(until_expiry - Duration::from_secs(1))
            .unwrap();
        let bytes = heartbeat.await.unwrap();
        assert_eq!(decode_segment(&bytes, true), vec![LinkFrame::Ping]);

        // Fill every remaining slot while the heartbeat owns the writer.
        let mut other_calls = Vec::new();
        for _ in 0..7 {
            let mut call = calls.create_fleet_session(
                &scope,
                connection,
                create_request(1),
                link.lifecycle_send_deadline(),
            );
            assert!(poll_fn(|context| Poll::Ready(call.as_mut().poll(context).is_pending())).await);
            other_calls.push(call);
        }
        assert_eq!(
            calls
                .create_fleet_session(
                    &scope,
                    connection,
                    create_request(1),
                    link.lifecycle_send_deadline()
                )
                .await,
            Err(LifecycleCallError::NotSent(LifecycleCallNotSent::Busy))
        );

        link.clock.advance(Duration::from_secs(1)).unwrap();
        assert_eq!(first.await, Err(LifecycleCallError::AnswerLost));
        assert_eq!(link.clock.monotonic_now(), expires);
        assert!(
            link.network
                .frames()
                .iter()
                .all(|frame| frame.kind != FrameKind::Ping)
        );
        let mut freed = calls.create_fleet_session(
            &scope,
            connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        assert!(
            poll_fn(|context| Poll::Ready(freed.as_mut().poll(context).is_pending())).await,
            "expiry must release a slot even with the writer still held"
        );

        drop(freed);
        drop(other_calls);
        link.writer_gate.release();
        link.network.release_and_wait().await;
        link.close().await;
    });
}

async fn approach_first_renewal(link: &SimulatedLink, start: ClockInstant, renewal: ClockInstant) {
    link.driver_clock
        .wait_until_pending(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    link.clock
        .advance(start.saturating_duration_since(link.clock.monotonic_now()))
        .unwrap();
    link.network.seen(0, FrameKind::Pong).await;
    link.driver_clock.wait_until_pending(renewal).await;
}

#[test]
fn a_queued_lifecycle_frame_yields_the_first_byte_gate_to_due_renewal() {
    run_link_case(|link| async move {
        let start = ClockInstant::ZERO.after(Duration::from_secs(539));
        let renewal = start.after(Duration::from_secs(1));
        approach_first_renewal(&link, start, renewal).await;
        let mut first_raw_write = link.writer_gate.hold();
        let after = link.network.frames().len();
        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let mut call = calls.create_fleet_session(
            &scope,
            &link.admission.connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        assert!(poll_fn(|context| Poll::Ready(call.as_mut().poll(context).is_pending())).await);

        let checkpoint = link.driver_clock.checkpoint();
        let before_mark = start.after(Duration::from_millis(500));
        link.clock.advance(Duration::from_millis(500)).unwrap();
        tokio::select! {
            biased;
            bytes = &mut first_raw_write => panic!("wrong build stages a lifecycle frame inside the reserved renewal window: {:?}", decode_segment(&bytes.unwrap(), true)),
            _ = link.driver_clock.observe(checkpoint, |timer| timer.deadline == renewal && timer.sampled_at == before_mark && !timer.elapsed) => {},
        }

        link.clock.advance(Duration::from_millis(500)).unwrap();
        let bytes = first_raw_write.await.unwrap();
        assert!(
            matches!(
                decode_segment(&bytes, true).as_slice(),
                [LinkFrame::Data(DataFrame::RenewMachineToken { .. })]
            ),
            "the first raw write at the mark must be the reserved renewal"
        );
        link.writer_gate.release();
        assert!(matches!(
            call.await.unwrap().into_wire(),
            CreateFleetSessionReply::Reserved { .. }
        ));
        let written = link
            .network
            .frames()
            .into_iter()
            .skip(after)
            .filter(|frame| matches!(frame.kind, FrameKind::Renewal | FrameKind::LifecycleCall))
            .collect::<Vec<_>>();
        assert!(
            matches!(written.as_slice(), [renewed, lifecycle] if renewed.kind == FrameKind::Renewal && lifecycle.kind == FrameKind::LifecycleCall
            && renewed.sent_at == renewal && renewed.connection_port == lifecycle.connection_port)
        );
        link.close().await;
    });
}

#[test]
fn a_partial_lifecycle_frame_at_the_reserved_mark_loses_its_answer_and_socket() {
    run_reserved_write_cut(TransportProgress::AcceptedPrefix);
}

#[test]
fn an_untouched_lifecycle_frame_at_the_reserved_mark_is_not_sent() {
    run_reserved_write_cut(TransportProgress::NoBytes);
}

enum TransportProgress {
    NoBytes,
    AcceptedPrefix,
}

fn run_reserved_write_cut(progress: TransportProgress) {
    run_link_case(|mut link| async move {
        let start = ClockInstant::ZERO.after(Duration::from_secs(535));
        let renewal = start.after(Duration::from_secs(5));
        approach_first_renewal(&link, start, renewal).await;
        let (staged, disposition) = match progress {
            TransportProgress::NoBytes => (
                link.writer_gate.hold(),
                LifecycleCallError::NotSent(LifecycleCallNotSent::LinkNotReady),
            ),
            TransportProgress::AcceptedPrefix => (
                link.writer_gate.hold_after_one_byte(),
                LifecycleCallError::AnswerLost,
            ),
        };
        let old = link.admission.connection.clone();
        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let mut call = calls.create_fleet_session(
            &scope,
            &old,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        let bytes = tokio::select! {
            biased;
            _ = &mut call => panic!("the lifecycle frame must stage at the raw writer"),
            staged = staged => staged.unwrap(),
        };
        assert!(!bytes.is_empty());
        link.clock.advance(Duration::from_secs(5)).unwrap();
        assert_eq!(
            call.await,
            Err(disposition),
            "wrong build classifies the staged frame incorrectly at the reserved mark"
        );
        link.lane
            .watch_admitted()
            .wait_for(Option::is_none)
            .await
            .unwrap();
        assert_eq!(link.clock.monotonic_now(), renewal);
        link.writer_gate.release();
        assert!(
            link.network
                .frames()
                .iter()
                .all(|frame| frame.kind != FrameKind::LifecycleCall)
        );
        link.reconnect().await;
        assert_ne!(link.admission.connection, old);
        assert!(matches!(
            calls
                .create_fleet_session(
                    &scope,
                    &link.admission.connection,
                    create_request(1),
                    link.lifecycle_send_deadline()
                )
                .await
                .unwrap()
                .into_wire(),
            CreateFleetSessionReply::Reserved { .. }
        ));
        link.close().await;
    });
}

#![expect(
    clippy::panic,
    reason = "The test names the unexpected typed or wire result."
)]

use central_link_client::{
    AdmittedLifecycleLink, LifecycleCallError, LifecycleCallFuture, LifecycleCallNotSent,
    LifecycleLinkReadiness, ReadyLinkLane, SessionLifecycleScope, link_frame_answer_deadline,
};
use central_link_protocol::{
    CentralCallReply, CorrelationId, DataFrame, LinkFrame, TypedCallResult,
};
use central_storage::SqlValue;
use collaboration_central_conformance::session_link_scenarios::session_records;
use collaboration_central_tokio::TokioWorkspaceHost;
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CentralIdEndOutcome, CreateFleetSession,
    CreateFleetSessionReply, CreateSessionKind, EndFleetSession, EndFleetSessionReply,
    EndSessionOutcome, EndSessionTarget, HarnessKind, NewEndSettlement, RouterEndCause,
    SessionCreator, SessionSecret, SessionSettlement, SessionTime,
};
use workspace_ids::{CentralSessionId, SessionRef};

use super::{
    MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, RuntimeClock, SimulatedLink,
    packet_flight::{FrameKind, InFlightFrame, decode_segment},
    run_link_case,
};

pub(super) fn fixture_scope(fixture: &MachineLinkFixture) -> SessionLifecycleScope {
    SessionLifecycleScope {
        organization: fixture.organization_id.clone(),
        machine: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
        enrollment: fixture.enrollment_id.clone(),
    }
}

pub(super) fn create_request(attempt: u64) -> CreateFleetSession {
    CreateFleetSession {
        operation_id: "019f0000-0000-7000-8000-000000004a01".parse().unwrap(),
        attempt: attempt.try_into().unwrap(),
        creator: SessionCreator::RouterOperator {},
        agent_program: HarnessKind::Claude,
        session_kind: CreateSessionKind::Ordinary {},
    }
}

fn reservation(reply: CreateFleetSessionReply) -> (CentralSessionId, SessionSecret, SessionTime) {
    let CreateFleetSessionReply::Reserved {
        session_id,
        secret,
        reserved_until,
    } = reply
    else {
        panic!("expected a reserved session, got {reply:?}");
    };
    (session_id, secret, reserved_until)
}

fn create_answer(frame: InFlightFrame) -> (CorrelationId, CreateFleetSessionReply) {
    let LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    }) = frame.frame
    else {
        panic!("expected the captured Central call reply");
    };
    let TypedCallResult::Answered {
        reply: CentralCallReply::CreateFleetSession(reply),
    } = *result
    else {
        panic!("expected Central to answer Create");
    };
    (correlation_id, reply.body)
}

async fn cut_committed_answer<Reply>(
    link: &mut SimulatedLink,
    mut call: LifecycleCallFuture<'_, Reply>,
) -> InFlightFrame {
    let after = link.network.hold_next(FrameKind::LifecycleReply);
    let held = tokio::select! {
        biased;
        _ = &mut call => panic!("the lifecycle call settled before the held answer reached the wire"),
        held = link.network.seen(after, FrameKind::LifecycleReply) => held,
    };
    let old = link.admission.connection.clone();
    link.reset_socket().await;
    assert!(
        matches!(call.await, Err(LifecycleCallError::AnswerLost)),
        "wrong build treats the committed lifecycle call as untouched"
    );
    link.reconnect().await;
    assert_ne!(link.admission.connection, old);
    link.network.release_and_wait().await;
    held
}

#[test]
fn committed_create_bind_and_end_reply_cuts_replay_without_a_second_registration() {
    run_link_case(|mut link| async move {
        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let first_connection = link.admission.connection.clone();
        assert_eq!(
            calls.lifecycle_readiness(&scope),
            LifecycleLinkReadiness::Ready(link.admission.clone())
        );
        let mut wrong = scope.clone();
        wrong.enrollment = "019f0000-0000-7000-8000-000000004a02"
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(
            calls.lifecycle_readiness(&wrong),
            LifecycleLinkReadiness::NotReady
        );
        assert_eq!(
            calls
                .create_fleet_session(
                    &wrong,
                    &first_connection,
                    create_request(1),
                    link.lifecycle_send_deadline()
                )
                .await,
            Err(LifecycleCallError::NotSent(
                LifecycleCallNotSent::LinkNotReady
            ))
        );

        let deadline = link.lifecycle_send_deadline();
        let cut_create = cut_committed_answer(
            &mut link,
            calls.create_fleet_session(&scope, &first_connection, create_request(1), deadline),
        )
        .await;
        let (_, lost_reply) = create_answer(cut_create);
        let (reserved_id, lost_secret, reserved_until) = reservation(lost_reply);
        let replacement = link.admission.connection.clone();
        assert_eq!(
            calls
                .create_fleet_session(
                    &scope,
                    &first_connection,
                    create_request(2),
                    link.lifecycle_send_deadline()
                )
                .await,
            Err(LifecycleCallError::NotSent(
                LifecycleCallNotSent::LinkNotReady
            )),
            "wrong build silently dispatches on a replacement socket"
        );
        assert_eq!(
            calls
                .create_fleet_session(
                    &scope,
                    &replacement,
                    create_request(1),
                    link.lifecycle_send_deadline()
                )
                .await
                .unwrap()
                .into_wire(),
            CreateFleetSessionReply::Superseded {}
        );
        let (replayed_id, secret, replayed_until) = reservation(
            calls
                .create_fleet_session(
                    &scope,
                    &replacement,
                    create_request(2),
                    link.lifecycle_send_deadline(),
                )
                .await
                .unwrap()
                .into_wire(),
        );
        assert_eq!(replayed_id, reserved_id);
        assert_eq!(replayed_until, reserved_until);
        assert!(
            secret != lost_secret,
            "the higher attempt must rotate the secret"
        );
        assert_eq!(
            calls
                .create_fleet_session(
                    &scope,
                    &replacement,
                    create_request(1),
                    link.lifecycle_send_deadline()
                )
                .await
                .unwrap()
                .into_wire(),
            CreateFleetSessionReply::Superseded {}
        );

        let address = SessionRef {
            machine_id: scope.machine.clone(),
            session_id: "mac-lifecycle-harness".parse().unwrap(),
        };
        let bind = BindFleetSession {
            session_id: replayed_id,
            harness_session_id: address.session_id.clone(),
            secret,
        };
        let deadline = link.lifecycle_send_deadline();
        cut_committed_answer(
            &mut link,
            calls.bind_fleet_session(&scope, &replacement, bind.clone(), deadline),
        )
        .await;
        let bound_connection = link.admission.connection.clone();
        assert_eq!(
            calls
                .bind_fleet_session(
                    &scope,
                    &bound_connection,
                    bind,
                    link.lifecycle_send_deadline()
                )
                .await
                .unwrap(),
            BindFleetSessionReply::Active {
                address: address.clone()
            }
        );
        assert_eq!(
            calls
                .create_fleet_session(
                    &scope,
                    &bound_connection,
                    create_request(2),
                    link.lifecycle_send_deadline()
                )
                .await
                .unwrap()
                .into_wire(),
            CreateFleetSessionReply::Active {
                session_id: reserved_id.clone(),
                address: address.clone()
            }
        );

        let end = EndFleetSession {
            target: EndSessionTarget::CentralId {
                session_id: reserved_id.clone(),
            },
            cause: RouterEndCause::EndedForGood {},
        };
        let deadline = link.lifecycle_send_deadline();
        let held_end = cut_committed_answer(
            &mut link,
            calls.end_fleet_session(&scope, &bound_connection, end.clone(), deadline),
        )
        .await;
        let LinkFrame::Data(DataFrame::CentralCallReply { result, .. }) = held_end.frame else {
            panic!("expected the captured End reply");
        };
        let TypedCallResult::Answered {
            reply: CentralCallReply::EndFleetSession(reply),
        } = *result
        else {
            panic!("expected Central to answer End");
        };
        let EndFleetSessionReply::Settled {
            settlement: NewEndSettlement::Ended { cause, time },
        } = reply.body
        else {
            panic!("expected the cut End answer to confirm its commit");
        };
        assert_eq!(cause, RouterEndCause::EndedForGood {});
        let replay = calls
            .end_fleet_session(
                &scope,
                &link.admission.connection,
                end,
                link.lifecycle_send_deadline(),
            )
            .await
            .unwrap();
        let EndSessionOutcome::ByCentralId(CentralIdEndOutcome::AlreadySettled(settlement)) =
            replay
        else {
            panic!("End did not return the retained settlement: {replay:?}");
        };
        assert!(
            matches!(settlement, SessionSettlement::Ended { time: actual, .. } if actual == time)
        );

        let fixture = link.central.clone();
        link.close().await;
        fixture.host.close().await;
        let reopened =
            TokioWorkspaceHost::open(&fixture.root_path().join("modern"), fixture.clock.clone())
                .await
                .unwrap();
        let organization = reopened.central(&scope.organization).await.unwrap();
        let rows = organization
            .runtime()
            .statement_host()
            .read(session_records())
            .await
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            vec![vec![
                SqlValue::Text(reserved_id.as_str().to_owned()),
                SqlValue::Text("ended".into()),
                SqlValue::Text(address.session_id.as_str().to_owned())
            ]]
        );
        drop(organization);
        reopened.close().await;
    });
}

#[test]
fn disconnect_at_the_first_byte_gate_reports_not_sent_and_never_reaches_central() {
    run_link_case(|link| async move {
        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let raw_ready = link.writer_gate.hold();
        let mut call = calls.create_fleet_session(
            &scope,
            &link.admission.connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        let staged = tokio::select! {
            biased;
            _ = &mut call => panic!("the offered lifecycle request must stage on the actual writer"),
            staged = raw_ready => staged.unwrap(),
        };
        assert!(matches!(
            decode_segment(&staged, true).as_slice(),
            [LinkFrame::Data(DataFrame::CallCentral { .. })]
        ));
        link._settings
            .send_modify(|settings| settings.binding = None);
        assert_eq!(
            call.await,
            Err(LifecycleCallError::NotSent(
                LifecycleCallNotSent::LinkNotReady
            )),
            "wrong build calls an untouched staged frame AnswerLost"
        );
        link.lane
            .watch_admitted()
            .wait_for(Option::is_none)
            .await
            .unwrap();
        link.writer_gate.release();
        assert!(
            link.network
                .frames()
                .iter()
                .all(|frame| frame.kind != FrameKind::LifecycleCall)
        );
        let central = link
            .central
            .host
            .central(&scope.organization)
            .await
            .unwrap();
        assert!(
            central
                .runtime()
                .statement_host()
                .read(session_records())
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        link.close().await;
    });
}

#[test]
fn a_retired_correlation_cannot_complete_the_next_call_on_the_same_socket() {
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
            _ = &mut first => panic!("the first lifecycle answer must stay in flight"),
            held = link.network.seen(after, FrameKind::LifecycleReply) => held,
        };
        let port = held.connection_port;
        let answer_due = link_frame_answer_deadline(held.sent_at, held.sent_at);
        link.clock
            .advance(answer_due.saturating_duration_since(link.clock.monotonic_now()))
            .unwrap();
        assert_eq!(first.await, Err(LifecycleCallError::AnswerLost));

        let (old, reply) = create_answer(held);
        let (old_id, old_secret, _) = reservation(reply);
        let central = link
            .central
            .host
            .central(&scope.organization)
            .await
            .unwrap();
        let mut barrier = central
            .runtime()
            .pause_points()
            .hold_central(fleet_runtime::PausePoint::BeforeSessionTransaction);
        let mut current = calls.create_fleet_session(
            &scope,
            connection,
            create_request(2),
            link.lifecycle_send_deadline(),
        );
        tokio::select! {
            biased;
            _ = &mut current => panic!("second call settled before its applying barrier"),
            () = barrier.until_held() => {},
        }
        let processed = link.read_probe.observe_next_read_turn();
        link.network.release_and_wait().await;
        let bytes = processed.await.unwrap();
        assert!(decode_segment(&bytes, false).iter().any(|frame| matches!(frame,
            LinkFrame::Data(DataFrame::CentralCallReply { correlation_id, .. }) if correlation_id == &old)));
        assert!(
            std::future::poll_fn(|context| std::task::Poll::Ready(
                current.as_mut().poll(context).is_pending()
            ))
            .await,
            "wrong build matches the retired correlation to the current call"
        );
        barrier.release();
        let (current_id, current_secret, _) = reservation(current.await.unwrap().into_wire());
        assert_eq!(current_id, old_id);
        assert!(current_secret != old_secret);
        let sent = link
            .network
            .seen_nth(after, FrameKind::LifecycleCall, 1)
            .await;
        assert_eq!(sent.connection_port, port);
        assert!(
            matches!(sent.frame, LinkFrame::Data(DataFrame::CallCentral { correlation_id, .. }) if correlation_id != old)
        );
        drop(central);
        link.close().await;
    });
}

#[test]
fn the_ninth_lifecycle_intake_is_busy_while_eight_slots_are_in_use() {
    run_link_case(|link| async move {
        let calls = link.lane.lifecycle_calls();
        let scope = fixture_scope(&link.central);
        let connection = &link.admission.connection;
        let raw_ready = link.writer_gate.hold();
        let mut first = calls.create_fleet_session(
            &scope,
            connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        tokio::select! {
            biased;
            _ = &mut first => panic!("the first call must occupy the writer"),
            staged = raw_ready => { staged.unwrap(); }
        }
        let mut waiting = vec![first];
        for _ in 0..7 {
            let mut call = calls.create_fleet_session(
                &scope,
                connection,
                create_request(1),
                link.lifecycle_send_deadline(),
            );
            assert!(
                std::future::poll_fn(|context| std::task::Poll::Ready(
                    call.as_mut().poll(context).is_pending()
                ))
                .await,
                "each call within the configured bound has a reserved slot"
            );
            waiting.push(call);
        }
        let mut extra = calls.create_fleet_session(
            &scope,
            connection,
            create_request(1),
            link.lifecycle_send_deadline(),
        );
        let disposition =
            std::future::poll_fn(|context| std::task::Poll::Ready(extra.as_mut().poll(context)))
                .await;
        assert_eq!(
            disposition,
            std::task::Poll::Ready(Err(LifecycleCallError::NotSent(LifecycleCallNotSent::Busy))),
            "wrong build lets the ninth caller wait or send"
        );
        drop(extra);

        link.writer_gate.release();
        let mut applied = 0;
        for call in waiting {
            match call.await.unwrap().into_wire() {
                CreateFleetSessionReply::Reserved { .. } => applied += 1,
                CreateFleetSessionReply::Superseded {} => {}
                other => panic!("unexpected Central Create outcome: {other:?}"),
            }
        }
        assert_eq!(applied, 1);
        assert_eq!(
            link.network
                .frames()
                .iter()
                .filter(|frame| frame.kind == FrameKind::LifecycleCall)
                .count(),
            8
        );
        link.close().await;
    });
}

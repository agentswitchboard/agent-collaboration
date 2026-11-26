//! Reachable opening refusals at ACP's ordered writer.

use super::*;
use crate::owner_session_setting::tests::ConnectionCatalogueBehavior;
use provider_session_machines::harness_lifecycle::{
    EventPort, EventSendFuture, SessionLifecycleFact,
};
use std::num::NonZeroUsize;
use switchboard_harness_contracts::{
    ConnectionEpoch, LifecycleMove, OwnerIncarnation, ReservedReply, WriterState,
};
use tokio::sync::oneshot;

struct IgnoreObservations;

impl EventPort<SessionLifecycleFact> for IgnoreObservations {
    fn send<'a>(&'a self, _: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async { Ok(()) })
    }
}

struct Reply<Fact>(oneshot::Sender<Fact>);

impl<Fact: Send> ReservedReply<Fact> for Reply<Fact> {
    fn send(self: Box<Self>, fact: Fact) {
        assert!(
            self.0.send(fact).is_ok(),
            "opening receiver must still be present"
        );
    }
}

#[tokio::test]
async fn a_lost_receipt_and_a_stopped_writer_do_not_offer_same_binding_reopen() {
    let owner = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let (wire, output) =
        OwnerAcpWire::new(NonZeroUsize::MIN, Arc::new(ConnectionCatalogueBehavior));
    let tasks = TaskTracker::new();
    let openings = OwnerOpenings::new(
        owner,
        Arc::new(wire),
        Arc::new(OnceLock::new()),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        tasks.clone(),
    );
    let open = |generation, reply| {
        let run = RunAttempt {
            generation: SessionGeneration::from_number(generation),
            attempt: LoadAttempt(Uuid::now_v7()),
        };
        let result = openings.open(
            OpenRequest::Restore {
                owner,
                run,
                session: HarnessSessionId("saved-session".into()),
                via: RestoreVia::Load,
                setup: OpenSetup {
                    cwd: std::path::PathBuf::from("/"),
                    work_roots: Default::default(),
                    scratch_folder: None,
                },
                requested_mode: None,
            },
            Box::new(Reply(reply)),
            Arc::new(IgnoreObservations),
            Box::pin(std::future::pending()),
        );
        (run, result)
    };
    let (close_reply, _close) = oneshot::channel();
    let close = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: provider_session_machines::session_lifecycle_machine::CloseAttempt(Uuid::now_v7()),
    };
    let no_route = openings
        .wire
        .enqueue_lifecycle_close(
            owner.incarnation,
            close,
            "saved-session".into(),
            Box::new(Reply(close_reply)),
        )
        .map_err(super::super::lifecycle_wire_refusal);
    let Err(no_route) = no_route else {
        panic!("close unexpectedly found a route")
    };
    assert_eq!(no_route, LifecycleRefusal::RouteUnavailable);
    assert_eq!(no_route.valid_moves(), &[LifecycleMove::Reopen]);

    let (reply, received) = oneshot::channel();
    let (first, offered) = open(1, reply);
    assert_eq!(offered, Ok(()), "live writer admits the new opening");
    drop(output);
    let OpeningFact::RestoreFailed { run, failure } =
        received.await.expect("lost receipt settles opening")
    else {
        panic!("the lost receipt did not produce an opening failure");
    };
    assert_eq!(run, first);
    assert_eq!(failure, OpeningFailure::OutcomeUnknown);
    assert_eq!(failure.valid_moves(), &[LifecycleMove::KeepOutcomeUnknown]);

    openings.retire(first.generation);
    for generation in [2, 3] {
        let (reply, _received) = oneshot::channel();
        let (_, offered) = open(generation, reply);
        let refusal = offered.expect_err("new attempt on the stopped writer");
        assert_eq!(refusal, LifecycleRefusal::Writer(WriterState::Stopped));
        assert_eq!(refusal.valid_moves(), &[LifecycleMove::StopOwner]);
    }
    drop(openings);
    tasks.close();
    tasks.wait().await;
}

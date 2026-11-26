use super::*;
use crate::{
    InteractionFuture, InteractionPort,
    fixture_policy::{FixturePayloads, FixturePolicy},
    owner_acp_binding::OwnerAcpBinding,
    owner_session_setting::tests::ConnectionCatalogueBehavior,
    owner_wire::OwnerAcpOutput,
};
use std::num::NonZeroUsize;
use switchboard_harness_contracts::{
    AuthorizationTarget, CancelDrainWait, HarnessBinding, InputId, OwnerIncarnation, ReservedReply,
    SessionGeneration, TurnId, TurnMembership,
};
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

const SESSION: &str = "cancel-work-fixture";

#[derive(Default)]
struct WaitingInteractionPort {
    cancelling: Notify,
}

impl InteractionPort for WaitingInteractionPort {
    type Context = ();
    type OperationId = u64;

    fn operation_id(_: &()) -> Self::OperationId {
        1
    }

    fn binding_retirement(_: &()) -> CancellationToken {
        CancellationToken::new()
    }

    fn cancel_all(&self, _: (), _: &'static str) -> InteractionFuture<'_, ()> {
        Box::pin(async {
            self.cancelling.notify_one();
            std::future::pending::<()>().await;
        })
    }
}

struct CapturedCancelReply(oneshot::Sender<WorkReply>);

impl ReservedReply<WorkReply> for CapturedCancelReply {
    fn send(self: Box<Self>, reply: WorkReply) {
        self.0
            .send(reply)
            .expect("fixture still holds the reply receiver");
    }
}

struct CancelFixture {
    binding: OwnerAcpBinding<FixturePayloads<WaitingInteractionPort>>,
    #[cfg(feature = "test-observation")]
    wire: Arc<OwnerAcpWire>,
    port: Arc<WaitingInteractionPort>,
    authorization: DispatchAuthorization,
    turn_cancellation: CancellationToken,
    retirement: CancellationToken,
    tasks: TaskTracker,
    _output: OwnerAcpOutput,
    _lease: OwnerApprovalLease<FixturePayloads<WaitingInteractionPort>>,
}

impl CancelFixture {
    fn new() -> Self {
        let port = Arc::new(WaitingInteractionPort::default());
        let active = Arc::new(Mutex::new(HashMap::new()));
        let approvals = OwnerApprovalBindings::new(
            Arc::clone(&active),
            Arc::new(FixturePolicy(Arc::clone(&port))),
            CancellationToken::new(),
        );
        let run = TransportRun {
            incarnation: OwnerIncarnation(Uuid::now_v7()),
            generation: SessionGeneration::from_number(1),
        };
        let turn = TurnId(Uuid::now_v7());
        let lease = approvals
            .install(SESSION.into(), run, turn, ())
            .expect("approval lease");
        let turn_cancellation = active
            .lock()
            .expect("the approval map")
            .get(SESSION)
            .expect("exact approval context")
            .cancelling
            .clone();
        let (wire, output) =
            OwnerAcpWire::new(NonZeroUsize::MIN, Arc::new(ConnectionCatalogueBehavior));
        wire.open_route(run, SESSION.into());
        wire.activate_route(run);
        wire.apply_membership(
            run.incarnation,
            TurnMembership::ObservedEligible {
                run: run.generation,
                turn,
            },
        );
        let wire = Arc::new(wire);
        let tasks = TaskTracker::new();
        Self {
            binding: OwnerAcpBinding::new(
                Arc::clone(&wire),
                SESSION.into(),
                approvals,
                tasks.clone(),
            ),
            #[cfg(feature = "test-observation")]
            wire,
            port,
            authorization: DispatchAuthorization {
                incarnation: run.incarnation,
                run: run.generation,
                input_id: InputId::generate(),
                authorization_seq: 1,
                target: AuthorizationTarget::ExistingTurn { turn },
            },
            turn_cancellation,
            retirement: CancellationToken::new(),
            tasks,
            _output: output,
            _lease: lease,
        }
    }

    fn begin_wait(&mut self) -> oneshot::Receiver<WorkReply> {
        let (send, receive) = oneshot::channel();
        let mut reply = Some(Box::new(CapturedCancelReply(send)));
        let mark = self.binding.begin_cancel(
            self.authorization.clone(),
            &mut || ReplyAdmission::Reserved(reply.take().expect("one cancel reservation")),
            Box::pin(self.retirement.clone().cancelled_owned()),
        );
        assert_eq!(mark, CancelDrainStart::Waiting);
        receive
    }

    async fn retire(&self) {
        self.retirement.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }
}

#[test]
fn cancel_marks_open_requests_before_a_refused_reply_reservation() {
    for (state, refusal) in [
        (WriterState::Full, CancelReservationRefusal::Full),
        (WriterState::Stopped, CancelReservationRefusal::Stopped),
    ] {
        let mut fixture = CancelFixture::new();
        assert!(!fixture.turn_cancellation.is_cancelled());

        let outcome = fixture.binding.begin_cancel(
            fixture.authorization.clone(),
            &mut || {
                assert!(fixture.turn_cancellation.is_cancelled());
                ReplyAdmission::Refused(state)
            },
            Box::pin(fixture.retirement.clone().cancelled_owned()),
        );

        assert_eq!(outcome, CancelDrainStart::Refused(refusal));
        assert!(
            fixture.tasks.is_empty(),
            "a refused reservation starts no wait"
        );
    }
}

#[tokio::test]
async fn retired_cancel_wait_drops_the_reservation_without_an_event() {
    let mut fixture = CancelFixture::new();
    let mut reply = fixture.begin_wait();
    fixture.port.cancelling.notified().await;
    assert!(matches!(
        reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    fixture.retire().await;

    assert!(matches!(
        reply.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
}

#[tokio::test]
async fn expired_cancel_with_no_pending_responses_retains_the_expired_variant() {
    let mut fixture = CancelFixture::new();
    let mut reply = fixture.begin_wait();
    fixture.port.cancelling.notified().await;
    assert!(matches!(
        reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    let taken = fixture
        .binding
        .send_cancel(fixture.authorization.clone(), CancelDrainWait::Expired);

    assert_eq!(
        taken,
        DispatchResult::CancelTaken {
            authorization: fixture.authorization.clone(),
            drain: CancelDrainReport::Expired {
                pending: Vec::new()
            },
        }
    );
    fixture.retire().await;
    assert!(matches!(
        reply.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
}

#[cfg(feature = "test-observation")]
#[test]
fn a_failed_cancel_handover_revokes_the_real_offer() {
    let mut fixture = CancelFixture::new();
    let capacity = fixture.wire.hold_writer_capacity_for_test();

    let refused = fixture
        .binding
        .send_cancel(fixture.authorization.clone(), CancelDrainWait::Drained);
    drop(capacity);

    assert_eq!(
        refused,
        DispatchResult::DefinitelyUnsent(fixture.authorization.clone())
    );
    assert_eq!(
        fixture.wire.take(&fixture.authorization),
        Ok(DispatchResult::DefinitelyUnsent(
            fixture.authorization.clone()
        ))
    );
}

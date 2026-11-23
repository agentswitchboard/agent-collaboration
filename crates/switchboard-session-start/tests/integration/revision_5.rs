use fleet_identity_protocol::{EndFleetSession, EndSessionTarget, RouterEndCause};
use serde_json::{Value, json};
use switchboard_session_start::{
    CentralRefusalNow, CentralStanding, CloseResult, DirectiveId, DirectiveResult, EndLeg,
    EndNotApplied, EndNotDue, Exposure, LinkContradiction, LinkResult, SetUpResult, SnapshotKind,
    StartAnswer, StartDirective, StartNotification, StartStateKind,
};

use super::{
    executor::{Executor, Timing},
    fixtures::{self, Journey},
    revision_4::drive_to_exact as drive,
};

#[test]
fn failed_reservation_save_destroys_pending_secret_with_all_notifications_dropped() {
    for save_failure in 1..=3 {
        let mut executor = Executor::new();
        drive(&mut executor, StartStateKind::SavingReservation);
        assert!(executor.has_pending_secret());
        executor.drop_notifications();
        executor.complete_step(save_failure, Timing::BeforeFence, true);
        assert_eq!(executor.machine.kind(), StartStateKind::Stopped);
        assert!(
            !executor.has_pending_secret(),
            "pending credential survived with notifications dropped"
        );
        assert_eq!(executor.secret_at(&fixtures::issue()), None);
        executor.feed(fixtures::ask(
            4,
            Ok(fixtures::envelope()),
            CentralStanding::Ready,
        ));
        assert_eq!(executor.machine.kind(), StartStateKind::Stopped);
        assert!(executor.pending.is_none());
        assert!(!executor.has_pending_secret());
    }
}

#[test]
fn stale_reserved_delivery_destroys_its_staged_secret_and_preserves_the_original_slot() {
    let mut executor = Executor::new();
    drive(&mut executor, StartStateKind::SavingReservation);
    assert!(executor.has_pending_secret());
    let save_id = executor.pending.as_ref().unwrap().id.clone();
    let stale = DirectiveId {
        sequence: save_id.sequence - 1,
        ..save_id
    };
    let previous = executor.machine.clone();
    assert!(!executor.feed_link_result(
        stale,
        DirectiveResult::SendCreate(LinkResult::Answered(fixtures::reserved())),
        81,
    ));
    assert_eq!(executor.machine, previous);
    assert!(executor.has_pending_secret());
    executor.complete_step(0, Timing::BeforeFence, false);
    executor.take(0);
    assert!(
        !executor.has_pending_secret(),
        "vault take owns the sole pending value"
    );
    executor.complete(false);
    assert_eq!(
        executor.secret_at(&fixtures::issue()),
        Some(fixtures::secret(17))
    );
    executor.deliver();
}

fn retry_bind(known_active: bool) -> Executor {
    let mut executor = Executor::new();
    if known_active {
        drive(&mut executor, StartStateKind::Opening);
        executor.retire_connection();
        executor.complete_step(0, Timing::BeforeFence, false);
    } else {
        drive(&mut executor, StartStateKind::BindSent);
        let lost = super::choices::results(
            &executor.pending.as_ref().unwrap().directive,
            switchboard_session_start::StartRevision::new(1),
        )
        .iter()
        .position(|result| *result == DirectiveResult::SendBind(LinkResult::AnswerLost))
        .unwrap();
        executor.complete_step(lost, Timing::BeforeFence, false);
    }
    assert_eq!(executor.machine.kind(), StartStateKind::SavingOutcome);
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::BindUnknown);
    let facts = executor.machine.facts().unwrap();
    assert_eq!(
        facts.active_proof.as_ref().map(|proof| &proof.address),
        known_active.then_some(fixtures::address()).as_ref()
    );
    assert!(
        executor.pending.is_none(),
        "BindUnknown waits for another ask"
    );
    executor.feed(fixtures::ask(
        2,
        Ok(fixtures::envelope()),
        CentralStanding::Ready,
    ));
    let (_, durable) = executor.durable_snapshot().unwrap();
    assert_eq!(durable.kind(), SnapshotKind::BindUnknown);
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::BindSent);
    executor.lose_original_hold();
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::SavingCleanUp);
    executor
}

#[test]
fn verified_active_after_retired_opening_allows_end_when_rebind_loses_hold() {
    let mut executor = retry_bind(true);
    assert_eq!(
        executor.machine.facts().unwrap().bind_exposure,
        Exposure::Witnessed
    );
    executor.complete_step(0, Timing::BeforeFence, false);
    let expected = EndFleetSession {
        target: EndSessionTarget::CentralId {
            session_id: fixtures::central_id(),
        },
        cause: RouterEndCause::GivenUp {},
    };
    assert!(
        matches!(executor.pending.as_ref().map(|pending| &pending.directive), Some(StartDirective::SendEnd(request)) if request == &expected),
        "saved cleanup must issue End for the bound central session"
    );
    assert_eq!(executor.machine.facts().unwrap().end, EndLeg::Due);
}

#[test]
fn hold_lost_after_a_possible_bind_never_sends_end() {
    let mut executor = retry_bind(false);
    assert_eq!(
        executor.machine.facts().unwrap().bind_exposure,
        Exposure::MayHaveReached
    );
    executor.complete_step(0, Timing::BeforeFence, false);
    assert!(
        matches!(
            executor.pending.as_ref().map(|pending| &pending.directive),
            Some(StartDirective::Save { .. })
        ),
        "a bind that only may have reached central must not issue End"
    );
    assert_eq!(executor.machine.kind(), StartStateKind::SavingOutcome);
    assert_eq!(
        executor.machine.facts().unwrap().end,
        EndLeg::NotDue(EndNotDue::BindMayHaveReached)
    );
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::Failed);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseCause {
    Revoked,
    Expired,
    Inactive,
}

impl CloseCause {
    fn spelling(self) -> &'static str {
        match self {
            Self::Revoked => "revoked",
            Self::Expired => "expired",
            Self::Inactive => "inactive",
        }
    }
}

/// Wire shape at central-link-protocol::TypedCallResult::Refused carrying LinkCallRefusal.
/// The integration crate uses the shape directly to preserve the dependency boundary.
fn wire_refusal(cause: CloseCause) -> Value {
    json!({"kind": "refused", "reason": {"kind": "authorityLapsed", "cause": cause.spelling()}})
}

fn advisory<Reply, Evidence: LinkContradiction>(wire: &Value) -> LinkResult<Reply, Evidence> {
    assert!(
        [
            CloseCause::Revoked,
            CloseCause::Expired,
            CloseCause::Inactive
        ]
        .into_iter()
        .any(|cause| wire_refusal(cause) == *wire),
        "fixture must be central's exactly correlated AuthorityLapsed reply"
    );
    LinkResult::RefusedBeforeApply(CentralRefusalNow::AuthorityLapsed)
}

fn assert_advisory(journey: &Journey, create: Exposure, bind: Exposure) {
    let Some(StartNotification::Tell {
        answer:
            StartAnswer::RefusedByCentralNow {
                reason: CentralRefusalNow::AuthorityLapsed,
                facts,
            },
        ..
    }) = journey.notices.last()
    else {
        panic!(
            "expected central refusal with exposure evidence, got {:?}",
            journey.notices.last()
        );
    };
    assert_eq!((facts.create_exposure, facts.bind_exposure), (create, bind));
    assert!(journey.pending.is_none());
}

#[test]
fn wire_advisory_refusal_preserves_earlier_exposure_on_create_and_bind() {
    let wire = wire_refusal(CloseCause::Revoked);
    for (earlier_lost, evidence) in [(false, Exposure::None), (true, Exposure::MayHaveReached)] {
        let mut create = Journey::creating();
        if earlier_lost {
            create.result(DirectiveResult::SendCreate(LinkResult::AnswerLost));
            create.saved();
            create.ask();
            create.saved();
        }
        create.result(DirectiveResult::SendCreate(advisory(&wire)));
        assert_eq!(create.machine.kind(), StartStateKind::CreateUnknown);
        assert_advisory(&create, evidence, Exposure::None);
        create.ask();
        create.saved();
        assert_eq!(create.machine.kind(), StartStateKind::CreateSent);

        let mut bind = Journey::binding();
        if earlier_lost {
            bind.result(DirectiveResult::SendBind(LinkResult::AnswerLost));
            bind.saved();
            bind.ask();
            bind.saved();
        }
        bind.result(DirectiveResult::SendBind(advisory(&wire)));
        assert_eq!(bind.machine.kind(), StartStateKind::BindUnknown);
        assert_advisory(&bind, Exposure::Witnessed, evidence);
        bind.ask();
        bind.saved();
        assert_eq!(bind.machine.kind(), StartStateKind::BindSent);
    }
}

fn ending() -> Journey {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Failed));
    journey.saved();
    journey
}

#[test]
fn authority_lapsed_end_is_transmitted_and_refused_before_apply() {
    let mut journey = ending();
    let wire = wire_refusal(CloseCause::Inactive);
    journey.result(DirectiveResult::SendEnd(advisory(&wire)));
    assert_eq!(
        journey.machine.facts().unwrap().end,
        EndLeg::NotApplied(EndNotApplied::AuthorityLapsed)
    );
    journey.result(DirectiveResult::CloseConversation(CloseResult::Closed));
    journey.saved();
    journey.ask();
    assert_eq!(
        journey
            .effects
            .iter()
            .filter(|kind| **kind == "SendEnd")
            .count(),
        1
    );
    assert_eq!(journey.machine.kind(), StartStateKind::Failed);
    assert!(journey.pending.is_none());
}

#[derive(Default)]
struct Authority {
    current: u64,
    ended: bool,
}

impl Authority {
    fn close(&mut self, connection: u64, cause: CloseCause) {
        if self.current != connection {
            return;
        }
        match cause {
            CloseCause::Expired => self.current += 1,
            CloseCause::Revoked | CloseCause::Inactive => self.ended = true,
        }
    }

    fn refresh_ended(&mut self) {
        self.ended = true;
    }
}

#[test]
fn expiry_and_stale_close_do_not_fence_but_current_terminal_authority_does() {
    let mut authority = Authority::default();
    let mut create = Journey::creating();
    let before = create.machine.clone();
    authority.close(0, CloseCause::Expired);
    assert_eq!(authority.current, 1);
    assert!(!authority.ended);
    authority.close(0, CloseCause::Revoked);
    assert!(
        !authority.ended,
        "a close from the expired socket cannot fence the current one"
    );
    assert_eq!(create.machine, before);
    create.result(DirectiveResult::SendCreate(advisory(&wire_refusal(
        CloseCause::Expired,
    ))));
    assert_eq!(create.machine.kind(), StartStateKind::CreateUnknown);
    assert_advisory(&create, Exposure::None, Exposure::None);
    for refresh in [false, true] {
        let mut current = Authority {
            current: 1,
            ended: false,
        };
        if refresh {
            current.refresh_ended();
        } else {
            current.close(1, CloseCause::Revoked);
        }
        assert!(current.ended);
        let mut journey = ending();
        journey.result(DirectiveResult::SendEnd(LinkResult::EnrollmentEnded));
        assert_eq!(
            journey.machine.facts().unwrap().end,
            EndLeg::AuthorityRefused
        );
        journey.result(DirectiveResult::CloseConversation(CloseResult::Closed));
        journey.saved();
        assert_eq!(journey.machine.kind(), StartStateKind::Fenced);
    }
}

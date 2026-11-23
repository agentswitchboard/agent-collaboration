//! Each contract row runs through the tokio machine socket.
use super::{
    Cell, NativeApprovalFixture, RefCell, Result, signed_clock, start_session_link_fixture,
};
use collaboration_central_conformance::parent_approval_scenarios::{Case, observe_case};

async fn observe(case: Case) -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let outcome = observe_case(&fixture, case).await;
    fixture.link.close().await?;
    outcome
}

macro_rules! cases {
    ($($test:ident => $case:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $test() -> Result<()> {
                observe(Case::$case).await
            }
        )+
    };
}
cases! {
    list_page => ListPage,
    list_refuses_session_proof => BadListProof,
    read_request => ReadRequest,
    read_not_visible => ReadNotVisible,
    read_refuses_session_proof => BadReadProof,
    decide_saved => DecideSaved,
    decide_already_decided => DecideAlreadyDecided,
    decide_at_deadline => DecideTooLate,
    decide_requires_person => DecidePersonRequired,
    decide_held_requires_person => DecideHeldPersonRequired,
    decide_invalid => DecideInvalid,
    decide_not_visible => DecideNotVisible,
    decide_operation_conflict => DecideConflict,
    decide_refuses_session_proof => BadDecideProof,
    pass_to_session => PassToSession,
    pass_to_people => PassToPeople,
    pass_already_decided => PassAlreadyDecided,
    pass_at_deadline => PassTooLate,
    pass_not_visible => PassNotVisible,
    pass_operation_conflict => PassConflict,
    pass_refuses_session_proof => BadPassProof,
    earlier_destination_still_decides => EarlierDestinationStillDecides,
    three_levels => ThreeLevels,
    ended_parent_retains_request_until_timeout => EndedParentRetainsRequest,
    notice_reconnect_and_read => NoticeReconnect,
    notice_closes_with_request => NoticeEnds,
    notice_not_here_stays_owed => NoticeNotHere,
    decide_and_pass => DecideAndPass,
}

#[tokio::test]
async fn deadline_crosses_while_pass_on_is_pending() -> Result<()> {
    use collaboration_central_conformance::{
        approval_link_scenarios::ApprovalLinkFixture,
        parent_approval_scenarios::pass_crosses_deadline,
    };
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let runtime = fixture
        .link
        .host
        .central(&fixture.link.organization_id)
        .await?;
    let hold = RefCell::new(None);
    let arm = async {
        hold.replace(Some(runtime.runtime().pause_points().hold_central(
            fleet_runtime::PausePoint::BeforeParentApprovalTransaction,
        )));
        Ok(())
    };
    let cross_deadline = async {
        let mut pause = hold.take().ok_or("missing C2 applying hold")?;
        pause.until_held().await;
        let advance = fixture.advance(1).await;
        pause.release();
        advance
    };
    let result = pass_crosses_deadline(&fixture, arm, cross_deadline).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn fault_rolls_back_session_end_and_approval() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let outcome = collaboration_central_conformance::parent_approval_scenarios::session_end_and_approval_rollback(&fixture).await;
    fixture.link.close().await?;
    outcome
}

#[tokio::test]
async fn fault_rolls_back_enrollment_and_approval() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let outcome = collaboration_central_conformance::parent_approval_scenarios::enrollment_and_approval_rollback(&fixture).await;
    fixture.link.close().await?;
    outcome
}

#[tokio::test]
async fn read_after_lost_reply_preserves_first_read_and_remains_listed() -> Result<()> {
    observe_delivery(C2DeliveryCase::LostRead).await
}

#[tokio::test]
async fn a_replacement_receives_pass_notice_before_origin_replies() -> Result<()> {
    observe_delivery(C2DeliveryCase::LostPass).await
}

#[tokio::test]
async fn queued_notice_stops_at_the_original_deadline() -> Result<()> {
    observe_delivery(C2DeliveryCase::Deadline).await
}

#[tokio::test]
async fn recovery_drops_staged_notice_after_read() -> Result<()> {
    observe_delivery(C2DeliveryCase::StalePage(SettlesRecoveredNotice::Read)).await
}

#[tokio::test]
async fn recovery_drops_staged_notice_after_ending() -> Result<()> {
    observe_delivery(C2DeliveryCase::StalePage(SettlesRecoveredNotice::Ending)).await
}

use collaboration_central_conformance::parent_approval_scenarios::{
    ParentApprovalPause, PauseControl, SettlesRecoveredNotice,
    pass_notice_reaches_a_separate_destination, pass_notice_survives_origin_loss,
    queued_notice_stops_at_deadline, read_survives_lost_reply, stale_recovery_page,
};

enum C2DeliveryCase {
    LostRead,
    LostPass,
    LostOtherMachine,
    Deadline,
    StalePage(SettlesRecoveredNotice),
    Race(DecisionCompetitor, DecisionCommitOrder),
}

async fn observe_delivery(case: C2DeliveryCase) -> Result<()> {
    use fleet_runtime::PausePoint;
    use std::collections::BTreeMap;

    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let runtime = fixture
        .link
        .host
        .central(&fixture.link.organization_id)
        .await?;
    let pauses = runtime.runtime().pause_points();
    let holds = &RefCell::new(BTreeMap::<ParentApprovalPause, fleet_runtime::HeldPause>::new());
    let control = |point, step| async move {
        match step {
            PauseControl::Arm => {
                let target = match point {
                    ParentApprovalPause::BeforeApply => PausePoint::BeforeParentApprovalTransaction,
                    ParentApprovalPause::BeforeReply => PausePoint::BeforeParentApprovalReply,
                    ParentApprovalPause::BeforeNoticeWrite => PausePoint::BeforeApprovalNoticeWrite,
                    ParentApprovalPause::AfterNoticeAttempt => {
                        PausePoint::AfterApprovalNoticeAttempt
                    }
                };
                assert!(
                    holds
                        .borrow_mut()
                        .insert(point, pauses.hold_central(target))
                        .is_none()
                );
            }
            PauseControl::Wait => {
                let mut hold = holds
                    .borrow_mut()
                    .remove(&point)
                    .ok_or("missing C2 test pause")?;
                hold.until_held().await;
                holds.borrow_mut().insert(point, hold);
            }
            PauseControl::Release => {
                holds
                    .borrow_mut()
                    .remove(&point)
                    .ok_or("missing C2 test pause")?
                    .release();
            }
        }
        Ok(())
    };
    let observed = match case {
        C2DeliveryCase::LostRead => read_survives_lost_reply(&fixture, control).await,
        C2DeliveryCase::LostPass => pass_notice_survives_origin_loss(&fixture, control).await,
        C2DeliveryCase::LostOtherMachine => {
            pass_notice_reaches_a_separate_destination(&fixture, control).await
        }
        C2DeliveryCase::Deadline => queued_notice_stops_at_deadline(&fixture, control).await,
        C2DeliveryCase::StalePage(settlement) => stale_recovery_page(&fixture, settlement).await,
        C2DeliveryCase::Race(competitor, order) => {
            decide_against(&fixture, control, competitor, order).await
        }
    };
    holds.borrow_mut().clear();
    fixture.link.close().await?;
    observed
}

use collaboration_central_conformance::parent_approval_scenarios::{
    ParentApprovalProof, observe_parent_proof,
};

macro_rules! proof_cases {
  ($($test:ident => $case:ident),+ $(,)?) => {
    $( #[tokio::test] async fn $test() -> Result<()> { observe_proof(ParentApprovalProof::$case).await } )+
  };
}
proof_cases! {
  held_start_and_ending_require_a_person => HeldOperations,
  restricted_answers_precede_conflicts_and_write_nothing => RestrictedAnswers,
  provider_question_returns_and_delivers_validated_answer => ProviderQuestion,
  list_continues_on_real_cursor => ListContinuation,
  session_hop_replays_without_moving_the_frontier => SessionReplay,
  ended_next_parent_retains_the_pass => EndedNextParent,
  missing_recorded_parent_at_raise_is_a_fault => MissingParentAtRaise,
  corrupt_recorded_parent_at_raise_is_a_fault => CorruptParentAtRaise,
  missing_recorded_parent_at_pass_is_a_fault => MissingParentAtPass,
  corrupt_recorded_parent_at_pass_is_a_fault => CorruptParentAtPass,
  invalid_read_fact_refuses_applying_and_recovery => InvalidReadFact,
  destination_position_gap_refuses_applying_and_recovery => PositionGap,
  forward_at_deadline_refuses_applying_and_recovery => ForwardAtDeadline,
  backwards_destination_refuses_applying_and_recovery => BackwardsDestination,
  read_after_terminal_time_refuses_applying => ReadAfterEnding,
}

async fn observe_proof(proof: ParentApprovalProof) -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = observe_parent_proof(&fixture, proof).await;
    fixture.link.close().await?;
    result
}

use central_link_protocol::approval_rpc::ApprovalHolderTarget;
use collaboration_central_conformance::approval_link_scenarios::ApprovalLinkFixture;
use collaboration_central_conformance::parent_approval_scenarios::{
    CompetingApprovalFixture, DecisionCommitOrder, DecisionCompetitor, decide_against,
};

macro_rules! races {
    ($(($test:ident, $competitor:ident, $order:ident)),+ $(,)?) => {
        $( #[tokio::test] async fn $test() -> Result<()> {
            observe_delivery(C2DeliveryCase::Race(DecisionCompetitor::$competitor, DecisionCommitOrder::$order)).await
        } )+
    }
}

races! {
    (decide_then_pass, PassOn, SessionFirst),
    (pass_then_decide, PassOn, CompetitorFirst),
    (decide_then_stop, Stop, SessionFirst),
    (stop_then_decide, Stop, CompetitorFirst),
    (decide_then_deadline, Deadline, SessionFirst),
    (deadline_then_decide, Deadline, CompetitorFirst),
    (decide_then_enrollment_end, EnrollmentEnd, SessionFirst),
    (enrollment_end_then_decide, EnrollmentEnd, CompetitorFirst),
    (decide_then_session_end, SessionEnd, SessionFirst),
    (session_end_then_decide, SessionEnd, CompetitorFirst),
}

async fn competing_target(
    fixture: &impl ApprovalLinkFixture,
    machine: &str,
) -> Result<ApprovalHolderTarget> {
    let mut target = fixture.target()?;
    let rows = fixture.read(central_storage::Statement {
        sql: "SELECT approval_id FROM login_machine_approvals WHERE machine_id=? AND state='enrolled'",
        args: vec![central_storage::SqlValue::Text(machine.to_owned())],
    }).await?;
    let [row] = rows.rows.as_slice() else {
        return Err("expected one competing enrollment".into());
    };
    let [central_storage::SqlValue::Text(enrollment)] = row.as_slice() else {
        return Err("missing competing enrollment id".into());
    };
    target.machine_id = machine.parse()?;
    target.enrollment_id = enrollment.to_owned().try_into()?;
    Ok(target)
}

impl CompetingApprovalFixture for NativeApprovalFixture {
    async fn connect_competing_machine(
        &self,
    ) -> Result<(ApprovalHolderTarget, reqwest_websocket::WebSocket)> {
        use super::super::{fixture::OTHER_MACHINE, typed_call_holder::connect_other_machine};
        let socket = connect_other_machine(&self.link).await?;
        Ok((competing_target(self, OTHER_MACHINE).await?, socket))
    }

    async fn refresh_competing_machine(&self, target: &ApprovalHolderTarget) -> Result<String> {
        use fleet_contracts::MachineTokenDecision;
        let runtime = self.link.host.central(&target.organization_id).await?;
        let request = serde_json::from_value(serde_json::json!({
            "organizationId": target.organization_id, "enrollmentId": target.enrollment_id, "clientId": target.enrollment_id,
        }))?;
        let MachineTokenDecision::Grant {
            decided_at,
            grant_expires_at,
            ..
        } = runtime.decide_machine_token(&request).await?
        else {
            return Err("competing machine renewal was refused".into());
        };
        collaboration_central_conformance::signin_fixtures::machine_token(
            target.organization_id.as_str(),
            target.machine_id.as_str(),
            target.enrollment_id.as_str(),
            decided_at.0,
            grant_expires_at.0,
        )
    }

    async fn redeliver_approval_alarm(&self) -> Result<()> {
        self.link
            .host
            .central(&self.link.organization_id)
            .await?
            .after_event()
            .await
            .map_err(|error| format!("the native approval alarm failed: {error:?}"))?;
        Ok(())
    }
}

#[tokio::test]
async fn pass_notice_reaches_another_machine_when_origin_reply_is_lost() -> Result<()> {
    observe_delivery(C2DeliveryCase::LostOtherMachine).await
}

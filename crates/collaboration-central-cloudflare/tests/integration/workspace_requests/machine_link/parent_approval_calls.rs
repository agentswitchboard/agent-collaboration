//! Each contract row runs through the workerd machine socket.
use super::{Fixture, Result};
use collaboration_central_conformance::parent_approval_scenarios::{Case, observe_case};

async fn observe(case: Case) -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let outcome = observe_case(&fixture, case).await;
    fixture.close().await?;
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
    ended_parent => EndedParent,
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
    let fixture = Fixture::with_production_dispatcher().await?;
    let runtime = fixture.runtime();
    let point = "beforeParentApprovalTransaction";
    let arm = async {
        if !runtime.control::<bool>("hold-central", &point).await? {
            return Err("C2 applying hold did not arm".into());
        }
        Ok(())
    };
    let cross_deadline = async {
        if !runtime
            .control::<bool>("until-held-central", &point)
            .await?
        {
            return Err("the parent pass did not reach applying".into());
        }
        let advance = fixture.advance(1).await;
        if !runtime.control::<bool>("release-central", &point).await? {
            return Err("C2 applying hold did not release".into());
        }
        advance
    };
    let result = pass_crosses_deadline(&fixture, arm, cross_deadline).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn fault_rolls_back_session_end_and_approval() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let outcome = collaboration_central_conformance::parent_approval_scenarios::session_end_and_approval_rollback(&fixture).await;
    fixture.close().await?;
    outcome
}

#[tokio::test]
async fn fault_rolls_back_enrollment_and_approval() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let outcome = collaboration_central_conformance::parent_approval_scenarios::enrollment_and_approval_rollback(&fixture).await;
    fixture.close().await?;
    outcome
}

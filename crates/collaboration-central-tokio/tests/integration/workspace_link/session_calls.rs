use super::fixture::{Fixture, INSTALLATION, OTHER_ORG, signed_clock};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use super::typed_calls::connect;
use central_link_protocol::{LinkCallRefusal, MachineLinkCloseCode};
use central_storage::{BatchOutcome, SqlValue};
use collaboration_central_conformance::fixture_link_reader::read_fixture_frame;
use collaboration_central_conformance::{
    ConformanceResult as Result,
    session_link_scenarios::{
        SessionLifecycleStage, create_bind_end, expect_create_refusal, expect_lapsed_session_call,
        prepare_session_call, refuse_lapsed_session_call, refuse_wrong_session_target,
        send_session_call, session_identity_records, session_mutation_records, session_records,
    },
};
use collaboration_central_tokio::test_support::start_session_link_fixture;

#[tokio::test]
async fn production_link_routes_session_lifecycle_to_tokio_organization_store() -> Result<()> {
    let fixture = start_session_link_fixture(signed_clock()?).await?;
    let observed: Result<()> = async {
        let mut socket = connect(&fixture).await?;
        let owner = fixture.host.central(&fixture.organization_id).await?;
        let organization_store = owner.runtime().statement_host();
        refuse_wrong_session_target(&mut socket, &OTHER_ORG.to_owned().try_into()?).await?;
        if !organization_store
            .read(session_records())
            .await?
            .rows
            .is_empty()
        {
            return Err("a refused call wrote a session".into());
        }
        let session = create_bind_end(&mut socket, &fixture.organization_id).await?;
        let rows = organization_store.read(session_records()).await?.rows;
        let expected = vec![vec![
            SqlValue::Text(session),
            SqlValue::Text("ended".into()),
            SqlValue::Text("typed-session-test".into()),
        ]];
        if rows != expected {
            return Err(format!("session lifecycle was not committed: {rows:?}").into());
        }
        Ok(())
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn production_link_refuses_all_session_calls_on_tokios_retained_revoked_socket() -> Result<()>
{
    for stage in [
        SessionLifecycleStage::Create,
        SessionLifecycleStage::Bind,
        SessionLifecycleStage::End,
    ] {
        let fixture = start_session_link_fixture(signed_clock()?).await?;
        let observed = check_revoked_socket(&fixture, stage).await;
        fixture.close().await?;
        observed?;
    }
    Ok(())
}

async fn check_revoked_socket(fixture: &Fixture, stage: SessionLifecycleStage) -> Result<()> {
    let mut socket = connect(fixture).await?;
    let request = prepare_session_call(&mut socket, &fixture.organization_id, stage).await?;
    let owner = fixture.host.central(&fixture.organization_id).await?;
    let organization_store = owner.runtime().statement_host();
    {
        let _turn = owner.runtime().take_central_turn().await;
        let reason = EnrollmentAtApply::OwnerRevoked
            .stored_end_reason()
            .ok_or("stored revoke reason")?;
        let ending = stored_ending_batch(INSTALLATION, fixture.enrollment_id.as_str(), reason)?;
        if !matches!(
            organization_store.run_batch(ending).await?,
            BatchOutcome::Applied { .. }
        ) {
            return Err(
                "could not revoke the enrollment while retaining the original socket".into(),
            );
        }
    }
    let before = organization_store.read(session_mutation_records()).await?;
    refuse_lapsed_session_call(&mut socket, &fixture.organization_id, request).await?;
    if organization_store
        .read(session_mutation_records())
        .await?
        .rows
        != before.rows
    {
        return Err("a revoked socket changed session records".into());
    }
    Ok(())
}

#[tokio::test]
async fn rechecks_session_authority_after_outer_restore_on_tokio() -> Result<()> {
    use fleet_runtime::PausePoint;

    for cause in [MachineLinkCloseCode::Expired, MachineLinkCloseCode::Revoked] {
        for stage in [
            SessionLifecycleStage::Create,
            SessionLifecycleStage::Bind,
            SessionLifecycleStage::End,
        ] {
            let fixture = start_session_link_fixture(signed_clock()?).await?;
            let observed: Result<()> = async {
                let mut socket = connect(&fixture).await?;
                let request = prepare_session_call(&mut socket, &fixture.organization_id, stage).await?;
                let owner = fixture.host.central(&fixture.organization_id).await?;
                let store = owner.runtime().statement_host();
                let before = store.read(session_mutation_records()).await?;
                let mut hold = owner.runtime().pause_points().hold_central(PausePoint::BeforeSessionTransaction);
                let correlation = send_session_call(&mut socket, &fixture.organization_id, request).await?;
                tokio::select! {
                    () = hold.until_held() => {},
                    _ = read_fixture_frame(&mut socket, "session calls link frame")=> return Err("call replied before the transaction barrier".into()),
                }
                if cause == MachineLinkCloseCode::Expired {
                    fixture.clock.advance(std::time::Duration::from_secs(600))?;
                } else {
                    // Retain the physical socket so the refusal is observable.
                    let reason = EnrollmentAtApply::OwnerRevoked.stored_end_reason().ok_or("stored revoke reason")?;
                    let ending = stored_ending_batch(INSTALLATION, fixture.enrollment_id.as_str(), reason)?;
                    if !matches!(store.run_batch(ending).await?, BatchOutcome::Applied { .. }) {
                        return Err("held-call enrollment ending did not commit".into());
                    }
                }
                hold.release();
                expect_lapsed_session_call(&mut socket, &correlation, cause).await?;
                if store.read(session_mutation_records()).await?.rows != before.rows {
                    return Err("an authority-lapsed call changed the session store".into());
                }
                Ok(())
            }.await;
            fixture.close().await?;
            observed?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn tokio_central_fault_rolls_back_create_and_keeps_the_link_open() -> Result<()> {
    let fixture = start_session_link_fixture(signed_clock()?).await?;
    let observed: Result<()> = async {
        let mut socket = connect(&fixture).await?;
        let owner = fixture.host.central(&fixture.organization_id).await?;
        let store = owner.runtime().statement_host();
        let sessions = store.read(session_mutation_records()).await?;
        let identities = store.read(session_identity_records()).await?;
        store.fail_next_reserved_session_reply();
        expect_create_refusal(
            &mut socket,
            &fixture.organization_id,
            LinkCallRefusal::CentralFault,
        )
        .await?;
        if store.read(session_mutation_records()).await?.rows != sessions.rows
            || store.read(session_identity_records()).await?.rows != identities.rows
        {
            return Err("CentralFault left session or actor rows behind".into());
        }
        create_bind_end(&mut socket, &fixture.organization_id).await?;
        Ok(())
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn tokio_wrong_target_refuses_before_the_failing_lender() -> Result<()> {
    use reqwest_websocket::Message;
    let fixture = start_session_link_fixture(signed_clock()?).await?;
    let observed: Result<()> = async {
        let mut socket = connect(&fixture).await?;
        let owner = fixture.host.central(&fixture.organization_id).await?;
        owner.runtime().statement_host().fail_next_session_loan();
        expect_create_refusal(&mut socket, &OTHER_ORG.to_owned().try_into()?, LinkCallRefusal::WrongTarget).await?;
        send_session_call(&mut socket, &fixture.organization_id, collaboration_central_conformance::session_link_scenarios::SessionLinkCall::Create).await?;
        if !matches!(read_fixture_frame(&mut socket, "session calls link frame").await?, Some(Ok(Message::Close { code, .. })) if u16::from(code) == 1011) {
            return Err("matching target did not reach the still-armed transaction fault".into());
        }
        Ok(())
    }.await;
    fixture.close().await?;
    observed
}

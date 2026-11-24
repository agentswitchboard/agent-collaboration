use super::fixture::{ConnectedLink, Fixture, MACHINE_ID, OTHER_ORGANIZATION};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
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

#[tokio::test]
async fn production_link_routes_session_lifecycle_to_workerd_organization_store() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed: Result<()> = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let organization_store = fixture.runtime();
        refuse_wrong_session_target(&mut link.socket, &OTHER_ORGANIZATION.to_owned().try_into()?)
            .await?;
        if !organization_store
            .read(session_records())
            .await?
            .rows
            .is_empty()
        {
            return Err("a refused call wrote a session".into());
        }
        let session = create_bind_end(&mut link.socket, &fixture.organization).await?;
        let rows = organization_store.read(session_records()).await?.rows;
        let expected = vec![vec![
            SqlValue::Text(session),
            SqlValue::Text("ended".into()),
            SqlValue::Text("typed-session-test".into()),
        ]];
        if rows != expected {
            return Err(format!("session lifecycle was not committed: {rows:?}").into());
        }
        link.close().await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn production_link_refuses_all_session_calls_on_workerds_retained_revoked_socket()
-> Result<()> {
    for stage in [
        SessionLifecycleStage::Create,
        SessionLifecycleStage::Bind,
        SessionLifecycleStage::End,
    ] {
        let fixture = Fixture::with_production_dispatcher().await?;
        let observed = check_revoked_socket(&fixture, stage).await;
        fixture.close().await?;
        observed?;
    }
    Ok(())
}

async fn check_revoked_socket(fixture: &Fixture, stage: SessionLifecycleStage) -> Result<()> {
    let mut link = ConnectedLink::open(fixture).await?;
    let request = prepare_session_call(&mut link.socket, &fixture.organization, stage).await?;
    let organization_store = fixture.runtime();
    let reason = EnrollmentAtApply::OwnerRevoked
        .stored_end_reason()
        .ok_or("stored revoke reason")?;
    let ending = stored_ending_batch(MACHINE_ID, fixture.enrollment.as_str(), reason)?;
    if !matches!(
        organization_store.run_batch(ending).await?,
        BatchOutcome::Applied { .. }
    ) {
        return Err("could not revoke the enrollment while retaining the original socket".into());
    }
    let before = organization_store.read(session_mutation_records()).await?;
    refuse_lapsed_session_call(&mut link.socket, &fixture.organization, request).await?;
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
async fn rechecks_session_authority_after_outer_restore_on_workerd() -> Result<()> {
    for cause in [MachineLinkCloseCode::Expired, MachineLinkCloseCode::Revoked] {
        for stage in [
            SessionLifecycleStage::Create,
            SessionLifecycleStage::Bind,
            SessionLifecycleStage::End,
        ] {
            let fixture = Fixture::with_production_dispatcher().await?;
            let observed: Result<()> = async {
                let mut link = ConnectedLink::open(&fixture).await?;
                let request = prepare_session_call(&mut link.socket, &fixture.organization, stage).await?;
                let runtime = fixture.runtime();
                let before = runtime.read(session_mutation_records()).await?;
                let point = "beforeSessionTransaction";
                if !runtime.control::<bool>("hold-central", &point).await? {
                    return Err("could not arm the applying barrier".into());
                }
                let correlation = send_session_call(&mut link.socket, &fixture.organization, request).await?;
                tokio::select! {
                    held = runtime.control::<bool>("until-held-central", &point) => {
                        if !held? { return Err("call did not enter the applying barrier".into()); }
                    }
                    _ = read_fixture_frame(&mut link.socket, "session calls link frame")=> return Err("call replied before the applying barrier".into()),
                }
                let applying: Result<()> = async {
                    if cause == MachineLinkCloseCode::Expired {
                        super::renewal::advance(&fixture, 600).await?;
                    } else {
                        let reason = EnrollmentAtApply::OwnerRevoked.stored_end_reason().ok_or("stored revoke reason")?;
                        let ending = stored_ending_batch(MACHINE_ID, fixture.enrollment.as_str(), reason)?;
                        if !matches!(runtime.run_batch(ending).await?, BatchOutcome::Applied { .. }) {
                            return Err("held-call enrollment ending did not commit".into());
                        }
                    }
                    Ok(())
                }.await;
                let released = runtime.control::<bool>("release-central", &point).await?;
                applying?;
                if !released {
                    return Err("applying barrier did not release".into());
                }
                expect_lapsed_session_call(&mut link.socket, &correlation, cause).await?;
                if runtime.read(session_mutation_records()).await?.rows != before.rows {
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
async fn workerd_central_fault_rolls_back_create_and_keeps_the_link_open() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed: Result<()> = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let store = fixture.runtime();
        let sessions = store.read(session_mutation_records()).await?;
        let identities = store.read(session_identity_records()).await?;
        if !store
            .control::<bool>("fail-reserved-session-reply", &true)
            .await?
        {
            return Err("could not arm the post-write reply fault".into());
        }
        expect_create_refusal(
            &mut link.socket,
            &fixture.organization,
            LinkCallRefusal::CentralFault,
        )
        .await?;
        if store.read(session_mutation_records()).await?.rows != sessions.rows
            || store.read(session_identity_records()).await?.rows != identities.rows
        {
            return Err("CentralFault left session or actor rows behind".into());
        }
        create_bind_end(&mut link.socket, &fixture.organization).await?;
        link.close().await
    }
    .await;
    fixture.close().await?;
    observed
}

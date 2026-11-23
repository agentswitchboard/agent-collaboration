use super::fixture::{ConnectedLink, Fixture, MACHINE_ID, OTHER_ORGANIZATION};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use central_storage::{BatchOutcome, SqlValue};
use collaboration_central_conformance::{
    ConformanceResult as Result,
    session_link_scenarios::{
        SessionLifecycleStage, create_bind_end, prepare_session_call, refuse_lapsed_session_call,
        refuse_wrong_target_and_unowned_calls, session_mutation_records, session_records,
    },
};

#[tokio::test]
async fn production_link_routes_session_lifecycle_to_workerd_organization_store() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let organization_store = fixture.runtime();
        refuse_wrong_target_and_unowned_calls(
            &mut link.socket,
            &fixture.organization,
            &OTHER_ORGANIZATION.to_owned().try_into()?,
        )
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

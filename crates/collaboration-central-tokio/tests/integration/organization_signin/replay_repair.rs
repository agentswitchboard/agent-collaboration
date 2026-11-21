use super::support::*;
use central_storage::StoreFailure;
use fleet_contracts::ProvisionFailure;
use fleet_contracts::ProvisionOrganization;
#[expect(
    clippy::panic_in_result_fn,
    reason = "A cancelled real commit followed by a failed alarm reconciliation must preserve retry classification and exact stored effects."
)]
#[tokio::test]
async fn known_provisioning_commit_reconciles_on_same_host_and_failed_repair_is_uncertain() -> Result
{
    let f = Fixture::new().await?;
    f.seed_machine().await?;
    // Pending durable work predates provisioning; C2 correctly cannot authenticate an unstamped org.
    let (_wake_id, due) = f.seed_pending_wake("provision-repair").await?;
    let runtime = f.runtime(A).await?;
    assert_eq!(runtime.runtime().statement_host().alarm(), None);
    let mut held = runtime.runtime().statement_host().hold_alarm_publication();
    let statement = signed::stamp(A, signed::NOW_SECONDS)?;
    let request = ProvisionOrganization {
        organization_id: org(A)?,
        organization_statement: statement.clone().try_into()?,
    };
    let task_runtime = runtime.clone();
    let task_request = request.clone();
    let pending =
        tokio::spawn(async move { task_runtime.provision_organization(&task_request).await });
    held.until_held().await;
    let committed = f.snapshot().await?;
    assert_eq!((committed.0.len(), committed.1.len()), (1, 1));
    assert_eq!(runtime.runtime().statement_host().alarm(), None);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(held);
    runtime
        .runtime()
        .statement_host()
        .fail_next_alarm_batch(StoreFailure::Unavailable);
    assert_eq!(
        runtime.provision_organization(&request).await,
        Err(ProvisionFailure::OutcomeUnknown)
    );
    assert_eq!(runtime.runtime().statement_host().alarm(), None);
    assert!(
        f.snapshot().await? == committed,
        "uncertain repair must not duplicate the known stamp/record"
    );
    assert_eq!(f.provision(A, &statement).await?.status, 200);
    assert_eq!(
        runtime
            .runtime()
            .statement_host()
            .alarm()
            .map(|at| at.unix_millis()),
        Some(due.timestamp_millis())
    );
    assert!(
        f.snapshot().await? == committed,
        "same-host reconciliation returns the durable first outcome"
    );
    f.close().await
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "The native driver must recover an observed publication fault without rewriting the committed stamp."
)]
#[tokio::test]
async fn native_stamp_commit_survives_publication_failure_and_the_real_driver_retries() -> Result {
    use central_storage::AlarmPublicationFailure;
    use fleet_runtime::PausePoint;

    let mut fixture = Fixture::new().await?;
    fixture.seed_machine().await?;
    let (wake, due) = fixture.seed_pending_wake("stamp-publication-retry").await?;
    let statement = signed::stamp(A, signed::NOW_SECONDS)?;
    let owner = fixture.runtime(A).await?;
    let runtime = owner.runtime();
    let store = runtime.statement_host();
    let completed = runtime.alarm_completion_count();
    assert_eq!(store.alarm(), None);
    store.fail_next_alarm_publication(AlarmPublicationFailure::NoLaterGridMark);
    let mut hook = runtime
        .pause_points()
        .hold_central(PausePoint::EngineCompletionEntered);
    let committed = {
        let primary = fixture.provision(A, &statement);
        tokio::pin!(primary);
        tokio::select! {
            () = hook.until_held() => {}
            response = &mut primary => panic!("provision replied before its completion hook: {:?}", response.map(|reply| reply.status)),
        }

        assert_eq!(
            store.take_alarm_publication_failures(),
            [AlarmPublicationFailure::NoLaterGridMark]
        );
        assert!(store.failed_alarm_publication_at().is_some());
        assert_eq!(store.alarm(), None);
        let committed = fixture.snapshot().await?;
        assert_eq!((committed.0.len(), committed.1.len()), (1, 1));
        // Keep the primary HTTP answer independent of the second publication.
        // With publication still failed, the real driver must use its next grid.
        store.fail_next_alarm_batch(StoreFailure::Unavailable);
        hook.release();
        let reply = primary.await?;
        assert_eq!(
            (reply.status, reply.body.as_slice()),
            (200, br#"{"result":"provisioned"}"#.as_slice())
        );
        committed
    };

    let wake_rows = "SELECT state FROM automation_wakes WHERE wake_id=?";
    fixture
        .clock
        .advance_to(runtime_primitives::SyncTime::from_unix_millis(
            due.timestamp_millis() - 1,
        ))
        .await?;
    assert_eq!(runtime.alarm_completion_count(), completed);
    assert_eq!(
        fixture.rows(A, wake_rows, vec![text(&wake)]).await?,
        vec![vec![text("active")]]
    );
    fixture
        .clock
        .advance(std::time::Duration::from_millis(1))
        .await?;
    runtime.until_alarm_completed(completed + 1).await;
    assert_eq!(
        fixture.rows(A, wake_rows, vec![text(&wake)]).await?,
        vec![vec![text("done")]]
    );
    assert_eq!(
        fixture
            .rows(
                A,
                "SELECT message_kind FROM mail_messages WHERE message_kind='wake'",
                vec![]
            )
            .await?,
        vec![vec![text("wake")]]
    );
    assert!(store.failed_alarm_publication_at().is_none());
    drop(owner);

    fixture.reopen().await?;
    let replay = fixture.provision(A, &statement).await?;
    assert_eq!(
        (replay.status, replay.body.as_slice()),
        (200, br#"{"result":"provisioned"}"#.as_slice())
    );
    assert_eq!(fixture.snapshot().await?, committed);
    assert_eq!(
        fixture
            .rows(
                A,
                "SELECT message_kind FROM mail_messages WHERE message_kind='wake'",
                vec![]
            )
            .await?,
        vec![vec![text("wake")]]
    );
    fixture.close().await
}

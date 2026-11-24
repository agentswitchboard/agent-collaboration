use approval_request_protocol::{RaiseReply, StopReply};
use central_engine_model::{CENTRAL_RECORD_RETENTION_MILLIS, LAST_ALARM_GRID_UNIX_MILLIS};
use fleet_approvals::ApprovalHolderCommand;

use super::{
    approval_store_cases::{fixture_with_owner, raise_and_commit},
    fixtures::{key, provider, provider_stop, time},
};

#[tokio::test]
async fn admitted_endings_saturate_retention_at_the_last_grid_mark() {
    let boundary = LAST_ALARM_GRID_UNIX_MILLIS - CENTRAL_RECORD_RETENTION_MILLIS;
    let (fixture, holder, requester) = fixture_with_owner().await;
    fixture.clock.set_millis(boundary - 2);
    let mut requests = Vec::new();
    for ordinal in 1..=3 {
        let request = provider(&key(&holder.run, ordinal), &requester, "boundary");
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &request).await.0,
            RaiseReply::Receipt { .. }
        ));
        requests.push(request);
    }
    for (request, ended) in requests.iter().zip([boundary - 1, boundary, boundary + 1]) {
        fixture.clock.set_millis(ended);
        let (reply, _) = fixture
            .apply(None, async |store| {
                store
                    .apply_holder_command(
                        &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                        ApprovalHolderCommand::Stop(provider_stop(request)),
                    )
                    .await
            })
            .await;
        assert!(matches!(
            reply.unwrap().reply,
            fleet_approvals::ApprovalHolderReply::Stopped(StopReply::Stands { .. })
        ));
    }
    let stored = fixture
        .inspect_rows(
            "SELECT CAST(erase_after_ms AS TEXT) FROM approval_requests ORDER BY request_id",
        )
        .await;
    assert_eq!(
        stored,
        [
            LAST_ALARM_GRID_UNIX_MILLIS - 1,
            LAST_ALARM_GRID_UNIX_MILLIS,
            LAST_ALARM_GRID_UNIX_MILLIS
        ]
        .map(|time| time.to_string())
    );
    assert!(
        fixture
            .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS - 1), 10)
            .await
            .is_empty()
    );
    let work = fixture
        .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 10)
        .await;
    assert_eq!(work.len(), 3);
    assert!(
        central_engine_model::next_five_minute_mark(time(LAST_ALARM_GRID_UNIX_MILLIS)).is_err()
    );
    assert!(
        work.iter()
            .all(|row| row.effective_due_ms == LAST_ALARM_GRID_UNIX_MILLIS)
    );
}

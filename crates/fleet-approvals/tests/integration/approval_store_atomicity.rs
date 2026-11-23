use approval_request_protocol::{
    DeliverEndingReply, PassOnReply, RaiseReply, RunRefusal, StopReply,
};
use central_storage::StoreFailure;
use fleet_approvals::{
    ApprovalApplyFailure, ApprovalHolderCommand, ApprovalHolderReply, ApprovalHolderScope,
    ApprovalRun, FactorStanding, MAX_OUTSTANDING_PER_RUN,
};

use super::{
    approval_store_cases::{
        decide_input, first_holder, fixture_with_owner, raise_and_commit, register, snapshot,
    },
    fixtures::{HOUR, key, person, provider, provider_stop, router_run, session, time},
    native_approval_store::NativeApprovalStoreFixture,
};

#[tokio::test]
async fn first_raise_forward_decide_delivery_and_run_replacement_roll_back_at_every_write() {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    register(&fixture, &holder).await;
    let requester = session(&holder.run.enrollment, 0xa130);
    let parent = session(&holder.run.enrollment, 0xa131);
    fixture
        .store_session(&holder.organization_id, &parent, None, true)
        .await;
    fixture
        .store_session(&holder.organization_id, &requester, Some(&parent), true)
        .await;
    let request = key(&holder.run, 1);
    let raise = provider(&request, &requester, "Rollback");
    assert_rollback_each_write(&fixture, 3, async |store| {
        store
            .apply_holder_command(
                &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                ApprovalHolderCommand::Raise(Box::new(raise.clone())),
            )
            .await
    })
    .await;
    assert_rollback_each_write(&fixture, 2, async |store| {
        store
            .fixture_pass_on_approval(&holder.organization_id, parent.clone(), &request.request_id)
            .await
    })
    .await;
    let repeat_pass = fixture
        .apply(None, async |store| {
            store
                .fixture_pass_on_approval(
                    &holder.organization_id,
                    parent.clone(),
                    &request.request_id,
                )
                .await
        })
        .await;
    assert_eq!(repeat_pass.0.unwrap().reply, PassOnReply::Forwarded {});
    assert!(repeat_pass.1.sql_writes.is_empty());
    let decided = assert_rollback_each_write(&fixture, 4, async |store| {
        store
            .fixture_decide_approval(
                &holder.organization_id,
                person(11, FactorStanding::Fresh),
                decide_input(&request.request_id, 101, 1),
            )
            .await
    })
    .await;
    let offer = decided.ending_offers.into_iter().next().unwrap();
    assert_rollback_each_write(&fixture, 1, async |store| {
        store
            .apply_holder_command(
                &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                ApprovalHolderCommand::DeliveryReply {
                    request: offer.request.clone(),
                    reply: DeliverEndingReply::Received {},
                },
            )
            .await
    })
    .await;

    let marked_request = key(&holder.run, 2);
    let unseen = provider(&marked_request, &requester, "Before raise");
    assert_rollback_each_write(&fixture, 1, async |store| {
        store
            .apply_holder_command(
                &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                ApprovalHolderCommand::Stop(provider_stop(&unseen)),
            )
            .await
    })
    .await;
    let stop_replay = fixture
        .apply(None, async |store| {
            store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                    ApprovalHolderCommand::Stop(provider_stop(&unseen)),
                )
                .await
        })
        .await;
    assert!(matches!(
        stop_replay.0.unwrap().reply,
        ApprovalHolderReply::Stopped(StopReply::Stands { .. })
    ));
    assert!(stop_replay.1.sql_writes.is_empty());

    let saved = provider(&key(&holder.run, 3), &requester, "Ends with run");
    raise_and_commit(&fixture, &holder, &saved).await;
    let replacement = ApprovalHolderScope {
        run: ApprovalRun {
            router_run: router_run(0xa132),
            ..holder.run.clone()
        },
        organization_id: holder.organization_id.clone(),
    };
    assert_rollback_each_write(&fixture, 4, async |store| {
        store
            .register_approval_run(
                &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&replacement),
            )
            .await
    })
    .await;
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &saved).await;
    assert_eq!(
        reply,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunRetired {}
        }
    );
    assert!(evidence.sql_writes.is_empty() && evidence.session_route_reads.is_empty());
}

#[tokio::test]
async fn retirement_at_capacity_commits_the_fence_endings_and_due_removals_together() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    for ordinal in 1..=u16::try_from(MAX_OUTSTANDING_PER_RUN).unwrap() {
        let raise = provider(&key(&holder.run, ordinal), &requester, "Outstanding");
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &raise).await.0,
            RaiseReply::Receipt { .. }
        ));
    }
    let over = provider(&key(&holder.run, 257), &requester, "Over capacity");
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &over).await;
    assert_eq!(reply, RaiseReply::TooManyOutstanding {});
    assert!(evidence.sql_writes.is_empty());

    let (retired, evidence) = fixture
        .apply(None, async |store| {
            store
                .retire_approval_run(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                )
                .await
        })
        .await;
    assert_eq!(retired.unwrap().reply, Ok(()));
    assert_eq!(evidence.sql_writes.len(), 1 + 2 * MAX_OUTSTANDING_PER_RUN);
    assert_eq!(
        evidence
            .sql_writes
            .iter()
            .filter(|(sql, _)| sql.contains("UPDATE approval_router_runs"))
            .count(),
        1
    );
    println!(
        "retirement: one transaction, {} guarded affected rows at cap {}",
        evidence.sql_writes.len(),
        MAX_OUTSTANDING_PER_RUN
    );
    assert_eq!(
        fixture.inspect_rows("SELECT CAST(COUNT(*) AS TEXT) FROM approval_requests WHERE record_state='ended' AND delivery_state='closed_without_receipt'").await,
        ["256"],
    );
    assert!(fixture.due_rows(time(HOUR), 300).await.is_empty());
}

#[tokio::test]
async fn session_end_enrollment_end_alarm_and_waiting_stop_roll_back_after_each_write() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let stop = provider(&key(&holder.run, 1), &requester, "Waiting stop");
    let expire = provider(&key(&holder.run, 2), &requester, "Alarm expiry");
    let cancel = provider(&key(&holder.run, 3), &requester, "Session ending");
    let other_session = session(&holder.run.enrollment, 0xa155);
    fixture
        .store_session(&holder.organization_id, &other_session, None, true)
        .await;
    let enrolled = provider(&key(&holder.run, 4), &other_session, "Enrollment ending");
    for raise in [&stop, &expire, &cancel, &enrolled] {
        assert!(matches!(
            raise_and_commit(&fixture, &holder, raise).await.0,
            RaiseReply::Receipt { .. }
        ));
    }
    assert_rollback_each_write(&fixture, 2, async |store| {
        store
            .apply_holder_command(
                &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                ApprovalHolderCommand::Stop(provider_stop(&stop)),
            )
            .await
    })
    .await;
    let due = fixture.due_rows(time(HOUR), 1).await;
    fixture.clock.set_millis(HOUR);
    let timed_out = assert_rollback_each_write(&fixture, 2, async |store| {
        store.apply_approval_deadline(&due[0].key, time(HOUR)).await
    })
    .await;
    assert_eq!(
        timed_out.ending_offers[0].request.request_id(),
        expire.request_id()
    );
    fixture.clock.set_millis(2);
    let premature_session_end = fixture
        .apply(None, async |store| {
            store
                .end_approval_session(&holder.organization_id, &requester)
                .await
        })
        .await;
    assert!(premature_session_end.0.unwrap().ending_offers.is_empty());
    assert!(premature_session_end.1.sql_writes.is_empty());

    let scope = fleet_approvals::ApprovalEnrollmentScope {
        organization_id: holder.organization_id.clone(),
        enrollment: holder.run.enrollment.clone(),
    };
    let premature_enrollment_end = fixture
        .apply(None, async |store| {
            store.end_approval_enrollment(&scope).await
        })
        .await;
    premature_enrollment_end.0.unwrap();
    assert!(premature_enrollment_end.1.sql_writes.is_empty());

    fixture
        .store_session(&holder.organization_id, &requester, None, false)
        .await;
    let cancelled = assert_rollback_each_write(&fixture, 2, async |store| {
        store
            .end_approval_session(&holder.organization_id, &requester)
            .await
    })
    .await;
    assert_eq!(
        cancelled.ending_offers[0].request.request_id(),
        cancel.request_id()
    );
    fixture.store_enrollment(&holder, "ended").await;
    // Two previously ended Owed rows close delivery, the last waiting row ends,
    // its due entry clears, and the run's fence retires.
    assert_rollback_each_write(&fixture, 5, async |store| {
        store.end_approval_enrollment(&scope).await
    })
    .await;
    assert!(fixture.due_rows(time(HOUR), 10).await.is_empty());
}

#[tokio::test]
async fn the_real_engine_lookup_has_constant_sqlite_work_with_twenty_and_ten_thousand_waiting() {
    let mut measured = Vec::new();
    for waiting in [20, 10_000] {
        let (fixture, holder, requester) = fixture_with_owner().await;
        let due = provider(&key(&holder.run, 1), &requester, "Indexed deadline");
        raise_and_commit(&fixture, &holder, &due).await;
        fixture.seed_due_population(waiting).await;
        assert_eq!(
            fixture.inspect_rows("SELECT CAST(COUNT(*) AS TEXT) FROM approval_requests WHERE record_state='waiting'").await,
            [waiting.to_string()],
        );
        let (rows, steps) = fixture.due_rows_with_progress(time(HOUR)).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].occurrence_ms, HOUR);
        println!("{waiting} waiting approvals, {steps} SQLite VM progress callbacks");
        measured.push(steps);
    }
    assert_eq!(measured[0], measured[1]);
    assert!((1..=150).contains(&measured[0]), "{measured:?}");
}

#[tokio::test]
async fn the_engine_diagnostic_includes_the_approval_owner_and_alarm_contribution() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    fixture
        .alter_fixture(
            "INSERT INTO signin_organization_identity
         VALUES ('019ac19f-aa00-7000-8000-00000000a100','fixture',1,1)",
        )
        .await;
    let due = provider(
        &key(&holder.run, 1),
        &requester,
        "Diagnostic approval deadline",
    );
    raise_and_commit(&fixture, &holder, &due).await;
    let comparison = fixture.compare_due_table(time(0), time(HOUR)).await;
    assert!(comparison.is_empty(), "{comparison:?}");

    fixture
        .alter_fixture("DELETE FROM engine_due_entries WHERE work_kind='approval_request'")
        .await;
    let comparison = fixture.compare_due_table(time(0), time(HOUR)).await;
    assert!(
        comparison.iter().any(|difference| matches!(
            difference,
            fleet_engine::DueTableDifference::WrongAlarmInputs { reference, projected }
                if reference.earliest_on_grid_due_unix_millis == Some(HOUR)
                    && projected.earliest_on_grid_due_unix_millis.is_none()
        )),
        "{comparison:?}"
    );
}
async fn assert_rollback_each_write<Reply>(
    fixture: &NativeApprovalStoreFixture,
    expected_writes: usize,
    apply: impl AsyncFn(
        &mut fleet_approvals::ApprovalApplyingStore<
            '_,
            super::native_approval_store::NativeApprovalTransaction,
            super::native_approval_store::StoredSessionFacts,
        >,
    )
        -> Result<fleet_approvals::PendingApprovalReply<Reply>, ApprovalApplyFailure>,
) -> super::native_approval_store::FixtureCommittedApply<Reply> {
    let before = snapshot(fixture).await;
    for ordinal in 1..=expected_writes {
        let (failed, evidence) = fixture.apply(Some(ordinal), &apply).await;
        assert!(matches!(
            failed,
            Err(ApprovalApplyFailure::Store(StoreFailure::Unavailable))
        ));
        assert_eq!(evidence.sql_writes.len(), ordinal);
        assert_eq!(
            snapshot(fixture).await,
            before,
            "rollback after write {ordinal}"
        );
    }
    let (committed, evidence) = fixture.apply(None, apply).await;
    assert_eq!(evidence.sql_writes.len(), expected_writes);
    committed.unwrap()
}

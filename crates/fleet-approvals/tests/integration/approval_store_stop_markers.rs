use approval_request_protocol::{
    CallOffer, CallStopCause, CancelReason, DeliverEndingRequest, HolderStopCause,
    InvocationDigest, ParkedCallHolder, ProviderStopCause, Raise, RaiseReply, SessionCallOperation,
    StopReceiptEnding, StopReply, StopRequest,
};
use central_storage::{CorruptReason, StoreFailure};
use fleet_approvals::{
    ApprovalApplyFailure, ApprovalHolderAdmission, ApprovalHolderCommand, ApprovalHolderReply,
    ApprovalHolderScope, ApprovalSession,
};

use super::{
    approval_store_cases::{fixture_with_owner, raise_and_commit},
    fixtures::{key, provider},
    native_approval_store::{
        ApprovalTransactionEvidence, FixtureCommittedApply, NativeApprovalStoreFixture,
    },
};

#[tokio::test]
async fn stop_markers_keep_the_family_of_a_shared_cause_across_reopens() {
    let (fixture, holder_scope, requesting_session) = fixture_with_owner().await;
    let [provider_raise, parked_raise] = refusal_raises(&holder_scope, &requesting_session);
    let cancelled = StopReply::Stands {
        ending: StopReceiptEnding::Cancelled {
            reason: CancelReason::Stopped {
                cause: HolderStopCause::HolderRefusedLocally,
            },
        },
    };

    for (raise, is_provider) in [(&provider_raise, true), (&parked_raise, false)] {
        let first_stop = local_refusal(raise, is_provider);
        let (first_reply, first_evidence) =
            apply_stop(&fixture, &holder_scope, first_stop.clone()).await;
        assert_eq!(
            first_reply.unwrap().reply,
            ApprovalHolderReply::Stopped(cancelled.clone())
        );
        assert_eq!(first_evidence.sql_writes.len(), 1);

        let (other_reply, other_evidence) =
            apply_stop(&fixture, &holder_scope, local_refusal(raise, !is_provider)).await;
        assert_eq!(
            other_reply.unwrap().reply,
            ApprovalHolderReply::Stopped(StopReply::NotVisible {})
        );
        assert!(other_evidence.sql_writes.is_empty());

        let (replay_reply, replay_evidence) = apply_stop(&fixture, &holder_scope, first_stop).await;
        assert_eq!(
            replay_reply.unwrap().reply,
            ApprovalHolderReply::Stopped(cancelled.clone())
        );
        assert!(replay_evidence.sql_writes.is_empty());

        let (raise_reply, raise_evidence) = raise_and_commit(&fixture, &holder_scope, raise).await;
        assert_eq!(raise_reply, RaiseReply::AlreadyStopped {});
        assert!(raise_evidence.sql_writes.is_empty());
    }

    assert_eq!(
        fixture
            .inspect_rows(
                "SELECT stop_family || ':' || stop_cause FROM approval_requests ORDER BY request_id"
            )
            .await,
        [
            "provider:holder_refused_locally",
            "unproven_call:holder_refused_locally"
        ],
    );

    for (raise, is_provider, corruption) in [
        (
            &provider_raise,
            true,
            "UPDATE approval_requests SET stop_cause='call_withdrawn' WHERE stop_family='provider'",
        ),
        (
            &parked_raise,
            false,
            "UPDATE approval_requests SET stop_cause='ask_replaced' WHERE stop_family='unproven_call'",
        ),
    ] {
        fixture.alter_fixture(corruption).await;
        let (reply, evidence) =
            apply_stop(&fixture, &holder_scope, local_refusal(raise, is_provider)).await;
        assert!(matches!(
            reply,
            Err(ApprovalApplyFailure::Store(StoreFailure::Corrupt {
                reason: CorruptReason::BadValue { ref column },
            })) if column == "approval_requests.stop_cause"
        ));
        assert!(evidence.sql_writes.is_empty());
    }
}

#[tokio::test]
async fn stop_after_raise_persists_the_typed_local_refusal_and_replays_it() {
    let (fixture, holder_scope, requesting_session) = fixture_with_owner().await;
    let [provider_raise, parked_raise] = refusal_raises(&holder_scope, &requesting_session);
    let mut expected_endings = Vec::new();
    for (raise, is_provider) in [(&provider_raise, true), (&parked_raise, false)] {
        let (raise_reply, _) = raise_and_commit(&fixture, &holder_scope, raise).await;
        assert!(matches!(raise_reply, RaiseReply::Receipt { .. }));

        let stop_request = local_refusal(raise, is_provider);
        let (stop_reply, stop_evidence) =
            apply_stop(&fixture, &holder_scope, stop_request.clone()).await;
        let saved_reply = stop_reply.unwrap().reply;
        assert_eq!(
            saved_reply,
            ApprovalHolderReply::Stopped(StopReply::Stands {
                ending: StopReceiptEnding::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: HolderStopCause::HolderRefusedLocally
                    },
                },
            })
        );
        assert!(!stop_evidence.sql_writes.is_empty());

        let (replay_reply, replay_evidence) =
            apply_stop(&fixture, &holder_scope, stop_request.clone()).await;
        assert_eq!(replay_reply.unwrap().reply, saved_reply);
        assert!(replay_evidence.sql_writes.is_empty());
        expected_endings.push(DeliverEndingRequest::from_stop(raise, &stop_request).unwrap());
    }

    let stored_endings: Vec<DeliverEndingRequest> = fixture
        .inspect_rows("SELECT ending_json FROM approval_requests ORDER BY request_id")
        .await
        .into_iter()
        .map(|stored| serde_json::from_str(&stored).unwrap())
        .collect();
    assert_eq!(stored_endings, expected_endings);
    assert_eq!(
        fixture.inspect_rows(
            "SELECT delivery_state FROM approval_requests WHERE stop_family IS NULL AND stop_cause IS NULL ORDER BY request_id"
        ).await,
        ["closed_without_receipt", "closed_without_receipt"],
    );
}

fn refusal_raises(
    holder_scope: &ApprovalHolderScope,
    requesting_session: &ApprovalSession,
) -> [Raise; 2] {
    let provider_raise = provider(
        &key(&holder_scope.run, 1),
        requesting_session,
        "Provider request",
    );
    let parked_raise = Raise::UnprovenCall {
        request_id: key(&holder_scope.run, 2).request_id,
        holder: ParkedCallHolder {
            router_run: holder_scope.run.router_run.clone(),
        },
        invocation: InvocationDigest::from_bound_bytes(b"held end request"),
        offer: CallOffer {
            operation: SessionCallOperation::EndSessionForGood {
                session_id: requesting_session.session_id.clone(),
            },
        },
    };
    [provider_raise, parked_raise]
}

fn local_refusal(raise: &Raise, provider_family: bool) -> StopRequest {
    let request_id = raise.request_id().clone();
    let router_run = raise.router_run().clone();
    let contents_digest = raise.contents_digest().unwrap();
    if provider_family {
        StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            cause: ProviderStopCause::HolderRefusedLocally,
        }
    } else {
        StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            cause: CallStopCause::HolderRefusedLocally,
        }
    }
}

async fn apply_stop(
    fixture: &NativeApprovalStoreFixture,
    holder_scope: &ApprovalHolderScope,
    stop_request: StopRequest,
) -> (
    Result<FixtureCommittedApply<ApprovalHolderReply>, ApprovalApplyFailure>,
    ApprovalTransactionEvidence,
) {
    fixture
        .apply(None, async |store| {
            store
                .apply_holder_command(
                    &ApprovalHolderAdmission::for_fixture_scope(holder_scope),
                    ApprovalHolderCommand::Stop(stop_request),
                )
                .await
        })
        .await
}

use super::{
    OutboxItemState, OutboxRowEffect, OutboxRowEvent, OutboxRowMachine, OutboxRowRefusal,
    OutboxRowState,
};
use crate::{
    LocalOutboxRefusal, MacOutbox, MachineLocalStore, NewOutboxItem, OutboxAccepted,
    OutboxCancellation::{NotConfirmedWithin15Minutes, NotSentWithin15Minutes},
    OutboxInsert, OutboxQueuedWrite, OutboxRefusal, OutboxRequestStatus, OutboxStoreError,
    OutboxWriteOutcome, PendingWriteState, StoredSenderOutcome,
    outbox_deadline::expires_at_ms,
    outbox_settlement::SettledWrite,
    outbox_store::{
        ANSWERED_RETENTION_MILLIS,
        outbox_row_state::OutboxPhase,
        outbox_rows::{decode_row, read_row},
        tests::{MARK, open, sender_outcomes::direct_write, uuid_v7},
    },
};
use hegel::{TestCase, generators as gs};
use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};
use std::{collections::BTreeSet, sync::Arc};
use workspace_model::{Timestamp, central_mail::MessageOutcome};

use OutboxItemState::{
    Answered, InOutbox, NotSent, Refused, Resent, Sent, SentAnswerLost, Unconfirmed,
};
use OutboxRowEvent::{
    Claim, EndDue, Prune, RecordLateOutcome, RecordOutcome, ReturnNotTransmitted, ReturnUnanswered,
    SenderDelivery,
};

const CREATED: i64 = MARK - 1_000;
const DEADLINE: i64 = CREATED + 900_000;

fn acceptance() -> OutboxAccepted {
    OutboxAccepted::SendMessage {
        message_id: uuid_v7(104).parse().unwrap(),
    }
}

fn notice(item: &NewOutboxItem) -> StoredSenderOutcome {
    StoredSenderOutcome {
        inbox_message_id: uuid_v7(105).parse().unwrap(),
        message_id: uuid_v7(104).parse().unwrap(),
        request_id: item.request.clone(),
        outcome: MessageOutcome::DeliveryNotConfirmed,
        reported_at: Timestamp::from_timestamp_millis(DEADLINE).unwrap(),
    }
}

fn row(item_state: OutboxItemState) -> OutboxRowState {
    let item = direct_write(100);
    let profile = super::body_free_profile(
        &item.request,
        &item.payload,
        item.payload.content().unwrap().target,
    )
    .unwrap();
    let payload = Arc::new(item.payload);
    let phase = match item_state {
        InOutbox => OutboxPhase::InOutbox(payload),
        SentAnswerLost => OutboxPhase::SentAnswerLost(payload),
        Sent => OutboxPhase::Sent(payload),
        Resent => OutboxPhase::Resent(payload),
        Answered => OutboxPhase::Answered(profile, acceptance()),
        Refused => OutboxPhase::Refused(
            profile,
            OutboxRefusal::Local(LocalOutboxRefusal::RequestIdConflict),
        ),
        NotSent => OutboxPhase::NotSent(profile),
        Unconfirmed => OutboxPhase::Unconfirmed(profile, None),
    };
    OutboxRowState {
        request: item.request,
        caller: item.caller,
        sent_at: Timestamp::from_timestamp_millis(CREATED).unwrap(),
        flush_at_ms: MARK,
        deadline_ms: DEADLINE,
        updated_at_ms: MARK,
        phase,
    }
}

#[test]
fn declared_edges_match_the_reducer() {
    let item = direct_write(100);
    let accepted = OutboxWriteOutcome::from_accepted(&item.payload, acceptance()).unwrap();
    let refused = OutboxWriteOutcome::from_refusal(
        &item.payload,
        OutboxRefusal::Local(LocalOutboxRefusal::RequestIdConflict),
    )
    .unwrap();
    let events = [
        Claim {
            now_ms: MARK,
            eligible_through_ms: MARK,
            required_deadline_after_ms: MARK,
        },
        ReturnNotTransmitted { now_ms: DEADLINE },
        ReturnUnanswered {
            now_ms: DEADLINE - 1,
        },
        ReturnUnanswered { now_ms: DEADLINE },
        EndDue { now_ms: DEADLINE },
        RecordOutcome {
            outcome: accepted.clone(),
            now_ms: MARK + 1,
        },
        RecordOutcome {
            outcome: refused,
            now_ms: MARK + 1,
        },
        RecordLateOutcome {
            outcome: accepted.clone(),
        },
        SenderDelivery {
            notice: notice(&item),
            now_ms: DEADLINE + 1,
        },
        Prune {
            cutoff_ms: DEADLINE,
        },
    ];
    let pending_late = row(Unconfirmed);
    let known_late =
        OutboxRowMachine::transition(&pending_late, RecordLateOutcome { outcome: accepted })
            .unwrap()
            .0;
    let rows = [
        row(InOutbox),
        row(SentAnswerLost),
        row(Sent),
        row(Resent),
        row(Answered),
        row(Refused),
        row(NotSent),
        pending_late,
        known_late,
    ];
    let cases = rows
        .iter()
        .flat_map(|row| events.iter().map(|event| (row.clone(), event.clone())));
    check_state_machine_transitions::<OutboxRowMachine>(cases).unwrap();
}

#[test]
fn cutoff_uses_the_departure_evidence_and_excludes_equality_from_claims() {
    for (before, untouched_wait, untouched_ending, untouched_reason) in [
        (Sent, InOutbox, NotSent, NotSentWithin15Minutes),
        (
            Resent,
            SentAnswerLost,
            Unconfirmed,
            NotConfirmedWithin15Minutes,
        ),
    ] {
        let (waiting, effects) =
            OutboxRowMachine::transition(&row(before), ReturnNotTransmitted { now_ms: DEADLINE })
                .unwrap();
        assert_eq!(waiting.phase.item_state(), untouched_wait);
        assert_eq!(waiting.updated_at_ms, DEADLINE);
        assert_eq!(effects, [OutboxRowEffect::Stamp]);

        let (ended, effects) =
            OutboxRowMachine::transition(&waiting, EndDue { now_ms: DEADLINE }).unwrap();
        assert_eq!(ended.phase.item_state(), untouched_ending);
        assert_eq!(ended.updated_at_ms, DEADLINE);
        assert_eq!(
            effects,
            [OutboxRowEffect::Finish {
                reason: untouched_reason
            }]
        );

        let (ended, effects) =
            OutboxRowMachine::transition(&row(before), ReturnUnanswered { now_ms: DEADLINE })
                .unwrap();
        assert_eq!(ended.phase.item_state(), Unconfirmed);
        assert_eq!(ended.updated_at_ms, DEADLINE);
        assert_eq!(
            effects,
            [OutboxRowEffect::Finish {
                reason: NotConfirmedWithin15Minutes
            }]
        );
    }

    let waiting = row(InOutbox);
    for (mark, threshold) in [(MARK - 1, MARK), (MARK, DEADLINE)] {
        assert!(matches!(
            OutboxRowMachine::transition(
                &waiting,
                Claim {
                    now_ms: MARK,
                    eligible_through_ms: mark,
                    required_deadline_after_ms: threshold
                },
            ),
            Err(TransitionError::Domain(OutboxRowRefusal::NotDue))
        ));
    }
}

#[hegel::test(test_cases = 32)]
fn generated_histories_preserve_settlement_order_and_retention(tc: TestCase) {
    let replay_returns = tc.draw(gs::vecs(gs::booleans()).max_size(5));
    let notice_at = DEADLINE + tc.draw(gs::integers::<i64>().min_value(1).max_value(120_000));
    let replay_gap = tc.draw(gs::integers::<i64>().min_value(1).max_value(1_000));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for late_first in [true, false] {
            verify_result_order(&replay_returns, notice_at, replay_gap, late_first).await;
        }
    });
}

async fn verify_result_order(
    replay_returns: &[bool],
    notice_at: i64,
    replay_gap: i64,
    late_first: bool,
) {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let item = direct_write(100);
    let mut replacement = item.clone();
    let OutboxQueuedWrite::SendMessage { message, .. } = &mut replacement.payload else {
        panic!("direct-message fixture");
    };
    message.body = "the same id is reusable after prune"
        .to_owned()
        .try_into()
        .unwrap();

    assert_eq!(
        MacOutbox::new(&mut database)
            .insert(&item, CREATED)
            .await
            .unwrap(),
        OutboxInsert::Stored
    );
    let mut may_have_left = false;
    for (attempt, possibly_sent) in replay_returns.iter().enumerate() {
        let started = MARK + i64::try_from(attempt).unwrap() * 2;
        claim_and_check(&mut database, &item, started, may_have_left).await;
        let mut outbox = MacOutbox::new(&mut database);
        if *possibly_sent {
            assert!(
                outbox
                    .return_unanswered(std::slice::from_ref(&item.request), started + 1)
                    .await
                    .unwrap()
                    .healthy
                    .is_empty()
            );
            may_have_left = true;
        } else {
            outbox
                .return_not_transmitted(&item.request, started + 1)
                .await
                .unwrap();
        }
        let (saved, body) = stored(&mut database, &item).await;
        assert_eq!(
            saved.phase.item_state(),
            if may_have_left {
                SentAnswerLost
            } else {
                InOutbox
            }
        );
        assert_eq!(saved.updated_at_ms, started + 1);
        assert_eq!(
            saved.status(),
            OutboxRequestStatus::InOutbox {
                flush_at_ms: item.flush_at_ms,
            },
        );
        assert!(body.contains("hello"));
    }
    claim_and_check(&mut database, &item, MARK + 20, may_have_left).await;
    let ended = MacOutbox::new(&mut database)
        .return_unanswered(std::slice::from_ref(&item.request), DEADLINE)
        .await
        .unwrap()
        .healthy;
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].reason, NotConfirmedWithin15Minutes);

    let receipt = notice(&item);
    let outcome = OutboxWriteOutcome::from_accepted(&item.payload, acceptance()).unwrap();
    let mut outbox = MacOutbox::new(&mut database);
    if late_first {
        outbox
            .record_late_outcome(&item.request, &outcome)
            .await
            .unwrap();
        outbox
            .record_late_outcome(&item.request, &outcome)
            .await
            .unwrap();
    }
    assert!(
        outbox
            .record_sender_delivery(&item.caller, &receipt, notice_at)
            .await
            .unwrap()
    );
    if !late_first {
        assert!(matches!(
            outbox.record_late_outcome(&item.request, &outcome).await,
            Err(OutboxStoreError::UnexpectedState {
                expected: "ended unconfirmed",
                ..
            })
        ));
    }
    assert!(
        outbox
            .record_sender_delivery(&item.caller, &receipt, notice_at + replay_gap)
            .await
            .unwrap()
    );
    drop(database);
    let mut database = open(&root).await;

    let (saved, json) = stored(&mut database, &item).await;
    let retention_start = if late_first {
        assert!(matches!(
            saved.phase,
            OutboxPhase::Unconfirmed(_, Some(SettledWrite::Answered(_)))
        ));
        DEADLINE
    } else {
        assert!(matches!(saved.phase, OutboxPhase::Answered(..)));
        notice_at
    };
    assert_eq!(saved.updated_at_ms, retention_start);
    assert!(
        !json.contains("hello"),
        "terminal payload still contains the queued body"
    );
    assert_eq!(saved.flush_at_ms, item.flush_at_ms);
    assert_eq!(saved.deadline_ms, expires_at_ms(CREATED));
    assert_eq!(saved.sent_at.timestamp_millis(), CREATED);
    let mut outbox = MacOutbox::new(&mut database);
    let expected = OutboxRequestStatus::Answered(acceptance());
    assert_eq!(
        outbox
            .request_status(&item.caller, &item.request)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        outbox
            .sender_outcome(&item.caller, &item.request)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        outbox.pending_for(&item.caller).await.unwrap()[0].state,
        PendingWriteState::Answered(acceptance())
    );
    assert_eq!(
        outbox.outgoing_for(&item.caller, 1).await.unwrap()[0].answered_at_ms,
        (!late_first).then_some(notice_at)
    );
    assert_eq!(
        outbox.insert(&item, notice_at + replay_gap).await.unwrap(),
        OutboxInsert::AlreadyStored(expected)
    );
    assert_eq!(
        outbox
            .insert(&replacement, notice_at + replay_gap)
            .await
            .unwrap(),
        OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict),
    );
    let retention_end = retention_start + ANSWERED_RETENTION_MILLIS;
    assert_eq!(
        outbox
            .delete_ended_before_retention(retention_end - 1)
            .await
            .unwrap()
            .healthy,
        0
    );
    assert_eq!(
        outbox
            .delete_ended_before_retention(retention_end)
            .await
            .unwrap()
            .healthy,
        1
    );
    assert_eq!(
        outbox
            .request_status(&item.caller, &item.request)
            .await
            .unwrap(),
        OutboxRequestStatus::NotFound
    );
    assert_eq!(
        outbox.insert(&replacement, retention_end).await.unwrap(),
        OutboxInsert::Stored
    );
    let (fresh, json) = stored(&mut database, &replacement).await;
    assert_eq!(fresh.phase.item_state(), InOutbox);
    assert_eq!(fresh.updated_at_ms, retention_end);
    assert!(json.contains("the same id is reusable after prune"));
}

async fn claim_and_check(
    database: &mut MachineLocalStore,
    item: &NewOutboxItem,
    now_ms: i64,
    replay: bool,
) {
    let batch = MacOutbox::new(database)
        .claim_next_batch(MARK, 1, now_ms, now_ms, &BTreeSet::new())
        .await
        .unwrap()
        .healthy;
    assert_eq!(batch.writes.len(), 1);
    assert_eq!(batch.writes[0].request, item.request);
    assert_eq!(batch.writes[0].payload, item.payload);
    let (saved, _) = stored(database, item).await;
    assert_eq!(saved.phase.item_state(), if replay { Resent } else { Sent });
    assert_eq!(saved.updated_at_ms, now_ms);
    assert_eq!(saved.status(), OutboxRequestStatus::Sent);
}

async fn stored(
    database: &mut MachineLocalStore,
    item: &NewOutboxItem,
) -> (OutboxRowState, String) {
    let raw = read_row(&mut database.connection, &item.request)
        .await
        .unwrap()
        .unwrap();
    let decoded = decode_row(&raw).unwrap();
    (decoded, String::from_utf8(raw.payload_json).unwrap())
}

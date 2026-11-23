use super::{
    CombinedRecipientSessionDirectory, RecipientSessionAnswer, RecipientSessionDirectory,
    RecipientSessionSources, SessionInventory, SessionInventoryAnswer, UnavailableSessionInventory,
    combine_inventory_answers,
};
use crate::recipient_inbox::{InboxOfferResult, RecipientInbox};
use central_link_client::ValidatedDeliveryOffer;
use central_link_protocol::{DeliveryMessage, DirectOfferBody, DirectOfferMessage, TimeLeft};
use hegel::generators as gs;
use router_clock::{ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{
    future::{Future, pending, ready},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, MachineLocalStore,
};
use workspace_ids::{MachineId, SessionId, SessionRef};
use workspace_model::{
    PostText,
    central_mail::{MessageDelivery, MessageSender, MessageState},
};

const fn observation(at_least: usize, complete: bool) -> SessionInventoryAnswer {
    if complete {
        SessionInventoryAnswer::Complete(at_least)
    } else {
        SessionInventoryAnswer::AtLeast(at_least)
    }
}

fn expected_answer(answers: [(usize, bool, bool); 3]) -> RecipientSessionAnswer {
    use RecipientSessionAnswer::{AmbiguousSession, CouldNotTell, KnownOnce, UnknownSession};
    let total = answers
        .iter()
        .map(|(count, _, _)| *count)
        .fold(0_usize, usize::saturating_add);
    match total {
        2.. => AmbiguousSession,
        _ if answers.iter().any(|(_, may_hide_match, _)| *may_hide_match) => CouldNotTell,
        1 => KnownOnce,
        _ if answers
            .iter()
            .any(|(_, _, absence_unproven)| *absence_unproven) =>
        {
            CouldNotTell
        }
        _ => UnknownSession,
    }
}

#[test]
fn all_three_source_combinations_obey_the_precedence_table() {
    use SessionInventoryAnswer::{AtLeast, Complete, NoneFoundAbsenceUnproven};
    let answers = [
        (Complete(0), (0, false, false)),
        (Complete(1), (1, false, false)),
        (Complete(2), (2, false, false)),
        (AtLeast(0), (0, true, false)),
        (AtLeast(1), (1, true, false)),
        (AtLeast(2), (2, true, false)),
        (NoneFoundAbsenceUnproven, (0, false, true)),
    ];
    for (first, first_fact) in answers {
        for (second, second_fact) in answers {
            for (third, third_fact) in answers {
                let sources = [first, second, third];
                assert_eq!(
                    combine_inventory_answers(sources),
                    expected_answer([first_fact, second_fact, third_fact]),
                    "{sources:?}",
                );
            }
        }
    }
    assert_eq!(
        combine_inventory_answers([]),
        RecipientSessionAnswer::UnknownSession
    );
}

#[hegel::test(test_cases = 160)]
fn larger_lower_bounds_do_not_overflow_or_hide_duplicates(case: hegel::TestCase) {
    use SessionInventoryAnswer::{AtLeast, Complete, NoneFoundAbsenceUnproven};
    let answers = std::array::from_fn(|_| {
        let count = case.draw(gs::integers::<usize>());
        match case.draw(gs::integers::<u8>().max_value(2)) {
            0 => (Complete(count), (count, false, false)),
            1 => (AtLeast(count), (count, true, false)),
            _ => (NoneFoundAbsenceUnproven, (0, false, true)),
        }
    });
    assert_eq!(
        combine_inventory_answers(answers.map(|(answer, _)| answer)),
        expected_answer(answers.map(|(_, fact)| fact))
    );
    assert_eq!(
        combine_inventory_answers([observation(usize::MAX, false), observation(1, true)]),
        RecipientSessionAnswer::AmbiguousSession
    );
}

struct SpecimenSource {
    duplicates: usize,
    incomplete: bool,
}

impl SessionInventory for SpecimenSource {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        let answer = match session_id.as_str() {
            "duplicate" => observation(self.duplicates, true),
            "unavailable" => observation(0, !self.incomplete),
            _ => observation(0, true),
        };
        Box::pin(ready(answer))
    }
}

fn offer(machine_id: &MachineId, message_id: &str, session_id: &str) -> ValidatedDeliveryOffer {
    let message = DeliveryMessage::Direct(DirectOfferMessage {
        message_id: message_id.parse().unwrap(),
        sender: MessageSender::Central,
        target: SessionRef {
            machine_id: machine_id.clone(),
            session_id: session_id.parse().unwrap(),
        },
        body: DirectOfferBody::Direct {
            body: PostText::try_from("recipient directory specimen".to_owned()).unwrap(),
            delivery: MessageDelivery::Auto,
            reply_to: None,
        },
        state: MessageState::Queued,
        source: None,
    });
    ValidatedDeliveryOffer::new(
        Box::new(message),
        Some(TimeLeft::try_from(123_000).unwrap()),
    )
    .unwrap()
}

#[tokio::test]
async fn real_directory_drives_durable_refusals_and_deferred_lookup_distinctly() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("machine_local.sqlite");
    let machine_id: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let clock =
        Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(1_763_380_800_000)).unwrap());
    let directory = Arc::new(CombinedRecipientSessionDirectory::new(
        RecipientSessionSources {
            codex: Arc::new(SpecimenSource {
                duplicates: 1,
                incomplete: false,
            }),
            managed_providers: Arc::new(SpecimenSource {
                duplicates: 1,
                incomplete: true,
            }),
            claude_terminal: Arc::new(SpecimenSource {
                duplicates: 0,
                incomplete: false,
            }),
        },
    ));
    let arrival = clock.utc_now();
    let mut inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        machine_id.clone(),
        directory,
        clock,
    );

    let unknown = "019f0000-0000-7000-8000-000000000091";
    let duplicate = "019f0000-0000-7000-8000-000000000092";
    let unavailable = "019f0000-0000-7000-8000-000000000093";
    let unknown_decision = InboxDecision::Refused {
        reason: InboxRefusalReason::UnknownSession,
    };
    assert_eq!(
        inbox
            .receive_offer(offer(&machine_id, unknown, "unknown"), arrival)
            .await
            .unwrap(),
        InboxOfferResult::Recorded {
            decision: unknown_decision
        }
    );
    assert_eq!(
        inbox
            .receive_offer(offer(&machine_id, unknown, "unknown"), arrival)
            .await
            .unwrap(),
        InboxOfferResult::AlreadyDecided {
            decision: unknown_decision
        }
    );
    assert_eq!(
        inbox
            .receive_offer(offer(&machine_id, duplicate, "duplicate"), arrival)
            .await
            .unwrap(),
        InboxOfferResult::Recorded {
            decision: InboxDecision::Refused {
                reason: InboxRefusalReason::AmbiguousSession
            }
        }
    );
    assert_eq!(
        inbox
            .receive_offer(offer(&machine_id, unavailable, "unavailable"), arrival)
            .await
            .unwrap(),
        InboxOfferResult::LookupDeferred
    );
    assert!(inbox.needs_local_retry());
    let mut durable = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let pending = durable.pending_receipts().await.unwrap();
    assert!(pending.corrupt_rows.is_empty());
    assert_eq!(pending.healthy.len(), 2);
    assert!(
        durable
            .receipt_for(&unavailable.parse().unwrap())
            .await
            .unwrap()
            .healthy
            .is_none()
    );
}

struct FixedObservation(usize);

impl SessionInventory for FixedObservation {
    fn lookup_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> super::InventoryRead<'lookup> {
        Box::pin(ready(observation(self.0, true)))
    }
}

struct HeldInventory;

impl SessionInventory for HeldInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> super::InventoryRead<'lookup> {
        Box::pin(pending())
    }
}

struct GatedInventory(tokio::sync::Notify);

impl SessionInventory for GatedInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> super::InventoryRead<'lookup> {
        Box::pin(async {
            self.0.notified().await;
            observation(1, true)
        })
    }
}

#[tokio::test]
async fn two_proved_claims_finish_without_a_source_held_before_or_after_them() {
    for held_first in [true, false] {
        let gated = Arc::new(GatedInventory(tokio::sync::Notify::new()));
        let (codex, claude_terminal): (Arc<dyn SessionInventory>, Arc<dyn SessionInventory>) =
            if held_first {
                (Arc::new(HeldInventory), gated.clone())
            } else {
                (gated.clone(), Arc::new(HeldInventory))
            };
        let directory = CombinedRecipientSessionDirectory::new(RecipientSessionSources {
            codex,
            managed_providers: Arc::new(FixedObservation(1)),
            claude_terminal,
        });
        let clock = ManualClock::starting_at(SyncTime::from_unix_millis(0)).unwrap();
        let target = "target".parse().unwrap();
        let deadline = clock.monotonic_now().after(Duration::from_secs(30));
        let mut lookup = directory.query_until(&target, &clock, deadline);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(lookup.as_mut().poll(&mut cx).is_pending());
        gated.0.notify_one();
        assert_eq!(
            lookup.as_mut().poll(&mut cx),
            Poll::Ready(RecipientSessionAnswer::AmbiguousSession)
        );
    }
}

#[tokio::test]
async fn budget_defers_with_one_or_zero_claims_when_a_source_is_still_pending() {
    for count in [0, 1] {
        let directory = CombinedRecipientSessionDirectory::new(RecipientSessionSources {
            codex: Arc::new(HeldInventory),
            managed_providers: Arc::new(FixedObservation(count)),
            claude_terminal: Arc::new(FixedObservation(0)),
        });
        let clock = ManualClock::starting_at(SyncTime::from_unix_millis(0)).unwrap();
        let target = "target".parse().unwrap();
        let budget = Duration::from_secs(30);
        let mut lookup =
            directory.query_until(&target, &clock, clock.monotonic_now().after(budget));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(lookup.as_mut().poll(&mut cx).is_pending());
        clock.advance(budget).unwrap();
        assert_eq!(
            lookup.as_mut().poll(&mut cx),
            Poll::Ready(RecipientSessionAnswer::CouldNotTell)
        );
    }
}

#[tokio::test]
async fn an_unavailable_authority_makes_zero_and_one_claim_inconclusive() {
    for other_claims in [0, 1] {
        let directory = CombinedRecipientSessionDirectory::new(RecipientSessionSources {
            codex: Arc::new(FixedObservation(other_claims)),
            managed_providers: Arc::new(FixedObservation(0)),
            claude_terminal: Arc::new(UnavailableSessionInventory),
        });
        assert_eq!(
            directory
                .query_recipient_session(&"target".parse().unwrap())
                .await,
            RecipientSessionAnswer::CouldNotTell
        );
    }
}

#[tokio::test]
async fn custody_keeps_two_proved_claims_when_answer_and_deadline_are_ready_together() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine-local.sqlite");
    let clock = Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(0)).unwrap());
    let gated = Arc::new(GatedInventory(tokio::sync::Notify::new()));
    let directory = CombinedRecipientSessionDirectory::new(RecipientSessionSources {
        codex: Arc::new(HeldInventory),
        managed_providers: Arc::new(FixedObservation(1)),
        claude_terminal: gated.clone(),
    });
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let message_id = "019f0000-0000-7000-8000-000000000095";
    let mut inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        machine.clone(),
        Arc::new(directory),
        clock.clone(),
    );
    let budget = Duration::from_secs(30);
    let advance = async {
        clock
            .not_due_check_now(clock.monotonic_now().after(budget))
            .await;
        gated.0.notify_one();
        clock.advance(budget).unwrap();
    };
    let (result, ()) = tokio::join!(
        inbox.receive_offer(offer(&machine, message_id, "target"), clock.utc_now()),
        advance,
    );
    let decision = InboxDecision::Refused {
        reason: InboxRefusalReason::AmbiguousSession,
    };
    assert_eq!(result.unwrap(), InboxOfferResult::Recorded { decision });
    let mut durable = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let receipt = durable
        .receipt_for(&message_id.parse().unwrap())
        .await
        .unwrap();
    assert_eq!(receipt.healthy.unwrap().decision, decision);
}

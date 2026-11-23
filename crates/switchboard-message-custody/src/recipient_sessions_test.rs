use super::{
    CombinedRecipientSessionDirectory, RecipientSessionAnswer, SessionInventory,
    SessionInventoryAnswer, combine_inventory_answers,
};
use crate::recipient_inbox::{InboxOfferResult, RecipientInbox};
use central_link_client::ValidatedDeliveryOffer;
use central_link_protocol::{DeliveryMessage, DirectOfferBody, DirectOfferMessage, TimeLeft};
use hegel::generators as gs;
use router_clock::{ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{
    future::{Future, ready},
    pin::Pin,
    sync::Arc,
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
    SessionInventoryAnswer { at_least, complete }
}

#[test]
fn all_three_source_combinations_obey_the_precedence_table() {
    use RecipientSessionAnswer::{AmbiguousSession, CouldNotTell, KnownOnce, UnknownSession};
    let answers = [
        observation(0, true),
        observation(1, true),
        observation(2, true),
        observation(0, false),
        observation(1, false),
        observation(2, false),
    ];
    for first in answers {
        for second in answers {
            for third in answers {
                let sources = [first, second, third];
                let expected = match (
                    sources.iter().map(|answer| answer.at_least).sum::<usize>(),
                    sources.iter().all(|answer| answer.complete),
                ) {
                    (0, true) => UnknownSession,
                    (1, true) => KnownOnce,
                    (0 | 1, false) => CouldNotTell,
                    _ => AmbiguousSession,
                };
                assert_eq!(combine_inventory_answers(sources), expected, "{sources:?}");
            }
        }
    }
    assert_eq!(combine_inventory_answers([]), UnknownSession);
}

#[hegel::test(test_cases = 160)]
fn larger_lower_bounds_do_not_overflow_or_hide_duplicates(case: hegel::TestCase) {
    use RecipientSessionAnswer::{AmbiguousSession, CouldNotTell, KnownOnce, UnknownSession};
    let answers: [_; 3] = std::array::from_fn(|_| {
        observation(
            case.draw(gs::integers::<usize>()),
            case.draw(gs::booleans()),
        )
    });
    let expected = if answers.iter().any(|answer| answer.at_least > 1)
        || answers.iter().filter(|answer| answer.at_least == 1).count() > 1
    {
        AmbiguousSession
    } else if answers.iter().any(|answer| !answer.complete) {
        CouldNotTell
    } else if answers.iter().any(|answer| answer.at_least == 1) {
        KnownOnce
    } else {
        UnknownSession
    };
    assert_eq!(combine_inventory_answers(answers), expected);
    assert_eq!(
        combine_inventory_answers([observation(usize::MAX, false), observation(1, true)]),
        AmbiguousSession
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
    let directory = Arc::new(CombinedRecipientSessionDirectory::new(vec![
        Arc::new(SpecimenSource {
            duplicates: 1,
            incomplete: false,
        }),
        Arc::new(SpecimenSource {
            duplicates: 1,
            incomplete: true,
        }),
        Arc::new(SpecimenSource {
            duplicates: 0,
            incomplete: false,
        }),
    ]));
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

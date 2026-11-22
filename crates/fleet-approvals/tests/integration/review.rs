use approval_request_protocol::{DecidedBy, PassOnReply, PassOnRequest, RaiseReply};
use central_engine_model::{Context, MAX_ALARM_UNIX_MILLIS, TransitionError};
use fleet_approvals::{
    AdmittedDecider, ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalDecisionReply,
    ApprovalEffect, ApprovalReceipt, ApprovalRefusal, ApprovalRouting, DestinationReplyMismatch,
    FactorStanding,
};

use super::fixtures::{
    HOUR, ObservedReply, UnusedIds, answer, decide_event, enrollment, key, person, person_id,
    provider, raise_event, raw_reply, reply, routing, run_event, session, setup, time, view_event,
};

#[test]
fn delegate_starts_with_only_its_exact_parent_and_only_first_raise_notifies() {
    let (mut book, run, requester, parent) = setup();
    let item = key(&run, 1);
    let foreign_parent = session(&enrollment(9000, 9001), 9002);
    for invalid_parent in [&requester, &foreign_parent] {
        let before = book.clone();
        let event = raise_event(
            &run,
            provider(&item, &requester, "Delegate"),
            routing(Some(invalid_parent)),
        );
        assert_eq!(
            book.apply(
                event,
                &mut Context {
                    now: time(1),
                    ids: &mut UnusedIds
                }
            ),
            Err(TransitionError::Domain(
                ApprovalRefusal::InvalidInitialRoute
            ))
        );
        assert_eq!(book, before);
    }

    let raise = raise_event(
        &run,
        provider(&item, &requester, "Delegate"),
        routing(Some(&parent)),
    );
    let raised = run_event(&mut book, raise.clone(), 0);
    let notices = |effects: &[ApprovalCollectionEffect]| {
        effects
            .iter()
            .filter(|effect| {
                matches!(
                    effect,
                    ApprovalCollectionEffect::Request(ApprovalEffect::StoreSessionNotice { .. })
                )
            })
            .count()
    };
    assert_eq!(notices(&raised), 1);
    assert!(raised.contains(&ApprovalCollectionEffect::Request(
        ApprovalEffect::StoreSessionNotice {
            key: item.clone(),
            session: parent.clone(),
        }
    )));
    assert_eq!(notices(&run_event(&mut book, raise, 1)), 0);

    let other = session(&run.enrollment, 8788);
    let owner = person(10, FactorStanding::Fresh);
    for (actor, operation) in [
        (owner.clone(), 100),
        (AdmittedDecider::Session(other.clone()), 101),
    ] {
        let event = decide_event(&item, actor, operation, answer(1));
        assert_eq!(
            reply(&run_event(&mut book, event, 1)),
            ObservedReply::Decide(ApprovalDecisionReply::NotVisible {})
        );
    }
    assert_eq!(
        reply(&run_event(&mut book, view_event(&item, 10), 1)),
        ObservedReply::Read(None)
    );
    let pass = |parent| ApprovalCollectionEvent::PassOn {
        key: item.clone(),
        parent,
        request: PassOnRequest {
            request_id: item.request_id.clone(),
        },
    };
    assert_eq!(
        reply(&run_event(&mut book, pass(other), 1)),
        ObservedReply::PassOn(PassOnReply::NotVisible {})
    );
    assert_eq!(
        reply(&run_event(&mut book, pass(parent.clone()), 1)),
        ObservedReply::PassOn(PassOnReply::Forwarded {})
    );
    assert!(matches!(
        reply(&run_event(&mut book, view_event(&item, 10), 1)),
        ObservedReply::Read(Some(_))
    ));
    let ending = run_event(&mut book, decide_event(&item, owner, 102, answer(1)), 2);
    assert_eq!(notices(&ending), 0);
    assert!(ending.iter().any(|effect| matches!(
        effect,
        ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding { .. })
    )));
    run_event(
        &mut book,
        ApprovalCollectionEvent::EndSession {
            session: parent.clone(),
        },
        3,
    );
    let next_item = key(&run, 2);
    let refused = raise_event(
        &run,
        provider(&next_item, &requester, "Ended parent"),
        routing(Some(&parent)),
    );
    let before = book.clone();
    assert_eq!(
        book.apply(
            refused,
            &mut Context {
                now: time(4),
                ids: &mut UnusedIds
            }
        ),
        Err(TransitionError::Domain(
            ApprovalRefusal::InvalidInitialRoute
        ))
    );
    assert_eq!(book, before);
}

#[test]
fn saved_replayed_standing_and_read_replies_keep_the_request_and_recipient_binding() {
    let (mut book, run, requester, _) = setup();
    let item = key(&run, 1);
    let other = key(&run, 2);
    let giver = DecidedBy::Person {
        person_id: person_id(10),
    };
    let another = DecidedBy::Person {
        person_id: person_id(11),
    };
    run_event(
        &mut book,
        raise_event(
            &run,
            provider(&item, &requester, "Owner"),
            ApprovalRouting::Owner,
        ),
        0,
    );
    let original = decide_event(&item, person(10, FactorStanding::Fresh), 100, answer(2));
    let won = run_event(&mut book, original.clone(), 1);
    let replayed = run_event(&mut book, original, 2);
    let standing = run_event(
        &mut book,
        decide_event(&item, person(10, FactorStanding::Fresh), 101, answer(1)),
        3,
    );
    for effects in [&won, &replayed, &standing] {
        let ApprovalReceipt::Decide(bound) = raw_reply(effects) else {
            panic!("expected Decide");
        };
        assert_eq!(
            bound.for_recipient(&item, &another),
            Err(DestinationReplyMismatch::Recipient)
        );
        assert_eq!(
            bound.for_recipient(&other, &giver),
            Err(DestinationReplyMismatch::Request)
        );
        let checked = bound.for_recipient(&item, &giver).unwrap();
        match checked {
            ApprovalDecisionReply::SavedDecision { answer: chosen, by } => {
                assert_eq!(chosen, &answer(2));
                assert_eq!(by, &giver);
            }
            ApprovalDecisionReply::Standing { ending } => {
                assert_eq!(
                    serde_json::to_value(ending).unwrap()["ending"]["decision"]["answer"],
                    2
                );
            }
            unexpected => panic!("expected giver projection, got {unexpected:?}"),
        }
    }
    for (person, recipient, has_own_value) in [(10, &giver, true), (11, &another, false)] {
        let read = run_event(&mut book, view_event(&item, person), 4);
        let ApprovalReceipt::Read(bound) = raw_reply(&read) else {
            panic!("expected Read");
        };
        let wrong = if has_own_value { &another } else { &giver };
        assert_eq!(
            bound.for_recipient(&item, wrong),
            Err(DestinationReplyMismatch::Recipient)
        );
        assert_eq!(
            bound.for_recipient(&other, recipient),
            Err(DestinationReplyMismatch::Request)
        );
        let view = bound
            .for_recipient(&item, recipient)
            .unwrap()
            .as_ref()
            .unwrap();
        let wire = serde_json::to_value(view).unwrap();
        if has_own_value {
            assert_eq!(wire["state"]["ending"]["decision"]["answer"], 2, "{wire}");
        } else {
            assert!(wire["state"]["ending"]["decision"].is_null(), "{wire}");
            assert_eq!(
                wire["state"]["ending"]["ending"]["by"],
                serde_json::to_value(&giver).unwrap(),
                "{wire}"
            );
        }
    }
}

#[test]
fn out_of_range_sample_or_restored_floor_cannot_poison_the_collection() {
    let (mut book, run, requester, _) = setup();
    let item = key(&run, 1);
    let alarm = ApprovalCollectionEvent::Alarm {
        key: item.clone(),
        deadline: time(1),
    };
    let unavailable_time = time(MAX_ALARM_UNIX_MILLIS + 1);
    let before = book.clone();
    assert_eq!(
        book.apply(
            alarm.clone(),
            &mut Context {
                now: unavailable_time,
                ids: &mut UnusedIds
            }
        ),
        Err(TransitionError::Domain(
            ApprovalRefusal::ApplyingTimeOutOfRange
        ))
    );
    assert_eq!(book, before);
    let mut restored = book.clone();
    restored.high_water = unavailable_time;
    assert_eq!(
        restored.apply(
            alarm.clone(),
            &mut Context {
                now: time(1),
                ids: &mut UnusedIds
            }
        ),
        Err(TransitionError::Domain(
            ApprovalRefusal::ApplyingTimeOutOfRange
        ))
    );
    assert!(run_event(&mut book, alarm, 2).is_empty());
    assert_eq!(book.high_water, time(2));
    let raise = raise_event(
        &run,
        provider(&item, &requester, "Ordinary"),
        ApprovalRouting::Owner,
    );
    let too_near_end = time(MAX_ALARM_UNIX_MILLIS + 1 - HOUR);
    assert_eq!(
        book.apply(
            raise.clone(),
            &mut Context {
                now: too_near_end,
                ids: &mut UnusedIds
            }
        ),
        Err(TransitionError::Domain(ApprovalRefusal::DeadlineOutOfRange))
    );
    assert_eq!(book.high_water, time(2));
    let received = run_event(&mut book, raise, 1);
    let ObservedReply::Raise(RaiseReply::Receipt { receipt }) = reply(&received) else {
        panic!("expected first receipt");
    };
    assert_eq!(receipt.window.deadline(), time(2 + HOUR));
}

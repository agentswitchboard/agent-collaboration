use approval_request_protocol::{
    ApprovalWindow, HolderStopCause, RaiseReceipt, RaiseReply, StopReply,
};
use fleet_approvals::{
    ApprovalCollection, ApprovalCollectionEffect, ApprovalDecisionReply, ApprovalEffect,
    ApprovalReceipt, ApprovalState,
};
use serde_json::json;

use super::super::fixtures::{HOUR, answer, operation_id, time};
use super::super::model_ledger::{
    Actor, Decision, Ledger, OperationEffect, ROWS, RUN_ENROLLMENTS, RaiseResult, Step, StopResult,
};
use super::{Scenario, wire_delivery, wire_waiting};

fn assert_decision(
    scenario: &Scenario,
    row: usize,
    actor: Actor,
    result: Decision,
    actual: &ApprovalDecisionReply,
) {
    match result {
        Decision::Won { choice } => assert_eq!(
            actual,
            &ApprovalDecisionReply::SavedDecision {
                answer: answer(choice),
                by: scenario.identity(row, actor)
            }
        ),
        Decision::Standing(standing) => {
            let ApprovalDecisionReply::Standing { ending } = actual else {
                panic!("expected standing, got {actual:?}");
            };
            assert_eq!(
                serde_json::to_value(ending).unwrap(),
                json!({"kind":"providerApproval","ending":scenario.recipient_ending(row, actor, standing)})
            );
        }
        Decision::NotVisible => assert_eq!(actual, &ApprovalDecisionReply::NotVisible {}),
        Decision::Invalid => assert_eq!(actual, &ApprovalDecisionReply::Invalid {}),
        Decision::TooLate => assert_eq!(actual, &ApprovalDecisionReply::TooLate {}),
        Decision::OperationConflict => {
            assert_eq!(actual, &ApprovalDecisionReply::OperationConflict {})
        }
    }
}

pub(super) fn assert_effects(
    scenario: &Scenario,
    ledger: &Ledger,
    step: Step,
    expected: &[OperationEffect],
    actual: &[ApprovalCollectionEffect],
) {
    use ApprovalCollectionEffect::Request;
    use ApprovalEffect::Reply;
    assert_eq!(
        actual.len(),
        expected.len(),
        "{step:?}: {actual:?} vs {expected:?}"
    );
    for (expected, actual) in expected.iter().zip(actual) {
        match (expected, actual) {
            (
                OperationEffect::Registered(reply),
                ApprovalCollectionEffect::Registered(observed),
            ) => assert_eq!(reply, observed),
            (
                OperationEffect::Notice(row),
                Request(ApprovalEffect::StoreSessionNotice { key, session }),
            ) => {
                assert_eq!(key, &scenario.key(*row));
                assert_eq!(session, &scenario.parent[RUN_ENROLLMENTS[ROWS[*row].run]]);
            }
            (
                OperationEffect::Due(row, deadline),
                Request(ApprovalEffect::RegisterDue {
                    key,
                    deadline: observed,
                }),
            ) => {
                assert_eq!(key, &scenario.key(*row));
                assert_eq!(observed, &time(*deadline));
            }
            (OperationEffect::Clear(row), Request(ApprovalEffect::ClearDue { key })) => {
                assert_eq!(key, &scenario.key(*row))
            }
            (
                OperationEffect::Offer(row, offer),
                Request(ApprovalEffect::OfferEnding { holder, request }),
            ) => {
                assert_eq!(holder, &scenario.runs[ROWS[*row].run]);
                assert_eq!(request.as_ref(), &scenario.frame(*row, *offer));
            }
            (
                OperationEffect::Save {
                    row,
                    actor,
                    operation,
                    result,
                },
                Request(ApprovalEffect::SaveDecision(saved)),
            ) => {
                let key = scenario.key(*row);
                assert_eq!(saved.key, key);
                assert_eq!(saved.actor, scenario.actor(*row, *actor));
                assert_eq!(saved.operation_id, operation_id(*operation));
                let payload = saved
                    .reply
                    .for_recipient(&key, &scenario.identity(*row, *actor))
                    .unwrap();
                assert_decision(scenario, *row, *actor, *result, payload);
            }
            (
                OperationEffect::Raise(row, result),
                Request(Reply(ApprovalReceipt::Raise(reply))),
            ) => {
                let wanted = match result {
                    RaiseResult::Receipt(at) => RaiseReply::Receipt {
                        receipt: RaiseReceipt {
                            request_id: scenario.key(*row).request_id,
                            window: ApprovalWindow::from_first_save(time(*at)).unwrap(),
                        },
                    },
                    RaiseResult::AlreadyStopped => RaiseReply::AlreadyStopped {},
                    RaiseResult::ContentsDiffer => RaiseReply::ContentsDiffer {},
                    RaiseResult::SessionEnded => RaiseReply::SessionEnded {},
                    RaiseResult::Refused(refusal) => {
                        RaiseReply::AuthorityRefused { refusal: *refusal }
                    }
                };
                assert_eq!(reply, &wanted);
            }
            (OperationEffect::Stop(result), Request(Reply(ApprovalReceipt::Stop(reply)))) => {
                let wanted = match result {
                    StopResult::Stands(ending) => StopReply::Stands { ending: *ending },
                    StopResult::NotVisible => StopReply::NotVisible {},
                    StopResult::Refused(refusal) => {
                        StopReply::AuthorityRefused { refusal: *refusal }
                    }
                };
                assert_eq!(reply, &wanted);
            }
            (
                OperationEffect::Decide(row, actor, result),
                Request(Reply(ApprovalReceipt::Decide(reply))),
            ) => {
                let payload = reply
                    .for_recipient(&scenario.key(*row), &scenario.identity(*row, *actor))
                    .unwrap();
                assert_decision(scenario, *row, *actor, *result, payload);
            }
            (
                OperationEffect::Read(row, actor, view),
                Request(Reply(ApprovalReceipt::Read(reply))),
            ) => {
                let payload = reply
                    .for_recipient(&scenario.key(*row), &scenario.identity(*row, *actor))
                    .unwrap();
                let wanted = view.map(|view| {
                    scenario.expected_read(
                        *row,
                        *actor,
                        view,
                        ledger.rows[*row].first_save.unwrap().0,
                    )
                });
                let observed = payload
                    .as_ref()
                    .map(|view| serde_json::to_value(view).unwrap());
                assert_eq!(observed, wanted);
            }
            (
                OperationEffect::Forward(row, actor, wanted),
                Request(Reply(ApprovalReceipt::PassOn(reply))),
            ) => {
                let payload = reply
                    .for_recipient(&scenario.key(*row), &scenario.identity(*row, *actor))
                    .unwrap();
                assert_eq!(payload, wanted);
            }
            (expected, actual) => panic!("{step:?}: expected {expected:?} but got {actual:?}"),
        }
    }
}

pub(super) fn assert_state(scenario: &Scenario, ledger: &Ledger, book: &ApprovalCollection) {
    assert_eq!(book.high_water, time(ledger.floor));
    assert_eq!(book.decisions.len(), ledger.decisions());
    for (row, expected) in ledger.rows.iter().enumerate() {
        let key = scenario.key(row);
        match book.requests.get(&key) {
            None => assert!(expected.first_save.is_none() && expected.marker.is_none()),
            Some(ApprovalState::StoppedBeforeRaise {
                contents_digest,
                cause,
                ..
            }) => {
                assert!(expected.first_save.is_none());
                assert_eq!(
                    contents_digest,
                    &scenario
                        .raise(row, expected.marker.unwrap())
                        .contents_digest()
                        .unwrap()
                );
                assert_eq!(cause, &HolderStopCause::AskReplaced);
            }
            Some(ApprovalState::Waiting { saved }) => {
                let (changed, at) = expected.first_save.unwrap();
                assert!(expected.ending.is_none());
                assert_eq!(saved.raise, scenario.raise(row, changed));
                assert_eq!(saved.window.raised_at(), time(at));
                assert_eq!(saved.window.deadline(), time(at + HOUR));
                assert_eq!(saved.waiting_status(), wire_waiting(expected.forwarded));
            }
            Some(ApprovalState::Ended { saved, closed }) => {
                let (changed, at) = expected.first_save.unwrap();
                let (ending, ended_at, delivery) = expected.ending.unwrap();
                assert_eq!(saved.raise, scenario.raise(row, changed));
                assert_eq!(saved.window.raised_at(), time(at));
                assert_eq!(saved.waiting_status(), wire_waiting(expected.forwarded));
                assert_eq!(
                    closed.ending.clone().into_ending(),
                    scenario.ending(row, ending)
                );
                assert_eq!(closed.delivery, wire_delivery(delivery));
                assert_eq!(closed.ended_at, time(ended_at));
            }
            Some(ApprovalState::Unraised) => panic!("unraised is not a retained row"),
        }
    }
}

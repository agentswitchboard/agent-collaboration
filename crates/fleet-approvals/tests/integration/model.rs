#[path = "model_observation.rs"]
mod observation;
use observation::{assert_effects, assert_state};

use approval_request_protocol::{
    ApprovalWindow, CancelReason, DecidedBy, DeliverEndingReply, DeliverEndingRequest,
    HolderStopCause, PassOnRequest, Raise, WaitingStatus,
};
use fleet_approvals::{
    AdmittedDecider, ApprovalActor, ApprovalCollection, ApprovalCollectionEvent,
    ApprovalEnrollment, ApprovalRequestKey, ApprovalRun, ApprovalSession, FactorStanding,
};
use hegel::generators as gs;
use serde_json::{Value, json};

use super::fixtures::{
    HOUR, answer, decide_event, enrollment, key, person_id, provider, provider_stop, raise_event,
    router_run, routing, run_event, session, setup, time,
};
use super::model_ledger::{
    Actor, Delivery, Ending, Ledger, Offer, ROWS, RUN_ENROLLMENTS, Step, View,
};

struct Scenario {
    enrollments: [ApprovalEnrollment; 2],
    parent: [ApprovalSession; 2],
    requester: [ApprovalSession; 2],
    unrelated: [ApprovalSession; 2],
    runs: [ApprovalRun; 3],
}

impl Scenario {
    fn setup() -> (Self, ApprovalCollection) {
        let (book, first, requester, parent) = setup();
        let old = first.enrollment.clone();
        let replacement = enrollment(8000, 9001);
        let runs = [
            first,
            ApprovalRun {
                enrollment: old.clone(),
                router_run: router_run(9001),
            },
            ApprovalRun {
                enrollment: replacement.clone(),
                router_run: router_run(9002),
            },
        ];
        let scenario = Self {
            parent: [parent, session(&replacement, 9010)],
            requester: [requester, session(&replacement, 9011)],
            unrelated: [session(&old, 8010), session(&replacement, 9012)],
            enrollments: [old, replacement],
            runs,
        };
        (scenario, book)
    }

    fn key(&self, row: usize) -> ApprovalRequestKey {
        key(&self.runs[ROWS[row].run], u16::try_from(row + 1).unwrap())
    }

    fn actor(&self, row: usize, actor: Actor) -> ApprovalActor {
        let scope = RUN_ENROLLMENTS[ROWS[row].run];
        match actor {
            Actor::FirstPerson => ApprovalActor::Person(person_id(10)),
            Actor::SecondPerson => ApprovalActor::Person(person_id(11)),
            Actor::Parent => ApprovalActor::Session(self.parent[scope].clone()),
            Actor::Unrelated => ApprovalActor::Session(self.unrelated[scope].clone()),
        }
    }

    fn identity(&self, row: usize, actor: Actor) -> DecidedBy {
        match self.actor(row, actor) {
            ApprovalActor::Person(person_id) => DecidedBy::Person { person_id },
            ApprovalActor::Session(session) => DecidedBy::Session {
                session: session.session_id,
            },
        }
    }

    fn raise(&self, row: usize, changed: bool) -> Raise {
        let requester = &self.requester[RUN_ENROLLMENTS[ROWS[row].run]];
        provider(
            &self.key(row),
            requester,
            if changed { "Changed" } else { "Original" },
        )
    }

    fn frame(&self, row: usize, offer: Offer) -> DeliverEndingRequest {
        DeliverEndingRequest::from_raise(
            &self.raise(row, offer.changed),
            self.ending(row, offer.ending),
        )
        .unwrap()
    }

    fn input(&self, step: Step, ledger: &Ledger) -> Option<ApprovalCollectionEvent> {
        Some(match step {
            Step::Raise { row, changed } => {
                let scope = ROWS[row];
                raise_event(
                    &self.runs[scope.run],
                    self.raise(row, changed),
                    routing(
                        scope
                            .delegate
                            .then_some(&self.parent[RUN_ENROLLMENTS[scope.run]]),
                    ),
                )
            }
            Step::Stop { row, changed } => ApprovalCollectionEvent::Stop {
                run: self.runs[ROWS[row].run].clone(),
                request: provider_stop(&self.raise(row, changed)),
            },
            Step::Decide {
                row,
                operation,
                choice,
                actor,
            } => {
                let decider = match self.actor(row, actor) {
                    ApprovalActor::Person(person_id) => AdmittedDecider::Person {
                        person_id,
                        factor: FactorStanding::Fresh,
                    },
                    ApprovalActor::Session(session) => AdmittedDecider::Session(session),
                };
                decide_event(&self.key(row), decider, operation, answer(choice))
            }
            Step::Read { row, actor } => {
                let ApprovalActor::Person(person_id) = self.actor(row, actor) else {
                    return None;
                };
                ApprovalCollectionEvent::Read {
                    key: self.key(row),
                    person_id,
                }
            }
            Step::Forward { row, actor } => {
                let ApprovalActor::Session(parent) = self.actor(row, actor) else {
                    return None;
                };
                let key = self.key(row);
                let request = PassOnRequest {
                    request_id: key.request_id.clone(),
                };
                ApprovalCollectionEvent::PassOn {
                    key,
                    parent,
                    request,
                }
            }
            Step::Alarm { row, stale_token } => {
                let raised_at = ledger.rows[row].first_save.map_or(0, |(_, at)| at);
                ApprovalCollectionEvent::Alarm {
                    key: self.key(row),
                    deadline: time(raised_at + HOUR + i64::from(stale_token)),
                }
            }
            Step::Deliver {
                row,
                reply,
                wrong_ending,
            } => {
                let mut offer = ledger.rows[row].offered?;
                if wrong_ending {
                    offer.ending = if offer.ending == Ending::TimedOut {
                        Ending::SessionEnded
                    } else {
                        Ending::TimedOut
                    };
                }
                ApprovalCollectionEvent::DeliveryReply {
                    run: self.runs[ROWS[row].run].clone(),
                    request: Box::new(self.frame(row, offer)),
                    reply,
                }
            }
            Step::Register(run) => ApprovalCollectionEvent::RegisterRun {
                run: self.runs[run].clone(),
            },
            Step::EndSession { enrollment, parent } => ApprovalCollectionEvent::EndSession {
                session: if parent {
                    self.parent[enrollment].clone()
                } else {
                    self.requester[enrollment].clone()
                },
            },
            Step::EndEnrollment(enrollment) => ApprovalCollectionEvent::EndEnrollment {
                enrollment: self.enrollments[enrollment].clone(),
            },
            Step::ReplaceEnrollment => ApprovalCollectionEvent::ActivateEnrollment {
                enrollment: self.enrollments[1].clone(),
            },
        })
    }

    fn ending(&self, row: usize, ending: Ending) -> approval_request_protocol::Ending {
        use approval_request_protocol::Ending as ProtocolEnding;
        match ending {
            Ending::Decided { choice, actor } => ProtocolEnding::Decided {
                answer: answer(choice),
                by: self.identity(row, actor),
            },
            Ending::TimedOut => ProtocolEnding::TimedOut {},
            Ending::Stopped => ProtocolEnding::Cancelled {
                reason: CancelReason::Stopped {
                    cause: HolderStopCause::AskReplaced,
                },
            },
            Ending::SessionEnded => ProtocolEnding::Cancelled {
                reason: CancelReason::SessionEnded {},
            },
            Ending::RequesterGone => ProtocolEnding::Cancelled {
                reason: CancelReason::RequesterGone {},
            },
            Ending::EnrollmentEnded => ProtocolEnding::Cancelled {
                reason: CancelReason::EnrollmentEnded {},
            },
        }
    }

    fn recipient_ending(&self, row: usize, actor: Actor, ending: Ending) -> Value {
        if let Ending::Decided {
            choice,
            actor: giver,
        } = ending
        {
            let by = self.identity(row, giver);
            if giver == actor {
                json!({"kind":"giver","decision":{"by":by,"answer":choice}})
            } else {
                json!({"kind":"standing","ending":{"kind":"decided","by":by}})
            }
        } else {
            json!({"kind":"standing","ending":self.ending(row, ending)})
        }
    }

    fn expected_read(&self, row: usize, actor: Actor, view: View, changed: bool) -> Value {
        let Raise::ProviderApproval { offer, .. } = self.raise(row, changed) else {
            panic!("the model fixture owns an approval offer");
        };
        let state = if let Some((ending, at, delivery)) = view.ending {
            json!({"kind":"ended","ending":self.recipient_ending(row, actor, ending),
                "endedAt":time(at), "delivery": wire_delivery(delivery)})
        } else {
            json!({"kind":"waiting","status":wire_waiting(view.forwarded)})
        };
        json!({"kind":"providerApproval","requestId":self.key(row).request_id,"offer":offer,
            "state":state,"window":ApprovalWindow::from_first_save(time(view.raised_at)).unwrap()})
    }
}

fn wire_waiting(forwarded: bool) -> WaitingStatus {
    if forwarded {
        WaitingStatus::PassedOn
    } else {
        WaitingStatus::Pending
    }
}

fn wire_delivery(delivery: Delivery) -> approval_request_protocol::Delivery {
    use approval_request_protocol::Delivery as Wire;
    match delivery {
        Delivery::Unpaid => Wire::Owed {},
        Delivery::Received => Wire::ReceivedByHolder {},
        Delivery::Closed => Wire::ClosedWithoutReceipt {},
    }
}

#[hegel::test(test_cases = 400)]
fn generated_histories_match_the_separate_smaller_central_ledger(case: hegel::TestCase) {
    let (scenario, mut book) = Scenario::setup();
    let mut ledger = Ledger::new();
    let mut compare = |step, sampled| {
        if let Some(input) = scenario.input(step, &ledger) {
            let wanted = ledger.apply(step, sampled);
            let actual = run_event(&mut book, input, sampled);
            assert_effects(&scenario, &ledger, step, &wanted, &actual);
            assert_state(&scenario, &ledger, &book);
        }
    };
    // Prefixes keep correlated delivery, visibility, and batch settlement reachable in short histories.
    let prefix = case.draw(gs::integers::<u8>().max_value(3));
    if prefix != 0 {
        compare(
            Step::Raise {
                row: 0,
                changed: false,
            },
            0,
        );
        compare(
            Step::Raise {
                row: 1,
                changed: false,
            },
            0,
        );
    }
    if prefix == 2 {
        compare(
            Step::Forward {
                row: 0,
                actor: Actor::Parent,
            },
            1,
        );
        compare(
            Step::Decide {
                row: 0,
                operation: 101,
                choice: 2,
                actor: Actor::FirstPerson,
            },
            2,
        );
        compare(
            Step::Decide {
                row: 1,
                operation: 101,
                choice: 2,
                actor: Actor::FirstPerson,
            },
            3,
        );
        compare(
            Step::Read {
                row: 0,
                actor: Actor::FirstPerson,
            },
            3,
        );
        compare(
            Step::Read {
                row: 0,
                actor: Actor::SecondPerson,
            },
            3,
        );
        compare(
            Step::Deliver {
                row: 0,
                reply: DeliverEndingReply::NotHeld {},
                wrong_ending: false,
            },
            4,
        );
        compare(
            Step::Deliver {
                row: 0,
                reply: DeliverEndingReply::Received {},
                wrong_ending: false,
            },
            5,
        );
    } else if prefix == 3 {
        compare(
            Step::EndSession {
                enrollment: 0,
                parent: false,
            },
            1,
        );
        compare(Step::Register(1), 2);
        compare(
            Step::Raise {
                row: 0,
                changed: false,
            },
            3,
        );
        compare(Step::ReplaceEnrollment, 4);
        compare(Step::Register(2), 5);
        compare(
            Step::Raise {
                row: 3,
                changed: false,
            },
            6,
        );
        compare(Step::EndEnrollment(1), 7);
    }
    for _ in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(32)) {
        let row = usize::from(case.draw(gs::integers::<u8>().max_value(3)));
        let flags = case.draw(gs::booleans());
        let sampled = case.draw(gs::sampled_from(vec![
            0,
            1,
            HOUR - 1,
            HOUR,
            HOUR + 1,
            2 * HOUR,
        ]));
        let step = match case.draw(gs::integers::<u8>().max_value(12)) {
            0 | 1 => Step::Raise {
                row,
                changed: flags,
            },
            2 => Step::Stop {
                row,
                changed: flags,
            },
            3 => Step::Decide {
                row,
                operation: case.draw(gs::integers::<u16>().min_value(101).max_value(105)),
                choice: case.draw(gs::integers::<u8>().min_value(1).max_value(6)),
                actor: case.draw(gs::sampled_from(vec![
                    Actor::FirstPerson,
                    Actor::SecondPerson,
                    Actor::Parent,
                    Actor::Unrelated,
                ])),
            },
            4 => Step::Forward {
                row,
                actor: if flags {
                    Actor::Parent
                } else {
                    Actor::Unrelated
                },
            },
            5 => Step::Read {
                row,
                actor: if flags {
                    Actor::FirstPerson
                } else {
                    Actor::SecondPerson
                },
            },
            6 => Step::Alarm {
                row,
                stale_token: flags,
            },
            7 => Step::Deliver {
                row,
                wrong_ending: flags,
                reply: case.draw(gs::sampled_from(vec![
                    DeliverEndingReply::Received {},
                    DeliverEndingReply::NotHeld {},
                    DeliverEndingReply::StopOwed {},
                    DeliverEndingReply::HolderNotReady {},
                    DeliverEndingReply::EndingDiffers {},
                ])),
            },
            8 => Step::Register(usize::from(case.draw(gs::integers::<u8>().max_value(2)))),
            9 => Step::EndSession {
                enrollment: usize::from(flags),
                parent: case.draw(gs::booleans()),
            },
            10 => Step::ReplaceEnrollment,
            _ => Step::EndEnrollment(usize::from(flags)),
        };
        compare(step, sampled);
    }
}

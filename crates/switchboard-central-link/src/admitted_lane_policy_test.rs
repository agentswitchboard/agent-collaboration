use std::{sync::Arc, time::Duration};

use central_link_client::{ClientAction, LinkClient, WelcomeClockAnchor};
use central_link_protocol::{DataFrame, FrameDirection, LinkFrame, decode_text};
use hegel::generators as gs;
use router_clock::ManualClock;

use super::*;

fn tick(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

fn correlation(number: u8) -> CorrelationId {
    format!("01890f2e-7b4c-7cc0-98c4-{number:012x}")
        .parse()
        .unwrap()
}

fn admitted_connection() -> AdmittedConnection {
    let central_time = "2030-01-01T00:00:00Z".parse().unwrap();
    let mut client = LinkClient::new(
        "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap(),
        Arc::new(ManualClock::new(central_time)),
    );
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("test link client must begin a connection attempt");
    };
    AdmittedConnection {
        connection: attempt,
        clock_anchor: WelcomeClockAnchor {
            central_time,
            observed_at: tick(0),
        },
    }
}

fn write_request(member: u8) -> LinkRequest {
    let wire = format!(
        r#"{{"kind":"request","request":{{"correlationId":"{}","caller":{{"kind":"session","value":{{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}}},"payload":{{"kind":"mail","request":{{"requestId":"01890f2e-7b4c-7cc0-98c4-00000000000{member}","operation":{{"kind":"setNoticeCadence","cadence":"m25"}}}}}}}}}}"#,
        String::from(correlation(member))
    );
    let LinkFrame::Data(DataFrame::Request { request }) =
        decode_text(&wire, FrameDirection::ToCentral).unwrap()
    else {
        panic!("test expected a write request");
    };
    *request
}

#[derive(Clone, Copy, Debug)]
enum GeneratedStep {
    Admit(usize),
    Offer(usize),
    BeginWrite,
    FlushWrite,
    Reply(usize, u8),
    Tick(u64),
    Close(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedStage {
    Idle,
    Queued,
    Writing,
    Awaiting(u64),
}

struct LaneOracle {
    active: Option<usize>,
    stage: ExpectedStage,
    observed: [Option<u64>; 2],
    elapsed: u64,
}

#[derive(Debug, Eq, PartialEq)]
enum OracleSettlement {
    NoSettlement,
    NotReached,
    BatchMembers([Option<u64>; 2]),
}

impl LaneOracle {
    fn apply(&mut self, step: GeneratedStep) -> OracleSettlement {
        match step {
            GeneratedStep::Admit(socket) if self.active.is_none() => {
                self.active = Some(socket);
                self.stage = ExpectedStage::Idle;
            }
            GeneratedStep::Offer(socket)
                if self.active == Some(socket) && self.stage == ExpectedStage::Idle =>
            {
                self.stage = ExpectedStage::Queued;
                self.observed = [None; 2];
            }
            GeneratedStep::BeginWrite if self.stage == ExpectedStage::Queued => {
                self.stage = ExpectedStage::Writing
            }
            GeneratedStep::FlushWrite if self.stage == ExpectedStage::Writing => {
                self.stage = ExpectedStage::Awaiting(self.elapsed + 3)
            }
            GeneratedStep::Reply(socket, member)
                if self.active == Some(socket)
                    && matches!(self.stage, ExpectedStage::Awaiting(deadline) if self.elapsed < deadline) =>
            {
                if let Some(seen) = member
                    .checked_sub(1)
                    .and_then(|index| self.observed.get_mut(usize::from(index)))
                {
                    seen.get_or_insert(self.elapsed);
                    if self.observed.iter().all(Option::is_some) {
                        self.stage = ExpectedStage::Idle;
                        return OracleSettlement::BatchMembers(self.observed);
                    }
                }
            }
            GeneratedStep::Tick(delta) => {
                self.elapsed += delta;
                if matches!(self.stage, ExpectedStage::Awaiting(deadline) if self.elapsed >= deadline)
                {
                    self.stage = ExpectedStage::Idle;
                    return OracleSettlement::BatchMembers(self.observed);
                }
            }
            GeneratedStep::Close(socket) if self.active == Some(socket) => {
                self.active = None;
                let custody = std::mem::replace(&mut self.stage, ExpectedStage::Idle);
                return match custody {
                    ExpectedStage::Idle => OracleSettlement::NoSettlement,
                    ExpectedStage::Queued => OracleSettlement::NotReached,
                    ExpectedStage::Writing | ExpectedStage::Awaiting(_) => {
                        OracleSettlement::BatchMembers(self.observed)
                    }
                };
            }
            _ => {}
        }
        OracleSettlement::NoSettlement
    }
}

#[hegel::test(test_cases = 250)]
fn generated_admitted_orders_preserve_exact_custody_and_partial_answers(case: hegel::TestCase) {
    let first_request = write_request(1);
    let sample_reply = ReplyPayload::central_unavailable(first_request.payload());
    let requests = LinkRequestBatch::new(vec![first_request, write_request(2)]).unwrap();
    let admitted = [admitted_connection(), admitted_connection()];
    let connection = |index| {
        admitted
            .get(index)
            .map(|view: &AdmittedConnection| view.connection.clone())
            .unwrap()
    };
    let generated = case.draw(
        gs::vecs(gs::sampled_from(vec![
            GeneratedStep::Admit(0),
            GeneratedStep::Admit(1),
            GeneratedStep::Offer(0),
            GeneratedStep::Offer(1),
            GeneratedStep::BeginWrite,
            GeneratedStep::FlushWrite,
            GeneratedStep::Reply(0, 1),
            GeneratedStep::Reply(0, 2),
            GeneratedStep::Reply(0, 3),
            GeneratedStep::Reply(1, 1),
            GeneratedStep::Reply(1, 2),
            GeneratedStep::Close(0),
            GeneratedStep::Close(1),
            GeneratedStep::Tick(0),
            GeneratedStep::Tick(1),
            GeneratedStep::Tick(3),
        ]))
        .min_size(1)
        .max_size(30),
    );
    let setup = [
        GeneratedStep::Admit(0),
        GeneratedStep::Offer(0),
        GeneratedStep::BeginWrite,
        GeneratedStep::FlushWrite,
        GeneratedStep::Reply(0, 1),
        GeneratedStep::Tick(1),
        GeneratedStep::Reply(0, 1),
        GeneratedStep::Reply(0, 2),
        GeneratedStep::Offer(0),
        GeneratedStep::BeginWrite,
        GeneratedStep::FlushWrite,
    ];
    let mut oracle = LaneOracle {
        active: None,
        stage: ExpectedStage::Idle,
        observed: [None; 2],
        elapsed: 0,
    };
    let mut state = LaneState::WaitingForAdmission;
    for step in setup.into_iter().chain(generated) {
        let event = match step {
            GeneratedStep::Admit(index) => LaneInput::Admit(admitted.get(index).unwrap().clone()),
            GeneratedStep::Offer(index) => LaneInput::Offer {
                connection: connection(index),
                expected: ExpectedReply::batch(&requests),
            },
            GeneratedStep::BeginWrite => LaneInput::WriteStarted,
            GeneratedStep::FlushWrite => LaneInput::WriteFlushed {
                deadline: tick(oracle.elapsed + 3),
            },
            GeneratedStep::Reply(index, member) => LaneInput::Reply {
                connection: connection(index),
                reply: SocketReply::Request {
                    correlation_id: correlation(member),
                    result: Box::new(sample_reply.clone()),
                },
                now: tick(oracle.elapsed),
            },
            GeneratedStep::Tick(delta) => LaneInput::Elapsed {
                now: tick(oracle.elapsed + delta),
            },
            GeneratedStep::Close(index) => LaneInput::Close {
                connection: connection(index),
            },
        };
        let event_tag = event.event_tag();
        let expected = oracle.apply(step);
        let actual = match AdmittedLanePolicy::transition(&state, event) {
            Ok((next, effects)) => {
                assert!(AdmittedLanePolicy::transitions().contains(&Transition {
                    from: state.state_tag(),
                    event: event_tag,
                    to: next.state_tag(),
                    terminal: next.is_terminal(),
                }));
                state = next;
                effects
            }
            Err(_) => Vec::new(),
        };
        let expected = match expected {
            OracleSettlement::NoSettlement => vec![],
            OracleSettlement::NotReached => vec![LaneEffect::Settle(LaneResult::Untouched(
                UntouchedCommand::ConnectionUnavailable,
            ))],
            OracleSettlement::BatchMembers(answered) => {
                vec![LaneEffect::Settle(LaneResult::Batch(
                    CheckedBatchResults::new(
                        &requests,
                        [1, 2].into_iter().zip(answered).map(|(member, answered)| {
                            if let Some(accepted_at) = answered {
                                WriteBatchMemberDisposition::Answered {
                                    reply: CorrelatedRequestReply {
                                        correlation_id: correlation(member),
                                        result: Box::new(sample_reply.clone()),
                                    },
                                    accepted_at: tick(accepted_at),
                                }
                            } else {
                                WriteBatchMemberDisposition::AnswerLost {
                                    correlation_id: correlation(member),
                                }
                            }
                        }),
                    )
                    .unwrap(),
                ))]
            }
        };
        assert_eq!(actual, expected, "effect after {step:?}");
        assert_eq!(
            state.admitted().map(|value| value.connection.clone()),
            oracle.active.map(connection),
            "admission after {step:?}"
        );
        assert_eq!(
            state.can_offer(),
            oracle.active.is_some() && oracle.stage == ExpectedStage::Idle,
            "offer gate after {step:?}"
        );
        assert_eq!(
            state.reply_deadline(),
            match oracle.stage {
                ExpectedStage::Awaiting(at) => Some(tick(at)),
                _ => None,
            },
            "deadline after {step:?}"
        );
    }
}

use super::{Effect, Input, Policy, State, at, attempt_pair, identifier, machine};
use central_link_client::{ConnectionAttempt, EnrollmentEnd, LinkFault};
use central_link_protocol::{MachineLinkCloseCode, TokenRenewalRefusal};
use hegel::generators as gs;
use state_machine_core::{StateMachine, TransitionError};

use crate::link_policy::{DialFailure, LeaseVersion, LeaseWindow, LinkRefusal, LinkStatus};

#[derive(Clone, Copy, Debug)]
enum OrderEvent {
    Binding(u8),
    Dial {
        attempt: bool,
        binding: u8,
        version: u64,
    },
    Unauthorized(bool),
    Welcome {
        attempt: bool,
        correct_machine: bool,
    },
    Tick(u64),
    Lease {
        attempt: bool,
        binding: u8,
        version: u64,
        correlation: u8,
    },
    Reply {
        attempt: bool,
        correlation: u8,
        accepted: bool,
    },
    Close {
        attempt: bool,
        ending: Option<MachineLinkCloseCode>,
    },
    SourceEnded(u8),
}

#[derive(Default)]
struct LinkOracle {
    binding: Option<u8>,
    socket: Option<bool>,
    newest_version: u64,
    minimum_next_version: Option<u64>,
    rejected_once: bool,
    fault_rearm: Option<u64>,
    confirmed_until: Option<u64>,
    request_renewal_at: Option<u64>,
    awaiting_lease: bool,
    awaiting_reply: Option<u8>,
    final_cause: Option<EnrollmentEnd>,
    elapsed: u64,
}

impl LinkOracle {
    fn deadline(&self) -> Option<u64> {
        self.fault_rearm
            .or(self.request_renewal_at)
            .or(self.confirmed_until)
    }

    fn status(&self) -> LinkStatus {
        if let Some(cause) = self.final_cause {
            LinkStatus::EnrollmentEnded { cause }
        } else if self.fault_rearm.is_some() {
            LinkStatus::Fault {
                kind: LinkFault::FreshCredentialRefused,
            }
        } else if self.confirmed_until.is_some() {
            LinkStatus::Welcomed {
                machine_id: machine(),
            }
        } else if self.binding.is_some() {
            LinkStatus::Connecting
        } else {
            LinkStatus::NotEnrolled
        }
    }

    fn clear_socket(&mut self) {
        self.socket = None;
        self.confirmed_until = None;
        self.request_renewal_at = None;
        self.awaiting_lease = false;
        self.awaiting_reply = None;
    }

    fn confirm_window(&mut self, until: u64) {
        self.confirmed_until = Some(until);
        let duration = until - self.elapsed;
        self.request_renewal_at = Some(until - (duration / 2).min(60));
    }

    fn step(
        &mut self,
        event: OrderEvent,
        attempts: &(ConnectionAttempt, ConnectionAttempt),
    ) -> Result<Vec<Effect>, TransitionError<LinkRefusal>> {
        let attempt_id = |slot: bool| {
            if slot {
                attempts.1.clone()
            } else {
                attempts.0.clone()
            }
        };
        let back_off = |slot| Effect::BackOff {
            attempt: attempt_id(slot),
        };
        let request_lease = |binding, floor: Option<u64>| Effect::NeedLease {
            binding,
            newer_than: floor.map(LeaseVersion),
        };
        let current = self.socket;
        let owned_binding = self.binding.unwrap_or_default();
        let mut effects = Vec::new();
        match event {
            OrderEvent::Binding(binding) if self.binding != Some(binding) => {
                effects.extend(current.map(back_off));
                *self = Self {
                    binding: Some(binding),
                    elapsed: self.elapsed,
                    ..Self::default()
                };
                effects.push(request_lease(binding, None));
            }
            OrderEvent::Dial {
                attempt,
                binding,
                version,
            } if self.binding == Some(binding)
                && current.is_none()
                && self.final_cause.is_none()
                && self.fault_rearm.is_none() =>
            {
                if self
                    .minimum_next_version
                    .is_some_and(|floor| version <= floor)
                {
                    return Err(TransitionError::Domain(LinkRefusal::UnusableLease));
                }
                self.socket = Some(attempt);
                self.newest_version = version;
            }
            OrderEvent::Unauthorized(attempt)
                if current == Some(attempt) && self.confirmed_until.is_none() =>
            {
                self.clear_socket();
                if self.rejected_once {
                    self.fault_rearm = Some(self.elapsed + 60);
                } else {
                    self.rejected_once = true;
                    self.minimum_next_version = Some(self.newest_version);
                    effects.extend([
                        back_off(attempt),
                        request_lease(owned_binding, self.minimum_next_version),
                    ]);
                }
            }
            OrderEvent::Welcome {
                attempt,
                correct_machine,
            } if current == Some(attempt) && self.confirmed_until.is_none() => {
                if !correct_machine {
                    return Err(TransitionError::Domain(LinkRefusal::WrongMachine));
                }
                self.confirm_window(self.elapsed + 120);
            }
            OrderEvent::Tick(amount) => {
                self.elapsed += amount;
                if self.fault_rearm.is_some_and(|at| self.elapsed >= at) {
                    self.fault_rearm = None;
                    self.rejected_once = false;
                    self.minimum_next_version = None;
                    effects.push(request_lease(owned_binding, None));
                } else if let Some(attempt) = current {
                    if self.confirmed_until.is_some_and(|at| self.elapsed >= at) {
                        self.clear_socket();
                        self.rejected_once = false;
                        self.minimum_next_version = Some(self.newest_version);
                        effects.extend([
                            back_off(attempt),
                            request_lease(owned_binding, self.minimum_next_version),
                        ]);
                    } else if self.request_renewal_at.is_some_and(|at| self.elapsed >= at) {
                        self.request_renewal_at = None;
                        self.awaiting_lease = true;
                        effects.push(request_lease(owned_binding, Some(self.newest_version)));
                    }
                }
            }
            OrderEvent::Lease {
                attempt,
                binding,
                version,
                correlation,
            } if current == Some(attempt) && owned_binding == binding && self.awaiting_lease => {
                if version <= self.newest_version {
                    return Err(TransitionError::Domain(LinkRefusal::UnusableLease));
                }
                self.newest_version = version;
                self.awaiting_lease = false;
                self.awaiting_reply = Some(correlation);
                effects.push(Effect::RenewOnSocket {
                    attempt: attempt_id(attempt),
                    correlation_id: identifier(correlation),
                });
            }
            OrderEvent::Reply {
                attempt,
                correlation,
                accepted,
            } if current == Some(attempt) && self.awaiting_reply == Some(correlation) => {
                self.awaiting_reply = None;
                if accepted {
                    self.confirm_window(self.elapsed + 180);
                }
            }
            OrderEvent::Close { attempt, ending } if current == Some(attempt) => {
                self.clear_socket();
                self.final_cause = match ending {
                    Some(MachineLinkCloseCode::Revoked) => Some(EnrollmentEnd::Revoked),
                    Some(MachineLinkCloseCode::Inactive) => Some(EnrollmentEnd::Inactive),
                    Some(MachineLinkCloseCode::Expired) | None => None,
                };
                if let Some(cause) = self.final_cause {
                    effects.push(Effect::EndEnrollment {
                        binding: owned_binding,
                        cause,
                    });
                } else {
                    self.rejected_once = false;
                    self.minimum_next_version = (ending == Some(MachineLinkCloseCode::Expired))
                        .then_some(self.newest_version);
                    effects.extend([
                        back_off(attempt),
                        request_lease(owned_binding, self.minimum_next_version),
                    ]);
                }
            }
            OrderEvent::SourceEnded(binding)
                if self.binding == Some(binding) && self.final_cause.is_none() =>
            {
                self.clear_socket();
                self.fault_rearm = None;
                self.final_cause = Some(EnrollmentEnd::LeaseRefused);
                effects.push(Effect::EndEnrollment {
                    binding,
                    cause: EnrollmentEnd::LeaseRefused,
                });
            }
            _ => return Err(TransitionError::Stale),
        }
        Ok(effects)
    }
}

fn production_input(
    event: OrderEvent,
    elapsed: u64,
    attempts: &(ConnectionAttempt, ConnectionAttempt),
) -> Input {
    let attempt_id = |slot: bool| {
        if slot {
            attempts.1.clone()
        } else {
            attempts.0.clone()
        }
    };
    let lease = |binding, version| LeaseWindow {
        binding,
        version: LeaseVersion(version),
        usable_until: at(elapsed + 600),
    };
    match event {
        OrderEvent::Binding(binding) => Input::BindingAvailable(binding),
        OrderEvent::Dial {
            attempt,
            binding,
            version,
        } => Input::DialStarted {
            attempt: attempt_id(attempt),
            lease: lease(binding, version),
            now: at(elapsed),
        },
        OrderEvent::Unauthorized(attempt) => Input::DialFailed {
            attempt: attempt_id(attempt),
            reason: DialFailure::Unauthorized,
            now: at(elapsed),
        },
        OrderEvent::Welcome {
            attempt,
            correct_machine,
        } => Input::Welcome {
            attempt: attempt_id(attempt),
            machine_id: if correct_machine {
                machine()
            } else {
                "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap()
            },
            deadline: at(elapsed + 120),
            now: at(elapsed),
        },
        OrderEvent::Tick(amount) => Input::Tick {
            now: at(elapsed + amount),
        },
        OrderEvent::Lease {
            attempt,
            binding,
            version,
            correlation,
        } => Input::RenewalLease {
            attempt: attempt_id(attempt),
            lease: lease(binding, version),
            correlation_id: identifier(correlation),
            now: at(elapsed),
        },
        OrderEvent::Reply {
            attempt,
            correlation,
            accepted: true,
        } => Input::RenewalAccepted {
            attempt: attempt_id(attempt),
            correlation_id: identifier(correlation),
            deadline: at(elapsed + 180),
            now: at(elapsed),
        },
        OrderEvent::Reply {
            attempt,
            correlation,
            accepted: false,
        } => Input::RenewalRefused {
            attempt: attempt_id(attempt),
            correlation_id: identifier(correlation),
            reason: TokenRenewalRefusal::ConnectionEnrollmentEnded,
        },
        OrderEvent::Close { attempt, ending } => Input::SocketClosed {
            attempt: attempt_id(attempt),
            code: ending,
        },
        OrderEvent::SourceEnded(binding) => Input::LeaseEnded { binding },
    }
}

#[hegel::test(test_cases = 250)]
fn generated_input_orders_match_an_independent_link_oracle(case: hegel::TestCase) {
    let samples = [false, true]
        .into_iter()
        .flat_map(|attempt| {
            [
                OrderEvent::Unauthorized(attempt),
                OrderEvent::Welcome {
                    attempt,
                    correct_machine: true,
                },
                OrderEvent::Welcome {
                    attempt,
                    correct_machine: false,
                },
                OrderEvent::Lease {
                    attempt,
                    binding: 1,
                    version: 2,
                    correlation: 1,
                },
                OrderEvent::Lease {
                    attempt,
                    binding: 2,
                    version: 3,
                    correlation: 2,
                },
                OrderEvent::Reply {
                    attempt,
                    correlation: 1,
                    accepted: true,
                },
                OrderEvent::Reply {
                    attempt,
                    correlation: 1,
                    accepted: false,
                },
                OrderEvent::Reply {
                    attempt,
                    correlation: 2,
                    accepted: true,
                },
                OrderEvent::Close {
                    attempt,
                    ending: None,
                },
                OrderEvent::Close {
                    attempt,
                    ending: Some(MachineLinkCloseCode::Expired),
                },
                OrderEvent::Close {
                    attempt,
                    ending: Some(MachineLinkCloseCode::Inactive),
                },
                OrderEvent::Close {
                    attempt,
                    ending: Some(MachineLinkCloseCode::Revoked),
                },
                OrderEvent::Dial {
                    attempt,
                    binding: 1,
                    version: 1,
                },
                OrderEvent::Dial {
                    attempt,
                    binding: 1,
                    version: 3,
                },
                OrderEvent::Dial {
                    attempt,
                    binding: 2,
                    version: 1,
                },
            ]
        })
        .chain([
            OrderEvent::Binding(1),
            OrderEvent::Binding(2),
            OrderEvent::SourceEnded(2),
            OrderEvent::Tick(0),
            OrderEvent::Tick(1),
            OrderEvent::Tick(59),
            OrderEvent::Tick(60),
            OrderEvent::Tick(120),
        ])
        .collect::<Vec<_>>();
    let inputs = case.draw(gs::vecs(gs::sampled_from(samples)).min_size(1).max_size(40));
    let attempts = attempt_pair();
    let mut oracle = LinkOracle::default();
    let mut state = State::new(machine());
    // Start each sequence at the renewal boundary. Generated suffixes can stay on this
    // socket, replace its binding, or redial, then interleave stale events and duplicates.
    let setup = [
        OrderEvent::Binding(1),
        OrderEvent::Dial {
            attempt: false,
            binding: 1,
            version: 1,
        },
        OrderEvent::Welcome {
            attempt: false,
            correct_machine: true,
        },
        OrderEvent::Tick(60),
        OrderEvent::Lease {
            attempt: false,
            binding: 1,
            version: 2,
            correlation: 1,
        },
    ];
    for (index, event) in setup.into_iter().chain(inputs).enumerate() {
        let input = production_input(event, oracle.elapsed, &attempts);
        let expected = oracle.step(event, &attempts);
        let actual = Policy::transition(&state, input).map(|(next, effects)| {
            state = next;
            effects
        });
        assert_eq!(
            actual, expected,
            "effects/refusal after step {index} {event:?}"
        );
        assert_eq!(
            state.status(),
            oracle.status(),
            "status after step {index} {event:?}"
        );
        assert_eq!(
            state.next_deadline(),
            oracle.deadline().map(at),
            "deadline after step {index} {event:?}"
        );
        assert_eq!(
            state.renewal_deadline(),
            oracle.confirmed_until.map(at),
            "confirmed deadline after step {index} {event:?}"
        );
    }
}

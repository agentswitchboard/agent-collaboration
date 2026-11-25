use approval_request_protocol::{
    ApprovalOffer, CallOffer, DeliverEndingReply, DeliverEndingRequest, InvocationDigest,
    OfferedOption, OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime, ParkedCallHolder,
    ProviderHolder, Raise, RegisterRunReply, RegisterRunRequest, SessionCallOperation,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{
    LinkAttemptDisposition, WaitEffect, WaitEvent, WaitState, WaitingRequest,
    gate_access::{GateWaitEvent, step_under_gate},
};
use switchboard_run_gate::{
    CallAttempt, CallKey, DialAttempt, LinkAttempt, RunAuthority, RunGate, RunGateEffect,
    RunGateEvent, RunGateState,
};

pub const RESPONDER: u8 = 1;
pub type GateEvent = RunGateEvent<u8, u8>;
pub type GateEffect = RunGateEffect<u8, u8>;
pub type RequestEvent = WaitEvent<u8, u8>;
pub type RequestEffect = WaitEffect<u8, u8, u8>;
pub type RequestMachine = WaitingRequest<u8, u8, u8>;

pub fn ending_reply(connection: u8, reply: DeliverEndingReply) -> RequestEffect {
    RequestEffect::EndingReply {
        authority: authority(),
        link: link(connection),
        reply,
    }
}

pub fn try_publish(
    wait: &WaitState<u8, u8>,
    gate: &RunGateState<u8, u8>,
    attempt: CallAttempt,
) -> state_machine_core::StateMachineResult<RequestMachine> {
    let link = gate.ready_link().ok_or(TransitionError::Stale)?.clone();
    step_under_gate(
        wait,
        gate,
        GateWaitEvent::Publish {
            authority: authority(),
            call: CallKey { link, attempt },
        },
    )
}

pub fn try_offer(
    wait: &WaitState<u8, u8>,
    gate: &RunGateState<u8, u8>,
    authority: RunAuthority<u8>,
    link: LinkAttempt<u8>,
    request: DeliverEndingRequest,
) -> state_machine_core::StateMachineResult<RequestMachine> {
    step_under_gate(
        wait,
        gate,
        GateWaitEvent::DeliverEnding {
            authority,
            link,
            request,
        },
    )
}

pub fn link(connection: u8) -> LinkAttempt<u8> {
    LinkAttempt {
        connection,
        dial: DialAttempt::new(u64::from(connection)),
    }
}

pub fn call(connection: u8, attempt: u64) -> CallKey<u8> {
    CallKey {
        link: link(connection),
        attempt: CallAttempt::new(attempt),
    }
}

pub fn authority() -> RunAuthority<u8> {
    RunAuthority {
        enrollment: 1,
        router_run: "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap(),
    }
}

pub fn raise() -> Raise {
    raise_for("01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap())
}

pub fn call_raise() -> Raise {
    Raise::UnprovenCall {
        request_id: raise().request_id().clone(),
        holder: ParkedCallHolder {
            router_run: authority().router_run,
        },
        invocation: InvocationDigest::from_bound_bytes(b"held end-for-good invocation"),
        offer: CallOffer {
            operation: SessionCallOperation::EndSessionForGood {
                session_id: "01890f2e-7b4c-7cc0-98c4-000000000003".parse().unwrap(),
            },
        },
    }
}

pub fn raise_for(request_id: workspace_ids::ApprovalRequestId) -> Raise {
    Raise::ProviderApproval {
        request_id,
        holder: ProviderHolder {
            session_id: "01890f2e-7b4c-7cc0-98c4-000000000003".parse().unwrap(),
            router_run: authority().router_run,
        },
        offer: ApprovalOffer {
            title: "Approval".to_owned().try_into().unwrap(),
            description: None,
            subject: None,
            options: OfferedOptions::new(vec![OfferedOption {
                id: OfferedOptionId::new(1).unwrap(),
                label: "Allow".to_owned().try_into().unwrap(),
                effect: OptionEffect::Allow,
                lasts: OptionLifetime::ThisRequest {},
            }])
            .unwrap(),
        },
    }
}

/// Test composition of the two pure reducers. The link and provider drive their declared inputs.
pub struct Mac {
    pub gate: RunGateState<u8, u8>,
    pub wait: WaitState<u8, u8>,
    next_attempt: u64,
}

impl Mac {
    pub fn new() -> Self {
        Self::with_raise(raise())
    }

    pub fn with_raise(raise: Raise) -> Self {
        Self {
            gate: RunGateState::new(authority()),
            wait: WaitState::bound(raise, RESPONDER).unwrap(),
            next_attempt: 1,
        }
    }

    pub fn gate_event(&mut self, event: GateEvent) -> Vec<GateEffect> {
        let (next, effects) = RunGate::transition(&self.gate, event).unwrap();
        self.gate = next;
        for effect in &effects {
            match effect {
                GateEffect::FenceWaits {
                    authority: ended, ..
                } => {
                    assert_eq!(ended, &authority());
                    self.wait_event(RequestEvent::AuthorityEnded);
                }
                GateEffect::RetireConnection { link } => self.retire_wait_link(link.clone()),
                GateEffect::Register { .. } => {}
            }
        }
        effects
    }

    pub fn welcome(&mut self, connection: u8) -> RegisterRunRequest {
        self.gate_event(GateEvent::DialStarted {
            link: link(connection),
        });
        self.gate_event(GateEvent::Welcome {
            authority: authority(),
            link: link(connection),
        });
        self.publish_registration(connection)
    }

    pub fn publish_registration(&mut self, connection: u8) -> RegisterRunRequest {
        let effects = self.gate_event(GateEvent::PublishRegistration {
            call: call(connection, 0),
        });
        assert_eq!(effects.len(), 1);
        effects
            .into_iter()
            .find_map(|effect| match effect {
                GateEffect::Register {
                    call: actual,
                    request,
                } => {
                    assert_eq!(actual, call(connection, 0));
                    Some(request)
                }
                GateEffect::FenceWaits { .. } | GateEffect::RetireConnection { .. } => None,
            })
            .unwrap()
    }

    pub fn registered(&mut self, connection: u8, reply: RegisterRunReply) {
        self.gate_event(GateEvent::RegistrationReply {
            call: call(connection, 0),
            reply,
        });
    }

    pub fn wait_event(&mut self, event: RequestEvent) -> Vec<RequestEffect> {
        let (next, effects) = RequestMachine::transition(&self.wait, event).unwrap();
        self.wait = next;
        for effect in &effects {
            if let RequestEffect::RunRefused(matched) = effect {
                self.gate_event(GateEvent::HolderAuthorityRefused(matched.clone()));
            }
        }
        effects
    }

    pub fn publish(&mut self) -> Vec<RequestEffect> {
        let attempt = CallAttempt::new(self.next_attempt);
        self.next_attempt += 1;
        let (next, effects) = try_publish(&self.wait, &self.gate, attempt).unwrap();
        self.wait = next;
        effects
    }

    pub fn offer(&mut self, connection: u8, request: DeliverEndingRequest) -> Vec<RequestEffect> {
        let (next, effects) = try_offer(
            &self.wait,
            &self.gate,
            authority(),
            link(connection),
            request,
        )
        .unwrap();
        self.wait = next;
        effects
    }

    pub fn lose_connection(&mut self, connection: u8) {
        self.gate_event(GateEvent::ConnectionLost {
            link: link(connection),
        });
        self.retire_wait_link(link(connection));
    }

    pub fn lose_answer(&mut self, connection: u8, attempt: u64) {
        assert!(
            self.wait_event(RequestEvent::AttemptSettled {
                call: call(connection, attempt),
                disposition: LinkAttemptDisposition::AnswerLost,
            })
            .is_empty()
        );
    }

    fn retire_wait_link(&mut self, link: LinkAttempt<u8>) {
        match RequestMachine::transition(&self.wait, RequestEvent::ConnectionRetired { link }) {
            Ok((next, effects)) => {
                assert!(effects.is_empty());
                self.wait = next;
            }
            Err(error) => assert_eq!(error, TransitionError::Stale),
        }
    }
}

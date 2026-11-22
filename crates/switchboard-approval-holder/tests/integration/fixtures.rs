use approval_request_protocol::{
    ApprovalOffer, OfferedOption, OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime,
    ProviderHolder, Raise, RegisterRunReply, RegisterRunRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::{RunAuthority, RunGate, RunGateEffect, RunGateEvent, RunGateState},
    waiting_request::{WaitEffect, WaitEvent, WaitState, WaitingRequest},
};

pub const RESPONDER: u8 = 1;
pub type GateEvent = RunGateEvent<u8, u8>;
pub type GateEffect = RunGateEffect<u8, u8>;
pub type RequestEvent = WaitEvent<u8, u8>;
pub type RequestEffect = WaitEffect<u8, u8>;

pub fn authority() -> RunAuthority<u8> {
    RunAuthority {
        enrollment: 1,
        router_run: "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap(),
    }
}

pub fn raise() -> Raise {
    raise_for("01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap())
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
}

impl Mac {
    pub fn new() -> Self {
        Self {
            gate: RunGateState::new(authority()),
            wait: WaitState::bound(raise(), RESPONDER).unwrap(),
        }
    }

    pub fn gate_event(&mut self, event: GateEvent) -> Vec<GateEffect> {
        let (next, effects) = RunGate::transition(&self.gate, event).unwrap();
        self.gate = next;
        for effect in &effects {
            if let GateEffect::FenceWaits {
                authority: ended, ..
            } = effect
            {
                assert_eq!(ended, &authority());
                self.wait_event(RequestEvent::AuthorityEnded);
            }
        }
        effects
    }

    pub fn welcome(&mut self, connection: u8) -> RegisterRunRequest {
        self.gate_event(GateEvent::Welcome {
            authority: authority(),
            connection,
        });
        self.publish_registration(connection)
    }

    pub fn publish_registration(&mut self, connection: u8) -> RegisterRunRequest {
        let effects = self.gate_event(GateEvent::PublishRegistration { connection });
        assert_eq!(effects.len(), 1);
        effects
            .into_iter()
            .find_map(|effect| match effect {
                GateEffect::Register {
                    connection: actual,
                    request,
                } => {
                    assert_eq!(actual, connection);
                    Some(request)
                }
                GateEffect::FenceWaits { .. } => None,
            })
            .unwrap()
    }

    pub fn registered(&mut self, connection: u8, reply: RegisterRunReply) {
        self.gate_event(GateEvent::RegistrationReply { connection, reply });
    }

    pub fn wait_event(&mut self, event: RequestEvent) -> Vec<RequestEffect> {
        let (next, effects) = WaitingRequest::transition(&self.wait, event).unwrap();
        self.wait = next;
        for effect in &effects {
            if let RequestEffect::RunRefused {
                connection,
                refusal,
            } = effect
            {
                self.gate_event(GateEvent::HolderAuthorityRefused {
                    connection: *connection,
                    refusal: *refusal,
                });
            }
        }
        effects
    }

    pub fn publish(&mut self) -> Vec<RequestEffect> {
        let connection = *self.gate.ready_connection().unwrap();
        self.wait_event(RequestEvent::Publish { connection })
    }

    pub fn lose_connection(&mut self, connection: u8) {
        self.gate_event(GateEvent::ConnectionLost { connection });
        match WaitingRequest::transition(
            &self.wait,
            RequestEvent::CallEndedWithoutReply { connection },
        ) {
            Ok((next, effects)) => {
                assert!(effects.is_empty());
                self.wait = next;
            }
            Err(error) => assert_eq!(error, TransitionError::Stale),
        }
    }
}

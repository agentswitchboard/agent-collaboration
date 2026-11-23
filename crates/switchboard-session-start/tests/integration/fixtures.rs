use fleet_identity_protocol::{
    AbandonedSessionCause, CreateFleetSessionReply, CreateSessionAttempt, CreateSessionKind,
    HarnessKind, SessionSecret, SessionSettlement, SessionTime,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_session_start::{
    CandidateAsker, CentralStanding, Directed, DirectiveId, DirectiveResult, EnvelopeRefusal,
    LaunchResult, LaunchSpecRef, LinkResult, LocalStartDigest, PutSecretResult, SaveResult,
    SetUpResult, SnapshotFacts, StartCandidate, StartDirective, StartEffect, StartEnvelope,
    StartFacts, StartInput, StartInstanceId, StartMachine, StartNotification, StartRefusal,
    StartRevision, StartScope, StartSnapshot, WaiterSlot,
};
use workspace_ids::{CentralSessionId, MachineId, SessionId, SessionRef};

pub(super) fn id(value: u64) -> String {
    format!("019c0000-0000-7000-8000-{value:012x}")
}

pub(super) fn instance(value: u64) -> StartInstanceId {
    StartInstanceId::new(id(value).parse().unwrap())
}

pub(super) fn machine_id(value: u64) -> MachineId {
    id(value).parse().unwrap()
}

pub(super) fn central_id() -> CentralSessionId {
    id(4).parse().unwrap()
}

pub(super) fn harness_id() -> SessionId {
    "held-conversation".parse().unwrap()
}

pub(super) fn address() -> SessionRef {
    SessionRef {
        machine_id: machine_id(1),
        session_id: harness_id(),
    }
}

pub(super) fn candidate() -> StartCandidate {
    StartCandidate {
        scope: StartScope {
            enrollment: id(2).try_into().unwrap(),
            operation_id: id(3).parse().unwrap(),
        },
        machine: machine_id(1),
        asker: CandidateAsker::Operator,
        agent_program: HarnessKind::Claude,
        kind: CreateSessionKind::Ordinary {},
        local_digest: LocalStartDigest::new([17; 32]),
        launch: LaunchSpecRef::new(4),
    }
}

pub(super) fn envelope() -> StartEnvelope {
    StartEnvelope::new(candidate()).unwrap()
}

pub(super) fn ask(
    slot: u64,
    candidate: Result<StartEnvelope, EnvelopeRefusal>,
    central: CentralStanding,
) -> StartInput {
    StartInput::Ask {
        slot: WaiterSlot::new(slot),
        candidate,
        central,
    }
}

pub(super) fn time() -> SessionTime {
    String::from("2026-11-23T08:00:00.000Z").try_into().unwrap()
}

pub(super) fn secret(seed: u8) -> SessionSecret {
    SessionSecret::from_bytes(&[seed; 32])
}

pub(super) fn reserved() -> CreateFleetSessionReply {
    CreateFleetSessionReply::Reserved {
        session_id: central_id(),
        secret: secret(17),
        reserved_until: time(),
    }
}

pub(super) fn abandoned() -> SessionSettlement {
    SessionSettlement::Abandoned {
        cause: AbandonedSessionCause::GivenUp {},
        time: time(),
    }
}

pub(super) fn attempt(value: u64) -> CreateSessionAttempt {
    value.try_into().unwrap()
}

pub(super) fn snapshot_facts(facts: StartFacts) -> Box<SnapshotFacts> {
    Box::new(SnapshotFacts {
        envelope: envelope(),
        attempt: attempt(1),
        facts,
    })
}

pub(super) struct Journey {
    pub machine: StartMachine,
    pub pending: Option<Directed>,
    pub notices: Vec<StartNotification>,
    pub effects: Vec<&'static str>,
    next_revision: u64,
}

impl Journey {
    pub(super) fn new() -> Self {
        Self::on(StartMachine::new(instance(90)))
    }

    pub(super) fn on(machine: StartMachine) -> Self {
        Self {
            machine,
            pending: None,
            notices: Vec::new(),
            effects: Vec::new(),
            next_revision: 1,
        }
    }

    pub(super) fn send(&mut self, input: StartInput) {
        let (next, effects) = StartMachine::transition(&self.machine, input).unwrap();
        self.machine = next;
        for effect in effects {
            match effect {
                StartEffect::Directive(directed) => {
                    self.effects.push(directive_tag(&directed.directive));
                    assert!(
                        self.pending.replace(directed).is_none(),
                        "two directives outstanding"
                    );
                }
                StartEffect::Notification(notification) => self.notices.push(notification),
            }
        }
        assert_eq!(
            self.machine.outstanding_id().as_ref(),
            self.pending.as_ref().map(|pending| &pending.id)
        );
    }

    pub(super) fn ask(&mut self) {
        self.send(ask(1, Ok(envelope()), CentralStanding::Ready));
    }

    pub(super) fn result(&mut self, result: DirectiveResult) {
        let id = self.pending.take().unwrap().id;
        self.send(StartInput::DirectiveResult { id, result });
    }

    pub(super) fn saved(&mut self) {
        let revision = StartRevision::new(self.next_revision);
        self.next_revision += 1;
        self.result(DirectiveResult::Save(SaveResult::Saved(revision)));
    }

    pub(super) fn refuse(&self, id: DirectiveId, result: DirectiveResult, refusal: StartRefusal) {
        let output =
            StartMachine::transition(&self.machine, StartInput::DirectiveResult { id, result });
        assert_eq!(output, Err(TransitionError::Domain(refusal)));
    }

    pub(super) fn snapshot(&self) -> &StartSnapshot {
        let StartDirective::Save { snapshot, .. } = &self.pending.as_ref().unwrap().directive
        else {
            panic!("expected save, got {:?}", self.pending)
        };
        snapshot
    }

    pub(super) fn creating() -> Self {
        let mut journey = Self::new();
        journey.ask();
        journey.saved();
        journey
    }

    pub(super) fn storing_secret() -> Self {
        let mut journey = Self::creating();
        journey.result(DirectiveResult::SendCreate(
            LinkResult::Answered(reserved()),
        ));
        journey
    }

    pub(super) fn saving_launch() -> Self {
        let mut journey = Self::storing_secret();
        journey.result(DirectiveResult::PutSecret(PutSecretResult::Stored));
        journey
    }

    pub(super) fn setting_up() -> Self {
        let mut journey = Self::saving_launch();
        journey.saved();
        journey.result(DirectiveResult::Launch(LaunchResult::Started {
            harness_session_id: harness_id(),
        }));
        journey.saved();
        journey
    }

    pub(super) fn binding() -> Self {
        let mut journey = Self::setting_up();
        journey.result(DirectiveResult::SetUp(SetUpResult::Done));
        journey.saved();
        journey
    }
}

pub(super) const fn directive_tag(directive: &StartDirective) -> &'static str {
    match directive {
        StartDirective::Save { .. } => "Save",
        StartDirective::SendCreate(_) => "SendCreate",
        StartDirective::PutSecret { .. } => "PutSecret",
        StartDirective::Launch => "Launch",
        StartDirective::SetUp => "SetUp",
        StartDirective::SendBind { .. } => "SendBind",
        StartDirective::CommitOpening { .. } => "CommitOpening",
        StartDirective::SendEnd(_) => "SendEnd",
        StartDirective::CloseConversation => "CloseConversation",
    }
}

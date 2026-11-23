use fleet_identity_protocol::{
    AbandonedSessionCause, CreateSessionAttempt, CreateSessionKind, HarnessKind, SessionSecret,
    SessionSettlement, SessionTime,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_session_start::{
    CandidateAsker, CentralStanding, ConversationFact, CreateAnswer, Directed, DirectiveId,
    DirectiveResult, EnvelopeRefusal, LaunchResult, LaunchSpecRef, LinkResult, LocalStartDigest,
    PutSecretResult, SaveResult, SetUpResult, SnapshotPhase, StartCandidate, StartDirective,
    StartEnvelope, StartFacts, StartInput, StartInstanceId, StartMachine, StartNotification,
    StartOutput, StartRefusal, StartRevision, StartScope, StartSnapshot, StoredFacts,
    StoredSnapshot, WaiterSlot,
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

pub(super) fn reserved() -> CreateAnswer {
    CreateAnswer::Reserved {
        session_id: central_id(),
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

pub(super) fn snapshot(phase: SnapshotPhase, facts: StoredFacts) -> StartSnapshot {
    let envelope = envelope();
    let scope = envelope.scope().clone();
    StartSnapshot::from_stored(
        &scope,
        StoredSnapshot {
            envelope,
            attempt: attempt(1),
            facts,
            phase,
        },
    )
    .unwrap()
}

pub(super) fn restore(
    instance: StartInstanceId,
    revision: StartRevision,
    snapshot: StartSnapshot,
) -> StartMachine {
    let scope = snapshot.start().envelope.scope().clone();
    StartMachine::restored(instance, &scope, revision, snapshot).unwrap()
}

pub(super) fn issue() -> switchboard_session_start::SecretIssue {
    switchboard_session_start::SecretIssue {
        enrollment: candidate().scope.enrollment,
        session_id: central_id(),
        attempt: attempt(1),
    }
}

pub(super) fn held_facts() -> StartFacts {
    use switchboard_session_start::{Exposure, LaunchFact, PointerFact, Reservation, SecretFact};
    let issue = issue();
    StartFacts::from_stored(StoredFacts {
        create_exposure: Exposure::Witnessed,
        reservation: Some(Reservation {
            session_id: central_id(),
            reserved_until: time(),
            attempt: attempt(1),
        }),
        secret: SecretFact::Stored(issue.clone()),
        pointer: PointerFact::Committed(issue),
        launch: LaunchFact::Started,
        harness_session_id: Some(harness_id()),
        conversation: ConversationFact::Held,
        bind_exposure: Exposure::Witnessed,
        active_proof: Some(switchboard_session_start::CheckedActive { address: address() }),
        ..Default::default()
    })
    .unwrap()
}

pub(super) fn only_output(effects: Vec<StartOutput>) -> StartOutput {
    let mut effects = effects.into_iter();
    let aggregate = effects.next().unwrap();
    assert!(
        effects.next().is_none(),
        "one aggregate per accepted transition"
    );
    aggregate
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
        let output = only_output(effects);
        if let Some(directed) = output.directive {
            self.effects.push(directive_tag(&directed.directive));
            assert!(
                self.pending.replace(directed).is_none(),
                "two directives outstanding"
            );
        }
        self.notices.extend(output.notifications);
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

    pub(super) fn saving_reservation() -> Self {
        let mut journey = Self::creating();
        journey.result(DirectiveResult::SendCreate(
            LinkResult::Answered(reserved()),
        ));
        journey
    }

    pub(super) fn storing_secret() -> Self {
        let mut journey = Self::saving_reservation();
        journey.saved();
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

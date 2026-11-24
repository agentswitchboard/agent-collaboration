use fleet_identity_protocol::{
    CreateFleetSession, CreateSessionAttempt, CreateSessionDigest, CreateSessionKind, EnrollmentId,
    HarnessKind, PresentedSessionSecret, SessionCreator,
};
use workspace_ids::{CentralSessionId, MachineId, SessionOperationId};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StartScope {
    pub enrollment: EnrollmentId,
    pub operation_id: SessionOperationId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartAsker {
    Operator,
    Session { session_id: CentralSessionId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CandidateAsker {
    Operator,
    Session { session_id: CentralSessionId },
    Unprovable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalStartDigest([u8; 32]);

impl LocalStartDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LaunchSpecRef(u64);

impl LaunchSpecRef {
    pub const fn new(reference: u64) -> Self {
        Self(reference)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartCandidate {
    pub scope: StartScope,
    pub machine: MachineId,
    pub asker: CandidateAsker,
    pub agent_program: HarnessKind,
    pub kind: CreateSessionKind,
    pub local_digest: LocalStartDigest,
    pub launch: LaunchSpecRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnvelopeRefusal {
    #[error("the asker has no provable identity")]
    NoProvableAsker,
    #[error("the requested harness is unsupported")]
    UnsupportedAgentProgram,
    #[error("delegate starts are unavailable")]
    DelegateNotInThisStep,
    #[error("the central create digest could not be computed")]
    DigestNotComputable,
}

/// The immutable contents accepted for one enrollment and operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartEnvelope {
    scope: StartScope,
    machine: MachineId,
    asker: StartAsker,
    agent_program: HarnessKind,
    local_digest: LocalStartDigest,
    central_digest: CreateSessionDigest,
    launch: LaunchSpecRef,
}

impl StartEnvelope {
    pub fn new(candidate: StartCandidate) -> Result<Self, EnvelopeRefusal> {
        let asker = match candidate.asker {
            CandidateAsker::Operator => StartAsker::Operator,
            CandidateAsker::Session { session_id } => StartAsker::Session { session_id },
            CandidateAsker::Unprovable => return Err(EnvelopeRefusal::NoProvableAsker),
        };
        match candidate.kind {
            CreateSessionKind::Ordinary {} => {}
            CreateSessionKind::Delegate { .. } => {
                return Err(EnvelopeRefusal::DelegateNotInThisStep);
            }
        }
        match candidate.agent_program {
            HarnessKind::Claude | HarnessKind::Cursor => {}
            HarnessKind::Codex => return Err(EnvelopeRefusal::UnsupportedAgentProgram),
        }
        let attempt =
            CreateSessionAttempt::try_from(1).map_err(|_| EnvelopeRefusal::DigestNotComputable)?;
        let central_digest =
            create_request(&candidate.scope, &asker, &candidate.agent_program, attempt)
                .contents_digest()
                .map_err(|_| EnvelopeRefusal::DigestNotComputable)?;
        Ok(Self {
            scope: candidate.scope,
            machine: candidate.machine,
            asker,
            agent_program: candidate.agent_program,
            local_digest: candidate.local_digest,
            central_digest,
            launch: candidate.launch,
        })
    }

    pub const fn scope(&self) -> &StartScope {
        &self.scope
    }

    pub const fn machine(&self) -> &MachineId {
        &self.machine
    }

    pub const fn asker(&self) -> &StartAsker {
        &self.asker
    }

    pub const fn agent_program(&self) -> &HarnessKind {
        &self.agent_program
    }

    pub const fn local_digest(&self) -> LocalStartDigest {
        self.local_digest
    }

    pub const fn central_digest(&self) -> &CreateSessionDigest {
        &self.central_digest
    }

    pub const fn launch(&self) -> LaunchSpecRef {
        self.launch
    }

    pub(crate) fn same_request(&self, candidate: &Self) -> bool {
        self.scope == candidate.scope
            && self.machine == candidate.machine
            && self.asker == candidate.asker
            && self.agent_program == candidate.agent_program
            && self.local_digest == candidate.local_digest
            && self.central_digest == candidate.central_digest
    }

    pub(crate) fn create_request(&self, attempt: CreateSessionAttempt) -> CreateFleetSession {
        create_request(&self.scope, &self.asker, &self.agent_program, attempt)
    }
}

fn create_request(
    scope: &StartScope,
    asker: &StartAsker,
    agent_program: &HarnessKind,
    attempt: CreateSessionAttempt,
) -> CreateFleetSession {
    let creator = match asker {
        StartAsker::Operator => SessionCreator::RouterOperator {},
        StartAsker::Session { session_id } => SessionCreator::Session {
            session_id: session_id.clone(),
            presented: PresentedSessionSecret::Nothing {},
        },
    };
    CreateFleetSession {
        operation_id: scope.operation_id.clone(),
        attempt,
        creator,
        agent_program: agent_program.clone(),
        session_kind: CreateSessionKind::Ordinary {},
    }
}

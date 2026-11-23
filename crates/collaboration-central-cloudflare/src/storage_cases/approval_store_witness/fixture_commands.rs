use approval_request_protocol::{
    Answer, DecideRequest, DeliverEndingReply, DeliverEndingRequest, PassOnRequest, Raise,
    StopRequest,
};
use fleet_approvals::{ApprovalEnrollment, ApprovalHolderScope, ApprovalRun, ApprovalSession};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, PresentedSessionSecret};
use serde::Deserialize;
use workspace_ids::{
    ApprovalRequestId, CentralSessionId, MachineId, PersonId, RequestId, RouterRunId,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ApprovalWitnessInput {
    pub at_ms: i64,
    pub fail_after_write: Option<usize>,
    pub command: ApprovalWitnessCommand,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct WitnessHolder {
    pub organization: OrganizationId,
    pub machine: MachineId,
    pub enrollment: EnrollmentId,
    pub run_id: RouterRunId,
}

impl From<WitnessHolder> for ApprovalHolderScope {
    fn from(holder: WitnessHolder) -> Self {
        Self {
            organization_id: holder.organization,
            run: ApprovalRun {
                enrollment: ApprovalEnrollment {
                    machine_id: holder.machine,
                    enrollment_id: holder.enrollment,
                },
                router_run: holder.run_id,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct WitnessSession {
    pub machine: MachineId,
    pub enrollment: EnrollmentId,
    pub session_id: CentralSessionId,
}

impl From<WitnessSession> for ApprovalSession {
    fn from(session: WitnessSession) -> Self {
        Self {
            enrollment: ApprovalEnrollment {
                machine_id: session.machine,
                enrollment_id: session.enrollment,
            },
            session_id: session.session_id,
        }
    }
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    tag = "kind",
    rename_all_fields = "camelCase"
)]
pub(super) enum WitnessDecider {
    Person { person_id: PersonId },
    Session { session: WitnessSession },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum StoredCorruption {
    RequestKind,
    RecordState,
    Ending,
    ProviderMarkerWithCallCause,
    CallMarkerWithProviderCause,
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    tag = "kind",
    rename_all_fields = "camelCase"
)]
pub(super) enum ApprovalWitnessCommand {
    Authenticated {
        holder: WitnessHolder,
        operation: SessionApprovalWitness,
    },
    Recover {
        holder: WitnessHolder,
    },
    StoreEnrollment {
        holder: WitnessHolder,
        live: bool,
    },
    StoreSession {
        organization: OrganizationId,
        session: WitnessSession,
        parent: Option<WitnessSession>,
        live: bool,
    },
    Inspect,
    Corrupt {
        request_id: ApprovalRequestId,
        field: StoredCorruption,
    },
    Register {
        holder: WitnessHolder,
    },
    Raise {
        holder: WitnessHolder,
        request: Box<Raise>,
    },
    Stop {
        holder: WitnessHolder,
        request: StopRequest,
    },
    Deliver {
        holder: WitnessHolder,
        request: Box<DeliverEndingRequest>,
        reply: DeliverEndingReply,
    },
    Reoffer {
        holder: WitnessHolder,
        request_id: ApprovalRequestId,
    },
    Decide {
        organization: OrganizationId,
        actor: WitnessDecider,
        request_id: ApprovalRequestId,
        operation_id: RequestId,
        value: Answer,
    },
    PassOn {
        organization: OrganizationId,
        parent: WitnessSession,
        request_id: ApprovalRequestId,
    },
    Read {
        organization: OrganizationId,
        person: PersonId,
        request_id: ApprovalRequestId,
    },
    List {
        organization: OrganizationId,
        person: PersonId,
        limit: u16,
    },
    Deadline {
        subject: String,
        deadline_ms: i64,
    },
    DueRows {
        cutoff_ms: i64,
        limit: usize,
    },
    SeedDuePopulation {
        waiting: u16,
    },
    Retire {
        holder: WitnessHolder,
    },
    EndSession {
        organization: OrganizationId,
        session: WitnessSession,
    },
    EndEnrollment {
        holder: WitnessHolder,
    },
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    tag = "kind",
    rename_all_fields = "camelCase"
)]
pub(super) enum SessionApprovalWitness {
    RegisterRun,
    Raise {
        request: Box<Raise>,
    },
    Decide {
        session_id: CentralSessionId,
        presented: PresentedSessionSecret,
        request: DecideRequest,
    },
    PassOn {
        session_id: CentralSessionId,
        presented: PresentedSessionSecret,
        request: PassOnRequest,
    },
}

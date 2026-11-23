use std::collections::{BTreeMap, BTreeSet};

use approval_request_protocol::{
    CentralTime, DeliverEndingReply, DeliverEndingRequest, PassOnRequest, Raise, RegisterRunReply,
    StopRequest,
};
use fleet_identity_protocol::EnrollmentId;
use workspace_ids::{MachineId, PersonId, RequestId, RouterRunId};

use crate::{
    AdmittedDecider, ApprovalDecideInput, ApprovalEffect, ApprovalEnrollment, ApprovalRequestKey,
    ApprovalRouting, ApprovalRun, ApprovalSession, ApprovalState, ProviderSessionAvailability,
    SavedDecision,
};

/// One organization's facts for a guarded apply, including every affected row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalCollection {
    pub high_water: CentralTime,
    pub active_enrollments: BTreeMap<MachineId, ActiveApprovalEnrollment>,
    pub ended_enrollments: BTreeSet<ApprovalEnrollment>,
    pub ended_sessions: BTreeSet<ApprovalSession>,
    pub requests: BTreeMap<ApprovalRequestKey, ApprovalState>,
    pub decisions: BTreeMap<RequestId, SavedDecision>,
}

impl ApprovalCollection {
    pub fn new(high_water: CentralTime) -> Self {
        Self {
            high_water,
            active_enrollments: BTreeMap::new(),
            ended_enrollments: BTreeSet::new(),
            ended_sessions: BTreeSet::new(),
            requests: BTreeMap::new(),
            decisions: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveApprovalEnrollment {
    pub enrollment_id: EnrollmentId,
    pub registered: Option<RouterRunId>,
    pub retired_runs: BTreeSet<RouterRunId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalCollectionEvent {
    ActivateEnrollment {
        enrollment: ApprovalEnrollment,
    },
    EndEnrollment {
        enrollment: ApprovalEnrollment,
    },
    /// The link has authenticated this authority after Welcome.
    RegisterRun {
        run: ApprovalRun,
    },
    Raise {
        run: ApprovalRun,
        raise: Box<Raise>,
        routing: ApprovalRouting,
        provider_session: ProviderSessionAvailability,
    },
    Stop {
        run: ApprovalRun,
        request: StopRequest,
    },
    Decide {
        key: ApprovalRequestKey,
        actor: AdmittedDecider,
        request: ApprovalDecideInput,
    },
    Read {
        key: ApprovalRequestKey,
        person_id: PersonId,
    },
    PassOn {
        key: ApprovalRequestKey,
        parent: ApprovalSession,
        request: PassOnRequest,
    },
    EndSession {
        session: ApprovalSession,
    },
    Alarm {
        key: ApprovalRequestKey,
        deadline: CentralTime,
    },
    Reoffer {
        key: ApprovalRequestKey,
    },
    /// The link has correlated a reply to this exact outstanding call and enrolled authority.
    DeliveryReply {
        run: ApprovalRun,
        request: Box<DeliverEndingRequest>,
        reply: DeliverEndingReply,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalCollectionEffect {
    Request(ApprovalEffect),
    Registered(RegisterRunReply),
}

use std::collections::{BTreeMap, BTreeSet};

use approval_request_protocol::{
    CentralTime, DeliverEndingReply, DeliverEndingRequest, PassOnRequest, Raise, RegisterRunReply,
    StopRequest,
};
use fleet_identity_protocol::EnrollmentId;
use workspace_ids::{MachineId, PersonId, RouterRunId};

use crate::{
    AdmittedDecider, ApprovalDecideInput, ApprovalEffect, ApprovalEnrollment, ApprovalRequestKey,
    ApprovalRouting, ApprovalRun, ApprovalSession, ApprovalState, ProviderSessionAvailability,
};

/// One organization's facts for a guarded apply, including every affected row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ApprovalCollection {
    pub active_enrollments: BTreeMap<MachineId, ActiveApprovalEnrollment>,
    pub ended_enrollments: BTreeSet<ApprovalEnrollment>,
    pub ended_sessions: BTreeSet<ApprovalSession>,
    pub requests: BTreeMap<ApprovalRequestKey, ApprovalState>,
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
        to: crate::ApprovalRouteDestination,
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

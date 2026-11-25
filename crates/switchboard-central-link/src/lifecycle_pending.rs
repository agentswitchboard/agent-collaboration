//! The lifecycle answers held by one admitted socket and enrollment.

use std::collections::BTreeMap;

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, LifecycleCallError, SessionLifecycleScope,
};
use central_link_protocol::{
    CentralCallReply, CorrelationId, MachineLinkCloseCode, TypedCallResult,
};
use router_clock::ClockInstant;

use crate::{
    lifecycle_lane::{LifecycleAdmissionGate, LifecycleCompletion},
    lifecycle_reply::LifecycleReplyCheck,
};

pub(super) struct WrittenLifecycleCall {
    pub(super) connection: ConnectionAttempt,
    pub(super) deadline: ClockInstant,
    pub(super) expected: LifecycleReplyCheck,
    pub(super) completion: LifecycleCompletion,
}

pub(super) struct LifecycleSocketCalls {
    admission: LifecycleAdmissionGate,
    scope: Option<SessionLifecycleScope>,
    pending: BTreeMap<CorrelationId, WrittenLifecycleCall>,
}

impl LifecycleSocketCalls {
    pub(super) fn new(admission: LifecycleAdmissionGate) -> Self {
        Self {
            admission,
            scope: None,
            pending: BTreeMap::new(),
        }
    }

    pub(super) fn set_scope(&mut self, scope: Option<SessionLifecycleScope>) {
        self.scope = scope;
    }

    pub(super) fn scope(&self) -> Option<&SessionLifecycleScope> {
        self.scope.as_ref()
    }

    pub(super) fn admit(&self, admitted: &AdmittedConnection) {
        if let Some(scope) = &self.scope {
            self.admission.admit(scope, admitted);
        }
    }

    pub(super) fn deadline(&self) -> Option<ClockInstant> {
        self.pending.values().map(|call| call.deadline).min()
    }

    pub(super) fn expire(&mut self, now: ClockInstant) {
        self.pending
            .retain(|_, call| now < call.deadline && call.completion.caller_is_waiting());
    }

    pub(super) fn record(&mut self, correlation: CorrelationId, call: WrittenLifecycleCall) {
        self.pending.insert(correlation, call);
    }

    pub(super) fn answer(
        &mut self,
        connection: &ConnectionAttempt,
        correlation: &CorrelationId,
        result: TypedCallResult<CentralCallReply>,
        now: ClockInstant,
    ) {
        let Some(scope) = &self.scope else { return };
        let Some(call) = self.pending.get(correlation) else {
            log::trace!("Dropping an untracked lifecycle reply on the Central link");
            return;
        };
        if &call.connection != connection {
            log::trace!("Dropping a lifecycle reply from another Central link attempt");
            return;
        }
        if let Some(call) = self.pending.remove(correlation)
            && now < call.deadline
        {
            call.completion
                .finish(call.expected.check(&scope.organization, result));
        }
    }

    pub(super) fn end(&mut self, code: Option<MachineLinkCloseCode>) {
        if let Some(scope) = &self.scope {
            self.admission.end(scope);
        }
        if let Some(code) = code {
            for (_, call) in std::mem::take(&mut self.pending) {
                call.completion
                    .finish(Err(LifecycleCallError::EnrollmentEnded(code)));
            }
        }
    }
}

impl Drop for LifecycleSocketCalls {
    fn drop(&mut self) {
        self.admission.withdraw();
    }
}

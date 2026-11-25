//! Check the reply against the exact request before releasing it to a caller.

use central_link_client::LifecycleCallError;
use central_link_protocol::{CentralCallReply, TypedCallResult};
use fleet_identity_protocol::{
    BindFleetSessionReply, CheckedCreateReply, CreateReplyContext, EndReplyContext,
    EndSessionOutcome, OrganizationId,
};
use workspace_ids::SessionRef;

pub(super) enum LifecycleReplyCheck {
    Create(CreateReplyContext),
    Bind(SessionRef),
    End(EndReplyContext),
}

pub(super) enum CheckedLifecycleReply {
    Create(CheckedCreateReply),
    Bind(BindFleetSessionReply),
    End(EndSessionOutcome),
}

pub(super) type LifecycleResult = Result<CheckedLifecycleReply, LifecycleCallError>;

impl LifecycleReplyCheck {
    pub(super) fn check(
        self,
        organization: &OrganizationId,
        result: TypedCallResult<CentralCallReply>,
    ) -> LifecycleResult {
        let reply = match result {
            TypedCallResult::Answered { reply } => reply,
            TypedCallResult::Refused { reason } => {
                return Err(LifecycleCallError::RefusedBeforeApply(reason));
            }
        };
        match (self, reply) {
            (Self::Create(context), CentralCallReply::CreateFleetSession(reply))
                if &reply.target == organization =>
            {
                context
                    .check(reply.body)
                    .map(CheckedLifecycleReply::Create)
                    .map_err(|_| LifecycleCallError::ContradictedReply)
            }
            (Self::Bind(expected), CentralCallReply::BindFleetSession(reply))
                if &reply.target == organization =>
            {
                if let BindFleetSessionReply::Active { address } = &reply.body
                    && address != &expected
                {
                    return Err(LifecycleCallError::ContradictedReply);
                }
                Ok(CheckedLifecycleReply::Bind(reply.body))
            }
            (Self::End(context), CentralCallReply::EndFleetSession(reply))
                if &reply.target == organization =>
            {
                context
                    .check(reply.body)
                    .map(CheckedLifecycleReply::End)
                    .map_err(|_| LifecycleCallError::ContradictedReply)
            }
            _ => Err(LifecycleCallError::ContradictedReply),
        }
    }
}

use approval_request_protocol::{CentralTime, RequestView};
use central_engine_model::{Context, IdSource, Machine, TransitionError};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_runtime_ports::{Clock, workspace_decode_error::corrupt};

use crate::{
    ApprovalCommitScope, ApprovalEffect, ApprovalEvent, ApprovalLifecycle, ApprovalReceipt,
    ApprovalRefusal, ApprovalReplyAddress, ApprovalRouting, ApprovalState, CommittedApprovalOffer,
    PendingApprovalHolderEffects, PendingApprovalReply,
    approval_store_scope::{ApprovalHolderScope, ApprovalRequestRowId},
    approval_store_transaction::ApprovalStatementTransaction,
    approval_store_writes::{
        plan_approval_state_change, plan_register_approval_deadline, plan_remove_approval_deadline,
    },
};

#[path = "approval_store_destination_commands.rs"]
mod destination_commands;
#[path = "approval_store_fixture_destinations.rs"]
#[cfg(feature = "test-support")]
mod fixture_destinations;
#[path = "approval_store_holder_commands.rs"]
mod holder_commands;
#[path = "approval_store_person_commands.rs"]
mod person_commands;
#[path = "approval_store_person_decide.rs"]
mod person_decide;
#[path = "approval_store_run_commands.rs"]
mod run_commands;

pub use holder_commands::{ApprovalHolderCommand, ApprovalHolderReply};

#[derive(Debug)]
pub enum ApprovalApplyFailure {
    Store(StoreFailure),
    Lifecycle(ApprovalRefusal),
    /// The enclosing transaction must roll back its preceding writes.
    GuardChanged,
}

impl From<StoreFailure> for ApprovalApplyFailure {
    fn from(failure: StoreFailure) -> Self {
        Self::Store(failure)
    }
}

struct ApprovalTransitionOutput<Reply> {
    reply: Reply,
    ending_offers: Vec<CommittedApprovalOffer>,
}

impl<Reply> ApprovalTransitionOutput<Reply> {
    fn with_reply(reply: Reply) -> Self {
        Self {
            reply,
            ending_offers: Vec::new(),
        }
    }

    fn replace_reply<NewReply>(self, reply: NewReply) -> ApprovalTransitionOutput<NewReply> {
        ApprovalTransitionOutput {
            reply,
            ending_offers: self.ending_offers,
        }
    }
}

#[derive(Debug)]
pub struct ApprovalReadPage {
    pub requests: Vec<RequestView>,
    pub next_after: Option<ApprovalRequestRowId>,
}

/// Owns the approval transitions on an applying transaction lent by Central.
pub struct ApprovalApplyingStore<'loan, Transaction, Sessions> {
    commit: &'loan ApprovalCommitScope,
    transaction: &'loan mut Transaction,
    sessions: &'loan Sessions,
    clock: &'loan dyn Clock,
    row_ids: &'loan mut dyn IdSource,
    holder_offers: Vec<CommittedApprovalOffer>,
}

impl<'loan, Transaction, Sessions> ApprovalApplyingStore<'loan, Transaction, Sessions>
where
    Transaction: ApprovalStatementTransaction,
{
    pub fn new(
        commit: &'loan ApprovalCommitScope,
        transaction: &'loan mut Transaction,
        sessions: &'loan Sessions,
        clock: &'loan dyn Clock,
        row_ids: &'loan mut dyn IdSource,
    ) -> Self {
        Self {
            commit,
            transaction,
            sessions,
            clock,
            row_ids,
            holder_offers: Vec::new(),
        }
    }

    pub fn finish_holder_effects(self) -> PendingApprovalHolderEffects {
        self.commit.pending_holder_effects(self.holder_offers)
    }

    fn stage_reply<Reply>(
        &mut self,
        address: ApprovalReplyAddress,
        mut transition: ApprovalTransitionOutput<Reply>,
    ) -> PendingApprovalReply<Reply> {
        self.holder_offers.append(&mut transition.ending_offers);
        self.commit.pending_reply(address, transition.reply)
    }

    fn sample_applying_time(&self) -> Result<CentralTime, StoreFailure> {
        CentralTime::from_timestamp_millis(self.clock.now().unix_millis())
            .ok_or_else(|| corrupt("approval_store.applying_time"))
    }

    async fn apply_request_event(
        &mut self,
        row_id: Option<&ApprovalRequestRowId>,
        scope: &ApprovalHolderScope,
        prior: &ApprovalState,
        event: ApprovalEvent,
        at: CentralTime,
    ) -> Result<ApprovalTransitionOutput<Option<ApprovalReceipt>>, ApprovalApplyFailure> {
        let mut context = Context {
            now: at,
            ids: &mut *self.row_ids,
        };
        let (next, effects) = match ApprovalLifecycle::transition(prior, event, &mut context) {
            Ok(transition) => transition,
            Err(TransitionError::Stale) => return Ok(ApprovalTransitionOutput::with_reply(None)),
            Err(TransitionError::Domain(refusal)) => {
                return Err(ApprovalApplyFailure::Lifecycle(refusal));
            }
            Err(TransitionError::NotAllowed { .. }) => {
                return Err(corrupt("approval_store.transition").into());
            }
        };

        let mut output = ApprovalTransitionOutput::with_reply(None);
        let mut due_writes = Vec::new();
        let new_row_id = if prior != &next && row_id.is_none() {
            Some(ApprovalRequestRowId::mint(context.ids)?)
        } else {
            None
        };
        let row_id = row_id.or(new_row_id.as_ref());

        for effect in effects {
            match effect {
                ApprovalEffect::Reply(receipt) => output.reply = Some(receipt),
                ApprovalEffect::OfferEnding { holder, request } => {
                    if holder != scope.run {
                        return Err(corrupt("approval_requests.ending_holder").into());
                    }
                    output
                        .ending_offers
                        .push(CommittedApprovalOffer::new(scope.clone(), request));
                }
                ApprovalEffect::StoreSessionNotice { key, session } => {
                    // This notice is the initial parent destination's durable notice slot.
                    let retained = next
                        .retained()
                        .ok_or_else(|| corrupt("approval_requests.notice"))?;
                    if retained.key != key
                        || retained.routing
                            != (ApprovalRouting::Delegate {
                                admitted_parent: session,
                            })
                    {
                        return Err(corrupt("approval_requests.notice").into());
                    }
                }
                ApprovalEffect::RegisterDue { key, deadline } => {
                    let row_id = row_id.ok_or_else(|| corrupt("approval_requests.due_key"))?;
                    if next.key() != Some(&key) {
                        return Err(corrupt("approval_requests.due_key").into());
                    }
                    due_writes.extend(plan_register_approval_deadline(row_id, deadline, at)?);
                }
                ApprovalEffect::ClearDue { key } => {
                    let row_id = row_id.ok_or_else(|| corrupt("approval_requests.due_key"))?;
                    if prior.key() != Some(&key) {
                        return Err(corrupt("approval_requests.due_key").into());
                    }
                    due_writes.extend(plan_remove_approval_deadline(row_id, at)?);
                }
            }
        }

        if let Some(row_id) = row_id {
            let mut writes = plan_approval_state_change(row_id, scope, prior, &next, &mut context)?;
            writes.append(&mut due_writes);
            for write in writes {
                self.execute_guarded_write(write).await?;
            }
        }
        Ok(output)
    }

    async fn execute_guarded_write(
        &mut self,
        write: GuardedStatement,
    ) -> Result<(), ApprovalApplyFailure> {
        let rows_changed = self
            .transaction
            .execute_approval_statement(write.statement)
            .await?;
        if !write.expect.admits(rows_changed) {
            return Err(ApprovalApplyFailure::GuardChanged);
        }
        Ok(())
    }
}

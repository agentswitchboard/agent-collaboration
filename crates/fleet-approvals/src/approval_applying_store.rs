use approval_request_protocol::{CentralTime, DeliverEndingRequest, RequestView};
use central_engine_model::{Context, IdSource, Machine, TransitionError};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_runtime_ports::{Clock, workspace_decode_error::corrupt};

use crate::{
    ApprovalEffect, ApprovalEvent, ApprovalLifecycle, ApprovalReceipt, ApprovalRefusal,
    ApprovalRouting, ApprovalRun, ApprovalSessionFacts, ApprovalState,
    approval_store_scope::{ApprovalHolderScope, ApprovalRowId},
    approval_store_transaction::ApprovalStoreTransaction,
    approval_store_writes::{
        plan_approval_state_change, plan_register_approval_deadline, plan_remove_approval_deadline,
    },
};

#[path = "approval_store_destination_commands.rs"]
mod destination_commands;
#[path = "approval_store_holder_commands.rs"]
mod holder_commands;
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

/// A delivery to this exact holder, ready to queue once the owning write commits.
#[derive(Debug, Eq, PartialEq)]
pub struct PendingApprovalOffer {
    pub holder: ApprovalRun,
    pub request: Box<DeliverEndingRequest>,
}

/// The transaction owner releases these outputs after committing its write.
#[derive(Debug)]
pub struct PendingApprovalApply<Reply> {
    pub reply: Reply,
    pub ending_offers: Vec<PendingApprovalOffer>,
}

impl<Reply> PendingApprovalApply<Reply> {
    fn with_reply(reply: Reply) -> Self {
        Self {
            reply,
            ending_offers: Vec::new(),
        }
    }

    fn replace_reply<NewReply>(self, reply: NewReply) -> PendingApprovalApply<NewReply> {
        PendingApprovalApply {
            reply,
            ending_offers: self.ending_offers,
        }
    }
}

#[derive(Debug)]
pub struct ApprovalReadPage {
    pub requests: Vec<RequestView>,
    pub next_after: Option<ApprovalRowId>,
}

/// Owns the approval transitions on an applying transaction lent by Central.
pub struct ApprovalApplyingStore<'loan, Transaction, Sessions> {
    transaction: &'loan mut Transaction,
    sessions: &'loan Sessions,
    clock: &'loan dyn Clock,
    row_ids: &'loan mut dyn IdSource,
}

impl<'loan, Transaction, Sessions> ApprovalApplyingStore<'loan, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub fn new(
        transaction: &'loan mut Transaction,
        sessions: &'loan Sessions,
        clock: &'loan dyn Clock,
        row_ids: &'loan mut dyn IdSource,
    ) -> Self {
        Self {
            transaction,
            sessions,
            clock,
            row_ids,
        }
    }

    fn sample_applying_time(&self) -> Result<CentralTime, StoreFailure> {
        CentralTime::from_timestamp_millis(self.clock.now().unix_millis())
            .ok_or_else(|| corrupt("approval_store.applying_time"))
    }

    async fn apply_request_event(
        &mut self,
        row_id: Option<&ApprovalRowId>,
        scope: &ApprovalHolderScope,
        prior: &ApprovalState,
        event: ApprovalEvent,
        at: CentralTime,
    ) -> Result<PendingApprovalApply<Option<ApprovalReceipt>>, ApprovalApplyFailure> {
        let mut context = Context {
            now: at,
            ids: &mut *self.row_ids,
        };
        let (next, effects) = match ApprovalLifecycle::transition(prior, event, &mut context) {
            Ok(transition) => transition,
            Err(TransitionError::Stale) => return Ok(PendingApprovalApply::with_reply(None)),
            Err(TransitionError::Domain(refusal)) => {
                return Err(ApprovalApplyFailure::Lifecycle(refusal));
            }
            Err(TransitionError::NotAllowed { .. }) => {
                return Err(corrupt("approval_store.transition").into());
            }
        };

        let mut output = PendingApprovalApply::with_reply(None);
        let mut due_writes = Vec::new();
        let new_row_id = if prior != &next && row_id.is_none() {
            Some(ApprovalRowId::mint(context.ids)?)
        } else {
            None
        };
        let row_id = row_id.or(new_row_id.as_ref());

        for effect in effects {
            match effect {
                ApprovalEffect::Reply(receipt) => output.reply = Some(receipt),
                ApprovalEffect::OfferEnding { holder, request } => {
                    output
                        .ending_offers
                        .push(PendingApprovalOffer { holder, request });
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

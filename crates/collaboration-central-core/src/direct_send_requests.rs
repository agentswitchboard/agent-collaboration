//! Compose admitted direct sends with the owning mail store.
use crate::WorkspaceRuntime;
use central_storage::{BatchOutcome, StatementBatch, StoreFailure};
use fleet_mail::{
    AdmittedDirectSend, DirectSendLookup, DirectSendRequestKey, DirectSendResult,
    RetainedDirectSend, RetainedDirectSendRead,
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores, workspace_decode_error::corrupt};
use workspace_model::{Identity, Timestamp, central_mail as mail};

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub(crate) async fn execute_direct_send(
        &self,
        identity: Identity,
        request: mail::CentralMailRequest,
        now: Timestamp,
        digest: String,
    ) -> Result<mail::CentralMailResult, StoreFailure> {
        use mail::{
            CentralMailAnswer as Answer, CentralMailRefusal as Refusal, CentralMailResult as Reply,
        };
        let cutoff = fleet_runtime::workspace_request_log::strict_cutoff(now);
        let existing_actor = self
            .access()
            .read_existing_identity_actor(&identity)
            .await?;
        let request_key = DirectSendRequestKey {
            request_id: request.request_id.clone(),
            digest,
            cutoff,
        };
        let retained = self
            .direct_messages
            .read_retained_send(DirectSendLookup {
                request: request_key.clone(),
                caller: existing_actor.clone(),
            })
            .await?;
        match retained {
            RetainedDirectSendRead::Missing => {}
            RetainedDirectSendRead::RequestIdReused => {
                return Ok(Reply::Refused(Refusal::Workspace(
                    workspace_model::RefusalReason::RequestIdReused,
                )));
            }
            RetainedDirectSendRead::Found(answer) => {
                let repair = self
                    .execution
                    .statement_host()
                    .run_batch(StatementBatch {
                        statements: vec![],
                        alarm: crate::engine::query_registry::alarm_update(),
                    })
                    .await;
                match repair {
                    Ok(BatchOutcome::Applied { .. }) => {}
                    Ok(_) => return Err(corrupt("runtime_request_log.alarm")),
                    Err(StoreFailure::Unavailable | StoreFailure::AnswerLost) => {
                        return Ok(Reply::OutcomeUnknown);
                    }
                    Err(failure) => return Err(failure),
                }
                return Ok(match answer {
                    RetainedDirectSend::Queued { message_id } => {
                        Reply::Done(Answer::Queued { message_id })
                    }
                    RetainedDirectSend::RefusedUnknownTarget { target } => {
                        Reply::Refused(Refusal::UnknownSession { target })
                    }
                });
            }
        }
        if let Some(reason) = crate::central_mail_time::refusal(&request.operation, now) {
            return Ok(Reply::Refused(reason));
        }
        let mail::CentralMailOperation::SendMessage {
            to,
            body,
            delivery,
            reply_to,
            sent_at,
        } = request.operation
        else {
            return Err(corrupt("direct send operation"));
        };
        let caller = match existing_actor {
            Some(actor) => actor,
            None => self.access().resolve_identity(&identity).await?,
        };
        let result = self
            .direct_messages
            .apply_direct_send(AdmittedDirectSend {
                request: request_key,
                caller,
                send: mail::SendMessageRequest {
                    to,
                    body,
                    delivery,
                    reply_to,
                },
                sent_at,
                accepted_at: now,
                ids: self.execution.shared_ids(),
            })
            .await;
        let answer = match result {
            Ok(DirectSendResult::Committed { message_id, ready }) => {
                (self.mail_delivery_ready)(ready);
                Reply::Done(Answer::Queued { message_id })
            }
            Ok(DirectSendResult::RetainedRefusal { target }) => {
                Reply::Refused(Refusal::UnknownSession { target })
            }
            Err(fleet_mail::DirectWriteFailure::Conflict) => Reply::Conflict,
            Err(fleet_mail::DirectWriteFailure::Read(
                fleet_mail::DirectReadFailure::Unavailable,
            )) => Reply::CentralUnavailable,
            Err(fleet_mail::DirectWriteFailure::AnswerLost) => Reply::OutcomeUnknown,
            Err(error) => return Err(error.into()),
        };
        #[cfg(feature = "test-support")]
        if self.execution.take_lost_reply(&request.request_id) {
            return Ok(Reply::OutcomeUnknown);
        }
        Ok(answer)
    }
}

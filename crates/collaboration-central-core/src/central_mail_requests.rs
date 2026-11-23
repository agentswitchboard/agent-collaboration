//! Central-only admitted request ledger; cached answers precede mutable decisions.
use crate::{ApplyingRequestFailure, WorkspaceCaller, applying_request_failure::applying_refusal};
use crate::{WorkspaceRuntime, central_statement_values, engine::query_registry::alarm_update};
use central_link_protocol::ReplyPayload;
use central_storage::{BatchOutcome, StatementBatch, StoreFailure};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_runtime_ports::{
    stored_values::{json, string},
    workspace_decode_error::corrupt,
};
use workspace_model as m;
use workspace_model::central_mail as mail;
impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub async fn request_central(
        &self,
        caller: &WorkspaceCaller,
        request: mail::CentralMailRequest,
    ) -> Result<mail::CentralMailResult, StoreFailure> {
        match self.request_central_with_authority(caller, request).await {
            Ok(reply) => Ok(reply),
            Err(ApplyingRequestFailure::Authority(cause)) => {
                Ok(ReplyPayload::mail_authority_refused(cause))
            }
            Err(ApplyingRequestFailure::Store(error)) => Err(error),
        }
    }

    pub async fn request_central_with_authority(
        &self,
        caller: &WorkspaceCaller,
        request: mail::CentralMailRequest,
    ) -> Result<mail::CentralMailResult, ApplyingRequestFailure> {
        // The request path answers as on main; the due batch that follows checks
        // the organization stamp once for this event before any unit (P3).
        let result = self.execute_central(caller, request).await;
        // Due work and publication cannot replace this event's durable answer.
        self.after_request_event().await;
        match result {
            Err(ApplyingRequestFailure::Store(
                StoreFailure::Unavailable | StoreFailure::AnswerLost,
            )) => Ok(mail::CentralMailResult::CentralUnavailable),
            result => result,
        }
    }
    async fn execute_central(
        &self,
        caller: &WorkspaceCaller,
        request: mail::CentralMailRequest,
    ) -> Result<mail::CentralMailResult, ApplyingRequestFailure> {
        use mail::{CentralMailRefusal as Refusal, CentralMailResult as Result};
        let _turn = self.execution.take_central_turn().await;
        if let Err(reason) = self
            .access()
            .admit_workspace(
                self,
                caller.caller(),
                caller.connection().attachment_ref(),
                None,
            )
            .await?
        {
            return applying_refusal(reason, |reason| Result::Refused(Refusal::Workspace(reason)));
        }
        let identity = match caller.caller() {
            m::Caller::Session(session) => m::Identity::AgentSession {
                session: session.clone(),
            },
            m::Caller::Person { person_id, .. } => m::Identity::Person {
                person_id: person_id.clone(),
            },
        };
        let digest = fleet_runtime::workspace_request_log::digest(&request.operation)?;
        let now = self.execution.now()?;
        let cutoff = (now - chrono::Duration::hours(24)).to_rfc3339();
        if matches!(
            &request.operation,
            mail::CentralMailOperation::SendMessage { .. }
        ) {
            return Ok(self
                .execute_direct_send(identity, request, now, digest)
                .await?);
        }
        let rows = self
            .execution
            .statement_host()
            .read(fleet_request_log::lookup_organization_request(
                &request.request_id,
                &cutoff,
            ))
            .await?;
        if let Some(row) = rows.rows.first() {
            let actor = self.access().resolve_identity(&identity).await?;
            if string(&rows, row, "caller")? != actor.as_str()
                || string(&rows, row, "content_digest")? != digest
            {
                return Ok(Result::Refused(Refusal::Workspace(
                    m::RefusalReason::RequestIdReused,
                )));
            }
            let answer: Result = json(string(&rows, row, "answer")?, "runtime_request_log.answer")?;
            if matches!(
                answer,
                Result::Conflict | Result::OutcomeUnknown | Result::CentralUnavailable
            ) {
                return Err(corrupt("runtime_request_log.answer").into());
            }
            // A committed request may have lost its alarm publication before its reply.
            let repair = async {
                central_statement_values::applied(
                    self.execution
                        .statement_host()
                        .run_batch(StatementBatch {
                            statements: vec![],
                            alarm: alarm_update(),
                        })
                        .await?,
                )
            }
            .await;
            match repair {
                Ok(()) => {}
                Err(StoreFailure::Unavailable | StoreFailure::AnswerLost) => {
                    return Ok(Result::OutcomeUnknown);
                }
                Err(
                    error
                    @ (StoreFailure::Corrupt { .. } | StoreFailure::AlarmUnpublishable { .. }),
                ) => return Err(error.into()),
            }
            return Ok(answer);
        }
        let fresh_wake =
            match crate::central_mail_time::decode_fresh_request(&request.operation, now) {
                Ok(command) => command,
                Err(reason) => return Ok(Result::Refused(reason)),
            };
        let actor = self.access().resolve_identity(&identity).await?;
        let decision = if let Some(command) = fresh_wake {
            self.plan_fresh_wake(command).await?
        } else if let mail::CentralMailOperation::SetNoticeCadence { cadence } = &request.operation
        {
            if !matches!(caller.caller(), m::Caller::Session(_)) {
                Err(Refusal::NotSession)
            } else {
                let statements = fleet_notices::plan_notice_cadence_held(
                    self.execution.statement_host(),
                    &actor,
                    *cadence,
                    now,
                )
                .await?;
                Ok((
                    statements,
                    mail::CentralMailAnswer::NoticeCadenceSet { cadence: *cadence },
                ))
            }
        } else {
            self.plan_timer_request(&request.operation, now).await?
        };
        let (mut statements, answer) = match decision {
            Ok((statements, answer)) => (statements, Result::Done(answer)),
            Err(reason) => (vec![], Result::Refused(reason)),
        };

        statements.extend(fleet_request_log::retain_organization_request(
            &request.request_id,
            &actor,
            &digest,
            &answer,
            &now.to_rfc3339(),
            &cutoff,
        )?);
        #[cfg(feature = "test-support")]
        if matches!(
            &answer,
            Result::Done(mail::CentralMailAnswer::WakeSet { .. })
        ) {
            self.execution
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::BeforeWakeRequestCommit)
                .await;
        }
        match self
            .execution
            .statement_host()
            .run_batch(StatementBatch {
                statements,
                alarm: alarm_update(),
            })
            .await
        {
            Ok(BatchOutcome::Applied { .. }) => {}
            // A rejected statement keeps main's store-outage answer on this path.
            Ok(BatchOutcome::StatementRejected { .. }) => return Ok(Result::CentralUnavailable),
            Ok(_) => return Ok(Result::Conflict),
            Err(StoreFailure::AnswerLost) => return Ok(Result::OutcomeUnknown),
            Err(StoreFailure::Unavailable) => return Ok(Result::CentralUnavailable),
            Err(error) => return Err(error.into()),
        }
        drop(_turn);
        if let Result::Done(mail::CentralMailAnswer::WakeSet { wake_id, due_at }) = &answer {
            // The committed receipt stands even if the guarded due transition needs recovery.
            let _ = self.fire_committed_overdue_wake(wake_id, *due_at).await;
        }
        #[cfg(feature = "test-support")]
        if self.execution.take_lost_reply(&request.request_id) {
            return Ok(Result::OutcomeUnknown);
        }
        Ok(answer)
    }
}

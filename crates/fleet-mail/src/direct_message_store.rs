//! Load direct message reducer state from `mail_messages`, guard settlement
//! against that row's version, and plan sender outcome inserts. The adapter
//! reads the sender's session from `access_actor_identities` separately.
use super::direct_message::{self as direct, DirectMessage, DirectMessageSubject};
use crate::{
    direct_message_rows::{
        DIRECT_MESSAGE_COLUMNS, DirectMessageStatus, decode_direct_message, decode_sender_session,
        failure_text,
    },
    message_insert::insert_message,
};
use central_engine_model::{InstanceKey, StateTag};
use central_storage::{GuardedStatement, SqlValue, Statement, StoreFailure};
use fleet_engine::{
    EngineHost, MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState,
    classify_subject_decode_failure, classify_subject_read_failure, guarded_attempt_upsert_sql,
    read_observed_subject_version,
};
use fleet_runtime_ports::CentralStore as _;
use fleet_runtime_ports::stored_values::format_utc_millis;
use fleet_runtime_ports::{
    guarded_statements::{exact, text},
    workspace_decode_error::corrupt,
};

pub struct DirectMessageStore<'a, R: EngineHost>(&'a R);

impl<'a, R: EngineHost> DirectMessageStore<'a, R> {
    pub fn new(runtime: &'a R) -> Self {
        Self(runtime)
    }
}

const DIRECT_MESSAGE_FAILURE: &str = guarded_attempt_upsert_sql!(
    "EXISTS(SELECT 1 FROM mail_messages WHERE message_id IS ?20 AND sender IS ?21 AND message_kind IS ?22 AND state IS ?23 AND failure_reason IS ?24 AND cancel_due_at IS ?25 AND sender_request_id IS ?26 AND created_at IS ?27 AND updated_at IS ?28 AND state NOT IN ('delivered','failed'))"
);

impl<R: EngineHost> MachineStoreAdapter<DirectMessage> for DirectMessageStore<'_, R> {
    async fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<VersionedSubjectState<direct::State>>, SubjectLoadFailure> {
        let rows = self
            .0
            .central_store()
            .read(Statement {
                sql: DIRECT_MESSAGE_COLUMNS,
                args: vec![text(&key.0)],
            })
            .await
            .map_err(classify_subject_read_failure)?;
        let Some(version) = read_observed_subject_version(&rows)? else {
            return Ok(None);
        };
        let row = decode_direct_message(&rows, &version.0)
            .map_err(|failure| classify_subject_decode_failure(failure, version.clone()))?;
        let subject = |sender_session| DirectMessageSubject {
            id: row.id.clone(),
            sender_session,
            sender_request_id: row.sender_request_id.clone(),
            cancel_due_at: row.cancel_due_at,
        };
        let state = match row.status {
            DirectMessageStatus::Delivered => direct::State::Delivered,
            DirectMessageStatus::Failed(reason) => direct::State::Failed(reason),
            DirectMessageStatus::Queued | DirectMessageStatus::Cancelled => {
                let identity = self
                    .0
                    .central_store()
                    .read(Statement {
                        sql: "SELECT identity_kind,identity FROM access_actor_identities WHERE actor_identity_id=?",
                        args: vec![text(&row.sender)],
                    })
                    .await
                    .map_err(classify_subject_read_failure)?;
                let session = decode_sender_session(&identity)
                    .map_err(|failure| classify_subject_decode_failure(failure, version.clone()))?;
                if row.status == DirectMessageStatus::Queued {
                    direct::State::Queued(subject(session))
                } else {
                    direct::State::Cancelled(subject(session))
                }
            }
        };
        Ok(Some(VersionedSubjectState {
            decoded_state: state,
            observed_subject_version: version,
        }))
    }
    fn plan_subject_state_write(
        &self,
        _key: &InstanceKey,
        prior: &VersionedSubjectState<direct::State>,
        next: &direct::State,
    ) -> Result<GuardedStatement, StoreFailure> {
        let failure = match next {
            direct::State::Failed(reason) => text(failure_text(*reason)?),
            direct::State::Queued(_) | direct::State::Cancelled(_) | direct::State::Delivered => {
                SqlValue::Null
            }
        };
        let mut args = vec![
            text(next.state_tag()),
            failure,
            text(format_utc_millis(self.0.current_engine_time()?)),
        ];
        args.extend(prior.observed_subject_version.0.clone());
        if args.len() != 12 {
            return Err(corrupt("mail_messages.version"));
        }
        Ok(exact(
            "UPDATE mail_messages SET state=?1,failure_reason=?2,updated_at=?3 WHERE message_id IS ?4 AND sender IS ?5 AND message_kind IS ?6 AND state IS ?7 AND failure_reason IS ?8 AND cancel_due_at IS ?9 AND sender_request_id IS ?10 AND created_at IS ?11 AND updated_at IS ?12",
            args,
        ))
    }
    fn plan_machine_effect_writes(
        &self,
        _key: &InstanceKey,
        effect: &direct::Effect,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        match effect {
            // An outcome message has no cancel mark and no outcome of its own.
            direct::Effect::TellSender(notice) => Ok(vec![insert_message(notice)?]),
        }
    }
    fn write_attempt_failure_sql(&self) -> &'static str {
        DIRECT_MESSAGE_FAILURE
    }

    fn mail_ready_from_effect(
        &self,
        effect: &direct::Effect,
    ) -> Option<fleet_contracts::MailDeliveryReady> {
        match effect {
            direct::Effect::TellSender(notice) => Some(notice.into()),
        }
    }
}

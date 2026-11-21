//! Resolve present sender facts on the owning executor.
use crate::DirectReadFailure;
use central_storage::StoreFailure;
use fleet_engine::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx::{ColumnIndex, Database, Decode, Encode, IntoArguments, Type};
use workspace_model::{
    ActorIdentityId, Identity, ResolvedSender,
    central_mail::{MessageBody, MessageRecord, MessageSender},
};

#[derive(sqlx::FromRow)]
struct StoredSender {
    identity_kind: String,
    identity: String,
}

#[derive(sqlx::FromRow)]
struct StoredLabel {
    label: String,
}

macro_rules! actor_query {
    ($emit:ident, $actor:expr) => {
        $emit!(
            "src/queries/read_message_sender_session.sql",
            $actor.to_string()
        )
    };
}
macro_rules! label_query {
    ($emit:ident, $machine:expr) => {
        $emit!(
            "src/queries/read_offer_machine_label.sql",
            $machine.to_string()
        )
    };
}
macro_rules! execute {
    ($file:literal, $argument:expr) => {
        sqlx::query_as::<T::Database, _>(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/",
            $file
        )))
        .bind($argument)
    };
}

pub(crate) async fn read_actor<T: CentralTransaction>(
    tx: &mut T,
    actor: &ActorIdentityId,
) -> Result<Option<Identity>, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let row: Option<StoredSender> = actor_query!(execute, actor)
        .fetch_optional(tx.executor())
        .await
        .map_err(|_| StoreFailure::Unavailable)?;
    row.map(|row| {
        let identity: Identity = serde_json::from_str(&row.identity)
            .map_err(|_| corrupt("access_actor_identities.identity"))?;
        if row.identity_kind != identity.stored_kind() {
            return Err(corrupt("access_actor_identities.identity_kind"));
        }
        Ok(identity)
    })
    .transpose()
}

pub(crate) async fn resolve_sender<T: CentralTransaction>(
    tx: &mut T,
    message: &MessageRecord,
) -> Result<ResolvedSender, DirectReadFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let identity = match &message.sender {
        MessageSender::Central => None,
        MessageSender::Actor(actor) => read_actor(tx, actor)
            .await
            .map_err(DirectReadFailure::from_store)?,
    };
    match identity {
        Some(Identity::AgentSession { session }) => {
            let row: Option<StoredLabel> = label_query!(execute, session.machine_id)
                .fetch_optional(tx.executor())
                .await
                .map_err(|_| DirectReadFailure::Unavailable)?;
            let machine_label = row
                .map(|row| {
                    row.label
                        .try_into()
                        .map_err(|_| DirectReadFailure::from_store(corrupt("login_machines.label")))
                })
                .transpose()?;
            Ok(ResolvedSender::Session {
                session,
                display_name: None,
                machine_label,
            })
        }
        Some(Identity::Person { person_id }) => Ok(ResolvedSender::Person {
            person_id,
            display_name: None,
        }),
        None | Some(Identity::RouterComponent { .. }) => {
            if matches!(message.body, MessageBody::Direct { .. }) {
                Err(DirectReadFailure::from_store(corrupt(
                    "mail_messages.sender",
                )))
            } else {
                Ok(ResolvedSender::Central {})
            }
        }
    }
}

#[cfg(test)]
#[path = "direct_store_sender_test.rs"]
mod tests;

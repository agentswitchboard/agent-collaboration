//! Local caller admission and the two direct-message operations.
use std::{future::Future, pin::Pin, sync::Arc};

use collaboration_protocol::message_tools::{
    AdmittedLocalSession, DirectMessageWrite, MachineId, MessageCallerRefusal,
    MessageOperationFuture, MessageSendOutcome, MessageToolRefusal, MessageUnitPort,
    MessageWriteStatus, PostText, RequestId, SessionId, SessionRef,
};
use router_clock::RuntimeClock;

/// The W1 directory adapter reports what it knows about one exact session ID on this Mac.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageSessionPresence {
    KnownOnce,
    UnknownSession,
    AmbiguousSession,
    CouldNotTell,
}

/// The composition owner supplies the W1 directory's exact local lookup to v0 admission.
pub trait MessageSessionDirectory: Send + Sync {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>>;
}

/// The local admission owner supplies the single admitted caller used by either tool.
pub trait MessageCallerAdmission: Send + Sync {
    fn admit(
        &self,
        reported: Option<SessionId>,
    ) -> MessageOperationFuture<'_, AdmittedLocalSession>;
}

/// Composition builds this named v0 stub over the local Fleet identity and W1 directory.
pub struct V0MessageAdmission {
    machine_id: MachineId,
    directory: Arc<dyn MessageSessionDirectory>,
}

impl V0MessageAdmission {
    pub fn new(machine_id: MachineId, directory: Arc<dyn MessageSessionDirectory>) -> Self {
        Self {
            machine_id,
            directory,
        }
    }

    async fn admit_v0(
        &self,
        reported: Option<SessionId>,
    ) -> Result<AdmittedLocalSession, MessageCallerRefusal> {
        let session_id = reported.ok_or(MessageCallerRefusal::CallerRequired)?;
        match self.directory.lookup(&session_id).await {
            MessageSessionPresence::KnownOnce => Ok(
                AdmittedLocalSession::admitted_by_local_session_directory(SessionRef {
                    machine_id: self.machine_id.clone(),
                    session_id,
                }),
            ),
            MessageSessionPresence::UnknownSession => Err(MessageCallerRefusal::UnknownSession),
            MessageSessionPresence::AmbiguousSession => Err(MessageCallerRefusal::AmbiguousSession),
            MessageSessionPresence::CouldNotTell => Err(MessageCallerRefusal::CouldNotTell),
        }
    }
}

impl MessageCallerAdmission for V0MessageAdmission {
    fn admit(
        &self,
        reported: Option<SessionId>,
    ) -> MessageOperationFuture<'_, AdmittedLocalSession> {
        Box::pin(async move {
            self.admit_v0(reported)
                .await
                .map_err(MessageToolRefusal::Caller)
        })
    }
}

struct MessageServices {
    admission: Arc<dyn MessageCallerAdmission>,
    unit: Arc<dyn MessageUnitPort>,
    clock: Arc<dyn RuntimeClock>,
}

/// The collaboration application holds one family configured by the Host's composition owner.
#[derive(Clone, Default)]
pub struct MessageOperations(Option<Arc<MessageServices>>);

impl MessageOperations {
    pub fn new(
        admission: Arc<dyn MessageCallerAdmission>,
        unit: Arc<dyn MessageUnitPort>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self(Some(Arc::new(MessageServices {
            admission,
            unit,
            clock,
        })))
    }

    fn bindings(&self) -> Result<&MessageServices, MessageToolRefusal> {
        self.0.as_deref().ok_or(MessageToolRefusal::NoMessageLink)
    }

    /// Checks the v0 transport report before either message operation can reach the outbox.
    pub async fn admit_caller(
        &self,
        reported: Option<SessionId>,
    ) -> Result<AdmittedLocalSession, MessageToolRefusal> {
        let services = self.bindings()?;
        services.unit.check_available()?;
        services.admission.admit(reported).await
    }

    pub async fn send(
        &self,
        caller: &AdmittedLocalSession,
        to: SessionRef,
        body: PostText,
        requested_id: Option<RequestId>,
    ) -> Result<MessageSendOutcome, MessageToolRefusal> {
        let bindings = self.bindings()?;
        let request_id = match requested_id {
            Some(id) => id,
            None => mint_request_id(bindings.clock.as_ref())?,
        };
        bindings
            .unit
            .submit(
                caller,
                DirectMessageWrite {
                    request_id,
                    to,
                    body,
                },
            )
            .await
    }

    pub async fn request_status(
        &self,
        caller: &AdmittedLocalSession,
        request_id: RequestId,
    ) -> Result<MessageWriteStatus, MessageToolRefusal> {
        self.bindings()?
            .unit
            .request_status(caller, request_id)
            .await
    }
}

fn mint_request_id(clock: &dyn RuntimeClock) -> Result<RequestId, MessageToolRefusal> {
    let sampled = clock.utc_now();
    let millis = u64::try_from(sampled.timestamp_millis())
        .ok()
        .filter(|millis| *millis < 1_u64 << 48)
        .ok_or(MessageToolRefusal::RequestIdUnavailable)?;
    let stamp = uuid::Timestamp::from_unix(
        uuid::ContextV7::new(),
        millis / 1_000,
        sampled.timestamp_subsec_nanos(),
    );
    uuid::Uuid::new_v7(stamp)
        .to_string()
        .parse()
        .map_err(|_| MessageToolRefusal::RequestIdUnavailable)
}

#[cfg(test)]
#[path = "message_operations_test.rs"]
mod tests;

use std::{
    sync::Mutex,
    task::{Context, Waker},
};

use tokio::sync::oneshot;

use super::*;

struct ExactDirectory;

impl MessageSessionDirectory for ExactDirectory {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(std::future::ready(match session_id.as_str() {
            "fixture-caller" => MessageSessionPresence::KnownOnce,
            "ambiguous" => MessageSessionPresence::AmbiguousSession,
            "incomplete" => MessageSessionPresence::CouldNotTell,
            _ => MessageSessionPresence::UnknownSession,
        }))
    }
}

struct HeldUnit {
    started: Mutex<Option<oneshot::Sender<(SessionRef, DirectMessageWrite)>>>,
    caller_released: Mutex<Option<oneshot::Receiver<()>>>,
    status: Mutex<Option<(SessionRef, RequestId)>>,
}

impl MessageUnitPort for HeldUnit {
    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        write: DirectMessageWrite,
    ) -> MessageOperationFuture<'call, MessageSendOutcome> {
        let reply = MessageSendOutcome::Uncertain {
            request_id: write.request_id.clone(),
        };
        let _ = self
            .started
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send((caller.address().clone(), write));
        let released = self.caller_released.lock().unwrap().take().unwrap();
        Box::pin(async move {
            released.await.unwrap();
            Ok(reply)
        })
    }

    fn request_status<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        request_id: RequestId,
    ) -> MessageOperationFuture<'call, MessageWriteStatus> {
        *self.status.lock().unwrap() = Some((caller.address().clone(), request_id));
        Box::pin(std::future::ready(Ok(
            MessageWriteStatus::StatusUnavailable,
        )))
    }
}

#[tokio::test]
async fn named_v0_admission_gates_send_and_status_and_keeps_the_request_identity() {
    let machine_id: MachineId = "019a0000-0000-7000-8000-000000000001".parse().unwrap();
    let request_id: RequestId = "019a0000-0000-7000-8000-000000000002".parse().unwrap();
    let target = SessionRef {
        machine_id: "019a0000-0000-7000-8000-000000000003".parse().unwrap(),
        session_id: "other-mac-session".parse().unwrap(),
    };
    let (started, observed) = oneshot::channel();
    let (release, caller_released) = oneshot::channel();
    let unit = Arc::new(HeldUnit {
        started: Mutex::new(Some(started)),
        caller_released: Mutex::new(Some(caller_released)),
        status: Mutex::new(None),
    });
    let clock = Arc::new(router_clock::ManualClock::new(
        "2026-11-23T02:00:00Z".parse().unwrap(),
    ));
    let admission = Arc::new(V0MessageAdmission::new(
        machine_id.clone(),
        Arc::new(ExactDirectory),
    ));
    let operations = MessageOperations::new(admission, unit.clone(), clock);

    assert!(matches!(
        MessageOperations::default().admit_caller(None).await,
        Err(MessageToolRefusal::NoMessageLink),
    ));
    for (reported, expected) in [
        (None, MessageCallerRefusal::CallerRequired),
        (Some("missing"), MessageCallerRefusal::UnknownSession),
        (Some("ambiguous"), MessageCallerRefusal::AmbiguousSession),
        (Some("incomplete"), MessageCallerRefusal::CouldNotTell),
    ] {
        let answer = operations
            .admit_caller(reported.map(|id| id.parse().unwrap()))
            .await;
        assert!(matches!(answer, Err(MessageToolRefusal::Caller(reason)) if reason == expected));
    }
    let caller = operations
        .admit_caller(Some("fixture-caller".parse().unwrap()))
        .await
        .unwrap();
    let mut sending = std::pin::pin!(operations.send(
        &caller,
        target.clone(),
        "fixture message".to_owned().try_into().unwrap(),
        Some(request_id.clone()),
    ));
    assert!(
        sending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let (sender, submitted) = observed.await.unwrap();
    assert_eq!(sender.machine_id, machine_id);
    assert_eq!(sender.session_id.as_str(), "fixture-caller");
    assert_eq!(submitted.request_id, request_id);
    assert_eq!(submitted.to, target);
    assert_eq!(submitted.body.as_str(), "fixture message");
    assert_eq!(
        operations.request_status(&caller, request_id.clone()).await,
        Ok(MessageWriteStatus::StatusUnavailable)
    );
    assert_eq!(
        unit.status.lock().unwrap().as_ref(),
        Some(&(sender, request_id.clone()))
    );
    release.send(()).unwrap();
    assert_eq!(
        sending.await,
        Ok(MessageSendOutcome::Uncertain { request_id })
    );
}

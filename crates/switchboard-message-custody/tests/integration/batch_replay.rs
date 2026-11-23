#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A scripted peer fixture fails at the unexpected command or observation."
)]

use central_link_client::{
    AdmittedConnection, ClientAction, CorrelatedRequestReply, LinkClient, LinkLaneCommand,
    LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane, RegisteredConnection, WriteBatchCommand,
    WriteBatchDisposition, WriteBatchMemberDisposition,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, ReplyPayload, decode_text, encode_text,
};
use hegel::{TestCase, generators};
use router_clock::{ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, OutboxAccepted, OutboxRequestStatus, OutboxStorage,
    PendingWriteState,
};
use switchboard_message_custody::flush_clock::FlushJitter;
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_submission::{Submission, SubmissionPayload},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::watch;
use workspace_ids::{MachineId, RequestId, SessionId};
use workspace_model::{ChangePosition, ItemRef, WorkspaceAnswer, WorkspaceReply, WorkspaceResult};

use super::outbox_timing::{manual_clock, open_outbox, thread_post, welcome_anchor};

const SHARED_MARK_MS: i64 = 1_763_380_800_000;

struct KnownRecipient;

struct NoFlushJitter;

impl FlushJitter for NoFlushJitter {
    fn jitter_millis(&self, _: SyncTime) -> i64 {
        0
    }
}

impl RecipientSessionDirectory for KnownRecipient {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(std::future::ready(RecipientSessionAnswer::KnownOnce))
    }
}

#[derive(Default)]
struct PeerHistory {
    attempts: Vec<Vec<RequestId>>,
    decisions: BTreeMap<RequestId, ReplyPayload>,
}

struct ScriptedBatchLane {
    admitted: watch::Sender<Option<AdmittedConnection>>,
    lost_first_answers: BTreeSet<RequestId>,
    history: Mutex<PeerHistory>,
}

impl ScriptedBatchLane {
    fn new(admitted: AdmittedConnection, lost_first_answers: BTreeSet<RequestId>) -> Self {
        Self {
            admitted: watch::channel(Some(admitted)).0,
            lost_first_answers,
            history: Mutex::new(PeerHistory::default()),
        }
    }

    fn respond_to_batch(&self, batch: WriteBatchCommand) -> WriteBatchDisposition {
        let Some(admitted) = self.admitted.borrow().clone() else {
            return WriteBatchDisposition::NotReached;
        };
        assert_eq!(batch.connection, admitted.connection);
        let wire = LinkFrame::Data(DataFrame::Batch {
            requests: batch.requests,
        });
        let text = encode_text(&wire, FrameDirection::ToCentral).unwrap();
        let LinkFrame::Data(DataFrame::Batch { requests }) =
            decode_text(&text, FrameDirection::ToCentral).unwrap()
        else {
            panic!("custody did not send a batch frame");
        };
        let mut history = self.history.lock().unwrap();
        let first_batch = history.attempts.is_empty();
        let mut sent_requests = Vec::new();
        let mut members = Vec::new();
        for request in requests.requests() {
            let id = request.payload().write_request_id().unwrap();
            sent_requests.push(id.clone());
            let position = history.decisions.len() + 1;
            let answer = history
                .decisions
                .entry(id.clone())
                .or_insert_with(|| post_created(position));
            let correlation_id = request.correlation_id().clone();
            let disposition = if first_batch && self.lost_first_answers.contains(id) {
                WriteBatchMemberDisposition::AnswerLost { correlation_id }
            } else {
                WriteBatchMemberDisposition::Answered {
                    reply: CorrelatedRequestReply {
                        correlation_id,
                        result: Box::new(answer.clone()),
                    },
                }
            };
            members.push(disposition);
        }
        history.attempts.push(sent_requests);
        if first_batch {
            self.admitted.send_replace(None);
        }
        WriteBatchDisposition::MemberResults {
            members: members.into_boxed_slice(),
        }
    }
}

impl ReadyLinkLane for ScriptedBatchLane {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>> {
        self.admitted.subscribe()
    }

    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>> {
        panic!("message custody requires only admission")
    }

    fn submit_write_batch(
        &self,
        batch: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>> {
        Box::pin(std::future::ready(self.respond_to_batch(batch)))
    }

    fn submit_command(
        &self,
        _: LinkLaneCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition> + Send + '_>> {
        panic!("this scenario issues only the batch command")
    }

    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}

fn post_created(position: usize) -> ReplyPayload {
    let post_id = format!("019f0000-0000-7000-8000-{position:012x}")
        .parse()
        .unwrap();
    ReplyPayload::Workspace(WorkspaceReply {
        result: WorkspaceResult::Done(WorkspaceAnswer::Created {
            item: ItemRef::Post(post_id),
            position: ChangePosition::try_from(u64::try_from(position).unwrap()).unwrap(),
        }),
        actors: Vec::new(),
    })
}

fn new_admission(machine: MachineId, clock: Arc<ManualClock>) -> AdmittedConnection {
    let mut client = LinkClient::new(machine, clock.clone());
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("the scripted socket needs a fresh connection id");
    };
    AdmittedConnection {
        connection: attempt,
        clock_anchor: welcome_anchor(clock.as_ref(), clock.utc_now().timestamp_millis()),
    }
}

async fn check_partial_replay(answers: &[bool]) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("machine_local.sqlite");
    let started_at = SHARED_MARK_MS - 20_000;
    let clock = manual_clock(started_at);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let items: Vec<_> = (1..=u16::try_from(answers.len()).unwrap())
        .map(thread_post)
        .collect();
    let lost: BTreeSet<_> = items
        .iter()
        .zip(answers)
        .filter(|(_, answered)| !**answered)
        .map(|(item, _)| item.request.clone())
        .collect();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        lost.clone(),
    ));
    let outbox = open_outbox(&database).await;
    let mut custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        OutboxWork::new(Box::new(outbox), clock.clone()),
        RecipientInbox::new(
            InboxStore::new(MachineLocalStore::open(&database).await.unwrap()),
            machine.clone(),
            Arc::new(KnownRecipient),
            clock.clone(),
        ),
    );
    for item in &items {
        let switchboard_machine_core_store::OutboxQueuedWrite::Workspace(request) = &item.payload
        else {
            panic!("the replay fixture supplies thread posts");
        };
        let submission = custody
            .submit(
                item.caller.clone(),
                SubmissionPayload::Workspace(request.clone()),
            )
            .await;
        assert!(matches!(submission, Submission::InOutbox(receipt)
            if receipt.id == item.request && receipt.flush_at.unix_millis() == SHARED_MARK_MS));
    }

    assert!(custody.flush_writes().await.unwrap().answered.is_empty());
    assert!(
        lane.history.lock().unwrap().attempts.is_empty(),
        "no frame before the shared minute"
    );
    clock.advance(Duration::from_secs(20)).unwrap();
    let first = custody.flush_writes().await.unwrap();
    assert!(first.issues.is_empty());
    assert_eq!(
        first.answered.len(),
        items.len() - lost.len(),
        "retain replies received before the drop"
    );
    let mut observed = open_outbox(&database).await;
    for item in &items {
        let state = observed
            .pending_for(&item.caller)
            .await
            .unwrap()
            .into_iter()
            .find(|write| write.request_id == item.request)
            .unwrap()
            .state;
        assert_eq!(
            matches!(state, PendingWriteState::SentAnswerLost { .. }),
            lost.contains(&item.request)
        );
    }

    lane.admitted
        .send_replace(Some(new_admission(machine, clock.clone())));
    clock.advance(Duration::from_secs(60)).unwrap();
    let replay = custody.flush_writes().await.unwrap();
    assert!(replay.issues.is_empty());
    let (sent_again, decisions) = {
        let history = lane.history.lock().unwrap();
        assert_eq!(history.attempts.len(), 2);
        (
            history.attempts.get(1).cloned().unwrap(),
            history.decisions.clone(),
        )
    };
    assert_eq!(sent_again, lost.into_iter().collect::<Vec<_>>());
    assert_eq!(decisions.len(), items.len());
    for item in items {
        let status = observed
            .request_status(&item.caller, &item.request)
            .await
            .unwrap();
        let expected = decisions.get(&item.request).unwrap();
        assert!(matches!((status, expected), (
            OutboxRequestStatus::Answered(OutboxAccepted::ThreadPost { post_id, position, actors }),
            ReplyPayload::Workspace(WorkspaceReply {
                result: WorkspaceResult::Done(WorkspaceAnswer::Created { item: ItemRef::Post(expected_id), position: expected_position }),
                actors: expected_actors,
            })
        ) if &post_id == expected_id && &position == expected_position && &actors == expected_actors));
    }
}

#[hegel::test(test_cases = 64)]
fn a_drop_replays_only_the_batch_members_whose_answers_were_lost(case: TestCase) {
    let count = case.draw(generators::integers::<u8>().min_value(0).max_value(5));
    let mut answers = vec![true, false];
    answers.extend((0..count).map(|_| case.draw(generators::booleans())));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(check_partial_replay(&answers));
}

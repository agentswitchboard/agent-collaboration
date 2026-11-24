use central_link_client::ValidatedDeliveryOffer;
use central_link_protocol::{DeliveryMessage, DirectOfferBody, DirectOfferMessage, TimeLeft};
use collaboration_protocol::message_tools::MessageToolRefusal;
use collaboration_service::collaboration_application::message_operations::MessageCallerAdmission;
use router_clock::{ManualClock, RuntimeClock};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, MachineLocalStore,
};
use switchboard_message_custody::{
    recipient_inbox::{InboxOfferResult, RecipientInbox},
    recipient_sessions::{CombinedRecipientSessionDirectory, RecipientSessionSources, SessionId},
};
use workspace_model::{
    PostText,
    central_mail::MessageRecord,
    central_mail::{MessageDelivery, MessageSender, MessageState},
};

const MACHINE: &str = "019f0000-0000-7000-8000-000000000061";
const ACCEPTED: &str = "019f0000-0000-7000-8000-000000000062";
const DEFERRED: &str = "019f0000-0000-7000-8000-000000000063";
const REFUSED: &str = "019f0000-0000-7000-8000-000000000064";

pub(crate) struct Admission {
    inbox: RecipientInbox,
    directory: Arc<CombinedRecipientSessionDirectory>,
    store: PathBuf,
    clock: Arc<ManualClock>,
}

impl Admission {
    pub(crate) async fn new(
        root: &Path,
        clock: Arc<ManualClock>,
        sources: RecipientSessionSources,
    ) -> Self {
        let store = root.join("machine-local.sqlite");
        let directory = Arc::new(CombinedRecipientSessionDirectory::new(sources));
        let inbox = RecipientInbox::new(
            InboxStore::new(MachineLocalStore::open(&store).await.unwrap()),
            MACHINE.parse().unwrap(),
            directory.clone(),
            clock.clone(),
        );
        Self {
            inbox,
            directory,
            store,
            clock,
        }
    }

    pub(super) async fn read_as_reported_caller(
        &self,
        session: &SessionId,
    ) -> Result<Vec<MessageRecord>, MessageToolRefusal> {
        let admission = crate::collaboration_runtime::v0_message_admission_for_test(
            MACHINE.parse().unwrap(),
            self.directory.clone(),
            self.clock.clone(),
        );
        let caller = admission.admit(Some(session.clone())).await?;
        let mut store = InboxStore::new(MachineLocalStore::open(&self.store).await.unwrap());
        let page = store
            .direct_inbox_messages(caller.address(), 10)
            .await
            .unwrap();
        assert!(page.corrupt_rows.is_empty());
        Ok(page.healthy)
    }

    pub(crate) async fn expect_accepted(&mut self, session: &SessionId) {
        assert_eq!(
            self.receive(session, ACCEPTED).await,
            InboxOfferResult::Recorded {
                decision: InboxDecision::Accepted
            },
        );
        let mut durable = InboxStore::new(MachineLocalStore::open(&self.store).await.unwrap());
        let receipt = durable
            .receipt_for(&ACCEPTED.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(receipt.healthy.unwrap().decision, InboxDecision::Accepted);
    }

    pub(super) async fn expect_deferred(&mut self, session: &SessionId) {
        assert_eq!(
            self.receive(session, DEFERRED).await,
            InboxOfferResult::LookupDeferred
        );
        let mut durable = InboxStore::new(MachineLocalStore::open(&self.store).await.unwrap());
        let receipt = durable
            .receipt_for(&DEFERRED.parse().unwrap())
            .await
            .unwrap();
        assert!(receipt.healthy.is_none());
        assert!(self.inbox.needs_local_retry());
    }

    pub(super) async fn expect_ambiguous(&mut self, session: &SessionId) {
        self.expect_refused(session, InboxRefusalReason::AmbiguousSession)
            .await;
    }

    pub(super) async fn expect_unknown(&mut self, session: &SessionId) {
        self.expect_refused(session, InboxRefusalReason::UnknownSession)
            .await;
        assert_eq!(
            self.receive(session, REFUSED).await,
            InboxOfferResult::AlreadyDecided {
                decision: InboxDecision::Refused {
                    reason: InboxRefusalReason::UnknownSession
                },
            }
        );
    }

    async fn expect_refused(&mut self, session: &SessionId, reason: InboxRefusalReason) {
        let decision = InboxDecision::Refused { reason };
        assert_eq!(
            self.receive(session, REFUSED).await,
            InboxOfferResult::Recorded { decision }
        );
        let mut durable = InboxStore::new(MachineLocalStore::open(&self.store).await.unwrap());
        let receipt = durable
            .receipt_for(&REFUSED.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(receipt.healthy.unwrap().decision, decision);
    }

    async fn receive(&mut self, session_id: &SessionId, message_id: &str) -> InboxOfferResult {
        let message = DeliveryMessage::Direct(DirectOfferMessage {
            message_id: message_id.parse().unwrap(),
            sender: MessageSender::Central,
            target: serde_json::from_value(serde_json::json!({
                "machineId": MACHINE,
                "sessionId": session_id,
            }))
            .unwrap(),
            body: DirectOfferBody::Direct {
                body: PostText::try_from("host authority witness".to_owned()).unwrap(),
                delivery: MessageDelivery::Auto,
                reply_to: None,
            },
            state: MessageState::Queued,
            source: None,
        });
        let offer = ValidatedDeliveryOffer::new(
            Box::new(message),
            Some(TimeLeft::try_from(123_000).unwrap()),
        )
        .unwrap();
        self.inbox
            .receive_offer(offer, self.clock.utc_now())
            .await
            .unwrap()
    }
}

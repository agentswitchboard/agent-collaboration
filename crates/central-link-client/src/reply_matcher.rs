//! Success shapes and echoed targets on the legacy request/reply wire.

use central_link_protocol::{ReplyPayload, RequestPayload};
use workspace_ids::{AutomationId, WakeId, WorkspaceId};
use workspace_model::{
    ItemKind, ItemRef, ListQuery, ReadScope, Timestamp, WorkspaceAnswer, WorkspaceOperation,
    WorkspaceRequest, WorkspaceResult,
    central_mail::{CentralMailAnswer, CentralMailOperation, CentralMailResult, NoticeCadence},
};

/// The link client derives the expected reply from the original request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplyExpectation(ExpectedFamily);

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedFamily {
    Mail(MailSuccess),
    Workspace(WorkspaceSuccess),
}

impl ReplyExpectation {
    pub fn from_request(request: &RequestPayload) -> Self {
        Self(match request {
            RequestPayload::Mail(request) => {
                ExpectedFamily::Mail(MailSuccess::for_operation(&request.operation))
            }
            RequestPayload::Workspace(request) => {
                ExpectedFamily::Workspace(WorkspaceSuccess::for_request(request))
            }
        })
    }

    pub fn accepts(&self, reply: &ReplyPayload) -> bool {
        match (&self.0, reply) {
            (
                ExpectedFamily::Mail(expected),
                ReplyPayload::Mail(CentralMailResult::Done(answer)),
            ) => expected.accepts(answer),
            (
                ExpectedFamily::Mail(_),
                ReplyPayload::Mail(
                    CentralMailResult::Refused(_)
                    | CentralMailResult::Conflict
                    | CentralMailResult::OutcomeUnknown
                    | CentralMailResult::CentralUnavailable,
                ),
            ) => true,
            (ExpectedFamily::Workspace(expected), ReplyPayload::Workspace(reply)) => {
                match &reply.result {
                    WorkspaceResult::Done(answer) => expected.accepts(answer),
                    WorkspaceResult::Refused(_)
                    | WorkspaceResult::NotFound { .. }
                    | WorkspaceResult::Conflict
                    | WorkspaceResult::OutcomeUnknown
                    | WorkspaceResult::CentralUnavailable => true,
                }
            }
            (ExpectedFamily::Mail(_), ReplyPayload::Workspace(_))
            | (ExpectedFamily::Workspace(_), ReplyPayload::Mail(_)) => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MailSuccess {
    CadenceSet(NoticeCadence),
    MessageQueued,
    WakeSet(Timestamp),
    WakeCancelled(WakeId),
    AutomationSet,
    AutomationDisabled(AutomationId),
}

impl MailSuccess {
    fn for_operation(operation: &CentralMailOperation) -> Self {
        match operation {
            CentralMailOperation::SetNoticeCadence { cadence } => Self::CadenceSet(*cadence),
            CentralMailOperation::SendMessage { .. } => Self::MessageQueued,
            CentralMailOperation::SetWake { due_at, .. } => Self::WakeSet(*due_at),
            CentralMailOperation::CancelWake { wake_id } => Self::WakeCancelled(wake_id.clone()),
            CentralMailOperation::SetAutomation { .. } => Self::AutomationSet,
            CentralMailOperation::DisableAutomation { automation_id } => {
                Self::AutomationDisabled(automation_id.clone())
            }
        }
    }

    fn accepts(&self, answer: &CentralMailAnswer) -> bool {
        match (self, answer) {
            (Self::CadenceSet(expected), CentralMailAnswer::NoticeCadenceSet { cadence }) => {
                expected == cadence
            }
            (Self::WakeSet(expected), CentralMailAnswer::WakeSet { due_at, .. }) => {
                expected == due_at
            }
            (Self::WakeCancelled(expected), CentralMailAnswer::WakeCancelled { wake_id }) => {
                expected == wake_id
            }
            (
                Self::AutomationDisabled(expected),
                CentralMailAnswer::AutomationDisabled { automation_id },
            ) => expected == automation_id,
            (Self::MessageQueued, CentralMailAnswer::Queued { .. })
            | (Self::AutomationSet, CentralMailAnswer::AutomationSet { .. }) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WorkspaceSuccess {
    WorkspaceCreated,
    WorkspaceUpdated(Option<WorkspaceId>),
    ItemCreated(ItemKind),
    ItemUpdated(ItemRef),
    IdempotentUpdate(ItemRef),
    KindUpdated(ItemKind),
    ItemRemoved(ItemRef),
    ItemRead(ItemRef),
    PageRead,
    MessageStatusRead,
    InboxRead,
    ScopeMarked(ReadScope),
}

impl WorkspaceSuccess {
    fn for_request(request: &WorkspaceRequest) -> Self {
        use WorkspaceOperation as Op;
        match &request.operation {
            Op::CreateWorkspace { .. } => Self::WorkspaceCreated,
            Op::UpdateWorkspace { .. } | Op::ArchiveWorkspace => {
                Self::WorkspaceUpdated(request.workspace_id.clone())
            }
            Op::CreateArea { .. } => Self::ItemCreated(ItemKind::Area),
            Op::CreateProject { .. } => Self::ItemCreated(ItemKind::Project),
            Op::CreateInitiative { .. } => Self::ItemCreated(ItemKind::Initiative),
            Op::CreateDeliverable { .. } => Self::ItemCreated(ItemKind::Deliverable),
            Op::CreateIssue { .. } => Self::ItemCreated(ItemKind::Issue),
            Op::CreateThread { .. } => Self::ItemCreated(ItemKind::Thread),
            Op::ThreadPost { .. } => Self::ItemCreated(ItemKind::Post),
            Op::AddBacklogItem { .. } => Self::ItemCreated(ItemKind::BacklogItem),
            Op::AddReference { .. } => Self::ItemCreated(ItemKind::Reference),
            Op::AddLink { .. } => Self::ItemCreated(ItemKind::Link),
            Op::WorkLogAdd { .. } => Self::ItemCreated(ItemKind::WorkLogEntry),
            Op::ArchiveArea { area_id } | Op::UpdateArea { area_id, .. } => {
                Self::ItemUpdated(ItemRef::Area(area_id.clone()))
            }
            Op::UpdateProject { project_id, .. } => {
                Self::ItemUpdated(ItemRef::Project(project_id.clone()))
            }
            Op::UpdateInitiative { initiative_id, .. } => {
                Self::ItemUpdated(ItemRef::Initiative(initiative_id.clone()))
            }
            Op::UpdateIssue { issue_id, .. } => Self::ItemUpdated(ItemRef::Issue(issue_id.clone())),
            Op::GrantProjectRole { project_id, .. }
            | Op::ReleaseProjectRole { project_id, .. }
            | Op::SetProjectStatus { project_id, .. } => {
                Self::IdempotentUpdate(ItemRef::Project(project_id.clone()))
            }
            Op::GrantDeliverableRole { deliverable_id, .. }
            | Op::ReleaseDeliverableRole { deliverable_id, .. } => {
                Self::IdempotentUpdate(ItemRef::Deliverable(deliverable_id.clone()))
            }
            Op::JoinThread { thread_id, .. }
            | Op::AddThreadMember { thread_id, .. }
            | Op::SetNotifications { thread_id, .. } => {
                Self::IdempotentUpdate(ItemRef::Thread(thread_id.clone()))
            }
            Op::LeaveThread { thread_id } | Op::ResolveThread { thread_id, .. } => {
                Self::ItemUpdated(ItemRef::Thread(thread_id.clone()))
            }
            Op::AddDependency { blocked, .. } => {
                Self::ItemUpdated(ItemRef::Deliverable(blocked.clone()))
            }
            Op::RemoveDependency { .. } => Self::KindUpdated(ItemKind::Deliverable),
            Op::SetIssueStatus { issue_id, .. } => {
                Self::IdempotentUpdate(ItemRef::Issue(issue_id.clone()))
            }
            Op::SetInitiativeStatus { initiative_id, .. } => {
                Self::IdempotentUpdate(ItemRef::Initiative(initiative_id.clone()))
            }
            Op::UpdateBacklogItem { item_id, .. } | Op::CloseBacklogItem { item_id, .. } => {
                Self::ItemUpdated(ItemRef::BacklogItem(item_id.clone()))
            }
            Op::RemoveBacklogItem { item_id } => {
                Self::ItemRemoved(ItemRef::BacklogItem(item_id.clone()))
            }
            Op::RemoveReference { reference_id } => {
                Self::ItemRemoved(ItemRef::Reference(reference_id.clone()))
            }
            Op::RemoveLink { link_id } => Self::ItemRemoved(ItemRef::Link(link_id.clone())),
            Op::WorkLogRedact { entry_id } => {
                Self::ItemUpdated(ItemRef::WorkLogEntry(entry_id.clone()))
            }
            Op::Show { item } => Self::ItemRead(item.clone()),
            Op::Inbox { .. } => Self::InboxRead,
            Op::MarkRead { scope, .. } => Self::ScopeMarked(scope.clone()),
            Op::List { query, .. } => match query {
                ListQuery::MessageStatus { .. } => Self::MessageStatusRead,
                _ => Self::PageRead,
            },
        }
    }

    fn accepts(&self, answer: &WorkspaceAnswer) -> bool {
        use WorkspaceAnswer as Answer;
        match (self, answer) {
            (Self::WorkspaceCreated, Answer::WorkspaceCreated { .. })
            | (Self::PageRead, Answer::Page { .. })
            | (Self::MessageStatusRead, Answer::MessageStatus { .. })
            | (Self::InboxRead, Answer::Inbox { .. }) => true,
            (Self::WorkspaceUpdated(expected), Answer::WorkspaceUpdated { workspace }) => {
                expected.as_ref() == Some(&workspace.workspace_id)
            }
            (Self::ItemCreated(kind), Answer::Created { item, .. })
            | (Self::KindUpdated(kind), Answer::Updated { item, .. }) => kind == &item.kind(),
            (
                Self::ItemUpdated(expected) | Self::IdempotentUpdate(expected),
                Answer::Updated { item, .. },
            )
            | (Self::IdempotentUpdate(expected), Answer::Unchanged { item })
            | (Self::ItemRemoved(expected), Answer::Removed { item, .. }) => expected == item,
            (Self::ItemRead(expected), Answer::Item { item }) => expected == &item.item_ref(),
            (Self::ScopeMarked(expected), Answer::Marked { scope, .. }) => expected == scope,
            _ => false,
        }
    }
}

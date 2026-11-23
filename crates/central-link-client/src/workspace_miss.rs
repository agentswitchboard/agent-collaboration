//! Targets the workspace rules can report missing for the original request.

use workspace_ids::{DependencyId, WorkspaceId};
use workspace_model::{
    ItemKind, ItemRef, ReadScope, RefusalReason, WorkspaceOperation, WorkspaceRequest,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkspaceMisses {
    workspace: Option<WorkspaceId>,
    targets: Vec<MissTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MissTarget {
    Exact(ItemRef),
    // The wire did not carry the ID of this server-resolved ancestor or relation source.
    ResolvedKind(ItemKind),
    Dependency(DependencyId),
}

impl WorkspaceMisses {
    pub(super) fn for_request(request: &WorkspaceRequest) -> Self {
        use MissTarget::{Dependency, Exact, ResolvedKind};
        use WorkspaceOperation as Op;
        let targets = match &request.operation {
            Op::Show { item } => vec![Exact(item.clone())],
            Op::UpdateWorkspace { .. } | Op::ArchiveWorkspace => request
                .workspace_id
                .iter()
                .map(|id| Exact(ItemRef::Workspace(id.clone())))
                .collect(),
            Op::ArchiveArea { area_id }
            | Op::UpdateArea { area_id, .. }
            | Op::CreateProject { area_id, .. }
            | Op::CreateThread { area_id, .. }
            | Op::AddBacklogItem { area_id, .. } => vec![Exact(ItemRef::Area(area_id.clone()))],
            Op::UpdateProject { project_id, .. }
            | Op::CreateDeliverable { project_id, .. }
            | Op::GrantProjectRole { project_id, .. }
            | Op::ReleaseProjectRole { project_id, .. }
            | Op::SetProjectStatus { project_id, .. }
            | Op::WorkLogAdd { project_id, .. } => {
                vec![Exact(ItemRef::Project(project_id.clone()))]
            }
            Op::UpdateInitiative { initiative_id, .. }
            | Op::SetInitiativeStatus { initiative_id, .. } => {
                vec![Exact(ItemRef::Initiative(initiative_id.clone()))]
            }
            Op::CreateIssue { deliverable_id, .. }
            | Op::GrantDeliverableRole { deliverable_id, .. }
            | Op::ReleaseDeliverableRole { deliverable_id, .. } => vec![
                Exact(ItemRef::Deliverable(deliverable_id.clone())),
                ResolvedKind(ItemKind::Project),
            ],
            Op::UpdateIssue { issue_id, .. } => vec![Exact(ItemRef::Issue(issue_id.clone()))],
            Op::SetIssueStatus { issue_id, .. } => vec![
                Exact(ItemRef::Issue(issue_id.clone())),
                ResolvedKind(ItemKind::Deliverable),
                ResolvedKind(ItemKind::Project),
            ],
            Op::JoinThread { thread_id, .. }
            | Op::AddThreadMember { thread_id, .. }
            | Op::LeaveThread { thread_id }
            | Op::SetNotifications { thread_id, .. }
            | Op::ThreadPost { thread_id, .. }
            | Op::ResolveThread { thread_id, .. }
            | Op::MarkRead {
                scope: ReadScope::Thread { thread_id },
                ..
            } => vec![Exact(ItemRef::Thread(thread_id.clone()))],
            Op::AddDependency { blocking, blocked } => vec![
                Exact(ItemRef::Deliverable(blocking.clone())),
                Exact(ItemRef::Deliverable(blocked.clone())),
            ],
            Op::RemoveDependency { dependency_id } => vec![
                Dependency(dependency_id.clone()),
                ResolvedKind(ItemKind::Deliverable),
            ],
            Op::UpdateBacklogItem { item_id, .. } | Op::RemoveBacklogItem { item_id } => {
                vec![Exact(ItemRef::BacklogItem(item_id.clone()))]
            }
            Op::CloseBacklogItem { item_id, link, .. } => {
                std::iter::once(Exact(ItemRef::BacklogItem(item_id.clone())))
                    .chain(link.iter().cloned().map(Exact))
                    .collect()
            }
            Op::AddReference { source, target, .. } => vec![
                Exact(ItemRef::from(source.clone())),
                Exact(target.as_item_ref().clone()),
            ],
            Op::AddLink { source, .. } => vec![Exact(ItemRef::from(source.clone()))],
            Op::RemoveReference { reference_id } => {
                removed_relation(ItemRef::Reference(reference_id.clone()))
            }
            Op::RemoveLink { link_id } => removed_relation(ItemRef::Link(link_id.clone())),
            Op::WorkLogRedact { entry_id } => vec![Exact(ItemRef::WorkLogEntry(entry_id.clone()))],
            Op::CreateWorkspace { .. }
            | Op::CreateArea { .. }
            | Op::CreateInitiative { .. }
            | Op::List { .. }
            | Op::Inbox { .. }
            | Op::MarkRead {
                scope: ReadScope::Inbox,
                ..
            } => Vec::new(),
        };
        Self {
            workspace: request.workspace_id.clone(),
            targets,
        }
    }

    fn accepts_item(&self, item: &ItemRef) -> bool {
        self.targets.iter().any(|allowed| match allowed {
            MissTarget::Exact(expected) => expected == item,
            MissTarget::ResolvedKind(kind) => *kind == item.kind(),
            MissTarget::Dependency(_) => false,
        })
    }

    pub(super) fn accepts_miss(&self, workspace: &WorkspaceId, item: &ItemRef) -> bool {
        self.workspace.as_ref() == Some(workspace) && self.accepts_item(item)
    }

    pub(super) fn accepts_refusal(&self, refusal: &RefusalReason) -> bool {
        match refusal {
            RefusalReason::NotFound { item } => self.accepts_item(item),
            RefusalReason::DependencyNotFound { dependency_id } => self.targets.iter().any(|allowed| {
                matches!(allowed, MissTarget::Dependency(expected) if expected == dependency_id)
            }),
            _ => true,
        }
    }
}

fn removed_relation(relation: ItemRef) -> Vec<MissTarget> {
    vec![
        MissTarget::Exact(relation),
        MissTarget::ResolvedKind(ItemKind::Post),
        MissTarget::ResolvedKind(ItemKind::WorkLogEntry),
        MissTarget::ResolvedKind(ItemKind::Deliverable),
    ]
}

#[cfg(test)]
#[path = "workspace_miss_test.rs"]
mod tests;

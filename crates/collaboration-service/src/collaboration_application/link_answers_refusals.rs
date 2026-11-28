//! The next step for each of Central's own refusals, matched exhaustively.
use collaboration_protocol::central_tool_port::{
    CentralMailRefusal, RefusalReason, TimingRequirement,
};

use super::Wording;

/// Central states the refusal in its own words; the Router adds the next step.
pub(super) fn workspace_refusal(reason: &RefusalReason) -> Wording {
    let next_step = match reason {
        RefusalReason::DependencyNotFound { .. } => "Check the dependency's ID, then try again.",
        RefusalReason::MachineRevoked => "Enroll this Mac again, then try again.",
        RefusalReason::RequestIdReused => "Send this request with a new request ID.",
        RefusalReason::WorkspaceNotAllowed => {
            "Check the workspaceId, or ask the workspace's owner to allow this Mac."
        }
        RefusalReason::WorkspaceNameInUse { .. } => "Choose a different workspace name.",
        RefusalReason::NotFound { .. } => "Check the item's ID, then try again.",
        RefusalReason::NotLead { .. } => "Ask the project's lead to grant the role.",
        RefusalReason::LeadRequired { .. } => "Name a new lead before removing the current one.",
        RefusalReason::NotCreator { .. } => "Ask the thread's creator to make this change.",
        RefusalReason::ThreadResolved { .. } => "Unresolve the thread first, or start a new one.",
        RefusalReason::NotParticipant { .. } => {
            "Call from a session that takes part in the thread."
        }
        RefusalReason::NotMember { .. } => "Join the thread, then try again.",
        RefusalReason::WouldCycle { .. } => "Choose a dependency that does not form a cycle.",
        RefusalReason::CrossProject { .. } => "Link deliverables within one project only.",
        RefusalReason::DuplicateDependency { .. } => "Nothing to add; the dependency exists.",
        RefusalReason::EmptyDescription => "Give the backlog item a description.",
        RefusalReason::BacklogItemClosed { .. } => "Reopen the backlog item first.",
        RefusalReason::SourceKindNotAllowed { .. } => "Use a source of an allowed kind.",
        RefusalReason::ChildrenNotClosed { .. } => "Close the open children first.",
        RefusalReason::ParentClosed { .. } => "Reopen the parent first.",
        RefusalReason::UnknownActor { .. } => "Check the actor's ID, then try again.",
        RefusalReason::UnheldAddressee => "Address a role that someone holds.",
        RefusalReason::InvalidInput { .. } => "Correct the named input, then try again.",
    };
    Wording::new(format!("Refused by Central: {reason}"), next_step)
}

/// Central's mail refusals carry no text of their own, so the Router words both parts.
pub(super) fn mail_refusal(reason: &CentralMailRefusal) -> Wording {
    let (state, next_step) = match reason {
        CentralMailRefusal::NotSession => (
            "Central requires a session caller".into(),
            "Call from an agent session.",
        ),
        CentralMailRefusal::TimeOutOfRange { field } => (
            format!("{field} is outside the range Central accepts"),
            "Choose a time within range, then try again.",
        ),
        CentralMailRefusal::TimingViolation { field, requirement } => (
            format!(
                "{field} {}",
                match requirement {
                    TimingRequirement::FiveMinuteGrid => "is not on a five-minute mark",
                    TimingRequirement::WithinSevenDays => "is more than seven days away",
                    TimingRequirement::FutureTime => "is not in the future",
                }
            ),
            "Choose a time that meets the rule, then try again.",
        ),
        CentralMailRefusal::Workspace(reason) => return workspace_refusal(reason),
        CentralMailRefusal::UnknownSession { .. } => (
            "Central cannot route to that session's machine".into(),
            "Check the machineId and sessionId, then try again.",
        ),
        CentralMailRefusal::NoOccurrence => (
            "the schedule has no occurrence".into(),
            "Choose a schedule that occurs at least once.",
        ),
        CentralMailRefusal::InvalidInput { field } => (
            format!("{field} is invalid"),
            "Correct the named input, then try again.",
        ),
        CentralMailRefusal::WakeNotFound { .. } => (
            "Central has no such wake".into(),
            "Check the wake's ID, then try again.",
        ),
        CentralMailRefusal::WakeAlreadyFired { .. } => (
            "the wake already fired".into(),
            "Nothing to cancel; set a new wake if one is needed.",
        ),
        CentralMailRefusal::AutomationNotFound { .. } => (
            "Central has no such automation".into(),
            "Check the automation's ID, then try again.",
        ),
        CentralMailRefusal::AutomationAlreadyDone { .. } => (
            "the automation is already finished".into(),
            "Nothing to change; create a new automation if one is needed.",
        ),
    };
    Wording::new(format!("Refused by Central: {state}"), next_step)
}

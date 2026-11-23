use approval_request_protocol::{OfferView, PrivateEnding, ProviderStopCause};
use switchboard_approval_ports::{
    InvalidApprovalTurnId, LocalApprovalRefusal, MacWaitCause, ReplyHandoverKind, ReplyKind,
    ReplySubmissionKind, ReplyableStopCause, TerminalRaiseRefusal, UncarriableAsk,
    UncarriableShape, WaitReleaseKind, approval_value_builders,
};

mod session_binding;

// These reads compile as an external consumer. A new case requires an explicit read.
fn read_offer(offer: OfferView) {
    match offer {
        OfferView::ProviderApproval { offer: _ } | OfferView::ProviderQuestion { offer: _ } => {}
    }
}

fn read_reply(reply: ReplyKind) {
    match reply {
        ReplyKind::CentralEnding(ending) => match ending {
            PrivateEnding::ProviderApproval { ending: _ }
            | PrivateEnding::ProviderQuestion { ending: _ } => {}
        },
        ReplyKind::LocalRefusal(refusal) => match refusal {
            LocalApprovalRefusal::UnregisteredSessionBinding
            | LocalApprovalRefusal::UnknownCentralSession
            | LocalApprovalRefusal::NoRoom => {}
            LocalApprovalRefusal::WaitStopped(cause) => match cause {
                ReplyableStopCause::TurnStopped
                | ReplyableStopCause::SessionClosed
                | ReplyableStopCause::AskReplaced => {}
            },
            LocalApprovalRefusal::RaiseRefused(reason) => match reason {
                TerminalRaiseRefusal::AlreadyStopped
                | TerminalRaiseRefusal::SessionEnded
                | TerminalRaiseRefusal::ContentsDiffer
                | TerminalRaiseRefusal::TooManyOutstanding => {}
            },
        },
    }
}

fn read_unadmitted(reason: UncarriableAsk) {
    match reason {
        UncarriableAsk::UnprovedSession
        | UncarriableAsk::UnprovedTool
        | UncarriableAsk::UnprovedTurn => {}
        UncarriableAsk::InvalidOffer(shape) | UncarriableAsk::InvalidAnswer(shape) => match shape {
            UncarriableShape::NumberField
            | UncarriableShape::ConstrainedField
            | UncarriableShape::PlanApproval
            | UncarriableShape::NonSessionForm
            | UncarriableShape::UnsupportedShape => {}
        },
    }
}

fn read_submission(kind: ReplySubmissionKind) -> u8 {
    match kind {
        ReplySubmissionKind::Handover(handover) => match handover {
            ReplyHandoverKind::NotHandedOver => 0,
            ReplyHandoverKind::HandedToConnection => 1,
            ReplyHandoverKind::CannotTell => 2,
        },
        ReplySubmissionKind::SlotGoneBeforeSubmission => 3,
    }
}

fn read_release(kind: WaitReleaseKind) -> u8 {
    match kind {
        WaitReleaseKind::ReleasedWithoutReply => 0,
        WaitReleaseKind::AlreadyGone => 1,
        WaitReleaseKind::CannotReleaseSilently => 2,
        WaitReleaseKind::CannotTell => 3,
    }
}

#[test]
fn public_contract_reads_every_closed_case() {
    let _compile_readers = (
        read_offer,
        read_reply,
        read_unadmitted,
        |error| match error {
            InvalidApprovalTurnId::InvalidUuid => {}
        },
        |cause| match cause {
            MacWaitCause::TurnStopRequested
            | MacWaitCause::SessionClosed
            | MacWaitCause::AgentConnectionLost
            | MacWaitCause::AgentAskWithdrawn => {}
        },
    );
    let (harness_values, _, _) = approval_value_builders();
    let observations = [
        ReplySubmissionKind::Handover(ReplyHandoverKind::NotHandedOver),
        ReplySubmissionKind::Handover(ReplyHandoverKind::HandedToConnection),
        ReplySubmissionKind::Handover(ReplyHandoverKind::CannotTell),
        ReplySubmissionKind::SlotGoneBeforeSubmission,
    ]
    .map(|kind| read_submission(harness_values.reply_submission_outcome(kind).kind()));
    assert_eq!(observations, [0, 1, 2, 3]);
    let releases = [
        WaitReleaseKind::ReleasedWithoutReply,
        WaitReleaseKind::AlreadyGone,
        WaitReleaseKind::CannotReleaseSilently,
        WaitReleaseKind::CannotTell,
    ]
    .map(|kind| read_release(harness_values.wait_release_outcome(kind).kind()));
    assert_eq!(releases, [0, 1, 2, 3]);
}

#[test]
fn ended_facts_map_to_exact_provider_stop_causes() {
    for (event, expected) in [
        (
            MacWaitCause::TurnStopRequested,
            ProviderStopCause::TurnStopped,
        ),
        (
            MacWaitCause::SessionClosed,
            ProviderStopCause::SessionClosed,
        ),
        (
            MacWaitCause::AgentConnectionLost,
            ProviderStopCause::ProviderConnectionLost,
        ),
        (
            MacWaitCause::AgentAskWithdrawn,
            ProviderStopCause::AskWithdrawn,
        ),
    ] {
        assert_eq!(event.provider_stop_cause(), expected, "{event:?}");
    }
}

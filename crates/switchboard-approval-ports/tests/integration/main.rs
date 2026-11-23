use approval_request_protocol::{OfferView, PrivateEnding, ProviderStopCause};
use switchboard_approval_ports::{
    InvalidApprovalTurnId, LocalApprovalRefusal, MacWaitCause, OpenedSessionFence,
    OpenedSessionIdentity, ReplyHandoverKind, ReplyKind, TerminalRaiseRefusal, UncarriableAsk,
    UncarriableShape, WaitReleaseKind, approval_value_builders,
};

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
            LocalApprovalRefusal::UnknownCentralSession
            | LocalApprovalRefusal::WaitStopped(_)
            | LocalApprovalRefusal::NoRoom => {}
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

fn read_handover(kind: ReplyHandoverKind) -> u8 {
    match kind {
        ReplyHandoverKind::NotHandedOver => 0,
        ReplyHandoverKind::HandedToConnection => 1,
        ReplyHandoverKind::CannotTell => 2,
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

fn read_opened_session(identity: OpenedSessionIdentity) {
    match identity {
        OpenedSessionIdentity::NotKnownToCentral
        | OpenedSessionIdentity::Opened {
            central_session_id: _,
        } => {}
        OpenedSessionIdentity::KnownButFenced { reason } => match reason {
            OpenedSessionFence::SettledByCentral
            | OpenedSessionFence::EndInProgress
            | OpenedSessionFence::EnrollmentEnded => {}
        },
    }
}

#[test]
fn public_contract_reads_every_closed_case() {
    let _compile_readers = (
        read_offer,
        read_reply,
        read_unadmitted,
        read_opened_session,
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
    let (harness_values, _) = approval_value_builders();
    let observations = [
        ReplyHandoverKind::NotHandedOver,
        ReplyHandoverKind::HandedToConnection,
        ReplyHandoverKind::CannotTell,
    ]
    .map(|kind| read_handover(harness_values.reply_handover(kind).kind()));
    assert_eq!(observations, [0, 1, 2]);
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

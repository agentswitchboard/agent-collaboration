use approval_request_protocol::{OfferView, PrivateEnding, ProviderStopCause};
use switchboard_approval_ports::{
    InvalidApprovalTurnId, LocalApprovalRefusal, MacWaitCause, ReplyHandoverKind, ReplyKind,
    UncarriableAsk, UncarriableShape, approval_value_builders,
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
            | LocalApprovalRefusal::RaiseRefused
            | LocalApprovalRefusal::NoRoom => {}
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
    let (harness_values, _) = approval_value_builders();
    let observations = [
        ReplyHandoverKind::NotHandedOver,
        ReplyHandoverKind::HandedToConnection,
        ReplyHandoverKind::CannotTell,
    ]
    .map(|kind| read_handover(harness_values.reply_handover(kind).kind()));
    assert_eq!(observations, [0, 1, 2]);
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

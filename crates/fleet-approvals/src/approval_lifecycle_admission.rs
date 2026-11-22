use approval_request_protocol::{Answer, OfferView, Refusal, check_answer};
use central_engine_model::TransitionError;

use crate::{
    AdmittedDecider, ApprovalDestination, ApprovalRefusal, FactorStanding, PassOnAuthority,
    RetainedApproval,
};

pub(super) fn check_destination(
    saved: &RetainedApproval,
    actor: &AdmittedDecider,
) -> Result<(), TransitionError<ApprovalRefusal>> {
    if !actor.is_destination(&saved.destinations) {
        return Err(destination_refusal(Refusal::NotVisible {}));
    }
    if let AdmittedDecider::Session(session) = actor
        && saved.requester() == Some(session)
    {
        return Err(destination_refusal(Refusal::RequesterCannotAnswer {}));
    }
    Ok(())
}

pub(super) fn validate_answer(
    saved: &RetainedApproval,
    actor: &AdmittedDecider,
    answer: &Answer,
) -> Result<(), TransitionError<ApprovalRefusal>> {
    let offer = saved.raise.offer_view();
    check_answer(&offer, answer)
        .map_err(|reason| destination_refusal(Refusal::InvalidAnswer { reason }))?;
    let needs_factor = match (&offer, answer) {
        (OfferView::ProviderApproval { offer }, Answer::Approval { option_id }) => offer
            .options
            .as_slice()
            .iter()
            .any(|option| &option.id == option_id && option.needs_person_with_fresh_factor()),
        (OfferView::UnprovenCall { .. }, _) => true,
        (OfferView::ProviderApproval { .. } | OfferView::ProviderQuestion { .. }, _) => false,
    };
    if needs_factor {
        match actor {
            AdmittedDecider::Person {
                factor: FactorStanding::Fresh,
                ..
            } => {}
            AdmittedDecider::Person {
                factor: FactorStanding::AbsentOrStale,
                ..
            } => {
                return Err(destination_refusal(Refusal::SecondFactorRequired {}));
            }
            AdmittedDecider::Session(_) => {
                return Err(destination_refusal(Refusal::KeptChoiceNeedsAPerson {}));
            }
        }
    }
    Ok(())
}

pub(super) fn check_parent(
    saved: &RetainedApproval,
    authority: PassOnAuthority,
) -> Result<workspace_ids::SessionRef, TransitionError<ApprovalRefusal>> {
    let PassOnAuthority::LiveParent(parent) = authority else {
        return Err(destination_refusal(Refusal::NotVisible {}));
    };
    if saved
        .requester()
        .is_some_and(|requester| requester != &parent)
        && saved
            .destinations
            .contains(&ApprovalDestination::Session(parent.clone()))
    {
        Ok(parent)
    } else {
        Err(destination_refusal(Refusal::NotVisible {}))
    }
}

pub(super) fn destination_refusal(refusal: Refusal) -> TransitionError<ApprovalRefusal> {
    TransitionError::Domain(ApprovalRefusal::Destination { refusal })
}

//! Session decision policy and its checked projection onto the C2 wire.
use approval_request_protocol::{
    Answer, ApprovalDecision, DecideApprovalAsSessionReply, DecideApprovalAsSessionRequest, Ending,
    OfferView, OptionLifetime, ProviderAnswer, SessionDecider, check_answer,
};
use fleet_actor_identity::AuthenticatedSession;
use fleet_runtime_ports::workspace_decode_error::corrupt;

use super::{
    ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput,
    destination_commands::approval_session_address,
};
use crate::{
    AdmittedDecider, ApprovalActor, ApprovalDecideInput, ApprovalDecisionReply,
    ApprovalSessionFacts, ApprovalState, PendingApprovalReply,
    approval_store_reads::read_parent_approval_request,
    approval_store_transaction::ApprovalStoreTransaction,
};

pub enum SessionDecisionApply {
    PersonRequired,
    Reply(PendingApprovalReply<DecideApprovalAsSessionReply>),
}

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub async fn decide_as_session(
        &mut self,
        admitted: &AuthenticatedSession,
        request: DecideApprovalAsSessionRequest,
    ) -> Result<SessionDecisionApply, ApprovalApplyFailure> {
        let (organization, session, address) = approval_session_address(admitted);
        // Parked-call decisions require a person regardless of the submitted request id.
        if matches!(request.value, Answer::UnprovenCall { .. }) {
            return Ok(SessionDecisionApply::PersonRequired);
        }
        let at = self.sample_applying_time()?;
        let record = read_parent_approval_request(
            self.transaction,
            organization,
            &request.request_id,
            &session,
            at,
        )
        .await?;
        let Some(record) = record else {
            return Ok(SessionDecisionApply::Reply(self.stage_reply(
                address,
                ApprovalTransitionOutput::with_reply(DecideApprovalAsSessionReply::NotVisible {}),
            )));
        };
        let actor = ApprovalActor::Session(session.clone());
        let input = ApprovalDecideInput {
            request_id: request.request_id,
            operation_id: request.operation_id,
            value: request.value,
        };
        if let ApprovalState::Waiting { saved } = &record.state
            && at < saved.window.deadline()
            && requires_person(&saved.raise.offer_view(), &input.value)
        {
            return Ok(SessionDecisionApply::PersonRequired);
        }
        let applied = self
            .decide_selected_approval(
                organization,
                AdmittedDecider::Session(session.clone()),
                input.clone(),
                &record,
            )
            .await?;
        let reply = match &applied.reply {
            ApprovalDecisionReply::SavedDecision { answer, by } if *by == actor.identity() => {
                let answer = match answer {
                    Answer::Approval { option_id } => ProviderAnswer::Approval {
                        option_id: option_id.clone(),
                    },
                    Answer::Question { answer } => ProviderAnswer::Question {
                        answer: answer.clone(),
                    },
                    Answer::UnprovenCall { .. } => {
                        return Err(corrupt("approval_requests.session_decision_kind").into());
                    }
                };
                DecideApprovalAsSessionReply::SavedDecision {
                    decision: ApprovalDecision {
                        answer,
                        by: SessionDecider::Session {
                            session: session.session_id,
                        },
                    },
                }
            }
            ApprovalDecisionReply::SavedDecision { .. } => {
                return Err(corrupt("approval_requests.session_decision_actor").into());
            }
            ApprovalDecisionReply::Standing { .. } => {
                let ApprovalState::Ended { closed, .. } = &record.state else {
                    return Err(corrupt("approval_requests.session_decision_standing").into());
                };
                match closed.ending.clone().into_ending() {
                    Ending::Decided { by, .. } => DecideApprovalAsSessionReply::AlreadyDecided {
                        by,
                        at: closed.ended_at,
                    },
                    Ending::Cancelled { .. } | Ending::TimedOut {} => {
                        DecideApprovalAsSessionReply::TooLate {}
                    }
                    Ending::UnprovenCallDecided { .. } => {
                        return Err(corrupt("approval_requests.session_decision_kind").into());
                    }
                }
            }
            ApprovalDecisionReply::Invalid {} => {
                let saved = record
                    .state
                    .retained()
                    .ok_or_else(|| corrupt("approval_requests.session_decision"))?;
                let reason = check_answer(&saved.raise.offer_view(), &input.value)
                    .err()
                    .ok_or_else(|| corrupt("approval_requests.session_decision_reason"))?;
                DecideApprovalAsSessionReply::Invalid { reason }
            }
            ApprovalDecisionReply::TooLate {} => DecideApprovalAsSessionReply::TooLate {},
            ApprovalDecisionReply::NotVisible {} => DecideApprovalAsSessionReply::NotVisible {},
            ApprovalDecisionReply::OperationConflict {} => {
                DecideApprovalAsSessionReply::OperationConflict {}
            }
        };
        Ok(SessionDecisionApply::Reply(
            self.stage_reply(address, applied.replace_reply(reply)),
        ))
    }
}

fn requires_person(offer: &OfferView, value: &Answer) -> bool {
    let (OfferView::ProviderApproval { offer }, Answer::Approval { option_id }) = (offer, value)
    else {
        return false;
    };
    offer
        .options
        .as_slice()
        .iter()
        .find(|option| &option.id == option_id)
        .is_some_and(|option| match option.lasts {
            OptionLifetime::ThisRequest {}
            | OptionLifetime::ThisTurn { .. }
            | OptionLifetime::ThisSession { .. } => false,
            OptionLifetime::KeptUntilChanged { .. } | OptionLifetime::NotVerified {} => true,
        })
}

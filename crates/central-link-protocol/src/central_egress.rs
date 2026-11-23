//! Checked outbound text and the existing fault answer for oversized replies.

use crate::{
    DataFrame, FrameDirection, LINK_MAX_EGRESS_TEXT_BYTES, LinkCallRefusal, LinkFrame,
    ProtocolError, ReplyPayload, RequestStatusAnswer, TypedCallResult, encode_text,
};

pub struct CentralEgress {
    pub text: String,
    /// Original encoded size when a request received its existing fault reply.
    pub oversized_reply_bytes: Option<usize>,
}

pub fn encode_central_egress(frame: &LinkFrame) -> Result<CentralEgress, ProtocolError> {
    let text = encode_text(frame, FrameDirection::ToRouter)?;
    let bytes = text.len();
    if bytes <= LINK_MAX_EGRESS_TEXT_BYTES {
        return Ok(CentralEgress {
            text,
            oversized_reply_bytes: None,
        });
    }
    let fallback = match frame {
        LinkFrame::Data(DataFrame::Reply {
            correlation_id,
            result,
        }) => DataFrame::Reply {
            correlation_id: correlation_id.clone(),
            result: Box::new(result.unavailable_answer()),
        },
        LinkFrame::Data(DataFrame::CentralCallReply { correlation_id, .. }) => {
            DataFrame::CentralCallReply {
                correlation_id: correlation_id.clone(),
                result: Box::new(TypedCallResult::Refused {
                    reason: LinkCallRefusal::CentralFault,
                }),
            }
        }
        LinkFrame::Data(DataFrame::RequestStatusReply {
            correlation_id,
            result,
        }) => match result.as_ref() {
            RequestStatusAnswer::Answered(reply) => DataFrame::RequestStatusReply {
                correlation_id: correlation_id.clone(),
                result: Box::new(RequestStatusAnswer::Answered(reply.unavailable_answer())),
            },
            RequestStatusAnswer::NotFound | RequestStatusAnswer::Refused(_) => {
                return Err(ProtocolError::EgressTooBig { bytes });
            }
        },
        LinkFrame::Ping
        | LinkFrame::Pong
        | LinkFrame::Data(
            DataFrame::Welcome { .. }
            | DataFrame::MacCapabilities(_)
            | DataFrame::Request { .. }
            | DataFrame::Batch { .. }
            | DataFrame::RequestStatus { .. }
            | DataFrame::Deliver { .. }
            | DataFrame::Receipts { .. }
            | DataFrame::ReceiptResults { .. }
            | DataFrame::CallCentral { .. }
            | DataFrame::CallMac { .. }
            | DataFrame::MacCallReply { .. }
            | DataFrame::RenewMachineToken { .. }
            | DataFrame::MachineTokenRenewal { .. },
        ) => return Err(ProtocolError::EgressTooBig { bytes }),
    };
    let text = encode_text(&LinkFrame::Data(fallback), FrameDirection::ToRouter)?;
    check_central_egress_length(text.len())?;
    Ok(CentralEgress {
        text,
        oversized_reply_bytes: Some(bytes),
    })
}

pub fn check_central_egress_length(bytes: usize) -> Result<(), ProtocolError> {
    if bytes <= LINK_MAX_EGRESS_TEXT_BYTES {
        Ok(())
    } else {
        Err(ProtocolError::EgressTooBig { bytes })
    }
}

impl ReplyPayload {
    fn unavailable_answer(&self) -> Self {
        match self {
            Self::Workspace(_) => Self::Workspace(workspace_model::WorkspaceReply {
                result: workspace_model::WorkspaceResult::CentralUnavailable,
                actors: Vec::new(),
            }),
            Self::Mail(_) => {
                Self::Mail(workspace_model::central_mail::CentralMailResult::CentralUnavailable)
            }
        }
    }
}

#[cfg(test)]
#[path = "central_egress_test.rs"]
mod tests;

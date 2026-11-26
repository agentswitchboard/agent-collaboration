use std::sync::Mutex;

use central_link_protocol::{
    CentralCall, CentralCallReply, CentralLinkRpc, DataFrame, FrameDirection, LinkCallRefusal,
    LinkFrame, TypedCallResult, decode_text, encode_text,
    person_link_rpc::{
        ListOrganizationWorkspacesRpc, ListPersonApprovalsRpc, OrganizationWorkspacePage,
    },
};
use fleet_rpc::Envelope;

#[derive(Clone, Copy)]
pub enum ApprovalRelayFault {
    RepeatListCursor,
    WrongDecideReply,
    MalformedDecideReply,
    DropSavedDecision,
    RefuseSavedDecisionOnSocket { socket: usize },
}

pub(super) fn scripted_approval_reply(
    fault: &Mutex<Option<ApprovalRelayFault>>,
    request: &CentralCall,
) -> Option<CentralCallReply> {
    let mut fault = fault.lock().unwrap();
    let (Some(ApprovalRelayFault::RepeatListCursor), CentralCall::ListPersonApprovals(request)) =
        (*fault, request)
    else {
        return None;
    };
    let cursor = request.body.cursor.clone()?;
    *fault = None;
    Some(ListPersonApprovalsRpc::reply(Envelope {
        target: request.target.clone(),
        body: approval_request_protocol::ApprovalListReply::Page {
            items: Vec::new(),
            next: Some(cursor),
        },
    }))
}

pub(super) fn approval_reply_with_fault(
    fault: &Mutex<Option<ApprovalRelayFault>>,
    socket: usize,
    mut text: String,
) -> Option<String> {
    let mut fault = fault.lock().unwrap();
    let Some(requested) = *fault else {
        return Some(text);
    };
    if let ApprovalRelayFault::RefuseSavedDecisionOnSocket { socket: expected } = requested
        && socket != expected
    {
        return Some(text);
    }
    let Ok(LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    })) = decode_text(&text, FrameDirection::ToRouter)
    else {
        return Some(text);
    };
    let TypedCallResult::Answered {
        reply: CentralCallReply::DecidePersonApproval(reply),
    } = *result
    else {
        return Some(text);
    };
    if !matches!(
        reply.body,
        approval_request_protocol::ApprovalDecideReply::SavedDecision { .. }
    ) {
        return Some(text);
    }
    *fault = None;
    match requested {
        ApprovalRelayFault::RepeatListCursor => {
            *fault = Some(requested);
            Some(text)
        }
        ApprovalRelayFault::MalformedDecideReply => {
            text.truncate(1);
            Some(text)
        }
        ApprovalRelayFault::DropSavedDecision => None,
        ApprovalRelayFault::RefuseSavedDecisionOnSocket { .. } => {
            let frame = LinkFrame::Data(DataFrame::CentralCallReply {
                correlation_id,
                result: Box::new(TypedCallResult::Refused {
                    reason: LinkCallRefusal::CentralFault,
                }),
            });
            Some(encode_text(&frame, FrameDirection::ToRouter).unwrap())
        }
        ApprovalRelayFault::WrongDecideReply => {
            let wrong = ListOrganizationWorkspacesRpc::reply(Envelope {
                target: reply.target,
                body: OrganizationWorkspacePage {
                    workspaces: Vec::new(),
                    next: None,
                },
            });
            let frame = LinkFrame::Data(DataFrame::CentralCallReply {
                correlation_id,
                result: Box::new(TypedCallResult::Answered { reply: wrong }),
            });
            Some(encode_text(&frame, FrameDirection::ToRouter).unwrap())
        }
    }
}

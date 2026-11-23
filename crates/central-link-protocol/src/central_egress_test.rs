use super::*;
use workspace_model::{ActorView, Identity, WorkspaceResult};

#[test]
fn oversized_workspace_page_gets_a_small_fault_with_the_same_correlation() {
    let correlation_id = "019f0000-0000-7000-8000-00000000e100".parse().unwrap();
    let actor = ActorView {
        actor_identity_id: "019f0000-0000-7000-8000-00000000e101".parse().unwrap(),
        identity: Identity::AgentSession {
            session: workspace_ids::SessionRef {
                machine_id: "019f0000-0000-7000-8000-00000000e102".parse().unwrap(),
                session_id: "é".repeat(2000).try_into().unwrap(),
            },
        },
    };
    let reply = workspace_model::WorkspaceReply {
        result: WorkspaceResult::Done(workspace_model::WorkspaceAnswer::Page {
            items: Vec::new(),
            next_cursor: None,
        }),
        actors: vec![actor; 260],
    };
    let frame = LinkFrame::Data(DataFrame::Reply {
        correlation_id,
        result: Box::new(ReplyPayload::Workspace(reply)),
    });
    let original = encode_text(&frame, FrameDirection::ToRouter).unwrap();
    assert!(original.len() > LINK_MAX_EGRESS_TEXT_BYTES);

    let checked = encode_central_egress(&frame).unwrap();
    assert_eq!(checked.oversized_reply_bytes, Some(original.len()));
    assert!(checked.text.len() < 1024);
    let LinkFrame::Data(DataFrame::Reply {
        correlation_id,
        result,
    }) = crate::decode_text(&checked.text, FrameDirection::ToRouter).unwrap()
    else {
        panic!("expected the existing workspace reply shape");
    };
    assert_eq!(
        String::from(correlation_id),
        "019f0000-0000-7000-8000-00000000e100"
    );
    assert_eq!(
        *result,
        ReplyPayload::Workspace(workspace_model::WorkspaceReply {
            result: WorkspaceResult::CentralUnavailable,
            actors: Vec::new(),
        })
    );
}

#[test]
fn outbound_budget_reserves_the_longest_masked_header() {
    assert!(check_central_egress_length(1_048_562).is_ok());
    assert!(matches!(
        check_central_egress_length(1_048_563),
        Err(ProtocolError::EgressTooBig { bytes: 1_048_563 })
    ));
}

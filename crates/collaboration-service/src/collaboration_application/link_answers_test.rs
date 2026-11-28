use std::collections::{BTreeSet, HashSet};

use collaboration_protocol::central_tool_port::{
    CentralMailRefusal, CentralMailResult, RefusalReason, WorkspaceResult,
};
use collaboration_protocol::link_tool_result::{
    InvalidRequestReason, LinkRefusal, LinkToolResult, MessageCallerRefusal, MessageFrontRefusal,
    RemovedField,
};
use serde_json::{Value, json};

use super::{
    Wording, link_tool_output, mail_result_wording, refusal_wording, workspace_result_wording,
};

const ID: &str = "019a0000-0000-7000-8000-000000000041";
const OTHER_ID: &str = "019a0000-0000-7000-8000-000000000042";

fn item() -> Value {
    json!({"kind":"thread","id":ID})
}

fn blocker() -> Value {
    json!({"item":item(),"title":"Open child","status":{"kind":"work","status":"open"}})
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}

/// One sample of every refusal reason Central's workspace rules return.
fn every_workspace_refusal() -> Vec<RefusalReason> {
    [
        json!({"kind":"dependencyNotFound","dependencyId":ID}),
        json!({"kind":"machineRevoked"}),
        json!({"kind":"requestIdReused"}),
        json!({"kind":"workspaceNotAllowed"}),
        json!({"kind":"workspaceNameInUse","name":"Operations"}),
        json!({"kind":"notFound","item":item()}),
        json!({"kind":"notLead","project":{"item":item(),"name":"Router"},"lead":ID}),
        json!({"kind":"leadRequired","project":{"item":item(),"name":"Router"}}),
        json!({"kind":"notCreator","creator":ID}),
        json!({"kind":"threadResolved","threadId":ID}),
        json!({"kind":"notParticipant","threadId":ID}),
        json!({"kind":"notMember","threadId":ID}),
        json!({"kind":"wouldCycle","path":[ID, OTHER_ID]}),
        json!({"kind":"crossProject","blocking":ID,"blocked":OTHER_ID}),
        json!({"kind":"duplicateDependency","blocking":ID,"blocked":OTHER_ID}),
        json!({"kind":"emptyDescription"}),
        json!({"kind":"backlogItemClosed","itemId":ID}),
        json!({"kind":"sourceKindNotAllowed","item":item()}),
        json!({"kind":"childrenNotClosed","blockers":[blocker()]}),
        json!({"kind":"parentClosed","parent":blocker()}),
        json!({"kind":"unknownActor","actorIdentityId":ID}),
        json!({"kind":"unheldAddressee"}),
        json!({"kind":"invalidInput","field":"title"}),
    ]
    .into_iter()
    .map(decode)
    .collect()
}

/// One sample of every refusal reason Central's mail rules return.
fn every_mail_refusal() -> Vec<CentralMailRefusal> {
    [
        json!({"kind":"notSession"}),
        json!({"kind":"timeOutOfRange","value":{"field":"dueAt"}}),
        json!({"kind":"timingViolation","value":{"field":"dueAt","requirement":"fiveMinuteGrid"}}),
        json!({"kind":"workspace","value":{"kind":"machineRevoked"}}),
        json!({"kind":"unknownSession","value":{"target":{"machineId":ID,"sessionId":"peer"}}}),
        json!({"kind":"noOccurrence"}),
        json!({"kind":"invalidInput","value":{"field":"body"}}),
        json!({"kind":"wakeNotFound","value":{"wakeId":ID}}),
        json!({"kind":"wakeAlreadyFired","value":{"wakeId":ID}}),
        json!({"kind":"automationNotFound","value":{"automationId":ID}}),
        json!({"kind":"automationAlreadyDone","value":{"automationId":ID}}),
    ]
    .into_iter()
    .map(decode)
    .collect()
}

fn every_link_refusal() -> Vec<LinkRefusal> {
    let invalid = [
        InvalidRequestReason::MalformedFrame,
        InvalidRequestReason::RequestIdPolicy {
            field: "requestId".into(),
        },
        InvalidRequestReason::InvalidBatch,
        InvalidRequestReason::InvalidReceiptBatch,
        InvalidRequestReason::InvalidOffer,
        InvalidRequestReason::WrongDirection,
        InvalidRequestReason::TooLargeToSend,
        InvalidRequestReason::UnknownTypedCall,
        InvalidRequestReason::UnmeasurableWrite,
        InvalidRequestReason::UnstampedMessageRequired,
        InvalidRequestReason::InvalidSendTime,
    ]
    .map(|reason| LinkRefusal::InvalidRequest { reason });
    let removed = [
        RemovedField::IncludeRedacted,
        RemovedField::ProjectQualifier,
        RemovedField::ProtocolVersion,
    ]
    .map(|field| LinkRefusal::RemovedInput { field });
    let callers = [
        MessageCallerRefusal::CallerRequired,
        MessageCallerRefusal::UnknownSession,
        MessageCallerRefusal::AmbiguousSession,
        MessageCallerRefusal::CouldNotTell,
    ]
    .map(|reason| LinkRefusal::Caller { reason });
    let link_states = [
        MessageFrontRefusal::AwaitingFirstRead,
        MessageFrontRefusal::NotEnrolled,
        MessageFrontRefusal::EnrollmentEnded,
        MessageFrontRefusal::MetadataUnreadable,
        MessageFrontRefusal::Starting,
        MessageFrontRefusal::Restarting,
        MessageFrontRefusal::EnrollmentMismatch,
        MessageFrontRefusal::ShuttingDown,
        MessageFrontRefusal::CouldNotStart,
    ]
    .map(|state| LinkRefusal::RouterNotReady { state });
    [
        LinkRefusal::CentralUnavailable,
        LinkRefusal::OutboxUnavailable,
        LinkRefusal::RequestIdConflict,
        LinkRefusal::InvalidArguments,
    ]
    .into_iter()
    .chain(invalid)
    .chain(removed)
    .chain(callers)
    .chain(link_states)
    .collect()
}

fn distinct_variants<T>(values: &[T]) -> usize {
    values
        .iter()
        .map(std::mem::discriminant)
        .collect::<HashSet<_>>()
        .len()
}

fn distinct_values<T: serde::Serialize>(values: &[T]) -> usize {
    values
        .iter()
        .map(|value| serde_json::to_value(value).map(|json| json.to_string()))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_or(0, |kinds| kinds.len())
}

fn assert_states_and_next_step(case: &str, wording: &Wording) {
    assert!(!wording.state.trim().is_empty(), "{case}: no state");
    assert!(
        wording.next_step.ends_with('.') && wording.next_step.split_whitespace().count() >= 2,
        "{case}: no next step in {wording:?}"
    );
}

#[test]
fn every_central_refusal_names_central_s_reason_and_a_next_step() {
    // Arrange
    let workspace = every_workspace_refusal();
    let mail = every_mail_refusal();
    assert_eq!(
        distinct_variants(&workspace),
        23,
        "one sample per workspace refusal"
    );
    assert_eq!(distinct_variants(&mail), 11, "one sample per mail refusal");

    for reason in workspace {
        // Act
        let wording = workspace_result_wording(&WorkspaceResult::Refused(reason.clone()));

        // Assert
        let wording = wording.unwrap_or_else(|| Wording::new("", ""));
        assert_states_and_next_step(&format!("{reason:?}"), &wording);
        assert_eq!(wording.state, format!("Refused by Central: {reason}"));
    }
    for reason in mail {
        let wording = mail_result_wording(&CentralMailResult::Refused(reason.clone()))
            .unwrap_or_else(|| Wording::new("", ""));
        assert_states_and_next_step(&format!("{reason:?}"), &wording);
        assert!(
            wording.state.starts_with("Refused by Central: "),
            "{reason:?}"
        );
    }
}

#[test]
fn every_router_refusal_and_lost_answer_names_its_state_and_a_next_step() {
    // Arrange
    let refusals = every_link_refusal();
    assert_eq!(
        distinct_values(&refusals),
        31,
        "one sample per Router refusal"
    );
    let unanswered = [
        WorkspaceResult::Conflict,
        WorkspaceResult::OutcomeUnknown,
        WorkspaceResult::CentralUnavailable,
    ];

    for code in refusals {
        // Act
        let wording = refusal_wording(&code);
        let output = link_tool_output(LinkToolResult::Refused { code: code.clone() });

        // Assert
        assert_states_and_next_step(&format!("{code:?}"), &wording);
        assert_eq!(
            output.text,
            format!("{}. {}", wording.state, wording.next_step)
        );
        assert!(!output.result.succeeded(), "{code:?} must be a tool error");
    }
    for result in unanswered {
        let wording = workspace_result_wording(&result).unwrap_or_else(|| Wording::new("", ""));
        assert_states_and_next_step(&format!("{result:?}"), &wording);
    }
    for result in [
        LinkToolResult::ReadAnswerLost,
        LinkToolResult::Uncertain {
            request_id: decode(json!(ID)),
        },
    ] {
        let output = link_tool_output(result);
        assert!(!output.result.succeeded());
        assert!(
            output.text.contains("again") || output.text.contains("status"),
            "{output:?}"
        );
    }
}

fn thread_show_answer(me: Value) -> LinkToolResult {
    LinkToolResult::Answered {
        reply: decode(
            json!({"kind":"workspace","reply":{"actors":[],"result":{"kind":"done","answer":{
            "kind":"item","item":{"kind":"thread","item":{
                "thread":{"threadId":ID,"areaId":OTHER_ID,"title":"Release checklist",
                    "creator":"019a0000-0000-7000-8000-000000000043","state":"resolved"},
                "me":me}}}}}}),
        ),
    }
}

#[test]
fn a_shown_thread_is_stated_one_fact_per_line_with_the_caller_s_standing_when_reported() {
    // Arrange
    let thread_lines = [
        "Thread: Release checklist".to_owned(),
        "State: resolved".to_owned(),
        format!("Thread ID: {ID}"),
        format!("Area ID: {OTHER_ID}"),
        "Created by: 019a0000-0000-7000-8000-000000000043".to_owned(),
    ];
    let member = json!({"role":"observer","notifications":"off","readThrough":7});
    let unread_member = json!({"role":"participant","notifications":"on","readThrough":null});

    // Act
    let without_me = link_tool_output(thread_show_answer(Value::Null));
    let with_me = link_tool_output(thread_show_answer(member));
    let unread = link_tool_output(thread_show_answer(unread_member));

    // Assert
    assert_eq!(without_me.text, thread_lines.join("\n"));
    assert_eq!(
        with_me.text,
        [
            thread_lines.join("\n"),
            "Your role: observer".into(),
            "Your notifications: off".into(),
            "You have read through change 7".into(),
        ]
        .join("\n")
    );
    assert!(
        unread.text.ends_with(
            "Your role: participant\nYour notifications: on\nYou have not read this thread yet"
        ),
        "{unread:?}"
    );
    assert!(with_me.result.succeeded() && without_me.result.succeeded());
}

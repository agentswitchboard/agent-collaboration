use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, RequestPayload, decode_text, encode_text,
};
use workspace_model::central_mail::{CentralMailOperation, CentralMailRequest, NoticeCadence};

use super::*;

fn request(number: u8) -> LinkRequest {
    let caller = serde_json::from_str(r#"{"kind":"session","value":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}"#).unwrap();
    LinkRequest::new(
        format!("01890f2e-7b4c-7cc0-98c4-{number:012x}")
            .parse()
            .unwrap(),
        caller,
        RequestPayload::Mail(CentralMailRequest {
            request_id: format!("01890f2e-7b4c-7cc0-98c4-{number:012x}")
                .parse()
                .unwrap(),
            operation: CentralMailOperation::SetNoticeCadence {
                cadence: NoticeCadence::M25,
            },
        }),
    )
    .unwrap()
}

#[test]
fn checked_batch_results_require_exact_count_and_ordered_member_identity() {
    let requests = LinkRequestBatch::new(vec![request(1), request(2)]).unwrap();
    let outcomes = requests
        .requests()
        .iter()
        .map(|request| WriteBatchMemberDisposition::AnswerLost {
            correlation_id: request.correlation_id().clone(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        CheckedBatchResults::new(&requests, []),
        Err(BatchResultsError::WrongCount)
    );
    assert_eq!(
        CheckedBatchResults::new(&requests, outcomes.iter().take(1).cloned()),
        Err(BatchResultsError::WrongCount)
    );
    assert_eq!(
        CheckedBatchResults::new(
            &requests,
            outcomes.iter().cloned().chain(outcomes.first().cloned())
        ),
        Err(BatchResultsError::WrongCount)
    );
    assert_eq!(
        CheckedBatchResults::new(&requests, outcomes.iter().rev().cloned()),
        Err(BatchResultsError::WrongMember { index: 0 })
    );
    assert_eq!(
        CheckedBatchResults::new(&requests, outcomes.iter().take(1).cloned().cycle().take(2)),
        Err(BatchResultsError::WrongMember { index: 1 })
    );
    let checked = CheckedBatchResults::new(&requests, outcomes).unwrap();
    assert_eq!(
        checked
            .members()
            .iter()
            .map(|member| &member.request_id)
            .collect::<Vec<_>>(),
        requests
            .requests()
            .iter()
            .filter_map(|request| request.payload().write_request_id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn validated_direct_offer_requires_the_provided_time_left() {
    let deliver = r#"{"kind":"deliver","timeLeft":1,"sender":{"kind":"central"},"message":{"messageId":"01890f2e-7b4c-7cc0-98c4-000000000009","sender":{"kind":"central"},"target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"},"body":{"kind":"direct","body":"hello","delivery":"auto","replyTo":null},"state":{"kind":"queued"},"source":null}}"#;
    let LinkFrame::Data(DataFrame::Deliver {
        message, time_left, ..
    }) = decode_text(deliver, FrameDirection::ToRouter).unwrap()
    else {
        panic!("expected a valid delivery fixture");
    };
    assert_eq!(
        ValidatedDeliveryOffer::new(message.clone(), None),
        Err(InvalidDeliveryOffer::WrongKindOrTime)
    );
    let valid = ValidatedDeliveryOffer::new(message.clone(), time_left).unwrap();
    assert_eq!(valid.into_parts(), (message, time_left));
}

#[test]
fn request_size_plus_batch_scaffolding_matches_the_production_encoder() {
    let requests = LinkRequestBatch::new(vec![request(1), request(2)]).unwrap();
    let counted = WRITE_BATCH_ENVELOPE_BYTES
        + 1
        + requests
            .requests()
            .iter()
            .map(|request| encoded_request_size(request).unwrap())
            .sum::<usize>();
    let actual = encode_text(
        &LinkFrame::Data(DataFrame::Batch { requests }),
        FrameDirection::ToCentral,
    )
    .unwrap();
    assert_eq!(counted, actual.len());
}

#[test]
fn checked_batch_results_require_the_requested_reply_operation_and_values() {
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult};

    let requests = LinkRequestBatch::new(vec![request(1)]).unwrap();
    let answered = |answer| WriteBatchMemberDisposition::Answered {
        reply: CorrelatedRequestReply {
            correlation_id: requests.requests()[0].correlation_id().clone(),
            result: Box::new(ReplyPayload::Mail(CentralMailResult::Done(answer))),
        },
        accepted_at: ClockInstant::ZERO.after(Duration::from_secs(4)),
    };
    let wrong_cadence = answered(CentralMailAnswer::NoticeCadenceSet {
        cadence: NoticeCadence::M55,
    });
    assert_eq!(
        CheckedBatchResults::new(&requests, [wrong_cadence]),
        Err(BatchResultsError::IncompatibleReply { index: 0 })
    );
    let wrong_operation = answered(CentralMailAnswer::WakeSet {
        wake_id: "01890f2e-7b4c-7cc0-98c4-000000000011".parse().unwrap(),
        due_at: "2030-01-01T00:00:00Z".parse().unwrap(),
    });
    assert_eq!(
        CheckedBatchResults::new(&requests, [wrong_operation]),
        Err(BatchResultsError::IncompatibleReply { index: 0 })
    );
    let expected = answered(CentralMailAnswer::NoticeCadenceSet {
        cadence: NoticeCadence::M25,
    });
    let checked = CheckedBatchResults::new(&requests, [expected.clone()]).unwrap();
    assert_eq!(checked.members()[0].disposition, expected);
}

#[test]
fn registration_evidence_requires_the_pending_attempt_correlation_target_and_run() {
    use std::sync::Arc;

    use router_clock::ManualClock;
    use serde_json::json;

    use crate::{ClientAction, LinkClient};

    let machine = "01890f2e-7b4c-7cc0-98c4-000000000002"
        .parse::<workspace_ids::MachineId>()
        .unwrap();
    let central_time = "2030-01-01T00:00:00Z".parse().unwrap();
    let clock = Arc::new(ManualClock::new(central_time));
    let take_attempt = || {
        let mut client = LinkClient::new(machine.clone(), clock.clone());
        let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
            panic!("expected a dial attempt");
        };
        attempt
    };
    let admitted = AdmittedConnection {
        connection: take_attempt(),
        clock_anchor: WelcomeClockAnchor {
            central_time,
            observed_at: ClockInstant::ZERO,
        },
    };
    let foreign_attempt = take_attempt();
    assert_ne!(admitted.connection, foreign_attempt);
    let correlation = "01890f2e-7b4c-7cc0-98c4-000000000012";
    let foreign_correlation = "01890f2e-7b4c-7cc0-98c4-000000000013";
    let organization = "01890f2e-7b4c-7cc0-98c4-000000000010";
    let foreign_organization = "01890f2e-7b4c-7cc0-98c4-000000000011";
    let run = "01890f2e-7b4c-7cc0-98c4-000000000014";
    let request = decode_text(&json!({
        "kind":"callCentral", "correlationId":correlation,
        "request":{"call":"registerRun","envelope":{"target":{"organizationId":organization},"body":{"routerRun":run}}}
    }).to_string(), FrameDirection::ToCentral).unwrap();
    let reply = |correlation, organization| {
        decode_text(&json!({
        "kind":"centralCallReply", "correlationId":correlation,
        "result":{"kind":"answered","reply":{"call":"registerRun","envelope":{"target":{"organizationId":organization},"body":{"kind":"registered"}}}}
    }).to_string(), FrameDirection::ToRouter).unwrap()
    };

    let mut pending = PendingRunRegistration::from_sent_frame(admitted.clone(), &request).unwrap();
    let matching_reply = reply(correlation, organization);
    assert!(
        pending
            .observe_reply(&foreign_attempt, &matching_reply)
            .is_none()
    );
    assert!(
        pending
            .observe_reply(
                &admitted.connection,
                &reply(foreign_correlation, organization)
            )
            .is_none()
    );
    assert!(
        pending
            .observe_reply(
                &admitted.connection,
                &reply(correlation, foreign_organization)
            )
            .is_none()
    );
    let evidence = pending
        .observe_reply(&admitted.connection, &matching_reply)
        .unwrap();
    let registered = RegisteredConnection::from_registration(evidence);
    assert_eq!(registered.admitted(), &admitted);
    assert_eq!(
        registered.router_run(),
        &run.parse::<RouterRunId>().unwrap()
    );
    assert!(
        pending
            .observe_reply(&admitted.connection, &matching_reply)
            .is_none()
    );
}

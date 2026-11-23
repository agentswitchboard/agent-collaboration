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

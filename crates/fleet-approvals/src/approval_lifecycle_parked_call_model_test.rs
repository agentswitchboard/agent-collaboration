use approval_request_protocol::{
    Answer, DecidedBy, Delivery, Ending, EndingView, HolderReport, OfferEnding, ParkedCallHolder,
    RaiseReceipt, RaiseReply, ReportDeliveryReply, WaitingStatus,
};
use hegel::generators as gs;

use crate::{
    AdmittedDecider, ApprovalDestination, ApprovalEffect, ApprovalEvent, ApprovalReceipt,
    ApprovalState, EndedApproval, FactorStanding, RetainedApproval,
};

use super::super::fixtures::{
    DEADLINE, call_raise, holder_report, option_id, person_id, request_id, router_run, step, time,
};

#[hegel::test(test_cases = 100)]
fn parked_call_history_matches_the_independent_handover_transcript(case: hegel::TestCase) {
    let choice = case.draw(gs::sampled_from(vec!["once", "decline"]));
    let report = case.draw(gs::sampled_from(vec![
        HolderReport::GoneBeforeHandover,
        HolderReport::ReplyWrittenEffectNotConfirmed,
        HolderReport::DeliveryUnknown,
        HolderReport::Applied,
    ]));
    let reoffers = case.draw(gs::integers::<i64>().min_value(0).max_value(4));
    let answered_at = case.draw(gs::integers::<i64>().min_value(1).max_value(DEADLINE - 1));
    let destinations = vec![ApprovalDestination::People];
    let receipt = RaiseReceipt {
        request_id: request_id(),
        raised_at: time(0),
        deadline: time(DEADLINE),
    };
    let saved = Box::new(RetainedApproval {
        raise: call_raise(),
        receipt: receipt.clone(),
        destinations: destinations.clone(),
    });
    let waiting = ApprovalState::Waiting {
        saved: saved.clone(),
        status: WaitingStatus::Pending,
    };
    assert_eq!(
        step(
            &ApprovalState::Unraised,
            ApprovalEvent::Raise {
                raise: Box::new(call_raise()),
                destinations
            },
            0
        ),
        Ok((
            waiting.clone(),
            vec![
                ApprovalEffect::RegisterDue {
                    request_id: request_id(),
                    deadline: time(DEADLINE)
                },
                ApprovalEffect::Reply(ApprovalReceipt::Raise(RaiseReply::RaiseReceipt { receipt })),
            ]
        ))
    );

    let answer = Answer::Call {
        option_id: option_id(choice),
    };
    let by = DecidedBy::Person {
        person_id: person_id(),
    };
    let ending = Ending::Decided {
        answer: answer.clone(),
        by: by.clone(),
    };
    let offered = ApprovalEffect::OfferEnding(Box::new(OfferEnding::UnprovenCall {
        request_id: request_id(),
        holder: ParkedCallHolder {
            router_run: router_run(),
        },
        ending: ending.clone(),
    }));
    let awaiting = ApprovalState::Ended {
        saved: saved.clone(),
        closed: EndedApproval {
            ending: ending.clone(),
            delivery: Delivery::AwaitingReport {},
            ended_at: time(answered_at),
        },
    };
    let (observed, effects) = step(
        &waiting,
        ApprovalEvent::Answer {
            actor: AdmittedDecider::Person {
                person_id: person_id(),
                factor: FactorStanding::Fresh,
            },
            answer: answer.clone(),
        },
        answered_at,
    )
    .unwrap();
    assert_eq!(observed, awaiting);
    assert_eq!(
        effects,
        [
            ApprovalEffect::ClearDue {
                request_id: request_id()
            },
            offered.clone(),
            ApprovalEffect::Reply(ApprovalReceipt::Answer(EndingView::DecidedWithAnswer {
                answer,
                by
            })),
        ]
    );

    for tick in 1..=reoffers {
        assert_eq!(
            step(
                &observed,
                ApprovalEvent::OfferAgain {
                    router_run: router_run()
                },
                answered_at + tick
            ),
            Ok((awaiting.clone(), vec![offered.clone()]))
        );
    }
    let reported = ApprovalState::Ended {
        saved,
        closed: EndedApproval {
            ending,
            delivery: Delivery::Reported { report },
            ended_at: time(answered_at),
        },
    };
    let ack = ApprovalEffect::Reply(ApprovalReceipt::Report(
        ReportDeliveryReply::DeliveryRecorded {},
    ));
    for (state, offset) in [(&observed, 1), (&reported, 2)] {
        assert_eq!(
            step(
                state,
                holder_report(router_run(), report),
                answered_at + reoffers + offset
            ),
            Ok((reported.clone(), vec![ack.clone()]))
        );
    }
}

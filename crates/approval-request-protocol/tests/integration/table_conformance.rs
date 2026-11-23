//! Independent inventory of the six core approvals calls.
use std::fmt::Debug;

use approval_request_protocol::{
    CancelReason, DecideReply, DecideReplyWire, DecideRequest, DeliverEndingReply,
    DeliverEndingRequest, DestinationEnding, HolderStopCause, PassOnReply, PassOnRequest,
    ProviderEndingView, Raise, RaiseReply, RegisterRunReply, RegisterRunRequest, RunRefusal,
    SessionDecider, StandingEnding, StopReceiptEnding, StopReply, StopRequest,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const REQUEST: &str = "01934a50-1000-7000-8000-000000000001";
const RUN: &str = "01934a50-1000-7000-8000-000000000002";
const OPERATION: &str = "01934a50-1000-7000-8000-000000000003";
const SESSION: &str = "01934a50-1000-7000-8000-000000000005";
const DIGEST: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

const AUTHORITY_REFUSALS: [(&str, &str); 3] = [
    ("authority/runNotRegistered", "runNotRegistered"),
    ("authority/runRetired", "runRetired"),
    ("authority/enrollmentEnded", "enrollmentEnded"),
];

fn authority_case(refusal: RunRefusal) -> &'static str {
    match refusal {
        RunRefusal::RunNotRegistered {} => "authority/runNotRegistered",
        RunRefusal::RunRetired {} => "authority/runRetired",
        RunRefusal::EnrollmentEnded {} => "authority/enrollmentEnded",
    }
}

fn stop_standing_case(ending: StopReceiptEnding) -> &'static str {
    match ending {
        StopReceiptEnding::TimedOut {} => "timedOut",
        StopReceiptEnding::DecidedEarlier {} => "decidedEarlier",
        StopReceiptEnding::Cancelled { reason } => match reason {
            CancelReason::RequesterGone {} => "requesterGone",
            CancelReason::SessionEnded {} => "sessionEnded",
            CancelReason::EnrollmentEnded {} => "enrollmentEnded",
            CancelReason::Stopped { cause } => match cause {
                HolderStopCause::TurnStopped => "turnStopped",
                HolderStopCause::AskReplaced => "askReplaced",
                HolderStopCause::AskWithdrawn => "askWithdrawn",
                HolderStopCause::SessionClosed => "sessionClosed",
                HolderStopCause::ProviderConnectionLost => "providerConnectionLost",
                HolderStopCause::CallWithdrawn => "callWithdrawn",
            },
        },
    }
}

fn assert_authority_reply<Wire: DeserializeOwned + Serialize + Debug>(
    name: &str,
    cases: &[(&str, Value)],
    classify: impl Fn(&Wire) -> &'static str,
) -> Result<(), String> {
    let mut cases = cases.to_vec();
    cases.extend(AUTHORITY_REFUSALS.map(|(case, refusal)| {
        (
            case,
            json!({"kind":"authorityRefused","refusal":{"kind":refusal}}),
        )
    }));
    assert_reply(name, &cases, classify)
}

fn ending() -> Value {
    json!({"kind":"providerApproval","ending":{"kind":"timedOut"}})
}

fn slice_case<Wire: DeserializeOwned + Serialize + Debug>(
    name: &str,
    specimen: Value,
) -> Result<Wire, String> {
    let decoded = serde_json::from_str::<Wire>(&specimen.to_string())
        .map_err(|error| format!("{name} rejected {specimen}: {error}"))?;
    let reencoded = serde_json::to_value(&decoded).map_err(|error| error.to_string())?;
    if reencoded != specimen {
        return Err(format!("{name} changed {specimen} to {reencoded}"));
    }
    Ok(decoded)
}

fn assert_reply<Wire: DeserializeOwned + Serialize + Debug>(
    name: &str,
    cases: &[(&str, Value)],
    classify: impl Fn(&Wire) -> &'static str,
) -> Result<(), String> {
    for (expected, specimen) in cases {
        let decoded = slice_case(name, specimen.clone())?;
        if classify(&decoded) != *expected {
            return Err(format!(
                "{name} decoded {expected} as {}",
                classify(&decoded)
            ));
        }
    }
    let first = cases
        .first()
        .ok_or_else(|| format!("{name} needs a reply specimen"))?;
    let mut missing = first.1.clone();
    missing
        .as_object_mut()
        .ok_or_else(|| format!("{name} reply needs an object"))?
        .remove("kind");
    for invalid in [json!({"kind":"unknownReply"}), missing, json!([first.0])] {
        if serde_json::from_str::<Wire>(&invalid.to_string()).is_ok() {
            return Err(format!("{name} accepted bad reply {invalid}"));
        }
    }
    Ok(())
}

#[test]
fn table_requests_and_reply_cases_match_the_core_protocol() -> Result<(), String> {
    slice_case::<RegisterRunRequest>("RegisterRun request", json!({"routerRun":RUN}))?;
    assert_reply::<RegisterRunReply>(
        "RegisterRun",
        &[
            ("registered", json!({"kind":"registered"})),
            ("runRetired", json!({"kind":"runRetired"})),
            ("runNotRegistered", json!({"kind":"runNotRegistered"})),
            ("enrollmentEnded", json!({"kind":"enrollmentEnded"})),
        ],
        |reply| match reply {
            RegisterRunReply::Registered {} => "registered",
            RegisterRunReply::RunRetired {} => "runRetired",
            RegisterRunReply::RunNotRegistered {} => "runNotRegistered",
            RegisterRunReply::EnrollmentEnded {} => "enrollmentEnded",
        },
    )?;

    slice_case::<Raise>(
        "Raise request",
        json!({
            "kind":"providerApproval","requestId":REQUEST,
            "holder":{"routerRun":RUN,"sessionId":SESSION},
            "offer":{"title":"Open the report","description":null,"subject":null,"options":[
                {"id":1,"label":"Allow once","effect":"allow","lasts":{"kind":"thisRequest"}}
            ]}
        }),
    )?;
    assert_authority_reply::<RaiseReply>(
        "Raise",
        &[
            (
                "receipt",
                json!({"kind":"receipt","receipt":{"requestId":REQUEST,"window":{
                    "raisedAt":"2026-11-22T10:00:00Z","deadline":"2026-11-22T11:00:00Z"
                }}}),
            ),
            ("alreadyStopped", json!({"kind":"alreadyStopped"})),
            ("sessionEnded", json!({"kind":"sessionEnded"})),
            ("contentsDiffer", json!({"kind":"contentsDiffer"})),
            ("tooManyOutstanding", json!({"kind":"tooManyOutstanding"})),
        ],
        |reply| match reply {
            RaiseReply::Receipt { .. } => "receipt",
            RaiseReply::AlreadyStopped {} => "alreadyStopped",
            RaiseReply::SessionEnded {} => "sessionEnded",
            RaiseReply::ContentsDiffer {} => "contentsDiffer",
            RaiseReply::TooManyOutstanding {} => "tooManyOutstanding",
            RaiseReply::AuthorityRefused { refusal } => authority_case(*refusal),
        },
    )?;

    for cause in [
        "turnStopped",
        "askReplaced",
        "askWithdrawn",
        "sessionClosed",
        "providerConnectionLost",
    ] {
        slice_case::<StopRequest>(
            "Stop request",
            json!({
                "kind":"provider","requestId":REQUEST,"routerRun":RUN,"contentsDigest":DIGEST,"cause":cause
            }),
        )?;
    }
    slice_case::<StopRequest>(
        "Unproven call Stop request",
        json!({
            "kind":"unprovenCall","requestId":REQUEST,"routerRun":RUN,"contentsDigest":DIGEST,"cause":"callWithdrawn"
        }),
    )?;
    let mut stop_cases = vec![
        (
            "timedOut",
            json!({"kind":"stands","ending":{"kind":"timedOut"}}),
        ),
        (
            "decidedEarlier",
            json!({"kind":"stands","ending":{"kind":"decidedEarlier"}}),
        ),
        ("notVisible", json!({"kind":"notVisible"})),
    ];
    stop_cases.extend(
        ["requesterGone", "sessionEnded", "enrollmentEnded"].map(|reason| {
            (
                reason,
                json!({"kind":"stands","ending":{"kind":"cancelled","reason":{"kind":reason}}}),
            )
        }),
    );
    stop_cases.extend(["turnStopped","askReplaced","askWithdrawn","sessionClosed","providerConnectionLost","callWithdrawn"].map(|cause| {
        (cause, json!({"kind":"stands","ending":{"kind":"cancelled","reason":{"kind":"stopped","cause":cause}}}))
    }));
    assert_authority_reply::<StopReply>("Stop", &stop_cases, |reply| match reply {
        StopReply::Stands { ending } => stop_standing_case(*ending),
        StopReply::NotVisible {} => "notVisible",
        StopReply::AuthorityRefused { refusal } => authority_case(*refusal),
    })?;

    slice_case::<DeliverEndingRequest>(
        "DeliverEnding request",
        json!({
            "requestId":REQUEST,"contentsDigest":DIGEST,"ending":ending()
        }),
    )?;
    assert_reply::<DeliverEndingReply>(
        "DeliverEnding",
        &[
            ("received", json!({"kind":"received"})),
            ("stopOwed", json!({"kind":"stopOwed"})),
            ("notHeld", json!({"kind":"notHeld"})),
            ("holderNotReady", json!({"kind":"holderNotReady"})),
            ("endingDiffers", json!({"kind":"endingDiffers"})),
        ],
        |reply| match reply {
            DeliverEndingReply::Received {} => "received",
            DeliverEndingReply::StopOwed {} => "stopOwed",
            DeliverEndingReply::NotHeld {} => "notHeld",
            DeliverEndingReply::HolderNotReady {} => "holderNotReady",
            DeliverEndingReply::EndingDiffers {} => "endingDiffers",
        },
    )?;

    let value = json!({"kind":"approval","optionId":1});
    let by = json!({"kind":"session","session":SESSION});
    slice_case::<DecideRequest>(
        "Decide request",
        json!({"requestId":REQUEST,"operationId":OPERATION,"value":value}),
    )?;
    assert_reply::<DecideReply>(
        "Decide",
        &[
            (
                "savedDecision",
                json!({"kind":"savedDecision","decision":{"by":by,"answer":value}}),
            ),
            (
                "standing",
                json!({"kind":"standing","ending":{"kind":"providerApproval",
            "ending":{"kind":"standing","ending":{"kind":"decided","by":by}}}}),
            ),
            ("notVisible", json!({"kind":"notVisible"})),
            ("invalid", json!({"kind":"invalid"})),
            ("tooLate", json!({"kind":"tooLate"})),
            ("operationConflict", json!({"kind":"operationConflict"})),
        ],
        |reply| match reply.wire_shape() {
            DecideReplyWire::SavedDecision { decision } => match &decision.by {
                SessionDecider::Session { .. } => "savedDecision",
            },
            DecideReplyWire::Standing { .. } => "standing",
            DecideReplyWire::NotVisible {} => "notVisible",
            DecideReplyWire::Invalid {} => "invalid",
            DecideReplyWire::TooLate {} => "tooLate",
            DecideReplyWire::OperationConflict {} => "operationConflict",
        },
    )?;

    slice_case::<PassOnRequest>("PassOn request", json!({"requestId":REQUEST}))?;
    assert_reply::<PassOnReply>(
        "PassOn",
        &[
            ("forwarded", json!({"kind":"forwarded"})),
            ("notVisible", json!({"kind":"notVisible"})),
            ("tooLate", json!({"kind":"tooLate"})),
            ("ended", json!({"kind":"ended"})),
        ],
        |reply| match reply {
            PassOnReply::Forwarded {} => "forwarded",
            PassOnReply::NotVisible {} => "notVisible",
            PassOnReply::TooLate {} => "tooLate",
            PassOnReply::Ended {} => "ended",
        },
    )
}

fn destination_standing_case<Answer, Cancellation>(
    ending: &DestinationEnding<Answer, Cancellation>,
) -> &'static str {
    match ending {
        DestinationEnding::Giver { .. } => "giver",
        DestinationEnding::Standing { ending } => match ending {
            StandingEnding::Decided { .. } => "decided",
            StandingEnding::Cancelled { .. } => "cancelled",
            StandingEnding::TimedOut {} => "timedOut",
        },
    }
}

#[test]
fn decide_standing_cases_cover_every_request_kind_and_disclosure() -> Result<(), String> {
    let person = json!({"kind":"person","personId":"01934a50-1000-7000-8000-000000000004"});
    for (request_kind, answer) in [
        ("providerApproval", json!(1)),
        ("providerQuestion", json!({"kind":"declined"})),
    ] {
        for (case, ending) in [
            (
                "giver",
                json!({"kind":"giver","decision":{"answer":answer,"by":person}}),
            ),
            (
                "decided",
                json!({"kind":"standing","ending":{"kind":"decided","by":person}}),
            ),
            (
                "cancelled",
                json!({"kind":"standing","ending":{"kind":"cancelled","reason":{"kind":"requesterGone"}}}),
            ),
            (
                "timedOut",
                json!({"kind":"standing","ending":{"kind":"timedOut"}}),
            ),
        ] {
            let wire = json!({"kind":"standing","ending":{"kind":request_kind,"ending":ending}});
            let reply: DecideReply = slice_case("Decide standing", wire.clone())?;
            let DecideReplyWire::Standing { ending } = reply.wire_shape() else {
                return Err("Decide standing decoded as another reply".to_owned());
            };
            let observed = match ending {
                ProviderEndingView::ProviderApproval { ending } => {
                    ("providerApproval", destination_standing_case(ending))
                }
                ProviderEndingView::ProviderQuestion { ending } => {
                    ("providerQuestion", destination_standing_case(ending))
                }
            };
            if observed != (request_kind, case) {
                return Err(format!(
                    "Decide standing decoded {request_kind}/{case} as {observed:?}"
                ));
            }
            super::map_only::assert_objects_require_named_maps::<DecideReply>(&wire)?;
        }
    }
    Ok(())
}

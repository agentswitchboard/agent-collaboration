//! Independent inventory of the final fuller Typed calls table.
use std::fmt::Debug;

use approval_request_protocol::{
    AuthorityProbeReply, AuthorityProbeRequest, DecideReply, DecideRequest, DeliverEndingReply,
    DeliverEndingRequest, PassOnReply, PassOnRequest, Raise, RaiseReply, RegisterRunReply,
    RegisterRunRequest, ReportDeliveryReply, ReportDeliveryRequest, StopReply, StopRequest,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const REQUEST: &str = "01934a50-1000-7000-8000-000000000001";
const RUN: &str = "01934a50-1000-7000-8000-000000000002";
const OPERATION: &str = "01934a50-1000-7000-8000-000000000003";
const PERSON: &str = "01934a50-1000-7000-8000-000000000004";
const SESSION: &str = "01934a50-1000-7000-8000-000000000005";
const DIGEST: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn refusal() -> Value {
    json!({"kind":"authorityRefused","refusal":{"kind":"runRetired"}})
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
fn table_requests_and_reply_cases_match_the_fuller_protocol() -> Result<(), String> {
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

    slice_case::<AuthorityProbeRequest>("AuthorityProbe request", json!({"routerRun":RUN}))?;
    assert_reply::<AuthorityProbeReply>(
        "AuthorityProbe",
        &[
            ("admitted", json!({"kind":"admitted"})),
            ("authorityRefused", refusal()),
        ],
        |reply| match reply {
            AuthorityProbeReply::Admitted {} => "admitted",
            AuthorityProbeReply::AuthorityRefused { .. } => "authorityRefused",
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
    assert_reply::<RaiseReply>(
        "Raise",
        &[
            (
                "receipt",
                json!({"kind":"receipt","receipt":{"requestId":REQUEST,"window":{
                    "raisedAt":"2026-11-22T10:00:00Z","deadline":"2026-11-22T11:00:00Z"
                }}}),
            ),
            ("alreadyStopped", json!({"kind":"alreadyStopped"})),
            ("alreadyEnded", json!({"kind":"alreadyEnded"})),
            ("sessionEnded", json!({"kind":"sessionEnded"})),
            ("contentsDiffer", json!({"kind":"contentsDiffer"})),
            ("tooManyOutstanding", json!({"kind":"tooManyOutstanding"})),
            ("authorityRefused", refusal()),
        ],
        |reply| match reply {
            RaiseReply::Receipt { .. } => "receipt",
            RaiseReply::AlreadyStopped {} => "alreadyStopped",
            RaiseReply::AlreadyEnded {} => "alreadyEnded",
            RaiseReply::SessionEnded {} => "sessionEnded",
            RaiseReply::ContentsDiffer {} => "contentsDiffer",
            RaiseReply::TooManyOutstanding {} => "tooManyOutstanding",
            RaiseReply::AuthorityRefused { .. } => "authorityRefused",
        },
    )?;

    slice_case::<StopRequest>(
        "Stop request",
        json!({
            "kind":"provider","requestId":REQUEST,"routerRun":RUN,"contentsDigest":DIGEST,"cause":"askReplaced"
        }),
    )?;
    assert_reply::<StopReply>(
        "Stop",
        &[
            (
                "stands",
                json!({"kind":"stands","ending":{"kind":"timedOut"}}),
            ),
            ("notVisible", json!({"kind":"notVisible"})),
            ("authorityRefused", refusal()),
        ],
        |reply| match reply {
            StopReply::Stands { .. } => "stands",
            StopReply::NotVisible {} => "notVisible",
            StopReply::AuthorityRefused { .. } => "authorityRefused",
        },
    )?;

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

    slice_case::<ReportDeliveryRequest>(
        "ReportDelivery request",
        json!({
            "requestId":REQUEST,"routerRun":RUN,"contentsDigest":DIGEST,"endingDigest":DIGEST,
            "evidence":{"kind":"native","evidence":{"kind":"possibleWrite"}}
        }),
    )?;
    assert_reply::<ReportDeliveryReply>(
        "ReportDelivery",
        &[
            ("deliveryRecorded", json!({"kind":"deliveryRecorded"})),
            ("noHandoverOwed", json!({"kind":"noHandoverOwed"})),
            ("reportDiffers", json!({"kind":"reportDiffers"})),
            ("notVisible", json!({"kind":"notVisible"})),
            ("authorityRefused", refusal()),
        ],
        |reply| match reply {
            ReportDeliveryReply::DeliveryRecorded {} => "deliveryRecorded",
            ReportDeliveryReply::NoHandoverOwed {} => "noHandoverOwed",
            ReportDeliveryReply::ReportDiffers {} => "reportDiffers",
            ReportDeliveryReply::NotVisible {} => "notVisible",
            ReportDeliveryReply::AuthorityRefused { .. } => "authorityRefused",
        },
    )?;

    let value = json!({"kind":"approval","optionId":1});
    let by = json!({"kind":"person","personId":PERSON});
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
        |reply| match reply {
            DecideReply::SavedDecision { .. } => "savedDecision",
            DecideReply::Standing { .. } => "standing",
            DecideReply::NotVisible {} => "notVisible",
            DecideReply::Invalid {} => "invalid",
            DecideReply::TooLate {} => "tooLate",
            DecideReply::OperationConflict {} => "operationConflict",
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

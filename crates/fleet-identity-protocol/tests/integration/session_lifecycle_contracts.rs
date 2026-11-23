//! Literal protocol cases are checked against exhaustive Rust matches.

use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSession, BindFleetSessionReply, CreateFleetSession,
    CreateFleetSessionReply, CreateSessionKind, CreateSessionRefusal, EndFleetSession,
    EndFleetSessionReply, EndSessionTarget, EndedSessionCause, HarnessKind, NewAbandonmentCause,
    NewEndSettlement, PresentedSessionSecret, RouterEndCause, SessionCreator, SessionSettlement,
    StartProject,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const SESSION: &str = "019a3dce-2000-7000-8000-000000000001";
const OPERATION: &str = "019a3dce-2000-7000-8000-000000000002";
const MACHINE: &str = "019a3dce-2000-7000-8000-000000000003";
const WORKSPACE: &str = "019a3dce-2000-7000-8000-000000000004";
const PROJECT: &str = "019a3dce-2000-7000-8000-000000000005";
const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TIME: &str = "2026-11-22T20:01:02.003Z";
const DIGEST: &str = "cY2WJ4B6gcECZd0T7vYLjgGZJR5nl_po0FG62Oj5U_w";

#[test]
fn create_requests_cover_both_kinds_and_every_creator_and_presentation() {
    for (agent_program, creator, session_kind, project, expected) in [
        (
            "codex",
            json!({"kind":"routerOperator"}),
            json!({"kind":"ordinary"}),
            named_project(),
            ("codex", "routerOperator", "ordinary"),
        ),
        (
            "claude",
            json!({"kind":"session","sessionId":SESSION,"presented":{"kind":"secret","secret":SECRET}}),
            json!({"kind":"delegate","parent":SESSION}),
            json!({"kind":"sameAsParent"}),
            ("claude", "session/secret", "delegate"),
        ),
        (
            "cursor",
            json!({"kind":"session","sessionId":SESSION,"presented":{"kind":"nothing"}}),
            json!({"kind":"ordinary"}),
            named_project(),
            ("cursor", "session/nothing", "ordinary"),
        ),
    ] {
        let wire = json!({
            "operationId": OPERATION, "attempt": 1, "creator": creator,
            "agentProgram": agent_program, "sessionKind": session_kind, "project": project,
        });
        let request: CreateFleetSession = assert_case(wire);
        let program = match request.agent_program {
            HarnessKind::Codex => "codex",
            HarnessKind::Claude => "claude",
            HarnessKind::Cursor => "cursor",
        };
        let creator = match request.creator {
            SessionCreator::RouterOperator {} => "routerOperator",
            SessionCreator::Session {
                session_id,
                presented,
            } => {
                assert_eq!(session_id.as_str(), SESSION);
                match presented {
                    PresentedSessionSecret::Secret { secret } => {
                        assert_eq!(secret.expose_secret(), SECRET);
                        "session/secret"
                    }
                    PresentedSessionSecret::Nothing {} => "session/nothing",
                }
            }
        };
        let kind = match request.session_kind {
            CreateSessionKind::Ordinary {} => "ordinary",
            CreateSessionKind::Delegate { parent } => {
                assert_eq!(parent.as_str(), SESSION);
                "delegate"
            }
        };
        assert_eq!((program, creator, kind), expected);
        match request.project {
            StartProject::Named { project_ref } => {
                assert_ne!(kind, "delegate");
                assert_eq!(project_ref.project.as_str(), PROJECT);
                assert_eq!(project_ref.workspace.as_str(), WORKSPACE);
            }
            StartProject::SameAsParent {} => assert_eq!(kind, "delegate"),
        }
        assert_eq!(request.operation_id.as_str(), OPERATION);
        assert_eq!(request.attempt.get(), 1);
    }
}

#[test]
fn create_agent_program_accepts_only_its_three_string_spellings() {
    let request = json!({
        "operationId": OPERATION, "attempt": 1, "creator": {"kind":"routerOperator"},
        "agentProgram": "codex", "sessionKind": {"kind":"ordinary"}, "project": named_project(),
    });
    for spelling in ["codex", "claude", "cursor"] {
        let mut positive = request.clone();
        positive["agentProgram"] = json!(spelling);
        assert_case::<CreateFleetSession>(positive);
    }
    for malformed in [
        json!({"codex":null}),
        json!(["codex"]),
        json!("unknownHarness"),
        Value::Null,
    ] {
        let mut negative = request.clone();
        negative["agentProgram"] = malformed;
        assert_rejected::<CreateFleetSession>(&negative);
        assert!(serde_json::from_value::<CreateFleetSession>(negative).is_err());
    }
    let mut missing = request;
    missing.as_object_mut().unwrap().remove("agentProgram");
    assert_rejected::<CreateFleetSession>(&missing);
}

#[test]
fn create_replies_have_every_reservation_admission_and_final_case() {
    for (wire, tag) in [
        (
            json!({"result":"reserved","sessionId":SESSION,"secret":SECRET,"reservedUntil":TIME,"workspace":WORKSPACE}),
            "reserved",
        ),
        (json!({"result":"superseded"}), "superseded"),
        (
            json!({"result":"active","sessionId":SESSION,"address":address()}),
            "active",
        ),
        (
            json!({"result":"settled","settlement":settlement("abandoned", "givenUp")}),
            "settled",
        ),
        (
            json!({"result":"refused","reason":{"kind":"creatorNotAllowed"}}),
            "refused/creatorNotAllowed",
        ),
        (
            json!({"result":"refused","reason":{"kind":"parentNotValid"}}),
            "refused/parentNotValid",
        ),
        (
            json!({"result":"refused","reason":{"kind":"projectRequired"}}),
            "refused/projectRequired",
        ),
        (
            json!({"result":"refused","reason":{"kind":"projectNotInWorkspace"}}),
            "refused/projectNotInWorkspace",
        ),
        (
            json!({"result":"refused","reason":{"kind":"machineNotAllowed"}}),
            "refused/machineNotAllowed",
        ),
        (
            json!({"result":"refused","reason":{"kind":"helperOutsideWorkspace"}}),
            "refused/helperOutsideWorkspace",
        ),
        (json!({"result":"conflict"}), "conflict"),
        (json!({"result":"noSecret"}), "noSecret"),
        (json!({"result":"noSuchSession"}), "noSuchSession"),
        (json!({"result":"sessionEnded"}), "sessionEnded"),
        (json!({"result":"wrongSecret"}), "wrongSecret"),
    ] {
        let actual = match assert_case(wire) {
            CreateFleetSessionReply::Reserved {
                session_id,
                secret,
                reserved_until,
                workspace,
            } => {
                assert_eq!(session_id.as_str(), SESSION);
                assert_eq!(secret.expose_secret(), SECRET);
                assert_eq!(reserved_until.as_str(), TIME);
                assert_eq!(workspace.as_str(), WORKSPACE);
                "reserved"
            }
            CreateFleetSessionReply::Superseded {} => "superseded",
            CreateFleetSessionReply::Active {
                session_id,
                address,
            } => {
                assert_eq!(session_id.as_str(), SESSION);
                assert_eq!(address.machine_id.as_str(), MACHINE);
                assert_eq!(address.session_id.as_str(), "harness/thread");
                "active"
            }
            CreateFleetSessionReply::Settled { settlement } => {
                assert_eq!(settlement_case(settlement), ("abandoned", "givenUp"));
                "settled"
            }
            CreateFleetSessionReply::Refused { reason } => match reason {
                CreateSessionRefusal::CreatorNotAllowed {} => "refused/creatorNotAllowed",
                CreateSessionRefusal::ParentNotValid {} => "refused/parentNotValid",
                CreateSessionRefusal::ProjectRequired {} => "refused/projectRequired",
                CreateSessionRefusal::ProjectNotInWorkspace {} => "refused/projectNotInWorkspace",
                CreateSessionRefusal::MachineNotAllowed {} => "refused/machineNotAllowed",
                CreateSessionRefusal::HelperOutsideWorkspace {} => "refused/helperOutsideWorkspace",
            },
            CreateFleetSessionReply::Conflict {} => "conflict",
            CreateFleetSessionReply::NoSecret {} => "noSecret",
            CreateFleetSessionReply::NoSuchSession {} => "noSuchSession",
            CreateFleetSessionReply::SessionEnded {} => "sessionEnded",
            CreateFleetSessionReply::WrongSecret {} => "wrongSecret",
        };
        assert_eq!(actual, tag);
    }
}

#[test]
fn bind_request_and_all_six_replies_are_closed() {
    let request: BindFleetSession = assert_case(json!({
        "sessionId": SESSION, "harnessSessionId": "harness/thread", "secret": SECRET,
    }));
    assert_eq!(request.session_id.as_str(), SESSION);
    assert_eq!(request.harness_session_id.as_str(), "harness/thread");
    assert_eq!(request.secret.expose_secret(), SECRET);

    for (wire, tag) in [
        (json!({"result":"active","address":address()}), "active"),
        (json!({"result":"wrongSecret"}), "wrongSecret"),
        (
            json!({"result":"alreadyBoundToDifferentId"}),
            "alreadyBoundToDifferentId",
        ),
        (json!({"result":"addressTaken"}), "addressTaken"),
        (
            json!({"result":"settled","settlement":settlement("abandoned", "timeRanOut")}),
            "settled",
        ),
        (json!({"result":"noSuchStart"}), "noSuchStart"),
    ] {
        let actual = match assert_case(wire) {
            BindFleetSessionReply::Active { address } => {
                assert_eq!(address.machine_id.as_str(), MACHINE);
                assert_eq!(address.session_id.as_str(), "harness/thread");
                "active"
            }
            BindFleetSessionReply::WrongSecret {} => "wrongSecret",
            BindFleetSessionReply::AlreadyBoundToDifferentId {} => "alreadyBoundToDifferentId",
            BindFleetSessionReply::AddressTaken {} => "addressTaken",
            BindFleetSessionReply::Settled { settlement } => {
                assert_eq!(settlement_case(settlement), ("abandoned", "timeRanOut"));
                "settled"
            }
            BindFleetSessionReply::NoSuchStart {} => "noSuchStart",
        };
        assert_eq!(actual, tag);
    }
}

#[test]
fn end_requests_and_all_four_replies_are_closed() {
    for (target, cause, expected) in [
        (
            json!({"kind":"centralId","sessionId":SESSION}),
            "endedForGood",
            ("centralId", "endedForGood"),
        ),
        (
            json!({"kind":"operationId","operationId":OPERATION,"contentsDigest":DIGEST}),
            "givenUp",
            ("operationId", "givenUp"),
        ),
    ] {
        let request: EndFleetSession = assert_case(json!({"target":target,"cause":{"kind":cause}}));
        let target = match request.target {
            EndSessionTarget::CentralId { session_id } => {
                assert_eq!(session_id.as_str(), SESSION);
                "centralId"
            }
            EndSessionTarget::OperationId {
                operation_id,
                contents_digest,
            } => {
                assert_eq!(operation_id.as_str(), OPERATION);
                assert_eq!(contents_digest.as_str(), DIGEST);
                "operationId"
            }
        };
        let cause = match request.cause {
            RouterEndCause::EndedForGood {} => "endedForGood",
            RouterEndCause::GivenUp {} => "givenUp",
        };
        assert_eq!((target, cause), expected);
    }

    for (wire, tag) in [
        (
            json!({"result":"settled","settlement":settlement("ended", "givenUp")}),
            "settled",
        ),
        (
            json!({"result":"alreadySettled","settlement":settlement("ended", "endedForGood")}),
            "alreadySettled",
        ),
        (json!({"result":"noSuchStart"}), "noSuchStart"),
        (json!({"result":"notYoursToEnd"}), "notYoursToEnd"),
    ] {
        let actual = match assert_case(wire) {
            EndFleetSessionReply::Settled { settlement } => {
                assert_eq!(new_settlement_case(settlement), ("ended", "givenUp"));
                "settled"
            }
            EndFleetSessionReply::AlreadySettled { settlement } => {
                assert_eq!(settlement_case(settlement), ("ended", "endedForGood"));
                "alreadySettled"
            }
            EndFleetSessionReply::NoSuchStart {} => "noSuchStart",
            EndFleetSessionReply::NotYoursToEnd {} => "notYoursToEnd",
        };
        assert_eq!(actual, tag);
    }
}

#[test]
fn fresh_end_and_already_settled_have_different_cause_sets() {
    for (state, cause) in [
        ("abandoned", "givenUp"),
        ("ended", "givenUp"),
        ("ended", "endedForGood"),
    ] {
        let wire = json!({"result":"settled","settlement":settlement(state, cause)});
        let EndFleetSessionReply::Settled { settlement } = assert_case(wire) else {
            panic!("expected a fresh End settlement")
        };
        assert_eq!(new_settlement_case(settlement), (state, cause));
    }
    for (state, cause) in [
        ("abandoned", "revoked"),
        ("abandoned", "machineRevoked"),
        ("abandoned", "timeRanOut"),
        ("ended", "revoked"),
        ("ended", "machineRevoked"),
    ] {
        assert_rejected::<EndFleetSessionReply>(
            &json!({"result":"settled","settlement":settlement(state, cause)}),
        );
        let wire = json!({"result":"alreadySettled","settlement":settlement(state, cause)});
        let EndFleetSessionReply::AlreadySettled { settlement } = assert_case(wire) else {
            panic!("expected a stored settlement")
        };
        assert_eq!(settlement_case(settlement), (state, cause));
    }
}

fn new_settlement_case(settlement: NewEndSettlement) -> (&'static str, &'static str) {
    match settlement {
        NewEndSettlement::Abandoned {
            cause: NewAbandonmentCause::GivenUp {},
            time,
        } => {
            assert_eq!(time.as_str(), TIME);
            ("abandoned", "givenUp")
        }
        NewEndSettlement::Ended { cause, time } => {
            assert_eq!(time.as_str(), TIME);
            let cause = match cause {
                RouterEndCause::GivenUp {} => "givenUp",
                RouterEndCause::EndedForGood {} => "endedForGood",
            };
            ("ended", cause)
        }
    }
}

#[test]
fn each_settlement_state_has_exactly_its_own_causes() {
    for (state, cause) in [
        ("abandoned", "givenUp"),
        ("abandoned", "revoked"),
        ("abandoned", "machineRevoked"),
        ("abandoned", "timeRanOut"),
        ("ended", "endedForGood"),
        ("ended", "givenUp"),
        ("ended", "revoked"),
        ("ended", "machineRevoked"),
    ] {
        assert_eq!(
            settlement_case(assert_case(settlement(state, cause))),
            (state, cause)
        );
    }

    for cause in ["revoked", "machineRevoked", "timeRanOut"] {
        assert_rejected::<EndFleetSession>(&json!({
            "target": {"kind":"centralId", "sessionId": SESSION},
            "cause": {"kind": cause},
        }));
    }
    assert_rejected::<SessionSettlement>(&settlement("ended", "timeRanOut"));
    assert_rejected::<SessionSettlement>(&settlement("abandoned", "endedForGood"));
}

fn settlement_case(settlement: SessionSettlement) -> (&'static str, &'static str) {
    match settlement {
        SessionSettlement::Abandoned { cause, time } => {
            assert_eq!(time.as_str(), TIME);
            let cause = match cause {
                AbandonedSessionCause::GivenUp {} => "givenUp",
                AbandonedSessionCause::Revoked {} => "revoked",
                AbandonedSessionCause::MachineRevoked {} => "machineRevoked",
                AbandonedSessionCause::TimeRanOut {} => "timeRanOut",
            };
            ("abandoned", cause)
        }
        SessionSettlement::Ended { cause, time } => {
            assert_eq!(time.as_str(), TIME);
            let cause = match cause {
                EndedSessionCause::EndedForGood {} => "endedForGood",
                EndedSessionCause::GivenUp {} => "givenUp",
                EndedSessionCause::Revoked {} => "revoked",
                EndedSessionCause::MachineRevoked {} => "machineRevoked",
            };
            ("ended", cause)
        }
    }
}

fn named_project() -> Value {
    json!({"kind":"named","projectRef":{"workspace":WORKSPACE,"project":PROJECT}})
}

fn address() -> Value {
    json!({"machineId": MACHINE, "sessionId":"harness/thread"})
}

fn settlement(state: &str, cause: &str) -> Value {
    json!({"state":state,"cause":{"kind":cause},"time":TIME})
}

fn assert_case<T: DeserializeOwned + Serialize>(wire: Value) -> T {
    let decoded: T = serde_json::from_str(&wire.to_string()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
    visit_record_negatives::<T>(&wire, "");
    decoded
}

fn assert_rejected<T: DeserializeOwned>(wire: &Value) {
    assert!(
        serde_json::from_str::<T>(&wire.to_string()).is_err(),
        "{} accepted {wire}",
        std::any::type_name::<T>(),
    );
}

// Visit every object in each literal, keeping the rest of the request or reply valid.
fn visit_record_negatives<T: DeserializeOwned>(root: &Value, pointer: &str) {
    let Some(fields) = root.pointer(pointer).and_then(Value::as_object) else {
        return;
    };

    let reject_replacement = |replacement| {
        let mut negative = root.clone();
        *negative.pointer_mut(pointer).unwrap() = replacement;
        assert_rejected::<T>(&negative);
    };

    let mut extra = fields.clone();
    extra.insert("unknownField".to_owned(), json!(true));
    reject_replacement(Value::Object(extra));
    reject_replacement(Value::Null);

    let tag = ["kind", "result", "state"]
        .into_iter()
        .find(|tag| fields.contains_key(*tag));
    let mut positional = fields
        .iter()
        .filter(|(key, _)| Some(key.as_str()) != tag)
        .map(|(_, value)| value.clone())
        .collect::<Vec<_>>();
    if let Some(tag) = tag {
        let mut invalid = fields.clone();
        invalid.insert(tag.to_owned(), json!("unknownTag"));
        reject_replacement(Value::Object(invalid));
    }

    // Force the tagged sequence's first value to be the real tag. Try every
    // field order, including each Serde-derived declaration's positional order.
    let mut check_array = |ordered: &[Value]| {
        let mut values = tag
            .map(|tag| fields[tag].clone())
            .into_iter()
            .collect::<Vec<_>>();
        values.extend_from_slice(ordered);
        reject_replacement(Value::Array(values));
    };
    permute(&mut positional, 0, &mut check_array);

    for field in fields.keys() {
        let mut missing = fields.clone();
        missing.remove(field);
        reject_replacement(Value::Object(missing));
        visit_record_negatives::<T>(root, &format!("{pointer}/{field}"));
    }
}

fn permute(values: &mut [Value], index: usize, visit: &mut impl FnMut(&[Value])) {
    if index == values.len() {
        visit(values);
    } else {
        for choice in index..values.len() {
            values.swap(index, choice);
            permute(values, index + 1, visit);
            values.swap(index, choice);
        }
    }
}

#[test]
fn create_canonical_contents_and_sha256_url_match_fixed_vectors() {
    let operator: CreateFleetSession = serde_json::from_value(json!({
        "operationId": OPERATION, "attempt": 1, "creator": {"kind":"routerOperator"},
        "agentProgram":"codex", "sessionKind":{"kind":"ordinary"}, "project":named_project(),
    }))
    .unwrap();
    assert_eq!(
        operator.canonical_contents().unwrap(),
        br#"{"agentProgram":"codex","creatorActorId":null,"creatorKind":"routerOperator","kind":"ordinary","parentActorId":null,"project":{"kind":"named","projectRef":{"project":"019a3dce-2000-7000-8000-000000000005","workspace":"019a3dce-2000-7000-8000-000000000004"}}}"#,
    );
    assert_eq!(operator.contents_digest().unwrap().as_str(), DIGEST);

    let delegate = json!({
        "operationId": OPERATION, "attempt": 1,
        "creator": {"kind":"session","sessionId":SESSION,"presented":{"kind":"secret","secret":SECRET}},
        "agentProgram":"claude", "sessionKind":{"kind":"delegate","parent":SESSION},
        "project":{"kind":"sameAsParent"},
    });
    let original: CreateFleetSession = serde_json::from_value(delegate.clone()).unwrap();
    assert_eq!(
        original.canonical_contents().unwrap(),
        br#"{"agentProgram":"claude","creatorActorId":"019a3dce-2000-7000-8000-000000000001","creatorKind":"session","kind":"delegate","parentActorId":"019a3dce-2000-7000-8000-000000000001","project":{"kind":"sameAsParent"}}"#,
    );
    assert_eq!(
        original.contents_digest().unwrap().as_str(),
        "ut9zWSMkRdKCfTjN8zFtGLYCd0_Aych5rOGfzk1UGSw"
    );

    let mut named_delegate = delegate.clone();
    named_delegate["project"] = named_project();
    let named: CreateFleetSession = serde_json::from_value(named_delegate.clone()).unwrap();
    assert_ne!(
        named.contents_digest().unwrap(),
        original.contents_digest().unwrap()
    );
    for (field, different_id) in [("workspace", MACHINE), ("project", SESSION)] {
        let mut changed = named_delegate.clone();
        changed["project"]["projectRef"][field] = json!(different_id);
        let changed: CreateFleetSession = serde_json::from_value(changed).unwrap();
        assert_ne!(
            named.contents_digest().unwrap(),
            changed.contents_digest().unwrap()
        );
    }

    let mut repeat = delegate;
    repeat["attempt"] = json!(2);
    repeat["creator"]["presented"] = json!({"kind":"nothing"});
    repeat["operationId"] = json!(MACHINE);
    let retried: CreateFleetSession = serde_json::from_value(repeat).unwrap();
    assert_eq!(
        retried.contents_digest().unwrap(),
        original.contents_digest().unwrap()
    );
}

#[test]
fn create_rejects_a_request_without_a_project() {
    assert_rejected::<CreateFleetSession>(&json!({
        "operationId": OPERATION,
        "attempt": 1,
        "creator": {"kind":"routerOperator"},
        "agentProgram": "codex",
        "sessionKind": {"kind":"ordinary"},
    }));
}

#[test]
fn create_reserved_rejects_a_reply_without_a_workspace() {
    assert_rejected::<CreateFleetSessionReply>(&json!({
        "result":"reserved",
        "sessionId":SESSION,
        "secret":SECRET,
        "reservedUntil":TIME,
    }));
}

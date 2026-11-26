use std::{
    ffi::OsString,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use central_link_protocol::{
    DataFrame, LinkFrame, MacCallKind,
    approval_rpc::{ApprovalHolderTarget, RegisterRunReply},
};
use central_person_link_client::{
    PersonLinkError,
    approvals::ApprovalRequestId,
    test_support::{
        fixture_authority::{FixturePerson, INITIAL_SECONDS},
        fixture_signin::{ApprovalRelayFault, ApprovalSignIn},
    },
};
use collaboration_central_conformance::{
    approval_link_scenarios::{
        ROUTER_RUN, announce_mac_calls, held_start, raise, receive_frame, register_run,
        require_receipt,
    },
    approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise},
    signin_fixtures::{
        ORGANIZATION, OWNER, OWNER_SUBJECT, POLICY, Person, SESSION, fresh_request_id,
        person_link::person_link_bearer,
    },
};
use collaboration_central_tokio::test_support::{
    MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, start_session_link_fixture,
};
use fleet_runtime::PausePoint;
use router_clock::ManualClock;
use serde_json::{Value, json};

use super::*;
use crate::central_command::CentralCommand;

#[path = "approval_outcome_test.rs"]
mod outcomes;

const TARGET: &str = "fixture's target.json";
const PERSON: FixturePerson = FixturePerson {
    organization: ORGANIZATION,
    person: OWNER,
    subject: OWNER_SUBJECT,
    policy: POLICY,
    session: SESSION,
};

struct Fixture {
    central: MachineLinkFixture,
    signin: ApprovalSignIn,
}

impl Fixture {
    async fn start() -> Self {
        Self::with_person_claims(|_| {}).await
    }

    async fn with_person_claims(configure: impl Fn(&mut Person) + Send + Sync + 'static) -> Self {
        let clock = Arc::new(ManualClock::new(
            chrono::DateTime::from_timestamp(INITIAL_SECONDS, 0).unwrap(),
        ));
        let central = start_session_link_fixture(clock.clone()).await.unwrap();
        let runtime = central
            .host
            .central(&central.organization_id)
            .await
            .unwrap();
        for floor in [
            json!({"kind":"organization", "organizationId":ORGANIZATION, "revision":3,
                "state":"active", "permissionPolicyDigest":POLICY}),
            json!({"kind":"person", "organizationId":ORGANIZATION, "personId":OWNER,
                "subjectId":OWNER_SUBJECT, "revision":4, "state":"active"}),
        ] {
            runtime
                .deliver_authorization_revision(&serde_json::from_value(floor).unwrap())
                .await
                .unwrap();
        }
        let signin = ApprovalSignIn::start(clock, central.origin(), PERSON, move |now, _| {
            let mut person = Person::owner(ORGANIZATION, now).unwrap();
            person.second_factor_at = None;
            configure(&mut person);
            person_link_bearer(&person, now, 300, 600).unwrap()
        })
        .await;
        Self { central, signin }
    }

    async fn seed(&self) -> Offers {
        let target = ApprovalHolderTarget {
            organization_id: self.central.organization_id.clone(),
            machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
            enrollment_id: self.central.enrollment_id.clone(),
            router_run: ROUTER_RUN.parse().unwrap(),
        };
        let mut holder = self.central.connect().await.unwrap();
        assert!(matches!(
            receive_frame(&mut holder).await.unwrap(),
            LinkFrame::Data(DataFrame::Welcome { .. })
        ));
        announce_mac_calls(&mut holder, vec![MacCallKind::DeliverEnding])
            .await
            .unwrap();
        assert_eq!(
            register_run(&mut holder, &target.organization_id, &target.router_run)
                .await
                .unwrap(),
            RegisterRunReply::Registered {}
        );
        let (session, _) = create_bound(&mut holder, &target.organization_id, 1, None)
            .await
            .unwrap();
        let mut approval = serde_json::to_value(
            provider_raise(&target, &session, 2, ProviderAsk::Approval).unwrap(),
        )
        .unwrap();
        approval
            .pointer_mut("/offer/options")
            .and_then(Value::as_array_mut)
            .unwrap()
            .extend([
                json!({"id":3, "label":"Allow", "effect":"allow", "lasts":{"kind":"keptUntilChanged", "kept":"agentRulesFile"}}),
                json!({"id":4, "label":"Decline", "effect":"decline", "lasts":{"kind":"thisTurn", "kept":"agentMemory"}}),
                json!({"id":5, "label":"Abort", "effect":"abort", "lasts":{"kind":"thisSession", "kept":"nowhere"}}),
                json!({"id":6, "label":"Allow", "effect":"allow", "lasts":{"kind":"notVerified"}}),
            ]);
        let approval = serde_json::from_value(approval).unwrap();
        let question = provider_raise(&target, &session, 3, ProviderAsk::Question).unwrap();
        let held = held_start(&target.router_run, 4).unwrap();
        for request in [&approval, &question, &held] {
            require_receipt(
                raise(&mut holder, &target.organization_id, request.clone())
                    .await
                    .unwrap(),
            )
            .unwrap();
        }
        Offers {
            approval: approval.request_id().clone(),
            question: question.request_id().clone(),
            held: held.request_id().clone(),
        }
    }

    fn operation(&self) -> RequestId {
        fresh_request_id(self.central.clock.utc_now().timestamp())
            .unwrap()
            .parse()
            .unwrap()
    }

    async fn close(self) {
        self.signin.close().await;
        self.central.close().await.unwrap();
    }
}

struct Offers {
    approval: ApprovalRequestId,
    question: ApprovalRequestId,
    held: ApprovalRequestId,
}

#[derive(Debug)]
struct Captured {
    status: ApprovalExit,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl Captured {
    fn text(&self) -> &str {
        std::str::from_utf8(&self.stdout).unwrap()
    }

    fn json<Response: serde::de::DeserializeOwned>(&self) -> Response {
        serde_json::from_slice(&self.stdout).unwrap()
    }
}

fn command(action: &[&str]) -> Result<ApprovalCommand, CentralCommandError> {
    let args = ["approvals"]
        .into_iter()
        .chain(action.iter().copied())
        .chain(["--target", TARGET]);
    match CentralCommand::parse(args.map(OsString::from).collect())? {
        CentralCommand::Approvals(command) => Ok(command),
        _ => Err(CentralCommandError::InvalidInput(
            "expected central approvals command",
        )),
    }
}

async fn capture(fixture: &Fixture, client: &mut PersonLinkClient, action: &[&str]) -> Captured {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = run(
        command(action).unwrap(),
        client,
        fixture.central.clock.as_ref(),
        &mut stdout,
        &mut stderr,
    )
    .await
    .unwrap();
    Captured {
        status,
        stdout,
        stderr,
    }
}

#[tokio::test]
async fn commands_list_and_read_options_questions_and_held_calls_on_real_central() {
    let fixture = Fixture::start().await;
    let offers = fixture.seed().await;
    let mut client = fixture.signin.client();

    let listed = capture(&fixture, &mut client, &["list", "--json"]).await;
    assert_eq!(listed.status, ApprovalExit::Success);
    assert!(listed.stderr.is_empty());
    let ApprovalListReply::Page { items, next } = listed.json();
    assert_eq!(next, None);
    assert_eq!(
        items
            .into_iter()
            .map(|item| item.request_id)
            .collect::<Vec<_>>(),
        [
            offers.question.clone(),
            offers.approval.clone(),
            offers.held.clone()
        ]
    );

    let read = capture(&fixture, &mut client, &["read", offers.approval.as_str()]).await;
    assert_eq!(read.status, ApprovalExit::Success);
    assert_eq!(
        read.text()
            .lines()
            .filter(|line| line.starts_with("Option "))
            .collect::<Vec<_>>(),
        [
            "Option 1  Allow  (effect allow, this request, kept nowhere)",
            "Option 3  Allow  (effect allow, kept until changed in the agent's rules file)",
            "Option 4  Decline  (effect decline, this turn, kept in agent memory)",
            "Option 5  Abort  (effect abort, this session, kept nowhere)",
            "Option 6  Allow  (effect allow, lifetime and storage not verified)",
        ]
    );
    let wire = capture(
        &fixture,
        &mut client,
        &["read", offers.approval.as_str(), "--json"],
    )
    .await;
    assert!(
        matches!(wire.json::<ApprovalReadReply>(), ApprovalReadReply::Request { view } if view.request_id() == &offers.approval)
    );
    let question = capture(&fixture, &mut client, &["read", offers.question.as_str()]).await;
    assert!(question.text().contains("Field 1\n"));
    let held = capture(&fixture, &mut client, &["read", offers.held.as_str()]).await;
    assert!(held.text().contains("Start Claude in /fixture"));

    let decided = capture(
        &fixture,
        &mut client,
        &["decide", offers.approval.as_str(), "--option", "1"],
    )
    .await;
    assert_eq!(decided.status.code(), 0);
    assert_eq!(decided.text(), "Decision saved.\n");
    let waiting = capture(&fixture, &mut client, &["list"]).await;
    assert!(!waiting.text().contains(offers.approval.as_str()));
    let all = capture(&fixture, &mut client, &["list", "--all", "--json"]).await;
    let ApprovalListReply::Page { items, next: None } = all.json() else {
        panic!("expected all approvals");
    };
    assert!(items.iter().any(|item| item.request_id == offers.approval
        && item.state
            == ApprovalSummaryState::Ended {
                ending: ApprovalSummaryEnding::Decided
            }));

    drop(client);
    fixture.close().await;
}

#[tokio::test]
async fn decide_preserves_wire_replies_and_distinguishes_exit_classes_on_real_central() {
    let fixture = Fixture::start().await;
    let offers = fixture.seed().await;
    let mut client = fixture.signin.client();
    let operation = fixture.operation();
    let missing = "019f0000-0000-7000-8000-00000000e111";

    for (request, flags, assigned_id, status, text, check) in [
        (
            offers.approval.as_str(),
            &["--option", "2"][..],
            fixture.operation(),
            5,
            "Option 2 was not offered.",
            "invalid",
        ),
        (
            offers.held.as_str(),
            &["--allow"][..],
            operation.clone(),
            0,
            "Decision saved.",
            "savedDecision",
        ),
        (
            offers.held.as_str(),
            &["--decline"][..],
            operation.clone(),
            7,
            "already bound",
            "operationConflict",
        ),
        (
            offers.held.as_str(),
            &["--allow"][..],
            fixture.operation(),
            3,
            "Already decided by person",
            "alreadyDecided",
        ),
        (
            missing,
            &["--allow"][..],
            fixture.operation(),
            6,
            "Request is not visible.",
            "notVisible",
        ),
    ] {
        let mut decide = vec!["decide", request, "--operation", assigned_id.as_str()];
        decide.extend_from_slice(flags);
        let result = capture(&fixture, &mut client, &decide).await;
        assert_eq!(result.status.code(), status);
        assert!(result.text().contains(text), "unexpected text {result:?}");
        if status == 3 {
            assert!(result.text().contains(OWNER) && result.text().contains(" at "));
        }
        assert_eq!(
            result.stderr,
            format!("Operation ID {assigned_id}\n").into_bytes()
        );
        decide.push("--json");
        let result = capture(&fixture, &mut client, &decide).await;
        assert_eq!(result.status.code(), status);
        let reply: ApprovalDecideReply = result.json();
        assert_eq!(serde_json::to_value(reply).unwrap()["kind"], check);
    }
    let read = capture(&fixture, &mut client, &["read", missing]).await;
    assert_eq!(read.text(), "Request is not visible.\n");

    drop(client);
    fixture
        .central
        .clock
        .advance(Duration::from_secs(3601))
        .unwrap();
    let mut client = fixture.signin.client();
    let operation = fixture.operation();
    let mut args = vec![
        "decide",
        offers.approval.as_str(),
        "--option",
        "1",
        "--operation",
        operation.as_str(),
    ];
    let late = capture(&fixture, &mut client, &args).await;
    assert_eq!(late.status.code(), 4);
    assert_eq!(late.text(), "Too late. The request has ended.\n");
    args.push("--json");
    let late = capture(&fixture, &mut client, &args).await;
    assert_eq!(
        late.json::<ApprovalDecideReply>(),
        ApprovalDecideReply::TooLate {}
    );

    drop(client);
    fixture.close().await;
}

#[derive(Clone, Default)]
struct ObservedStderr(Arc<Mutex<(Vec<u8>, usize)>>);

impl Write for ObservedStderr {
    fn write(&mut self, output: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("stderr fixture lock poisoned"))?
            .0
            .extend_from_slice(output);
        Ok(output.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("stderr fixture lock poisoned"))?
            .1 += 1;
        Ok(())
    }
}

#[tokio::test]
async fn generated_operation_is_flushed_before_a_lost_reply_and_recovers_over_the_same_link_contract()
 {
    let fixture = Fixture::start().await;
    let offers = fixture.seed().await;
    let mut client = fixture.signin.client();
    let runtime = fixture
        .central
        .host
        .central(&fixture.central.organization_id)
        .await
        .unwrap();
    let mut pause = runtime
        .runtime()
        .pause_points()
        .hold_central(PausePoint::AfterPersonApprovalCommit);
    fixture
        .signin
        .relay
        .script_once(ApprovalRelayFault::DropSavedDecision);
    let mut stdout = Vec::new();
    let mut stderr = ObservedStderr::default();
    let observe = stderr.clone();
    let run_decide = run(
        command(&["decide", offers.held.as_str(), "--allow", "--json"]).unwrap(),
        &mut client,
        fixture.central.clock.as_ref(),
        &mut stdout,
        &mut stderr,
    );
    let after_commit = async {
        pause.until_held().await;
        let operation = {
            let evidence = fixture.signin.relay.evidence.lock().unwrap();
            let [first] = evidence.approval_calls.as_slice() else {
                panic!("expected a decide in flight");
            };
            let central_link_protocol::CentralCall::DecidePersonApproval(request) = &first.request
            else {
                panic!("expected decide on the socket");
            };
            request.body.operation_id.clone()
        };
        let (data, flushes) = &*observe.0.lock().unwrap();
        assert_eq!(data, format!("Operation ID {operation}\n").as_bytes());
        assert_eq!(*flushes, 1);
        assert_eq!(
            Uuid::parse_str(operation.as_str())
                .unwrap()
                .get_version_num(),
            7
        );
        pause.release();
        operation
    };
    let (status, operation) = tokio::join!(run_decide, after_commit);

    assert_eq!(status.unwrap().code(), 8);
    let unknown: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        unknown,
        json!({
            "kind":"unknown", "operationId":operation,
            "cause": PersonLinkError::Socket.to_string(),
            "retryCommand": format!("codex-router central approvals decide --target='fixture'\\''s target.json' {} --allow --operation {operation} --json", offers.held),
        })
    );
    assert_eq!(
        fixture
            .signin
            .relay
            .evidence
            .lock()
            .unwrap()
            .dropped_decisions,
        1
    );
    drop(client);
    let mut client = fixture.signin.client();
    let retried = capture(
        &fixture,
        &mut client,
        &[
            "decide",
            offers.held.as_str(),
            "--allow",
            "--operation",
            operation.as_str(),
            "--json",
        ],
    )
    .await;
    assert_eq!(retried.status.code(), 0);
    assert!(matches!(
        retried.json::<ApprovalDecideReply>(),
        ApprovalDecideReply::SavedDecision { .. }
    ));

    let explicit_operation = fixture.operation();
    fixture
        .signin
        .relay
        .script_once(ApprovalRelayFault::DropSavedDecision);
    let unknown_text = capture(
        &fixture,
        &mut client,
        &[
            "decide",
            offers.approval.as_str(),
            "--option",
            "1",
            "--operation",
            explicit_operation.as_str(),
        ],
    )
    .await;
    assert_eq!(unknown_text.status.code(), 8);
    assert_eq!(
        unknown_text.text().lines().next(),
        Some(
            format!(
                "Outcome unknown for operation {explicit_operation}. Cause: {}.",
                PersonLinkError::Socket
            )
            .as_str()
        )
    );
    assert_eq!(unknown_text.text().lines().last(), Some(format!(
        "codex-router central approvals decide --target='fixture'\\''s target.json' {} --option 1 --operation {explicit_operation}",
        offers.approval,
    ).as_str()));

    drop(client);
    fixture.close().await;
}

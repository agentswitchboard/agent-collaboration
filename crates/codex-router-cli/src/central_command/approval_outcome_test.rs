use super::*;
use std::sync::atomic::{AtomicI64, Ordering};

#[tokio::test]
async fn refusal_on_replay_reports_unknown_and_recovers_with_the_same_operation() {
    let revision = Arc::new(AtomicI64::new(4));
    let claims_revision = revision.clone();
    let fixture = Fixture::with_person_claims(move |person| {
        person.revision = claims_revision.load(Ordering::Acquire);
    })
    .await;
    let request = fixture.seed().await.held;
    let operation = fixture.operation();
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
    let mut client = fixture.signin.client();
    let args = [
        "decide",
        request.as_str(),
        "--allow",
        "--operation",
        operation.as_str(),
    ];

    let revise = async {
        pause.until_held().await;
        fixture
            .signin
            .relay
            .script_once(ApprovalRelayFault::RefuseSavedDecisionOnSocket { socket: 2 });
        runtime
            .deliver_authorization_revision(
                &serde_json::from_value(json!({
                    "kind":"person", "organizationId":ORGANIZATION, "personId":OWNER,
                    "subjectId":OWNER_SUBJECT, "revision":5, "state":"active",
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        revision.store(5, Ordering::Release);
        pause.release();
    };
    let (result, ()) = tokio::join!(capture(&fixture, &mut client, &args), revise);

    assert_eq!(result.status.code(), 8);
    assert_eq!(
        result.text(),
        format!(
            "Outcome unknown for operation {operation}. Cause: refused on replay: CentralFault.\n\
         Retry with the same operation ID and answer:\n\
         codex-router central approvals decide --target='fixture'\\''s target.json' {request} --allow --operation {operation}\n"
        )
    );
    assert_eq!(
        result.stderr,
        format!("Operation ID {operation}\n").into_bytes()
    );
    {
        let evidence = fixture.signin.relay.evidence.lock().unwrap();
        let [first, replay] = evidence.approval_calls.as_slice() else {
            panic!("expected a first attempt and one replay");
        };
        assert_eq!(
            evidence.closes,
            [central_link_protocol::PersonLinkCloseCode::Reconnect.code()]
        );
        assert_eq!((first.socket, replay.socket), (1, 2));
        assert_ne!(first.correlation, replay.correlation);
        assert_eq!(first.request, replay.request);
    }

    drop(client);
    let mut reopened = fixture.signin.client();
    let result = capture(
        &fixture,
        &mut reopened,
        &[
            "decide",
            request.as_str(),
            "--allow",
            "--operation",
            operation.as_str(),
            "--json",
        ],
    )
    .await;
    assert_eq!(result.status.code(), 0);
    assert!(matches!(
        result.json::<ApprovalDecideReply>(),
        ApprovalDecideReply::SavedDecision { .. }
    ));
    drop(reopened);
    fixture.close().await;
}

#[tokio::test]
async fn real_central_refusal_reports_the_reason_without_retry_guidance() {
    let fixture = Fixture::with_person_claims(|person| {
        person
            .permissions
            .retain(|grant| grant != "approval:decide");
    })
    .await;
    let request = fixture.seed().await.held;
    let operation = fixture.operation();
    let mut client = fixture.signin.client();
    let mut args = vec![
        "decide",
        request.as_str(),
        "--allow",
        "--operation",
        operation.as_str(),
    ];

    let result = capture(&fixture, &mut client, &args).await;

    assert_eq!(result.status.code(), 9);
    assert_eq!(
        result.text(),
        "Central refused the request: not permitted. The decision was not applied.\n"
    );
    assert_eq!(
        result.stderr,
        format!("Operation ID {operation}\n").into_bytes()
    );

    args.push("--json");
    let result = capture(&fixture, &mut client, &args).await;
    assert_eq!(result.status.code(), 9);
    assert_eq!(
        result.json::<Value>(),
        json!({"kind":"refused", "reason":{"kind":"notPermitted"}})
    );

    drop(client);
    fixture.close().await;
}

#[tokio::test]
async fn committed_decide_with_an_invalid_reply_reports_unknown_and_recovers() {
    let fixture = Fixture::start().await;
    let offers = fixture.seed().await;

    for (fault, request, choice) in [
        (
            ApprovalRelayFault::MalformedDecideReply,
            &offers.approval,
            &["--option", "1"][..],
        ),
        (
            ApprovalRelayFault::WrongDecideReply,
            &offers.held,
            &["--allow"][..],
        ),
    ] {
        let operation = fixture.operation();
        let mut args = vec![
            "decide",
            request.as_str(),
            "--operation",
            operation.as_str(),
            "--json",
        ];
        args.extend_from_slice(choice);
        fixture.signin.relay.script_once(fault);
        let mut client = fixture.signin.client();

        let result = capture(&fixture, &mut client, &args).await;

        assert_eq!(result.status.code(), 8);
        assert_eq!(
            result.stderr,
            format!("Operation ID {operation}\n").into_bytes()
        );
        let flags = choice.join(" ");
        assert_eq!(
            result.json::<Value>(),
            json!({
                "kind": "unknown", "operationId": operation, "cause": PersonLinkError::Protocol.to_string(),
                "retryCommand": format!("codex-router central approvals decide --target='fixture'\\''s target.json' {request} {flags} --operation {operation} --json"),
            })
        );
        drop(client);
        let mut reopened = fixture.signin.client();
        let replay = capture(&fixture, &mut reopened, &args).await;
        assert_eq!(replay.status.code(), 0);
        assert!(matches!(
            replay.json::<ApprovalDecideReply>(),
            ApprovalDecideReply::SavedDecision { .. }
        ));
    }

    fixture.close().await;
}

#[tokio::test]
async fn real_upgrade_refusal_reports_decision_was_not_sent() {
    let fixture = Fixture::start().await;
    let offers = fixture.seed().await;
    let runtime = fixture
        .central
        .host
        .central(&fixture.central.organization_id)
        .await
        .unwrap();
    runtime
        .deliver_authorization_revision(
            &serde_json::from_value(json!({
                "kind":"person", "organizationId":ORGANIZATION, "personId":OWNER,
                "subjectId":OWNER_SUBJECT, "revision":5, "state":"inactive",
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let operation = fixture.operation();
    let mut client = fixture.signin.client();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let result = run(
        command(&[
            "decide",
            offers.held.as_str(),
            "--allow",
            "--operation",
            operation.as_str(),
            "--json",
        ])
        .unwrap(),
        &mut client,
        fixture.central.clock.as_ref(),
        &mut stdout,
        &mut stderr,
    )
    .await;

    let error = result.unwrap_err();
    assert!(matches!(error, CentralCommandError::ApprovalNotSent(_)));
    assert_eq!(
        error.to_string(),
        "Decision was not sent: Sign-in or Central refused the person WebSocket with HTTP 401 Unauthorized"
    );
    assert!(stdout.is_empty());
    assert_eq!(stderr, format!("Operation ID {operation}\n").into_bytes());
    {
        let evidence = fixture.signin.relay.evidence.lock().unwrap();
        assert!(evidence.approval_calls.is_empty());
        assert_eq!(evidence.refused_upgrades, [401]);
    }
    drop(client);
    fixture.close().await;
}

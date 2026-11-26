use super::*;

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

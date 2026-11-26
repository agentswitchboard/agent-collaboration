use super::*;

use collaboration_central_conformance::signin_fixtures::{machine_token, stamp};
use fleet_contracts::{DeviceLoginDecision, MachineTokenDecision, ProvisionOrganization};
use reqwest_websocket::RequestBuilderExt;

#[tokio::test]
async fn typed_refusal_keeps_the_waiting_request_and_operation_log_unchanged() {
    let fixture = ApprovalFixture::with_person_claims(300, |person| {
        person
            .permissions
            .retain(|grant| grant != "approval:decide");
    })
    .await;
    let (target, mut holder) = fixture.holder().await;
    let request = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 1).unwrap(),
    )
    .await;
    let operation_id = operation(&fixture);
    let statement = "SELECT record_state, ended_at, ending_json, delivery_state FROM approval_requests WHERE request_id = ?1";
    let args = vec![SqlValue::Text(request.request_id().to_string())];
    let before = fixture.rows(statement, args.clone()).await;
    assert_eq!(
        before,
        [vec![
            SqlValue::Text("waiting".into()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null
        ]]
    );
    assert!(fixture.operation_rows().await.is_empty());
    let mut client = fixture.signin.client();

    let outcome = client
        .decide_approval(request.request_id().clone(), operation_id.clone(), allow())
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ApprovalDecisionOutcome::Refused {
            reason: central_link_protocol::LinkCallRefusal::NotPermitted
        }
    );
    assert_eq!(fixture.rows(statement, args).await, before);
    assert!(fixture.operation_rows().await.is_empty());
    {
        let evidence = fixture.signin.relay.evidence.lock().unwrap();
        let [attempt] = evidence.approval_calls.as_slice() else {
            panic!("expected exactly one decide attempt");
        };
        assert!(
            matches!(&attempt.request, CentralCall::DecidePersonApproval(call)
            if &call.body.request_id == request.request_id() && call.body.operation_id == operation_id)
        );
        assert!(evidence.closes.is_empty());
    }
    heartbeat(&mut holder).await.unwrap();
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn expiry_before_apply_reconnects_before_the_first_commit() {
    let fixture = ApprovalFixture::with_first_snapshot(1).await;
    let (target, mut holder) = fixture.holder().await;
    let request = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 1).unwrap(),
    )
    .await;
    let operation_id = operation(&fixture);
    let runtime = fixture
        .central
        .host
        .central(&fixture.central.organization_id)
        .await
        .unwrap();
    let pauses = runtime.runtime().pause_points();
    let mut first = pauses.hold_central(PausePoint::BeforeSignInSample);
    let mut client = fixture.signin.client();

    let apply = client.decide_approval(request.request_id().clone(), operation_id.clone(), allow());
    let expire = async {
        first.until_held().await;
        let mut replay = pauses.hold_central(PausePoint::BeforePersonApprovalTurn);
        fixture
            .central
            .clock
            .advance(Duration::from_secs(1))
            .unwrap();
        first.release();
        replay.until_held().await;
        assert!(fixture.operation_rows().await.is_empty());
        assert_eq!(
            fixture
                .rows(
                    "SELECT record_state FROM approval_requests WHERE request_id = ?1",
                    vec![SqlValue::Text(request.request_id().to_string())]
                )
                .await,
            [vec![SqlValue::Text("waiting".into())]]
        );
        replay.release();
    };
    let (outcome, ()) = tokio::join!(apply, expire);

    assert_eq!(outcome.unwrap(), saved(allow()));
    assert_eq!(
        fixture.operation_rows().await,
        [vec![SqlValue::Text(operation_id.to_string())]]
    );
    fixture
        .ending(&mut holder, &target, &request, held_ending())
        .await;
    {
        let evidence = fixture.signin.relay.evidence.lock().unwrap();
        let [first, replay] = evidence.approval_calls.as_slice() else {
            panic!("expected exactly two decide attempts");
        };
        assert_eq!(evidence.closes, [PersonLinkCloseCode::Reconnect.code()]);
        assert_eq!((first.socket, replay.socket), (1, 2));
        assert_ne!(first.correlation, replay.correlation);
        assert_eq!(first.request, replay.request);
    }
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn a_real_request_in_another_organization_is_not_visible() {
    let fixture = ApprovalFixture::start().await;
    let foreign = "019f0000-0000-7000-8000-00000000f002";
    let organization_id =
        fleet_identity_protocol::OrganizationId::try_from(foreign.to_owned()).unwrap();
    let runtime = fixture
        .central
        .host
        .central(&organization_id)
        .await
        .unwrap();
    let now = fixture.central.clock.utc_now().timestamp();
    runtime
        .provision_organization(&ProvisionOrganization {
            organization_id: organization_id.clone(),
            organization_statement: stamp(foreign, now).unwrap().try_into().unwrap(),
        })
        .await
        .unwrap();
    let enrolled = runtime.decide_device_login(&serde_json::from_value(json!({
        "organizationId":foreign, "approver":OWNER, "permissionChecked":"machine:enroll",
        "requestId":fresh_request_id(now).unwrap(), "machineId":MACHINE_LINK_FIXTURE_MACHINE,
        "label":"Foreign approval fixture", "allowedWorkspaces":[],
        "approverSecondFactorAt":now-60, "codeExpiresAt":now+600,
    })).unwrap()).await.unwrap();
    let DeviceLoginDecision::Approved { enrollment_id, .. } = enrolled else {
        panic!("expected foreign machine enrollment");
    };
    let token = runtime
        .decide_machine_token(
            &serde_json::from_value(json!({
                "organizationId":foreign, "enrollmentId":enrollment_id, "clientId":enrollment_id,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let MachineTokenDecision::Grant {
        grant_expires_at, ..
    } = token
    else {
        panic!("expected foreign machine grant");
    };
    let bearer = machine_token(
        foreign,
        MACHINE_LINK_FIXTURE_MACHINE,
        enrollment_id.as_str(),
        now,
        grant_expires_at.0,
    )
    .unwrap();
    let mut holder = fixture
        .central
        .client()
        .unwrap()
        .get(fixture.central.url(foreign))
        .bearer_auth(bearer)
        .upgrade()
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    assert!(matches!(
        receive_frame(&mut holder).await.unwrap(),
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    announce_mac_calls(&mut holder, vec![MacCallKind::DeliverEnding])
        .await
        .unwrap();
    let run = ROUTER_RUN.parse().unwrap();
    assert_eq!(
        register_run(&mut holder, &organization_id, &run)
            .await
            .unwrap(),
        RegisterRunReply::Registered {}
    );
    let request = held_start(&run, 173).unwrap();
    require_receipt(
        raise(&mut holder, &organization_id, request.clone())
            .await
            .unwrap(),
    )
    .unwrap();
    let key = SqlValue::Text(request.request_id().to_string());
    let readback = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT record_state FROM approval_requests WHERE request_id = ?1",
            args: vec![key],
        })
        .await
        .unwrap();
    assert_eq!(readback.rows, [vec![SqlValue::Text("waiting".into())]]);

    let mut client = fixture.signin.client();
    let read = client
        .read_approval(request.request_id().clone())
        .await
        .unwrap();
    let decided = client
        .decide_approval(request.request_id().clone(), operation(&fixture), allow())
        .await
        .unwrap();

    assert_eq!(read, ApprovalReadReply::NotVisible {});
    assert_eq!(
        decided,
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::NotVisible {})
    );
    assert!(fixture.operation_rows().await.is_empty());
    drop(client);
    drop(holder);
    fixture.close().await;
}

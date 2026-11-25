use std::{sync::Arc, time::Duration};

use axum::{Json, routing::get};
use central_link_protocol::PersonLinkCloseCode;
use central_person_link_client::{FleetTarget, PersonWorkspaceClient};
use collaboration_central_conformance::signin_fixtures::{
    CREATE, ORGANIZATION, OWNER, OWNER_SUBJECT, POLICY, canonical, create_body, create_intent,
    fresh_request_id,
};
use collaboration_central_core::{SignInTrustSettings, organization_signin_router};
use collaboration_central_tokio::{TokioWorkspaceHost, WorkspaceLink};
use fleet_contracts::ProvisionOrganization;
use router_clock::ManualClock;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::{
    fixture_authority::{
        ADMIN_AUDIENCE, AUDIENCE, CLIENT_ID, FixtureCredential, INITIAL_SECONDS as NOW, IssuerKey,
        PUBLIC_ORIGIN, fleet_claims, sha256_url,
    },
    fixture_signin::{PERSON_AUDIENCE, TestSignIn, TestTlsListener},
};

#[tokio::test]
async fn lists_every_page_once_after_real_central_ends_authority() {
    let clock = Arc::new(ManualClock::new(
        chrono::DateTime::from_timestamp(NOW, 0).unwrap(),
    ));
    let signing = Arc::new(IssuerKey::generate());
    let credential = FixtureCredential::new();
    let (signin_listener, certificate) =
        TestTlsListener::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let issuer = format!("https://{}", signin_listener.tcp.local_addr().unwrap());
    let central_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let central_origin = format!("http://{}", central_listener.local_addr().unwrap());
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        TokioWorkspaceHost::open_with_signin(
            root.path(),
            clock.clone(),
            SignInTrustSettings {
                issuer: issuer.clone(),
                audience: AUDIENCE.to_owned(),
                admin_audience: ADMIN_AUDIENCE.to_owned(),
                person_link_audience: Some(PERSON_AUDIENCE.to_owned()),
                machine_audience: "https://central.example.invalid/machine".to_owned(),
                public_origin: PUBLIC_ORIGIN.to_owned(),
                jwks_url: format!("{central_origin}/jwks"),
                service_id: Some(
                    "019f0000-0000-7000-8000-00000000d200"
                        .to_owned()
                        .try_into()
                        .unwrap(),
                ),
            },
        )
        .await
        .unwrap(),
    );
    let workspace_link = WorkspaceLink::new(host.clone());
    let keys = signing.jwks();
    let central_routes = workspace_link
        .router()
        .merge(organization_signin_router(host.clone()))
        .route("/jwks", get(move || async move { Json(keys) }));
    let stop = CancellationToken::new();
    let central_shutdown = stop.clone();
    let central = tokio::spawn(async move {
        axum::serve(central_listener, central_routes)
            .with_graceful_shutdown(central_shutdown.cancelled_owned())
            .await
            .unwrap();
    });

    let expected = seed_directory(&host, &central_origin, &issuer, &signing).await;
    let signin = TestSignIn::new(
        clock.clone(),
        issuer.clone(),
        central_origin,
        signing,
        credential.clone(),
    );
    let routes = signin.router();
    let signin_shutdown = stop.clone();
    let front = tokio::spawn(async move {
        axum::serve(signin_listener, routes)
            .with_graceful_shutdown(signin_shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    let target = FleetTarget::from_json(
        &serde_json::to_vec(&json!({
            "issuer":issuer, "organizationId":ORGANIZATION,
            "centralAdminAudience":ADMIN_AUDIENCE, "ownerFleetCliClientId":CLIENT_ID,
            "centralPublicOrigin":PUBLIC_ORIGIN,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut client =
        PersonWorkspaceClient::with_trusted_root(target, credential, clock.clone(), certificate)
            .unwrap();

    let first = client.list_workspaces().await.unwrap();
    assert_rows(&first, &expected);
    {
        let evidence = signin.evidence.lock().unwrap();
        assert_eq!((evidence.refreshes, evidence.upgrades), (1, 1));
        assert!(evidence.closes.is_empty());
    }

    clock.advance(Duration::from_secs(301)).unwrap();
    let second = client.list_workspaces().await.unwrap();
    assert_rows(&second, &expected);
    assert!(signin.rotation_saved());
    {
        let evidence = signin.evidence.lock().unwrap();
        assert_eq!((evidence.refreshes, evidence.upgrades), (2, 2));
        assert_eq!(evidence.closes, [PersonLinkCloseCode::Reconnect.code()]);
        let [first_page, first_next, expired, replay, second_next] = evidence.reads.as_slice()
        else {
            assert_eq!(
                evidence.reads.len(),
                5,
                "two pages per listing and one retry"
            );
            return;
        };
        assert_eq!(
            [
                first_page.socket,
                first_next.socket,
                expired.socket,
                replay.socket,
                second_next.socket
            ],
            [1, 1, 1, 2, 2]
        );
        assert!(first_page.request.after.is_none());
        assert_eq!(
            first_next.request.after.as_ref(),
            Some(&first.get(99).unwrap().workspace_id)
        );
        assert_eq!(expired.request, replay.request);
        assert_ne!(expired.correlation, replay.correlation);
        assert_eq!(second_next.request.after, first_next.request.after);
        assert_eq!(
            second_next.request.operation_id,
            replay.request.operation_id
        );
        assert_ne!(first_page.request.operation_id, replay.request.operation_id);
    }
    drop(client);
    stop.cancel();
    workspace_link.begin_shutdown();
    front.await.unwrap();
    signin.sockets.close();
    signin.sockets.wait().await;
    central.await.unwrap();
    workspace_link.shutdown().await;
    host.close().await;
}

fn assert_rows(
    rows: &[central_link_protocol::person_link_rpc::OrganizationWorkspace],
    expected: &[String],
) {
    let mut actual: Vec<_> = rows.iter().map(|row| row.name.as_str()).collect();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

async fn seed_directory(
    host: &TokioWorkspaceHost,
    central_origin: &str,
    issuer: &str,
    signing: &IssuerKey,
) -> Vec<String> {
    let organization = ORGANIZATION.to_owned().try_into().unwrap();
    let runtime = host.central(&organization).await.unwrap();
    let statement = signing.sign(
        "fleet-organization-stamp+jwt",
        &json!({
            "purpose":"fleet-organization-stamp", "version":1, "iss":issuer, "aud":AUDIENCE,
            "sub":ORGANIZATION, "organization_id":ORGANIZATION, "iat":NOW-60,"exp":NOW+86_400,
        }),
    );
    runtime
        .provision_organization(&ProvisionOrganization {
            organization_id: organization,
            organization_statement: statement.try_into().unwrap(),
        })
        .await
        .unwrap();
    for revision in [
        json!({"kind":"organization","organizationId":ORGANIZATION,"revision":3,"state":"active","permissionPolicyDigest":POLICY}),
        json!({"kind":"person","organizationId":ORGANIZATION,"personId":OWNER,"subjectId":OWNER_SUBJECT,"revision":4,"state":"active"}),
    ] {
        runtime
            .deliver_authorization_revision(&serde_json::from_value(revision).unwrap())
            .await
            .unwrap();
    }

    let owner = collaboration_central_conformance::signin_fixtures::ProofKey::generate().unwrap();
    let mut claims = fleet_claims(issuer, NOW, &owner.thumbprint().unwrap());
    claims
        .as_object_mut()
        .unwrap()
        .insert("iat".to_owned(), json!(NOW - 1));
    let access = signing.sign("at+jwt", &claims);
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut names = Vec::new();
    for number in 0..101 {
        let operation = fresh_request_id(NOW).unwrap();
        let name = format!("Workspace {number:03}");
        let intent = create_intent(ORGANIZATION, &operation, &name);
        let proof = owner.proof(&json!({
            "jti":fresh_request_id(NOW).unwrap(), "htm":"POST","htu":format!("{PUBLIC_ORIGIN}{CREATE}"),
            "iat":NOW, "ath":sha256_url(access.as_bytes()), "intent":sha256_url(canonical(&intent).as_bytes()),
        })).unwrap();
        let response = http
            .post(format!("{central_origin}{CREATE}"))
            .header("authorization", format!("DPoP {access}"))
            .header("dpop", proof)
            .json(&create_body(ORGANIZATION, &operation, &name))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "fixture workspace setup"
        );
        let result: Value = response.json().await.unwrap();
        assert_eq!(
            result.get("result").and_then(Value::as_str),
            Some("applied")
        );
        names.push(name);
    }
    names
}

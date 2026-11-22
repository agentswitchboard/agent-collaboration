//! Connect and renewal classification for unavailable and unknown issuer keys.
use super::super::fixture::{Fixture, receive, send};
use super::super::link_support::{caller, connect_client, create_workspace, workspace_reply};
use super::{answer_to, not_active, renew_frame};
use central_link_protocol::{LinkFrame, TokenRenewalRefusal, TokenRenewalResult};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use serde_json::json;
use std::time::Duration;
use workspace_model::WorkspaceResult;

/// `token` with its header naming a key central has never cached, so verifying
/// it needs a key-set fetch.
fn with_uncached_key(token: &str) -> Result<String> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let rest = token.split_once('.').ok_or("token header")?.1;
    let header =
        URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"at+jwt","kid":"stamp-uncached"}"#);
    Ok(format!("{header}.{rest}"))
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "The renewal answer and the still-open link are observed on the real socket."
)]
#[tokio::test]
async fn a_renewal_whose_key_cannot_be_fetched_is_central_unavailable() -> Result<()> {
    // Arrange: sign-in's key set becomes unreachable.
    let fixture = Fixture::new().await?;
    let (_client, _attempt, mut socket, _) = connect_client(&fixture, &fixture.token).await?;
    fixture.keys().fail_fetches();
    let token = with_uncached_key(&fixture.token)?;

    // Act
    send(
        &mut socket,
        &renew_frame("019f0000-0000-7000-8000-00000000c351", &token)?,
    )
    .await?;
    let answer = answer_to(&mut socket, "019f0000-0000-7000-8000-00000000c351").await?;
    send(&mut socket, &LinkFrame::Ping).await?;
    let still_open = receive(&mut socket).await?;

    // Assert: transient, and the link stays up on its current connection.
    assert_eq!(
        answer,
        TokenRenewalResult::Refused {
            reason: TokenRenewalRefusal::CentralUnavailable
        }
    );
    assert_eq!(still_open, LinkFrame::Pong);
    fixture.close().await
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "The real link must preserve retry/refusal bodies and the previous deadline."
)]
#[tokio::test]
async fn machine_link_distinguishes_issuer_cache_outages_from_unknown_keys() -> Result<()> {
    use fleet_signin::IssuerCacheOperation;
    use reqwest_websocket::RequestBuilderExt;
    let fixture = Fixture::new().await?;
    let (mut client, attempt, mut socket, _) = connect_client(&fixture, &fixture.token).await?;
    let organization = signed::ORGANIZATION;
    let owner = fixture
        .host
        .central(&organization.to_owned().try_into()?)
        .await?;
    let unknown = with_uncached_key(&fixture.token)?;
    let phases = [
        (
            Some(IssuerCacheOperation::Read),
            None,
            0,
            TokenRenewalRefusal::CentralUnavailable,
            503,
        ),
        (
            None,
            Some(json!({})),
            1,
            TokenRenewalRefusal::CentralUnavailable,
            503,
        ),
        (
            Some(IssuerCacheOperation::Remember),
            Some(signed::published_keys_except(&["stamp-a".to_owned()])?),
            1,
            TokenRenewalRefusal::CentralUnavailable,
            503,
        ),
        (None, None, 1, TokenRenewalRefusal::TokenRejected, 401),
    ];
    for (arm, document, fetch_count, refusal, expected_status) in phases {
        let before = fixture.keys().fetches();
        if let Some(operation) = arm {
            assert!(owner.fail_next_issuer_cache(operation));
        }
        if let Some(document) = document.clone() {
            fixture
                .keys()
                .hold_next_fetch()
                .send_issuer_reply(signed::IssuerReply::Document(document))?;
        }
        let response = fixture
            .client()?
            .get(fixture.url(organization))
            .bearer_auth(&unknown)
            .upgrade()
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), expected_status);
        assert_eq!(fixture.keys().fetches(), before + fetch_count);

        let before = fixture.keys().fetches();
        if let Some(operation) = arm {
            assert!(owner.fail_next_issuer_cache(operation));
        }
        if let Some(document) = document {
            fixture
                .keys()
                .hold_next_fetch()
                .send_issuer_reply(signed::IssuerReply::Document(document))?;
        }
        let correlation = "019f0000-0000-7000-8000-00000000c361";
        send(&mut socket, &renew_frame(correlation, &unknown)?).await?;
        assert_eq!(
            answer_to(&mut socket, correlation).await?,
            TokenRenewalResult::Refused { reason: refusal }
        );
        assert_eq!(fixture.keys().fetches(), before + fetch_count);
        // Read, parse and write failure each preserves warm admission with no refresh.
        let verified = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(fixture.signin_socket.clone())
            .build()?
            .post("http://localhost/internal/organization-statement-digest")
            .json(&json!({"organizationId": organization}))
            .send()
            .await?;
        assert_eq!(verified.status().as_u16(), 200);
        assert_eq!(fixture.keys().fetches(), before + fetch_count);
    }

    fixture.clock.advance(Duration::from_secs(599))?;
    let request = client.begin_request(
        caller()?,
        create_workspace(
            "019f0000-0000-7000-8000-00000000d361",
            "Before the retained expiry",
        )?,
    )?;
    send(&mut socket, &request).await?;
    let (_, answer) = workspace_reply(&mut client, &attempt, &mut socket).await?;
    assert!(matches!(answer.result, WorkspaceResult::Done(_)));
    fixture.clock.advance(Duration::from_secs(1))?;
    let correlation = "019f0000-0000-7000-8000-00000000c362";
    send(&mut socket, &renew_frame(correlation, &fixture.token)?).await?;
    assert_eq!(answer_to(&mut socket, correlation).await?, not_active());
    drop(socket);
    fixture.close().await
}

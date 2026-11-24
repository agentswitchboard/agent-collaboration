//! Connect and renewal classification over the actual Workerd socket.
use super::super::{
    baseline,
    fixture::{self, Fixture, ORGANIZATION},
};
use super::{advance, area, fresh, is_done, renew, renewal, retryable_expiry};
use central_link_protocol::{TokenRenewalRefusal, TokenRenewalResult};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};

#[tokio::test]
async fn a_renewal_whose_key_cannot_be_fetched_is_central_unavailable() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = key_fetch_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn key_fetch_journey(fixture: &Fixture) -> Result<()> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let (mut link, _, workspace, _, _) = baseline(fixture).await?;
    fixture.keys().fail_fetches();
    // A fresh token whose header names a key central never cached.
    let fresh = fresh(fixture, 0, 900)?;
    let rest = fresh.split_once('.').ok_or("token header")?.1;
    let header =
        URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"at+jwt","kid":"stamp-uncached"}"#);

    let answer = renewal(
        &mut link,
        renew(
            "019f0000-0000-7000-8000-00000000c351",
            &format!("{header}.{rest}"),
        )?,
    )
    .await?;
    let still_open = link
        .call_as(
            fixture::session_caller()?,
            area(
                "019f0000-0000-7000-8000-00000000cf51",
                "Still open",
                &workspace,
            )?,
        )
        .await?;

    let unavailable = TokenRenewalResult::Refused {
        reason: TokenRenewalRefusal::CentralUnavailable,
    };
    if answer != unavailable {
        return Err(format!("an unreachable key set is CentralUnavailable: {answer:?}").into());
    }
    if !is_done(&still_open.reply) {
        return Err(format!("the link stays up: {:?}", still_open.reply).into());
    }
    drop(link);
    Ok(())
}

#[tokio::test]
async fn machine_link_distinguishes_issuer_cache_outages_from_unknown_keys() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = issuer_cache_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn issuer_cache_journey(fixture: &Fixture) -> Result<()> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use reqwest_websocket::RequestBuilderExt;
    use serde_json::json;

    let (mut link, _, workspace, _, _) = baseline(fixture).await?;
    let runtime = fixture.runtime();
    let rest = fixture.token.split_once('.').ok_or("token header")?.1;
    let header =
        URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"at+jwt","kid":"stamp-uncached"}"#);
    let unknown = format!("{header}.{rest}");
    for (arm, document, fetch_count, refusal, expected_status) in [
        (
            Some("read"),
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
            Some("remember"),
            Some(signed::published_keys_except(&["stamp-a".to_owned()])?),
            1,
            TokenRenewalRefusal::CentralUnavailable,
            503,
        ),
        (None, None, 1, TokenRenewalRefusal::TokenRejected, 401),
    ] {
        let before = fixture.keys().fetches();
        if let Some(operation) = arm {
            assert!(
                runtime
                    .control::<bool>("fail-issuer-cache", &operation)
                    .await?
            );
        }
        if let Some(document) = document.clone() {
            fixture
                .keys()
                .hold_next_fetch()
                .send_issuer_reply(signed::IssuerReply::Document(document))?;
        }
        let response = fixture
            .client
            .get(fixture.link_url(ORGANIZATION))
            .bearer_auth(&unknown)
            .upgrade()
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), expected_status);
        assert_eq!(fixture.keys().fetches(), before + fetch_count);

        let before = fixture.keys().fetches();
        if let Some(operation) = arm {
            assert!(
                runtime
                    .control::<bool>("fail-issuer-cache", &operation)
                    .await?
            );
        }
        if let Some(document) = document {
            fixture
                .keys()
                .hold_next_fetch()
                .send_issuer_reply(signed::IssuerReply::Document(document))?;
        }
        let answer = renewal(
            &mut link,
            renew("019f0000-0000-7000-8000-00000000c361", &unknown)?,
        )
        .await?;
        assert_eq!(answer, TokenRenewalResult::Refused { reason: refusal });
        assert_eq!(fixture.keys().fetches(), before + fetch_count);
        let verified = fixture
            .client
            .post(format!(
                "{}/internal/organization-statement-digest",
                fixture.host.signin_origin()
            ))
            .json(&json!({"organizationId": ORGANIZATION}))
            .send()
            .await?;
        assert_eq!(verified.status().as_u16(), 200);
        assert_eq!(fixture.keys().fetches(), before + fetch_count);
    }

    fixture.host.c2_evict(&fixture.organization).await?;
    advance(fixture, 599).await?;
    let before = link
        .call_as(
            fixture::session_caller()?,
            area(
                "019f0000-0000-7000-8000-00000000cf61",
                "Before the retained expiry",
                &workspace,
            )?,
        )
        .await?;
    assert!(is_done(&before.reply));
    advance(fixture, 1).await?;
    assert_eq!(
        renewal(
            &mut link,
            renew(
                "019f0000-0000-7000-8000-00000000c362",
                &fresh(fixture, 600, 1200)?
            )?
        )
        .await?,
        retryable_expiry()
    );
    drop(link);
    Ok(())
}

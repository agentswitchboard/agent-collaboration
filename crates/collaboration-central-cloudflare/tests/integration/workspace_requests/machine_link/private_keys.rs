use super::{ConnectedLink, Fixture, ORGANIZATION, fixture::open_sign_in, renewal};
use central_link_protocol::TokenRenewalResult;
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use fleet_contracts::PROVISION_ORGANIZATION_ROUTE;
use reqwest_websocket::CloseCode;
use serde_json::json;
use signed::person_link::{PersonLinkFixture, install_floors, open_person};

#[tokio::test]
async fn private_keys_admission_and_cache_rejects_public_url_build() -> Result<()> {
    let fixture = Fixture::with_private_keys_probe().await?;
    let mut machine = ConnectedLink::open(&fixture).await?;
    renewal::advance(&fixture, 1).await?;
    let token = renewal::fresh(&fixture, 1, 601)?;
    let renewed = renewal::renewal(
        &mut machine,
        renewal::renew("019f0000-0000-7000-8000-00000000cf0a", &token)?,
    )
    .await?;
    if !matches!(renewed, TokenRenewalResult::Renewed { .. }) {
        return Err(format!("machine renewal was refused: {renewed:?}").into());
    }
    machine.close().await?;

    install_floors(ORGANIZATION, &async |path, body, headers| {
        let mut request = fixture
            .client
            .post(format!("{}{path}", fixture.host.signin_origin()))
            .json(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await?;
        Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
    })
    .await?;
    let now = signed::NOW_SECONDS + 1;
    let person = signed::Person::owner(ORGANIZATION, now)?;
    let person_url = format!(
        "{}/v1/organizations/{ORGANIZATION}/person-link",
        fixture.host.public_origin()
    );
    let socket = open_person(
        &PersonLinkFixture {
            client: &fixture.client,
            person_url: &person_url,
            machine_url: &fixture.link_url(ORGANIZATION),
            organization: ORGANIZATION,
            machine_token: &fixture.token,
        },
        &person,
        now,
        100,
        140,
    )
    .await?;
    socket.close(CloseCode::Normal, None).await?;
    fixture.host.assert_jwks_requests(1).await?;

    // An unknown kid forces one refresh even while the first document is fresh.
    let unknown = signed::sign(
        &json!({"typ":"fleet-organization-stamp+jwt","alg":"EdDSA","kid":"unpublished"}),
        &signed::claims(ORGANIZATION, now),
        9,
    )?;
    let status = provision(&fixture.client, &fixture.host, &unknown).await?;
    if status != 422 {
        return Err(format!("unpublished key got HTTP {status}, expected 422").into());
    }
    fixture.host.assert_jwks_requests(2).await?;

    // That refresh at t+1 resets the expiry. Use the clock one second before
    // expiry, then exactly at expiry, with a fresh signed stamp each time.
    for (advance, offset, requests) in [(299, 1, 2), (1, 2, 3)] {
        renewal::advance(&fixture, advance).await?;
        let stamp = signed::first_statement(ORGANIZATION, now, offset)?;
        let status = provision(&fixture.client, &fixture.host, &stamp).await?;
        if status != 200 {
            return Err(format!("stamp at cache boundary got HTTP {status}, expected 200").into());
        }
        fixture.host.assert_jwks_requests(requests).await?;
    }
    fixture.close().await
}

#[tokio::test]
async fn cold_private_keys_outage_rejects_public_fallback_build() -> Result<()> {
    let keys = signed::SignInKeys::start().await?;
    keys.fail_fetches();
    assert_cold_unavailable(Some(&keys.url), 1).await?;
    keys.shutdown().await
}

#[tokio::test]
async fn cold_missing_private_keys_binding_is_unavailable() -> Result<()> {
    assert_cold_unavailable(None, 0).await
}

async fn assert_cold_unavailable(fixture_url: Option<&str>, private_gets: usize) -> Result<()> {
    let (host, _, _plan_root) = open_sign_in(fixture_url, false, true).await?;
    let client = reqwest::Client::builder().no_proxy().build()?;
    // The public trap has the key that could verify this stamp.
    let stamp = signed::stamp_under_second_key(ORGANIZATION, signed::NOW_SECONDS)?;
    let status = provision(&client, &host, &stamp).await?;
    host.assert_jwks_requests(private_gets).await?;
    if status != 503 {
        return Err(format!("cold unavailable keys got HTTP {status}, expected 503").into());
    }
    host.close().await;
    Ok(())
}

async fn provision(
    client: &reqwest::Client,
    host: &super::super::workerd_host::WorkerdHost,
    stamp: &str,
) -> Result<u16> {
    let response = client
        .post(format!(
            "{}{PROVISION_ORGANIZATION_ROUTE}",
            host.signin_origin()
        ))
        .json(&json!({"organizationId": ORGANIZATION, "organizationStatement": stamp}))
        .send()
        .await?;
    Ok(response.status().as_u16())
}

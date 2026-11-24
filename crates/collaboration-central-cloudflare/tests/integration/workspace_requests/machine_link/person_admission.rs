use super::{Fixture, ORGANIZATION};
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::person_link::{
        PersonLinkFixture, check_person_link_admission, check_workspace_list,
    },
};

#[tokio::test]
async fn workerd_person_upgrade_and_lifetime_rejects_machine_only_carrier() -> ConformanceResult<()>
{
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = person_link_url(fixture.host.public_origin());
        check_person_link_admission(
            PersonLinkFixture {
                client: &fixture.client,
                person_url: &person_url,
                machine_url: &machine_url,
                organization: ORGANIZATION,
                machine_token: &fixture.token,
            },
            async |path, body, headers| {
                let mut request = fixture
                    .client
                    .post(format!("{}{path}", fixture.host.signin_origin()))
                    .json(body);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
            },
            async |seconds| super::renewal::advance(&fixture, seconds).await,
        )
        .await?;
        check_empty_person_upgrade_response(&fixture, &person_url, None, 401).await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_person_workspace_call_rejects_machine_only_carrier() -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = person_link_url(fixture.host.public_origin());
        check_workspace_list(
            PersonLinkFixture {
                client: &fixture.client,
                person_url: &person_url,
                machine_url: &machine_url,
                organization: ORGANIZATION,
                machine_token: &fixture.token,
            },
            async |path, body, headers| {
                let mut request = fixture
                    .client
                    .post(format!("{}{path}", fixture.host.signin_origin()))
                    .json(body);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
            },
        )
        .await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_private_person_link_404_rejects_leftover_upgrade_route() -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let private_url = person_link_url(&fixture.host.signin_origin());
    let result = async {
        check_empty_person_upgrade_response(&fixture, &private_url, None, 404).await?;
        // Send a bearer as well, so a private route that only forwards bearer requests fails.
        check_empty_person_upgrade_response(&fixture, &private_url, Some(&fixture.token), 404).await
    }
    .await;
    fixture.close().await?;
    result
}

fn person_link_url(origin: &str) -> String {
    format!("{origin}/v1/organizations/{ORGANIZATION}/person-link")
}

async fn check_empty_person_upgrade_response(
    fixture: &Fixture,
    url: &str,
    bearer: Option<&str>,
    expected: u16,
) -> ConformanceResult<()> {
    let request = fixture
        .client
        .get(url)
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==");
    let request = match bearer {
        Some(token) => request.bearer_auth(token),
        None => request,
    };
    let response = request.send().await?;
    if response.status().as_u16() != expected {
        return Err(format!("{url}: expected HTTP {expected}, got {}", response.status()).into());
    }
    if !response.bytes().await?.is_empty() {
        return Err(format!("{url}: expected an empty HTTP {expected} response").into());
    }
    Ok(())
}

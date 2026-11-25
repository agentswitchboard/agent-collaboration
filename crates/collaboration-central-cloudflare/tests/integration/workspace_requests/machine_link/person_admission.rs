use super::{Fixture, ORGANIZATION, fixture::open_sign_in};
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::{
        NOW_SECONDS, Person,
        person_link::{
            CreateCallCase, PersonLinkFixture, check_person_link_admission, check_workspace_create,
            check_workspace_list, person_link_bearer,
        },
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
        check_empty_person_upgrade_response(&fixture.client, &person_url, None, 401).await
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
        check_empty_person_upgrade_response(&fixture.client, &private_url, None, 404).await?;
        // Send a bearer as well, so a private route that only forwards bearer requests fails.
        check_empty_person_upgrade_response(
            &fixture.client,
            &private_url,
            Some(&fixture.token),
            404,
        )
        .await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_missing_keys_on_person_upgrade_rejects_unconfigured_verifier_build()
-> ConformanceResult<()> {
    let (host, _, _plan_root) = open_sign_in(None, false, true).await?;
    let client = reqwest::Client::builder().no_proxy().build()?;
    let person_url = person_link_url(host.public_origin());
    let person = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let bearer = person_link_bearer(&person, NOW_SECONDS, 100, 140)?;

    check_empty_person_upgrade_response(&client, &person_url, None, 401).await?;
    let upgrade =
        check_empty_person_upgrade_response(&client, &person_url, Some(&bearer), 503).await;
    host.assert_jwks_requests(0).await?;
    host.close().await;
    upgrade
}

fn person_link_url(origin: &str) -> String {
    format!("{origin}/v1/organizations/{ORGANIZATION}/person-link")
}

async fn check_empty_person_upgrade_response(
    client: &reqwest::Client,
    url: &str,
    bearer: Option<&str>,
    expected: u16,
) -> ConformanceResult<()> {
    let request = client
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
        return Err(format!(
            "person upgrade expected HTTP {expected}, got {}",
            response.status()
        )
        .into());
    }
    if !response.bytes().await?.is_empty() {
        return Err(format!("person upgrade expected an empty HTTP {expected} response").into());
    }
    Ok(())
}

async fn check_create(case: CreateCallCase) -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = person_link_url(fixture.host.public_origin());
        check_workspace_create(
            PersonLinkFixture {
                client: &fixture.client,
                person_url: &person_url,
                machine_url: &machine_url,
                organization: ORGANIZATION,
                machine_token: &fixture.token,
            },
            case,
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
            async |elapsed| super::renewal::advance_time(&fixture, elapsed).await,
        )
        .await
    }
    .await;
    fixture.close().await?;
    result
}

macro_rules! create_case {
    ($name:ident, $case:ident) => {
        #[tokio::test]
        async fn $name() -> ConformanceResult<()> {
            check_create(CreateCallCase::$case).await
        }
    };
}

create_case!(
    workerd_person_create_rejects_exclusive_factor_start,
    AtFactorTime
);
create_case!(
    workerd_person_create_rejects_exclusive_factor_end,
    AtFactorLimit
);
create_case!(
    workerd_person_create_rejects_shortened_stale_factor_window,
    PastFactorLimit
);
create_case!(
    workerd_person_create_rejects_shortened_missing_factor_window,
    MissingFactor
);
create_case!(
    workerd_person_create_rejects_shortened_future_factor_window,
    FutureFactor
);
create_case!(
    workerd_person_create_rejects_permission_conflict_projection,
    NoPermission
);
create_case!(
    workerd_person_create_rejects_name_collision_permission_projection,
    DuplicateName
);
create_case!(
    workerd_person_create_rejects_changed_operation_name_projection,
    ChangedOperation
);
create_case!(
    workerd_person_create_rejects_conflicting_cross_transport_replay,
    RetainedAcrossTransports
);

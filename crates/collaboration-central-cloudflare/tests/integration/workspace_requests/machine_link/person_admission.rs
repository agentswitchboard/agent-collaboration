use super::{Fixture, ORGANIZATION};
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::person_link::{
        CreateCallCase, PersonLinkFixture, check_person_link_admission, check_workspace_create,
        check_workspace_list,
    },
};

#[tokio::test]
async fn workerd_person_upgrade_and_lifetime() -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = format!(
            "{}/v1/organizations/{ORGANIZATION}/person-link",
            fixture.host.signin_origin()
        );
        check_person_ingress(&fixture, &person_url).await?;
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
        .await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_person_lists_organization_workspaces() -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = format!(
            "{}/v1/organizations/{ORGANIZATION}/person-link",
            fixture.host.signin_origin()
        );
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

async fn check_person_ingress(fixture: &Fixture, person_url: &str) -> ConformanceResult<()> {
    let carrier_url = format!(
        "{}/v1/organizations/{ORGANIZATION}/person-link",
        fixture.host.public_origin()
    );
    for (url, expected) in [(person_url, 401), (carrier_url.as_str(), 404)] {
        let response = fixture
            .client
            .get(url)
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .send()
            .await?;
        if response.status().as_u16() != expected {
            return Err(format!(
                "person ingress expected HTTP {expected}, got {}",
                response.status()
            )
            .into());
        }
        if expected == 401 && !response.bytes().await?.is_empty() {
            return Err("private person route returned a body without a credential".into());
        }
    }
    Ok(())
}

async fn check_create(case: CreateCallCase) -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = format!(
            "{}/v1/organizations/{ORGANIZATION}/person-link",
            fixture.host.signin_origin()
        );
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

create_case!(workerd_person_create_at_factor_time, AtFactorTime);
create_case!(workerd_person_create_at_factor_limit, AtFactorLimit);
create_case!(workerd_person_create_past_factor_limit, PastFactorLimit);
create_case!(workerd_person_create_missing_factor, MissingFactor);
create_case!(workerd_person_create_future_factor, FutureFactor);
create_case!(workerd_person_create_without_permission, NoPermission);
create_case!(workerd_person_create_duplicate_name, DuplicateName);
create_case!(workerd_person_create_changed_operation, ChangedOperation);
create_case!(
    workerd_person_create_retained_across_transports,
    RetainedAcrossTransports
);

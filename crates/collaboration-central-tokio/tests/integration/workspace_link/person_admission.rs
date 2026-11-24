use super::fixture::start_fixture;
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::person_link::{
        CreateCallCase, PersonLinkFixture, check_person_link_admission, check_workspace_create,
        check_workspace_list,
    },
};
use std::time::Duration;

#[tokio::test]
async fn native_person_upgrade_and_lifetime() -> ConformanceResult<()> {
    let fixture = start_fixture().await?;
    let result = async {
        let client = fixture.client()?;
        let private = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(fixture.signin_socket.clone())
            .build()?;
        let machine_url = fixture.url(fixture.organization_id.as_str());
        let person_url = format!(
            "{}/person-link",
            machine_url
                .strip_suffix("/link")
                .ok_or("machine route fixture")?
        );
        check_person_link_admission(
            PersonLinkFixture {
                client: &client,
                person_url: &person_url,
                machine_url: &machine_url,
                organization: fixture.organization_id.as_str(),
                machine_token: &fixture.token,
            },
            async |path, body, headers| {
                let mut request = private
                    .post(format!("http://central.local{path}"))
                    .json(body);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
            },
            async |seconds| {
                fixture.clock.advance(Duration::from_secs(seconds))?;
                Ok(())
            },
        )
        .await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn native_person_lists_organization_workspaces() -> ConformanceResult<()> {
    let fixture = start_fixture().await?;
    let result = async {
        let client = fixture.client()?;
        let private = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(fixture.signin_socket.clone())
            .build()?;
        let machine_url = fixture.url(fixture.organization_id.as_str());
        let person_url = format!(
            "{}/person-link",
            machine_url
                .strip_suffix("/link")
                .ok_or("machine route fixture")?
        );
        check_workspace_list(
            PersonLinkFixture {
                client: &client,
                person_url: &person_url,
                machine_url: &machine_url,
                organization: fixture.organization_id.as_str(),
                machine_token: &fixture.token,
            },
            async |path, body, headers| {
                let mut request = private
                    .post(format!("http://central.local{path}"))
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

async fn check_create(case: CreateCallCase) -> ConformanceResult<()> {
    let fixture = start_fixture().await?;
    let result = async {
        let client = fixture.client()?;
        let private = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(fixture.signin_socket.clone())
            .build()?;
        let machine_url = fixture.url(fixture.organization_id.as_str());
        let person_url = format!(
            "{}/person-link",
            machine_url
                .strip_suffix("/link")
                .ok_or("machine route fixture")?
        );
        check_workspace_create(
            PersonLinkFixture {
                client: &client,
                person_url: &person_url,
                machine_url: &machine_url,
                organization: fixture.organization_id.as_str(),
                machine_token: &fixture.token,
            },
            case,
            async |path, body, headers| {
                let mut request = private
                    .post(format!("http://central.local{path}"))
                    .json(body);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
            },
            async |elapsed| {
                fixture.clock.advance(elapsed)?;
                Ok(())
            },
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

create_case!(native_person_create_at_factor_time, AtFactorTime);
create_case!(native_person_create_at_factor_limit, AtFactorLimit);
create_case!(native_person_create_past_factor_limit, PastFactorLimit);
create_case!(native_person_create_missing_factor, MissingFactor);
create_case!(native_person_create_future_factor, FutureFactor);
create_case!(native_person_create_without_permission, NoPermission);
create_case!(native_person_create_duplicate_name, DuplicateName);
create_case!(native_person_create_changed_operation, ChangedOperation);
create_case!(
    native_person_create_retained_across_transports,
    RetainedAcrossTransports
);

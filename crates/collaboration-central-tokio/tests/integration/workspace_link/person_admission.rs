use super::fixture::{signed_clock, start_fixture};
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::person_link::{
        CreateCallCase, PersonLinkFixture, check_person_link_admission, check_workspace_create,
        check_workspace_list,
        organization_reads::{OrganizationReadCase, check_organization_read},
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

create_case!(
    native_person_create_rejects_exclusive_factor_start,
    AtFactorTime
);
create_case!(
    native_person_create_rejects_exclusive_factor_end,
    AtFactorLimit
);
create_case!(
    native_person_create_rejects_shortened_stale_factor_window,
    PastFactorLimit
);
create_case!(
    native_person_create_rejects_shortened_missing_factor_window,
    MissingFactor
);
create_case!(
    native_person_create_rejects_shortened_future_factor_window,
    FutureFactor
);
create_case!(
    native_person_create_rejects_permission_conflict_projection,
    NoPermission
);
create_case!(
    native_person_create_rejects_name_collision_permission_projection,
    DuplicateName
);
create_case!(
    native_person_create_rejects_changed_operation_name_projection,
    ChangedOperation
);
create_case!(
    native_person_create_rejects_conflicting_cross_transport_replay,
    RetainedAcrossTransports
);

async fn check_read_case(case: OrganizationReadCase) -> ConformanceResult<()> {
    let fixture =
        collaboration_central_tokio::test_support::start_session_link_fixture(signed_clock()?)
            .await?;
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
        check_organization_read(
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
            case,
        )
        .await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn native_person_lookup_rejects_signed_post_only_or_missing_as_fault_build()
-> ConformanceResult<()> {
    check_read_case(OrganizationReadCase::Lookup).await
}

#[tokio::test]
async fn native_person_audit_rejects_signed_post_only_or_inexact_cursor_build()
-> ConformanceResult<()> {
    check_read_case(OrganizationReadCase::AuditPages).await
}

#[tokio::test]
async fn native_person_reads_reject_shared_view_permission_build() -> ConformanceResult<()> {
    check_read_case(OrganizationReadCase::Permissions).await
}

#[tokio::test]
async fn native_person_reads_reject_machine_or_cross_organization_dispatch_build()
-> ConformanceResult<()> {
    check_read_case(OrganizationReadCase::PrincipalBoundaries).await
}

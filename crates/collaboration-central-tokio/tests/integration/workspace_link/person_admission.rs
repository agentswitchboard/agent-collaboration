use super::fixture::start_fixture;
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::person_link::{PersonLinkFixture, check_person_link_admission},
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

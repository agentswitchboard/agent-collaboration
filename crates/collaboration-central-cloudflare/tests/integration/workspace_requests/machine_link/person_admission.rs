use super::{Fixture, ORGANIZATION};
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::person_link::{PersonLinkFixture, check_person_link_admission},
};

#[tokio::test]
async fn workerd_person_upgrade_and_lifetime() -> ConformanceResult<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = async {
        let machine_url = fixture.link_url(ORGANIZATION);
        let person_url = format!(
            "{}/person-link",
            machine_url
                .strip_suffix("/link")
                .ok_or("machine route fixture")?
        );
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

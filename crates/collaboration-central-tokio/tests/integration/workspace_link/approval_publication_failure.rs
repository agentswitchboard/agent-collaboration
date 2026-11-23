use std::cell::{Cell, RefCell};

use central_storage::AlarmPublicationFailure;
use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::approval_routes::check_signed_holder_delivery_after_publication_failure,
};
use collaboration_central_tokio::test_support::start_session_link_fixture;

use super::{NativeApprovalFixture, signed_clock};

#[tokio::test]
async fn tokio_releases_committed_person_ending_when_alarm_publication_fails()
-> ConformanceResult<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = async {
        let runtime = fixture
            .link
            .host
            .central(&fixture.link.organization_id)
            .await?;
        let store = runtime.runtime().statement_host();
        let client = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(fixture.link.signin_socket.clone())
            .build()?;
        check_signed_holder_delivery_after_publication_failure(
            &fixture,
            async |path, body, headers| {
                let mut request = client
                    .post(format!("http://central.local{path}"))
                    .json(body);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
            },
            async || {
                store.fail_next_alarm_publication(AlarmPublicationFailure::NoLaterGridMark);
                Ok(())
            },
        )
        .await?;
        if store.take_alarm_publication_failures() != [AlarmPublicationFailure::NoLaterGridMark] {
            return Err(
                "the real native alarm publisher did not report the injected failure".into(),
            );
        }
        Ok(())
    }
    .await;
    fixture.link.close().await?;
    observed
}

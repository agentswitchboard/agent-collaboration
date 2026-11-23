use super::{ConnectedLink, Fixture, ORGANIZATION, Result, signed};
use central_link_protocol::{DataFrame, LinkFrame};
use collaboration_central_conformance::{
    approval_link_scenarios::receive_frame,
    machine_link_boundary::{check_expired_heartbeat, check_input_sizes, expect_pong},
};
use reqwest_websocket::{RequestBuilderExt, WebSocket};

#[tokio::test]
async fn text_heartbeat_closes_an_expired_worker_link() -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        expect_pong(&mut link.socket).await?;
        super::renewal::advance(&fixture, 660).await?;
        check_expired_heartbeat(&mut link.socket).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn worker_input_accepts_one_mib_and_closes_only_the_oversized_link() -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let mut other = connect_other(&fixture).await?;
        check_input_sizes(&mut link.socket, &mut other).await
    }
    .await;
    fixture.close().await?;
    observed
}

pub(super) const OTHER_MACHINE: &str = "019f0000-0000-7000-8000-00000000cee5";

pub(super) async fn connect_other(fixture: &Fixture) -> Result<WebSocket> {
    let token = signed::enroll_another_machine(
        &fixture.client,
        &fixture.host.signin_origin(),
        ORGANIZATION,
        OTHER_MACHINE,
        "019f0000-0000-7000-8000-00000000cee6",
        signed::NOW_SECONDS,
    )
    .await?;
    let mut socket = fixture
        .client
        .get(fixture.link_url(ORGANIZATION))
        .bearer_auth(token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    assert!(matches!(receive_frame(&mut socket).await?,
        LinkFrame::Data(DataFrame::Welcome { machine_id, .. }) if machine_id.as_str() == OTHER_MACHINE));
    Ok(socket)
}

use super::MACHINE_ID;
use collaboration_central_conformance::machine_link_boundary::{
    WelcomeInterleaving, assert_replacement_backlog, assert_welcome, offered_message,
    queue_direct_mail, register_mail_session,
};

#[tokio::test]
async fn worker_claims_direct_mail_committed_before_welcome() -> Result<()> {
    observe_welcome_mail(WelcomeInterleaving::BeforeWrite).await
}

#[tokio::test]
async fn worker_claims_direct_mail_committed_after_welcome_before_backlog() -> Result<()> {
    observe_welcome_mail(WelcomeInterleaving::AfterWrite).await
}

async fn connect_primary(fixture: &Fixture) -> Result<WebSocket> {
    Ok(fixture
        .client
        .get(fixture.link_url(ORGANIZATION))
        .bearer_auth(&fixture.token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?)
}

async fn observe_welcome_mail(order: WelcomeInterleaving) -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let recipient = workspace_ids::SessionRef {
            machine_id: MACHINE_ID.parse()?,
            session_id: "welcome-claim-recipient".to_owned().try_into()?,
        };
        let sender = workspace_ids::SessionRef {
            machine_id: OTHER_MACHINE.parse()?,
            session_id: "welcome-claim-sender".to_owned().try_into()?,
        };
        let mut registered = connect_primary(&fixture).await?;
        assert_welcome(&mut registered).await?;
        register_mail_session(&mut registered, &recipient, 0xc300).await?;
        registered
            .close(reqwest_websocket::CloseCode::Normal, None)
            .await?;
        let mut source = connect_other(&fixture).await?;
        register_mail_session(&mut source, &sender, 0xc301).await?;
        let runtime = fixture.runtime();
        let point = match order {
            WelcomeInterleaving::BeforeWrite => "beforeMachineLinkWelcome",
            WelcomeInterleaving::AfterWrite => "afterMachineLinkWelcome",
        };
        assert!(runtime.control::<bool>("hold-central", &point).await?);
        let now =
            chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture clock")?;
        let (target, message) = futures_util::join!(connect_primary(&fixture), async {
            assert!(
                runtime
                    .control::<bool>("until-held-central", &point)
                    .await?
            );
            let result = queue_direct_mail(&mut source, &sender, &recipient, 0xc302, now).await;
            assert!(runtime.control::<bool>("release-central", &point).await?);
            result
        });
        let mut target = target?;
        let message = message?;
        assert_welcome(&mut target).await?;
        assert_eq!(offered_message(&mut target).await?, message);
        if matches!(order, WelcomeInterleaving::AfterWrite) {
            fixture.host.c2_evict(&fixture.organization).await?;
            assert_eq!(
                queue_direct_mail(&mut source, &sender, &recipient, 0xc302, now).await?,
                message
            );
        }
        let sentinel = queue_direct_mail(&mut source, &sender, &recipient, 0xc303, now).await?;
        assert_eq!(
            offered_message(&mut target).await?,
            sentinel,
            "a previously offered message was written again on the same socket"
        );
        let mut replacement = connect_primary(&fixture).await?;
        assert_replacement_backlog(&mut replacement, message, sentinel).await
    }
    .await;
    fixture.close().await?;
    observed
}

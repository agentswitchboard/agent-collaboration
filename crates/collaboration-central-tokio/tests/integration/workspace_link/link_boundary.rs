use std::time::Duration;

use super::{
    Result,
    fixture::{receive, start_fixture},
    typed_call_holder::connect_other_machine,
};
use collaboration_central_conformance::machine_link_boundary::{
    check_expired_heartbeat, check_input_sizes, expect_pong,
};

#[tokio::test]
async fn text_heartbeat_closes_an_expired_native_link() -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = async {
        let mut socket = fixture.connect().await?;
        receive(&mut socket).await?;
        expect_pong(&mut socket).await?;
        fixture.clock.advance(Duration::from_secs(660))?;
        check_expired_heartbeat(&mut socket).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn native_input_accepts_one_mib_and_closes_only_the_oversized_link() -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = async {
        let mut socket = fixture.connect().await?;
        receive(&mut socket).await?;
        let mut other = connect_other_machine(&fixture).await?;
        check_input_sizes(&mut socket, &mut other).await
    }
    .await;
    fixture.close().await?;
    observed
}

use super::fixture::{INSTALLATION, OTHER_MACHINE};
use collaboration_central_conformance::{
    machine_link_boundary::{
        WelcomeInterleaving, assert_replacement_backlog, assert_welcome, offered_message,
        queue_direct_mail, register_mail_session,
    },
    signin_fixtures,
};
use fleet_runtime::PausePoint;

#[tokio::test]
async fn native_claims_direct_mail_committed_before_welcome() -> Result<()> {
    observe_welcome_mail(WelcomeInterleaving::BeforeWrite).await
}

#[tokio::test]
async fn native_claims_direct_mail_committed_after_welcome_before_backlog() -> Result<()> {
    observe_welcome_mail(WelcomeInterleaving::AfterWrite).await
}

async fn observe_welcome_mail(order: WelcomeInterleaving) -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = async {
        let recipient = workspace_ids::SessionRef {
            machine_id: INSTALLATION.parse()?,
            session_id: "welcome-claim-recipient".to_owned().try_into()?,
        };
        let sender = workspace_ids::SessionRef {
            machine_id: OTHER_MACHINE.parse()?,
            session_id: "welcome-claim-sender".to_owned().try_into()?,
        };
        let mut registered = fixture.connect().await?;
        assert_welcome(&mut registered).await?;
        register_mail_session(&mut registered, &recipient, 0xc300).await?;
        registered
            .close(reqwest_websocket::CloseCode::Normal, None)
            .await?;
        let mut source = connect_other_machine(&fixture).await?;
        register_mail_session(&mut source, &sender, 0xc301).await?;
        let central = fixture.host.central(&fixture.organization_id).await?;
        let mut hold = central.runtime().pause_points().hold_central(match order {
            WelcomeInterleaving::BeforeWrite => PausePoint::BeforeMachineLinkWelcome,
            WelcomeInterleaving::AfterWrite => PausePoint::AfterMachineLinkWelcome,
        });
        let now = chrono::DateTime::from_timestamp(signin_fixtures::NOW_SECONDS, 0)
            .ok_or("fixture clock")?;
        let (target, message) = futures_util::join!(fixture.connect(), async {
            hold.until_held().await;
            let result = queue_direct_mail(&mut source, &sender, &recipient, 0xc302, now).await;
            hold.release();
            result
        });
        let mut target = target?;
        let message = message?;
        assert_welcome(&mut target).await?;
        assert_eq!(offered_message(&mut target).await?, message);
        let sentinel = queue_direct_mail(&mut source, &sender, &recipient, 0xc303, now).await?;
        assert_eq!(
            offered_message(&mut target).await?,
            sentinel,
            "a previously offered message was written again on the same socket"
        );
        let mut replacement = fixture.connect().await?;
        assert_replacement_backlog(&mut replacement, message, sentinel).await
    }
    .await;
    fixture.close().await?;
    observed
}

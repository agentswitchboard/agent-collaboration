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
        check_input_sizes(&mut socket, &mut other, INSTALLATION).await
    }
    .await;
    fixture.close().await?;
    observed
}

use super::fixture::{INSTALLATION, OTHER_MACHINE};
use collaboration_central_conformance::{
    machine_link_boundary::{
        WelcomeInterleaving, assert_empty_welcome, assert_replacement_backlog, assert_welcome,
        offered_message, queue_direct_mail, read_pong_checkpoint, register_mail_session,
        send_pong_checkpoint,
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
        assert_empty_welcome(&mut registered).await?;
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
        send_pong_checkpoint(&mut target).await?;
        assert_welcome(&mut target).await?;
        assert_eq!(offered_message(&mut target).await?, message);
        read_pong_checkpoint(&mut target).await?;
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

#[expect(
    clippy::panic_in_result_fn,
    reason = "The socket assertions name the failed observation."
)]
#[tokio::test]
async fn native_fragmented_text_checks_the_reassembled_byte_limit() -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{
        Message,
        client::IntoClientRequest,
        protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        },
    };

    let fixture = start_fixture().await?;
    let observed = async {
        let mut request = fixture
            .url(fixture.organization_id.as_str())
            .replace("http://", "ws://")
            .into_client_request()?;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", fixture.token).parse()?,
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
        let first = socket.next().await.ok_or("expected Welcome")??;
        assert!(first.to_text()?.starts_with("{\"kind\":\"welcome\""));
        let (_, mut text) =
            collaboration_central_conformance::machine_link_boundary::text_at_input_limit(
                INSTALLATION,
            )?;
        text.push(' ');
        let split = text.len() / 2;
        let trailing = text.split_off(split);
        assert_eq!(text.len() + trailing.len(), 1_048_577);
        assert!(text.len() < 1_048_576 && trailing.len() < 1_048_576);
        socket
            .send(Message::Frame(Frame::message(
                text,
                OpCode::Data(Data::Text),
                false,
            )))
            .await?;
        socket
            .send(Message::Frame(Frame::message(
                trailing,
                OpCode::Data(Data::Continue),
                true,
            )))
            .await?;
        let close = socket
            .next()
            .await
            .ok_or("expected fragmented-message close 1009")??;
        assert!(matches!(close, Message::Close(Some(frame)) if u16::from(frame.code) == 1009));
        Ok(())
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn native_awaits_an_authority_close_held_during_its_first_poll() -> Result<()> {
    use collaboration_central_conformance::machine_link_boundary::expect_close;
    use futures_util::{SinkExt, StreamExt};
    let fixture = start_fixture().await?;
    let observed = async {
        let mut socket = fixture.connect().await?;
        assert_welcome(&mut socket).await?;
        let central = fixture.host.central(&fixture.organization_id).await?;
        let mut hold = central.runtime().pause_points().hold_central(PausePoint::BeforeMachineAuthorityCloseWrite);
        fixture.clock.advance(Duration::from_secs(660))?;
        socket.send(reqwest_websocket::Message::Text("ping".into())).await?;
        tokio::select! {
            () = hold.until_held() => {}
            early = socket.next() => return Err(format!("expected close to be pending, got {early:?}").into()),
        }
        hold.release();
        expect_close(&mut socket, 4102).await
    }.await;
    fixture.close().await?;
    observed
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "The socket assertions name the failed observation."
)]
#[tokio::test]
async fn native_live_mail_recovers_a_transient_read_and_reconnects_after_an_ambiguous_write()
-> Result<()> {
    let fixture = start_fixture().await?;
    let observed = async {
        let (mut target, mut source, recipient, sender) = open_mail_sockets(&fixture).await?;
        let runtime = fixture.host.central(&fixture.organization_id).await?;
        let now = chrono::DateTime::from_timestamp(signin_fixtures::NOW_SECONDS, 0)
            .ok_or("fixture time")?;
        let mut completed = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::AfterMachineMailOffer);
        runtime.fail_next_mail_offer(true);
        let first = queue_direct_mail(&mut source, &sender, &recipient, 0xcc01, now).await?;
        completed.until_held().await;
        send_pong_checkpoint(&mut target).await?;
        completed.release();
        assert_eq!(offered_message(&mut target).await?, first);
        read_pong_checkpoint(&mut target).await?;
        runtime.fail_next_mail_offer(false);
        let unwritten = queue_direct_mail(&mut source, &sender, &recipient, 0xcc02, now).await?;
        collaboration_central_conformance::machine_link_boundary::expect_close(&mut target, 1011)
            .await?;
        let mut replacement = fixture.connect().await?;
        assert_replacement_backlog(&mut replacement, first, unwritten).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn native_busy_socket_refills_claims_across_welcome_pages() -> Result<()> {
    use collaboration_central_conformance::machine_link_boundary::{
        drain_bounded_backlog, queue_backlog,
    };
    let fixture = start_fixture().await?;
    let observed = async {
        let (target, mut source, recipient, sender) = open_mail_sockets(&fixture).await?;
        target
            .close(reqwest_websocket::CloseCode::Normal, None)
            .await?;
        let now = chrono::DateTime::from_timestamp(signin_fixtures::NOW_SECONDS, 0)
            .ok_or("fixture time")?;
        let queued = queue_backlog(&mut source, &sender, &recipient, now).await?;
        let mut target = fixture.connect().await?;
        drain_bounded_backlog(
            &mut target,
            queued,
            now,
            async || Ok(()),
            async |_, _| Ok(()),
        )
        .await
    }
    .await;
    fixture.close().await?;
    observed
}

async fn open_mail_sockets(
    fixture: &super::fixture::Fixture,
) -> Result<(
    reqwest_websocket::WebSocket,
    reqwest_websocket::WebSocket,
    workspace_ids::SessionRef,
    workspace_ids::SessionRef,
)> {
    let recipient = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "offer-recipient".parse()?,
    };
    let sender = workspace_ids::SessionRef {
        machine_id: OTHER_MACHINE.parse()?,
        session_id: "offer-sender".parse()?,
    };
    let mut target = fixture.connect().await?;
    assert_empty_welcome(&mut target).await?;
    register_mail_session(&mut target, &recipient, 0xcc11).await?;
    let mut source = connect_other_machine(fixture).await?;
    register_mail_session(&mut source, &sender, 0xcc12).await?;
    Ok((target, source, recipient, sender))
}

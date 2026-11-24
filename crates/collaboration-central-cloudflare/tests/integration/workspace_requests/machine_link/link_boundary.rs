use super::{ConnectedLink, Fixture, ORGANIZATION, Result, signed};
use central_link_protocol::{DataFrame, LinkFrame};
use collaboration_central_conformance::{
    approval_link_scenarios::receive_frame,
    machine_link_boundary::{check_input_sizes, expect_close, expect_pong},
};
use reqwest_websocket::{RequestBuilderExt, WebSocket};

#[tokio::test]
async fn worker_automatically_answers_ping_until_the_next_handled_frame_closes_expired_link()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        expect_pong(&mut link.socket).await?;
        super::renewal::advance(&fixture, 660).await?;
        expect_pong(&mut link.socket).await?;
        let checkpoint = arm_handled_frame(&fixture, "capabilities").await?;
        collaboration_central_conformance::approval_link_scenarios::announce_mac_calls(
            &mut link.socket,
            vec![],
        )
        .await?;
        expect_close_after_handled_frame(&fixture, &mut link.socket, checkpoint, 4102).await
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
        check_input_sizes(&mut link.socket, &mut other, MACHINE_ID).await
    }
    .await;
    fixture.close().await?;
    observed
}

pub(super) const OTHER_MACHINE: &str = "019f0000-0000-7000-8000-00000000cee5";

pub(super) async fn connect_other(fixture: &Fixture) -> Result<WebSocket> {
    connect_other_at(fixture, signed::NOW_SECONDS).await
}

async fn connect_other_at(fixture: &Fixture, now_seconds: i64) -> Result<WebSocket> {
    let token = signed::enroll_another_machine(
        &fixture.client,
        &fixture.host.signin_origin(),
        ORGANIZATION,
        OTHER_MACHINE,
        "019f0000-0000-7000-8000-00000000cee6",
        now_seconds,
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
    WelcomeInterleaving, assert_empty_welcome, assert_replacement_backlog, assert_welcome,
    offered_message, queue_direct_mail, read_pong_checkpoint, register_mail_session,
    send_pong_checkpoint,
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
        assert_empty_welcome(&mut registered).await?;
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
        send_pong_checkpoint(&mut target).await?;
        assert_welcome(&mut target).await?;
        assert_eq!(offered_message(&mut target).await?, message);
        read_pong_checkpoint(&mut target).await?;
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

#[expect(
    clippy::panic_in_result_fn,
    reason = "The socket assertions name the failed observation."
)]
#[tokio::test]
async fn worker_live_mail_recovers_a_transient_read_and_reconnects_after_an_ambiguous_write()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let (mut target, mut source, recipient, sender) = open_mail_sockets(&fixture).await?;
        let now = chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?;
        assert!(
            fixture
                .runtime()
                .control::<bool>("fail-mail-offer", &true)
                .await?
        );
        let read_checkpoint = arm_mail_frame(&fixture, 0xcc01).await?;
        let first = queue_direct_mail(&mut source, &sender, &recipient, 0xcc01, now).await?;
        wait_handled_frame(&fixture, read_checkpoint).await?;
        send_pong_checkpoint(&mut target).await?;
        assert_eq!(offered_message(&mut target).await?, first);
        read_pong_checkpoint(&mut target).await?;
        let checkpoint = arm_mail_frame(&fixture, 0xcc02).await?;
        assert!(
            fixture
                .runtime()
                .control::<bool>("fail-mail-offer", &false)
                .await?
        );
        let unwritten = queue_direct_mail(&mut source, &sender, &recipient, 0xcc02, now).await?;
        expect_close_after_handled_frame(&fixture, &mut target, checkpoint, 1011).await?;
        let mut replacement = connect_primary(&fixture).await?;
        assert_replacement_backlog(&mut replacement, first, unwritten).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn worker_busy_socket_refills_claims_across_welcome_pages() -> Result<()> {
    use collaboration_central_conformance::machine_link_boundary::{
        drain_bounded_backlog, queue_backlog,
    };
    let fixture = Fixture::new().await?;
    let observed = async {
        let (target, mut source, recipient, sender) = open_mail_sockets(&fixture).await?;
        target
            .close(reqwest_websocket::CloseCode::Normal, None)
            .await?;
        let now = chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?;
        let queued = queue_backlog(&mut source, &sender, &recipient, now).await?;
        let mut target = connect_primary(&fixture).await?;
        use collaboration_central_conformance::machine_link_boundary::ReceiptCheckpoint;
        let armed = std::cell::Cell::new(None);
        drain_bounded_backlog(
            &mut target,
            queued,
            now,
            async || {
                fixture.host.c2_evict(&fixture.organization).await?;
                Ok(())
            },
            async |correlation, phase| {
                match phase {
                    ReceiptCheckpoint::Arm => armed.set(Some(
                        arm_handled_frame(&fixture, &String::from(correlation.clone())).await?,
                    )),
                    ReceiptCheckpoint::Wait => {
                        wait_handled_frame(
                            &fixture,
                            armed.take().ok_or("receipt checkpoint was not armed")?,
                        )
                        .await?
                    }
                }
                Ok(())
            },
        )
        .await
    }
    .await;
    fixture.close().await?;
    observed
}

async fn open_mail_sockets(
    fixture: &Fixture,
) -> Result<(
    WebSocket,
    WebSocket,
    workspace_ids::SessionRef,
    workspace_ids::SessionRef,
)> {
    let recipient = workspace_ids::SessionRef {
        machine_id: MACHINE_ID.parse()?,
        session_id: "offer-recipient".parse()?,
    };
    let sender = workspace_ids::SessionRef {
        machine_id: OTHER_MACHINE.parse()?,
        session_id: "offer-sender".parse()?,
    };
    let mut target = connect_primary(fixture).await?;
    assert_empty_welcome(&mut target).await?;
    register_mail_session(&mut target, &recipient, 0xcc11).await?;
    let mut source = connect_other(fixture).await?;
    register_mail_session(&mut source, &sender, 0xcc12).await?;
    Ok((target, source, recipient, sender))
}

async fn arm_mail_frame(fixture: &Fixture, key: u32) -> Result<u64> {
    let correlation =
        collaboration_central_conformance::machine_link_boundary::mail_fixture_correlation(key)?;
    arm_handled_frame(fixture, &String::from(correlation)).await
}

async fn arm_handled_frame(fixture: &Fixture, key: &str) -> Result<u64> {
    Ok(fixture
        .runtime()
        .control(
            "hold-engine",
            &serde_json::json!({
                "point": "afterMachineFrameHandled",
                "key": key,
            }),
        )
        .await?)
}

async fn wait_handled_frame(fixture: &Fixture, checkpoint: u64) -> Result<()> {
    let runtime = fixture.runtime();
    if !runtime
        .control::<bool>("until-held-engine", &checkpoint)
        .await?
        || !runtime
            .control::<bool>("release-engine", &checkpoint)
            .await?
    {
        return Err("fixture did not pass its handled-frame checkpoint".into());
    }
    Ok(())
}

async fn expect_close_after_handled_frame(
    fixture: &Fixture,
    socket: &mut WebSocket,
    checkpoint: u64,
    code: u16,
) -> Result<()> {
    wait_handled_frame(fixture, checkpoint).await?;
    // The handler finished its outbound attempt. If the socket was left open,
    // the platform replies with Pong and the missing close fails by assertion.
    let _ = send_pong_checkpoint(socket).await;
    expect_close(socket, code).await
}

#[tokio::test]
async fn worker_closes_an_expired_attachment_before_an_outbound_mail_offer() -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let recipient = workspace_ids::SessionRef {
            machine_id: MACHINE_ID.parse()?,
            session_id: "expired-offer-recipient".parse()?,
        };
        let sender = workspace_ids::SessionRef {
            machine_id: OTHER_MACHINE.parse()?,
            session_id: "outbound-authority-sender".parse()?,
        };
        let mut target = connect_primary(&fixture).await?;
        assert_empty_welcome(&mut target).await?;
        register_mail_session(&mut target, &recipient, 0xcc21).await?;
        super::renewal::advance(&fixture, 120).await?;
        let mut source = connect_other_at(&fixture, signed::NOW_SECONDS + 120).await?;
        register_mail_session(&mut source, &sender, 0xcc22).await?;
        super::renewal::advance(&fixture, 540).await?;
        let now =
            chrono::DateTime::from_timestamp(signed::NOW_SECONDS + 660, 0).ok_or("fixture time")?;
        expect_pong(&mut target).await?;
        let checkpoint = arm_mail_frame(&fixture, 0xcc23).await?;
        queue_direct_mail(&mut source, &sender, &recipient, 0xcc23, now).await?;
        expect_close_after_handled_frame(&fixture, &mut target, checkpoint, 4102).await?;
        expect_pong(&mut source).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_expiry_alarm_refills_deferred_mail_on_a_heartbeat_only_socket() -> Result<()> {
    use collaboration_central_conformance::machine_link_boundary::{
        fill_early_mail_claims, renew_mail_test_link,
    };
    let fixture = Fixture::new().await?;
    let observed = async {
        let (mut target, mut source, recipient, sender) = open_mail_sockets(&fixture).await?;
        let early =
            chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?;
        fill_early_mail_claims(&mut target, &mut source, &sender, &recipient, early).await?;
        super::renewal::advance(&fixture, 500).await?;
        let checkpoint = arm_mail_frame(&fixture, 0x33100).await?;
        let late = queue_direct_mail(
            &mut source,
            &sender,
            &recipient,
            0x33100,
            early + chrono::TimeDelta::seconds(500),
        )
        .await?;
        wait_handled_frame(&fixture, checkpoint).await?;
        super::renewal::advance(&fixture, 50).await?;
        renew_mail_test_link(&mut target, super::renewal::fresh(&fixture, 550, 1150)?).await?;
        expect_pong(&mut target).await?;
        super::renewal::advance(&fixture, 550).await?;
        if fixture.runtime().dispatch_due_alarm().await?.is_none() {
            return Err("the Workerd direct-message expiry alarm did not fire".into());
        }
        if offered_message(&mut target).await? != late {
            return Err(
                "recipient missed the deferred message released by the Workerd alarm".into(),
            );
        }
        expect_pong(&mut target).await
    }
    .await;
    fixture.close().await?;
    observed
}

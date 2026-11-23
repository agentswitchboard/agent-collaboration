//! Observe deadlines and authority close paths while the application clock is held.

use super::super::{fixture, renewal};
use super::{
    ConnectedLink, Fixture, MACHINE_ID, Result, assert_mac_failure, assert_probe_answer,
    central_probe, invoke_mac, mac_probe, reply_to_mac_probe, workerd_control_http_client,
};
use central_link_protocol::{
    MachineLinkCloseCode, TypedCallResult, fixture_central_probe::encode_probe_call,
    test_calls::ProbeRequest,
};
use fleet_contracts::MachineTokenDecision;
use fleet_rpc::Envelope;
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use reqwest_websocket::Message;

#[tokio::test]
async fn the_invocation_deadline_cancels_waiting_authority_before_any_write() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise_held_authority(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise_held_authority(fixture: &Fixture) -> Result<()> {
    let mut link = ConnectedLink::open(fixture).await?;
    let runtime = fixture.runtime();
    let point = "afterSignInSample";
    assert!(runtime.control::<bool>("hold-central", &point).await?);
    let client = workerd_control_http_client();
    let grant = fixture::grant(&fixture.host, &client, &fixture.enrollment);
    let expire = async {
        assert!(
            runtime
                .control::<bool>("until-held-central", &point)
                .await?
        );
        let invoke = invoke_mac(fixture, 31);
        let expire_clock = async {
            assert!(
                runtime
                    .control::<bool>("until-admission-waiting", &1_usize)
                    .await?
            );
            renewal::advance(fixture, 5).await
        };
        let (answer, ()) = tokio::try_join!(invoke, expire_clock)?;
        let result = assert_mac_failure(answer, StatusCode::CONFLICT, "neverReached").await;
        assert!(runtime.control::<bool>("release-central", &point).await?);
        result
    };
    let (grant, ()) = tokio::try_join!(grant, expire)?;
    assert!(matches!(grant, MachineTokenDecision::Grant { .. }));
    assert!(matches!(
        central_probe(&mut link.socket, &MACHINE_ID.parse()?, 32).await?,
        TypedCallResult::Answered { .. }
    ));
    let (_, answer) = tokio::try_join!(
        async {
            let correlation = mac_probe(&mut link.socket, 33).await?;
            reply_to_mac_probe(&mut link.socket, correlation, 33).await
        },
        invoke_mac(fixture, 33),
    )?;
    assert_probe_answer(answer, 33).await?;
    link.close().await
}

#[tokio::test]
async fn a_failed_typed_refusal_still_closes_authority_and_settles_the_outbound_waiter()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise_faulted_close(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise_faulted_close(fixture: &Fixture) -> Result<()> {
    let mut link = ConnectedLink::open(fixture).await?;
    renewal::advance(fixture, 599).await?;
    let drive = async {
        let correlation = mac_probe(&mut link.socket, 34).await?;
        let runtime = fixture.runtime();
        assert!(
            runtime
                .control::<bool>("typed-mac-waiting", &correlation)
                .await?
        );
        assert!(
            runtime
                .control::<bool>("fail-typed-refusal-send", &true)
                .await?
        );
        renewal::advance(fixture, 1).await?;
        let text = encode_probe_call(
            "019f0000-0000-7000-8000-00000000d711".parse()?,
            Envelope {
                target: MACHINE_ID.parse()?,
                body: ProbeRequest { sequence: 35 },
            },
        )?;
        link.socket.send(Message::Text(text)).await?;
        let close = link.socket.next().await;
        assert!(
            matches!(close, Some(Ok(Message::Close { code, .. })) if
            MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Expired)),
            "{close:?}"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (_, answer) = tokio::try_join!(drive, invoke_mac(fixture, 34))?;
    // The clock advanced to the authority deadline, four seconds short of the call timeout.
    assert_mac_failure(answer, StatusCode::SERVICE_UNAVAILABLE, "answerLost").await
}

use super::link_support::{caller, create_workspace};
use super::typed_calls::{
    answer_mac_call, connect, expect_call_failure, expect_probe_reply, probe_request,
    socket_barrier, start_mac_call,
};
use super::{Fixture, INSTALLATION, receive, send, start_fixture};
use central_link_protocol::{DataFrame, LinkFrame, LinkRequest, test_calls::MacProbe};
use collaboration_central_conformance::{
    ConformanceResult as Result, machine_link_boundary::expect_transport_pong,
};
use collaboration_central_core::TYPED_LINK_CALL_TIMEOUT;
use fleet_rpc::CallFailure;
use fleet_runtime::PausePoint;
use futures_util::FutureExt;
use workspace_ids::MachineId;

#[tokio::test]
async fn mac_typed_calls_expired_or_cancelled_before_write_emit_no_frame() -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = verify_before_write_expiry_and_cancel(&fixture).await;
    fixture.close().await?;
    observed
}

async fn verify_before_write_expiry_and_cancel(fixture: &Fixture) -> Result<()> {
    let mut socket = connect(fixture).await?;
    socket_barrier(&mut socket).await?;
    let machine: MachineId = INSTALLATION.parse()?;
    let central = fixture.host.central(&fixture.organization_id).await?;
    let execution = central.runtime();
    let turn = execution.take_central_turn().await;
    let mut expired = std::pin::pin!(fixture.host.call_machine::<MacProbe>(
        &fixture.organization_id,
        &machine,
        probe_request(machine.clone(), 1),
    ));
    tokio::select! {
        result = &mut expired => return Err(format!("queued call settled before the authority wait: {result:?}").into()),
        () = execution.until_admission_waiting(1) => {},
    }
    fixture.clock.advance(TYPED_LINK_CALL_TIMEOUT)?;
    expect_call_failure(expired.await, CallFailure::NeverReached)?;
    expect_transport_pong(&mut socket).await?;

    let mut cancelled = Box::pin(fixture.host.call_machine::<MacProbe>(
        &fixture.organization_id,
        &machine,
        probe_request(machine.clone(), 2),
    ));
    tokio::select! {
        result = &mut cancelled => return Err(format!("queued call settled before cancellation: {result:?}").into()),
        () = execution.until_admission_waiting(1) => {},
    }
    drop(cancelled);
    expect_transport_pong(&mut socket).await?;
    drop(turn);

    let retry = start_mac_call(fixture, &mut socket, 3).await?;
    answer_mac_call(&mut socket, &retry.correlation, INSTALLATION, 3).await?;
    expect_probe_reply(retry, 3).await
}

#[tokio::test]
async fn mac_typed_call_replacement_before_write_cannot_reach_either_socket() -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = verify_queued_replacement(&fixture).await;
    fixture.close().await?;
    observed
}

async fn verify_queued_replacement(fixture: &Fixture) -> Result<()> {
    let central = fixture.host.central(&fixture.organization_id).await?;
    let mut original = connect(fixture).await?;
    socket_barrier(&mut original).await?;
    let mut hold = central
        .runtime()
        .pause_points()
        .hold_central(PausePoint::BeforeWorkspaceRequestApplying);
    let request = LinkRequest::new(
        "019f0000-0000-7000-8000-00000000cd18".parse()?,
        caller()?,
        create_workspace(
            "019f0000-0000-7000-8000-00000000cd19",
            "Hold original link before writing",
        )?,
    )?;
    send(
        &mut original,
        &LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    tokio::select! {
        () = hold.until_held() => {},
        frame = receive(&mut original) => return Err(format!("original link answered before its hold: {:?}", frame?).into()),
    }
    let machine: MachineId = INSTALLATION.parse()?;
    let mut queued = std::pin::pin!(fixture.host.call_machine::<MacProbe>(
        &fixture.organization_id,
        &machine,
        probe_request(machine.clone(), 1),
    ));
    if let Some(result) = queued.as_mut().now_or_never() {
        return Err(format!("queued call settled before replacement: {result:?}").into());
    }
    let mut replacement = connect(fixture).await?;
    let result = queued.await;
    hold.release();
    expect_call_failure(result, CallFailure::NeverReached)?;
    socket_barrier(&mut replacement).await?;
    let current = start_mac_call(fixture, &mut replacement, 2).await?;
    answer_mac_call(&mut replacement, &current.correlation, INSTALLATION, 2).await?;
    expect_probe_reply(current, 2).await
}

#[tokio::test]
async fn mac_typed_call_cancelled_after_write_ignores_the_late_answer() -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = async {
        let mut socket = connect(&fixture).await?;
        let written = start_mac_call(&fixture, &mut socket, 1).await?;
        written.result.abort();
        if !matches!(written.result.await, Err(error) if error.is_cancelled()) {
            return Err("written caller did not cancel".into());
        }
        let current = start_mac_call(&fixture, &mut socket, 2).await?;
        answer_mac_call(&mut socket, &written.correlation, INSTALLATION, 99).await?;
        socket_barrier(&mut socket).await?;
        answer_mac_call(&mut socket, &current.correlation, INSTALLATION, 2).await?;
        expect_probe_reply(current, 2).await
    }
    .await;
    fixture.close().await?;
    observed
}

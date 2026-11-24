use central_link_protocol::{FrameDirection, LinkFrame, decode_text, encode_text};
use collaboration_central_conformance::fixture_link_reader::read_fixture_frame;
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use collaboration_central_tokio::test_support::start_machine_link_fixture;
pub(super) use collaboration_central_tokio::test_support::{
    MACHINE_LINK_FIXTURE_MACHINE as INSTALLATION, MachineLinkFixture as Fixture,
};
use futures_util::SinkExt;
use reqwest_websocket::{Message, WebSocket};
use router_clock::ManualClock;
use runtime_primitives::SyncTime;
use std::sync::Arc;

pub(super) const OTHER_MACHINE: &str = "019f0000-0000-7000-8000-00000000ce02";
pub(super) const OTHER_ORG: &str = "019f0000-0000-7000-8000-00000000ce03";

pub(super) async fn start_fixture() -> Result<Fixture> {
    start_machine_link_fixture(signed_clock()?).await
}

pub(super) fn signed_clock() -> Result<Arc<ManualClock>> {
    Ok(Arc::new(ManualClock::starting_at(
        SyncTime::from_unix_millis(signed::NOW_SECONDS * 1000),
    )?))
}

pub(super) async fn receive(socket: &mut WebSocket) -> Result<LinkFrame> {
    let message = read_fixture_frame(socket, "fixture link frame")
        .await?
        .ok_or("link ended before a frame")??;
    let Message::Text(text) = message else {
        return Err("expected a link text frame".into());
    };
    Ok(decode_text(&text, FrameDirection::ToRouter)?)
}

pub(super) async fn send(socket: &mut WebSocket, frame: &LinkFrame) -> Result<()> {
    socket
        .send(Message::Text(encode_text(
            frame,
            FrameDirection::ToCentral,
        )?))
        .await?;
    Ok(())
}

//! Encode and read typed frames on the fixture's real approval socket.

use crate::{
    ConformanceResult as Result,
    fixture_link_reader::{ExpectedFrameMissing, read_fixture_frame},
};
use central_link_protocol::{FrameDirection, LinkFrame, decode_text, encode_text};
use futures_util::{
    SinkExt,
    future::{Either, select},
};
use reqwest_websocket::{Message, WebSocket};
use std::{future::Future, pin::pin};

pub async fn receive_frame(socket: &mut WebSocket) -> Result<LinkFrame> {
    receive_expected_text(socket, "application frame").await
}

pub(crate) async fn receive_expected_text(
    socket: &mut WebSocket,
    expected: &'static str,
) -> Result<LinkFrame> {
    let message = read_fixture_frame(socket, expected)
        .await?
        .ok_or(ExpectedFrameMissing(expected))??;
    let Message::Text(text) = message else {
        return Err(ExpectedFrameMissing(expected).into());
    };
    Ok(decode_text(&text, FrameDirection::ToRouter)?)
}

/// The fixture supplies a checkpoint that proves the expected writer made
/// no frame available. Independent store reads cannot advance that checkpoint.
pub(crate) async fn receive_frame_before(
    socket: &mut WebSocket,
    expected: &'static str,
    unavailable: impl Future<Output = Result<()>>,
) -> Result<LinkFrame> {
    match select(
        pin!(receive_expected_text(socket, expected)),
        pin!(unavailable),
    )
    .await
    {
        Either::Left((frame, _)) => frame,
        Either::Right((checkpoint, _)) => {
            checkpoint?;
            Err(ExpectedFrameMissing(expected).into())
        }
    }
}

pub(crate) async fn send_frame(socket: &mut WebSocket, frame: LinkFrame) -> Result<()> {
    socket
        .send(Message::Text(encode_text(
            &frame,
            FrameDirection::ToCentral,
        )?))
        .await?;
    Ok(())
}

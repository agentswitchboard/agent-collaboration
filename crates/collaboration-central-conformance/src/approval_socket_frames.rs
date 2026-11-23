//! Encode and read typed frames on the fixture's real approval socket.

use crate::ConformanceResult as Result;
use central_link_protocol::{FrameDirection, LinkFrame, decode_text, encode_text};
use futures_util::{
    SinkExt, StreamExt,
    future::{Either, select},
};
use reqwest_websocket::{Message, WebSocket};
use std::{future::Future, pin::pin};

pub async fn receive_frame(socket: &mut WebSocket) -> Result<LinkFrame> {
    let Message::Text(text) = socket.next().await.ok_or("approval socket ended")?? else {
        return Err("expected application text on approval socket".into());
    };
    Ok(decode_text(&text, FrameDirection::ToRouter)?)
}

#[derive(Debug)]
struct ApprovalFrameBudgetExhausted;

impl std::fmt::Display for ApprovalFrameBudgetExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "expected approval socket frame before the fixture exhausted its observation budget",
        )
    }
}

impl std::error::Error for ApprovalFrameBudgetExhausted {}

pub(crate) async fn receive_frame_before(
    socket: &mut WebSocket,
    budget: impl Future<Output = Result<()>>,
) -> Result<LinkFrame> {
    match select(pin!(receive_frame(socket)), pin!(budget)).await {
        Either::Left((frame, _)) => frame,
        Either::Right((limit, _)) => {
            limit?;
            Err(ApprovalFrameBudgetExhausted.into())
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

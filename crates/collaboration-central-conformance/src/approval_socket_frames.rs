//! Encode and read typed frames on the fixture's real approval socket.

use crate::ConformanceResult as Result;
use central_link_protocol::{FrameDirection, LinkFrame, decode_text, encode_text};
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{Message, WebSocket};

pub async fn receive_frame(socket: &mut WebSocket) -> Result<LinkFrame> {
    let Message::Text(text) = socket.next().await.ok_or("approval socket ended")?? else {
        return Err("expected application text on approval socket".into());
    };
    Ok(decode_text(&text, FrameDirection::ToRouter)?)
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

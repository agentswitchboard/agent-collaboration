use central_link_protocol::{DataFrame, FrameDirection, LinkFrame};
use worker::{Result, WebSocket};

pub(super) fn send(socket: &WebSocket, frame: DataFrame) -> Result<()> {
    let text =
        central_link_protocol::encode_text(&LinkFrame::Data(frame), FrameDirection::ToRouter)
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
    socket.send_with_str(text)
}

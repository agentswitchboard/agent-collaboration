use central_link_protocol::{
    CentralLinkRpc, DataFrame, FrameDirection, LINK_MAX_TEXT_BYTES, LinkFrame, PersonLinkCloseCode,
    decode_text, encode_text, person_link_rpc::ListOrganizationWorkspacesRpc,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, header};
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};

use crate::{
    FleetTarget, PersonLinkError,
    connection_machine::{ConnectionEvent, ReadAttempt},
    fleet_credential::UpgradeCredential,
};

pub(crate) async fn upgrade(
    http: &Client,
    target: &FleetTarget,
    credential: UpgradeCredential,
) -> Result<WebSocket, PersonLinkError> {
    let config = tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(LINK_MAX_TEXT_BYTES))
        .max_message_size(Some(LINK_MAX_TEXT_BYTES));
    let response = http
        .get(target.person_link_url())
        .header(header::AUTHORIZATION, credential.authorization)
        .header("dpop", credential.proof)
        .upgrade()
        .web_socket_config(config)
        .send()
        .await
        .map_err(|_| PersonLinkError::Upgrade)?;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Err(PersonLinkError::UpgradeRefused(response.status()));
    }
    response
        .into_websocket()
        .await
        .map_err(|_| PersonLinkError::Upgrade)
}

pub(crate) async fn send_read(
    socket: &mut WebSocket,
    attempt: ReadAttempt,
) -> Result<(), PersonLinkError> {
    let text = encode_text(
        &LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: attempt.correlation,
            request: Box::new(ListOrganizationWorkspacesRpc::request(attempt.read)),
        }),
        FrameDirection::ToCentral,
    )
    .map_err(|_| PersonLinkError::Protocol)?;
    socket
        .send(Message::Text(text))
        .await
        .map_err(|_| PersonLinkError::Socket)
}

pub(crate) async fn receive_event(
    socket: &mut WebSocket,
) -> Result<ConnectionEvent, PersonLinkError> {
    loop {
        let message = socket
            .next()
            .await
            .ok_or(PersonLinkError::Socket)?
            .map_err(|_| PersonLinkError::Socket)?;
        match message {
            Message::Text(text) => {
                let frame = decode_text(&text, FrameDirection::ToRouter)
                    .map_err(|_| PersonLinkError::Protocol)?;
                let LinkFrame::Data(DataFrame::CentralCallReply {
                    correlation_id,
                    result,
                }) = frame
                else {
                    return Err(PersonLinkError::Protocol);
                };
                return Ok(ConnectionEvent::Reply {
                    correlation: correlation_id,
                    result,
                });
            }
            Message::Close { code, .. } => {
                let code = u16::from(code);
                if code == PersonLinkCloseCode::Reconnect.code() {
                    return Ok(ConnectionEvent::LifetimeClosed);
                }
                return Err(PersonLinkError::Closed(code));
            }
            Message::Ping(_) => socket.flush().await.map_err(|_| PersonLinkError::Socket)?,
            Message::Pong(_) => {}
            Message::Binary(_) => return Err(PersonLinkError::Protocol),
        }
    }
}

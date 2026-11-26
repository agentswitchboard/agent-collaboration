use central_link_protocol::{
    DataFrame, FrameDirection, LINK_MAX_TEXT_BYTES, LinkFrame, PersonLinkCloseCode, decode_text,
    encode_text,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, header};
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
#[cfg(test)]
use std::collections::VecDeque;

use crate::{
    FleetTarget, PersonLinkError,
    connection_machine::{CallAttempt, ConnectionEvent},
    fleet_credential::UpgradeCredential,
};

#[cfg(test)]
type PeerReplies = Box<dyn FnMut(&str) -> VecDeque<String> + Send>;
#[cfg(not(test))]
type LiveSocket = WebSocket;
#[cfg(test)]
type LiveSocket = Box<WebSocket>;

pub(crate) enum PersonSocket {
    Live(LiveSocket),
    #[cfg(test)]
    FailedWrite {
        on_write: PeerReplies,
        replies: VecDeque<String>,
    },
}

impl PersonSocket {
    fn live(socket: WebSocket) -> Self {
        #[cfg(test)]
        let socket = Box::new(socket);
        Self::Live(socket)
    }
}

pub(crate) async fn upgrade(
    http: &Client,
    target: &FleetTarget,
    credential: UpgradeCredential,
) -> Result<PersonSocket, PersonLinkError> {
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
        .map(PersonSocket::live)
        .map_err(|_| PersonLinkError::Upgrade)
}

pub(crate) fn encode_call(attempt: CallAttempt) -> Result<String, PersonLinkError> {
    encode_text(
        &LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: attempt.correlation,
            request: Box::new(attempt.call.body.into_request(attempt.call.target)),
        }),
        FrameDirection::ToCentral,
    )
    .map_err(|_| PersonLinkError::CallEncoding)
}

pub(crate) async fn send_call(
    socket: &mut PersonSocket,
    text: String,
) -> Result<(), PersonLinkError> {
    match socket {
        PersonSocket::Live(socket) => socket
            .send(Message::Text(text))
            .await
            .map_err(|_| PersonLinkError::Socket),
        #[cfg(test)]
        PersonSocket::FailedWrite { on_write, replies } => {
            *replies = on_write(&text);
            Err(PersonLinkError::Socket)
        }
    }
}

pub(crate) async fn receive_event(
    socket: &mut PersonSocket,
) -> Result<ConnectionEvent, PersonLinkError> {
    match socket {
        PersonSocket::Live(socket) => receive_live_event(socket).await,
        #[cfg(test)]
        PersonSocket::FailedWrite { replies, .. } => {
            let text = replies.pop_front().ok_or(PersonLinkError::Closed(1011))?;
            decode_reply(&text)
        }
    }
}

async fn receive_live_event(socket: &mut WebSocket) -> Result<ConnectionEvent, PersonLinkError> {
    loop {
        let message = socket
            .next()
            .await
            .ok_or(PersonLinkError::Socket)?
            .map_err(|_| PersonLinkError::Socket)?;
        match message {
            Message::Text(text) => return decode_reply(&text),
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

fn decode_reply(text: &str) -> Result<ConnectionEvent, PersonLinkError> {
    let frame =
        decode_text(text, FrameDirection::ToRouter).map_err(|_| PersonLinkError::Protocol)?;
    let LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    }) = frame
    else {
        return Err(PersonLinkError::Protocol);
    };
    Ok(ConnectionEvent::Reply {
        correlation: correlation_id,
        result,
    })
}

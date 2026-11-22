use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::oneshot;

#[derive(Default)]
pub(super) struct IssuerReplies {
    next: Mutex<VecDeque<PendingReply>>,
}

struct PendingReply {
    arrived: oneshot::Sender<()>,
    answer: oneshot::Receiver<IssuerReply>,
}

pub struct HeldIssuerFetch {
    arrived: oneshot::Receiver<()>,
    answer: oneshot::Sender<IssuerReply>,
}

pub enum IssuerReply {
    Document(Value),
    CacheableDocument {
        document: Value,
        max_age_seconds: u64,
    },
    Unavailable,
}

impl IssuerReplies {
    pub(super) fn hold_next(&self) -> HeldIssuerFetch {
        let (arrived, arrival) = oneshot::channel();
        let (answer, response) = oneshot::channel();
        self.next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(PendingReply {
                arrived,
                answer: response,
            });
        HeldIssuerFetch {
            arrived: arrival,
            answer,
        }
    }

    pub(super) async fn next_response(&self) -> Option<Response> {
        let pending = self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()?;
        let _ = pending.arrived.send(());
        Some(match pending.answer.await {
            Ok(IssuerReply::Document(body)) => Json(body).into_response(),
            Ok(IssuerReply::CacheableDocument {
                document,
                max_age_seconds,
            }) => (
                [(
                    axum::http::header::CACHE_CONTROL,
                    format!("public, max-age={max_age_seconds}"),
                )],
                Json(document),
            )
                .into_response(),
            Ok(IssuerReply::Unavailable) | Err(_) => {
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            }
        })
    }
}

impl HeldIssuerFetch {
    pub async fn until_requested(&mut self) -> Result<(), oneshot::error::RecvError> {
        (&mut self.arrived).await
    }

    pub fn send_issuer_reply(self, reply: IssuerReply) -> crate::ConformanceResult<()> {
        self.answer
            .send(reply)
            .map_err(|_| "issuer fetch no longer awaiting its reply".into())
    }
}

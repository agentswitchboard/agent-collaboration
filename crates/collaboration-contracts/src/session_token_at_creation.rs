//! The token-issuance boundary of a newly created Router session.

use std::{future::Future, pin::Pin};

use collaboration_protocol::SessionRef;

pub type SessionTokenIssuanceFuture<'session> =
    Pin<Box<dyn Future<Output = SessionTokenIssuance> + Send + 'session>>;

/// A creation outcome may report issuance without exposing credential material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTokenIssuance {
    Issued,
    NotIssued(SessionTokenNotIssued),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTokenNotIssued {
    DeferredInV0,
    Refused,
    Unavailable,
}

/// Requests issuance for the newly assigned, endpoint-scoped session identity.
pub trait SessionTokenAtCreation: Send + Sync {
    fn request<'session>(
        &'session self,
        session: &'session SessionRef,
    ) -> SessionTokenIssuanceFuture<'session>;
}

/// The v0 stub. The owner ruled, “When creating a session it should call
/// central to get a token. But we can have a stub.”
pub struct V0SessionTokenIssuer;

impl SessionTokenAtCreation for V0SessionTokenIssuer {
    fn request<'session>(
        &'session self,
        _session: &'session SessionRef,
    ) -> SessionTokenIssuanceFuture<'session> {
        Box::pin(std::future::ready(SessionTokenIssuance::NotIssued(
            SessionTokenNotIssued::DeferredInV0,
        )))
    }
}

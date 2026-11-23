//! The shared idempotency comparison for a session create.

use serde::Serialize;
use sha2::{Digest as _, Sha256};
use workspace_ids::CentralSessionId;

use crate::{
    CreateFleetSession, CreateSessionDigest, CreateSessionKind, HarnessKind, SessionCreator,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateDigestContents<'a> {
    creator_kind: &'static str,
    creator_actor_id: Option<&'a CentralSessionId>,
    kind: &'static str,
    agent_program: &'a HarnessKind,
    parent_actor_id: Option<&'a CentralSessionId>,
}

impl CreateFleetSession {
    /// Canonical JSON of the stable contents shared by all attempts at this start.
    pub fn canonical_contents(&self) -> Result<Vec<u8>, CanonicalCreateContentsError> {
        let (creator_kind, creator_actor_id) = match &self.creator {
            SessionCreator::Session { session_id, .. } => ("session", Some(session_id)),
            SessionCreator::RouterOperator {} => ("routerOperator", None),
        };
        let (kind, parent_actor_id) = match &self.session_kind {
            CreateSessionKind::Ordinary {} => ("ordinary", None),
            CreateSessionKind::Delegate { parent } => ("delegate", Some(parent)),
        };
        serde_json_canonicalizer::to_vec(&CreateDigestContents {
            creator_kind,
            creator_actor_id,
            kind,
            agent_program: &self.agent_program,
            parent_actor_id,
        })
        .map_err(|_| CanonicalCreateContentsError)
    }

    pub fn contents_digest(&self) -> Result<CreateSessionDigest, CanonicalCreateContentsError> {
        Ok(CreateSessionDigest::from_sha256(
            &Sha256::digest(self.canonical_contents()?).into(),
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("could not canonicalize create session contents")]
pub struct CanonicalCreateContentsError;

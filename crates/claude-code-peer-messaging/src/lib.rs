//! Claude Code's owner-local session registry and peer socket transport.
mod claude_code_peer_socket;
mod claude_code_session_registry;
mod native_claude_job_reader;
mod peer_session_census;
mod process_ancestry;
mod registry_process_start;
pub use native_claude_job_reader::{
    NativeClaudeBackgroundJob, NativeClaudeJobInventory, NativeClaudeJobReadError,
    NativeClaudeJobReader,
};
pub use peer_session_census::{
    ManagedClaudeClaim, ManagedClaudeLaunches, PeerCensusAnswer, PeerCensusOutcome,
    PeerSessionCensus,
};
pub use registry_process_start::ProcessIncarnation;

pub use claude_code_peer_socket::{
    ClaudeCodePeerSocket, PeerSocketUnsentCause, PeerSocketWriteOutcome,
};
pub use claude_code_session_registry::{
    ClaudeCodeSessionRegistry, PeerClaim, PeerRegistryError, PeerSessionInventory,
    PeerSessionLookup, PeerSessionRecord, PeerSessionStatus, PeerSessionSummary,
};

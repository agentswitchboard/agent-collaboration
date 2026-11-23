//! Closed causes shared by the durable snapshots and the runtime answers.

use workspace_ids::{CentralSessionId, SessionRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupCall {
    Create,
    Bind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldLostDuring {
    SetUp,
    Bind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindDispute {
    WrongSecret,
    AlreadyBoundToDifferentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailureReason {
    CouldNotSave,
    CreatorNotAllowed,
    Conflict,
    ActiveWithoutLocalProof {
        session_id: CentralSessionId,
        address: SessionRef,
    },
    NoSecret,
    CentralFault {
        call: CleanupCall,
    },
    ReservationRanOut,
    NotLaunched,
    SecretNotStored,
    SecretStoreNotKnown,
    LaunchFailed,
    DuplicateHarnessId,
    SetUpFailed,
    HoldLost {
        during: HoldLostDuring,
    },
    AddressTaken,
    AssociationDisputed(BindDispute),
    NoSuchStart,
    OpeningNotPrepared,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownReason {
    AttemptCeiling,
    Launch,
    SetUp,
    Opening,
    Contradicted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    RecordNotCommitted,
    RecordConflict,
    RecordNotKnown,
}

//! The three session lifecycle calls on the authenticated link.

use crate::{CreateSessionAttempt, CreateSessionDigest, HarnessKind, SessionSecret, SessionTime};
use workspace_ids::{CentralSessionId, SessionId, SessionOperationId, SessionRef};

named_wire_record! {
    pub struct CreateFleetSession as "CreateFleetSession"
    [rename_all = "camelCase", deny_unknown_fields] {
        pub operation_id: SessionOperationId,
        pub attempt: CreateSessionAttempt,
        pub creator: SessionCreator,
        pub agent_program: HarnessKind,
        pub session_kind: CreateSessionKind,
    }
}

named_wire_record! {
    pub enum SessionCreator as "SessionCreator"
    [tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields] {
        Session {
            session_id: CentralSessionId,
            presented: PresentedSessionSecret,
        },
        RouterOperator {},
    }
}

named_wire_record! {
    pub enum PresentedSessionSecret as "PresentedSessionSecret"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        Secret { secret: SessionSecret },
        Nothing {},
    }
}

named_wire_record! {
    pub enum CreateSessionKind as "CreateSessionKind"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        Ordinary {},
        Delegate { parent: CentralSessionId },
    }
}

named_wire_record! {
    pub enum CreateFleetSessionReply as "CreateFleetSessionReply"
    [tag = "result", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields] {
        Reserved {
            session_id: CentralSessionId,
            secret: SessionSecret,
            reserved_until: SessionTime,
        },
        Superseded {},
        Active {
            session_id: CentralSessionId,
            address: SessionRef,
        },
        Settled { settlement: SessionSettlement },
        Refused { reason: CreateSessionRefusal },
        Conflict {},
        NoSecret {},
        NoSuchSession {},
        SessionEnded {},
        WrongSecret {},
    }
}

named_wire_record! {
    pub enum CreateSessionRefusal as "CreateSessionRefusal"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        CreatorNotAllowed {},
        ParentNotValid {},
    }
}

named_wire_record! {
    pub struct BindFleetSession as "BindFleetSession"
    [rename_all = "camelCase", deny_unknown_fields] {
        pub session_id: CentralSessionId,
        pub harness_session_id: SessionId,
        pub secret: SessionSecret,
    }
}

named_wire_record! {
    pub enum BindFleetSessionReply as "BindFleetSessionReply"
    [tag = "result", rename_all = "camelCase", deny_unknown_fields] {
        Active {
            address: SessionRef,
        },
        WrongSecret {},
        AlreadyBoundToDifferentId {},
        AddressTaken {},
        Settled { settlement: SessionSettlement },
        NoSuchStart {},
    }
}

named_wire_record! {
    pub struct EndFleetSession as "EndFleetSession"
    [rename_all = "camelCase", deny_unknown_fields] {
        pub target: EndSessionTarget,
        pub cause: RouterEndCause,
    }
}

named_wire_record! {
    pub enum EndSessionTarget as "EndSessionTarget"
    [tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields] {
        CentralId { session_id: CentralSessionId },
        OperationId {
            operation_id: SessionOperationId,
            contents_digest: CreateSessionDigest,
        },
    }
}

named_wire_record! {
    pub enum RouterEndCause as "RouterEndCause"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        EndedForGood {},
        GivenUp {},
    }
}

named_wire_record! {
    pub enum EndFleetSessionReply as "EndFleetSessionReply"
    [tag = "result", rename_all = "camelCase", deny_unknown_fields] {
        Settled { settlement: SessionSettlement },
        AlreadySettled { settlement: SessionSettlement },
        NoSuchStart {},
        NotYoursToEnd {},
    }
}

named_wire_record! {
    pub enum SessionSettlement as "SessionSettlement"
    [tag = "state", rename_all = "camelCase", deny_unknown_fields] {
        Abandoned {
            cause: AbandonedSessionCause,
            time: SessionTime,
        },
        Ended {
            cause: EndedSessionCause,
            time: SessionTime,
        },
    }
}

named_wire_record! {
    pub enum AbandonedSessionCause as "AbandonedSessionCause"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        GivenUp {},
        Revoked {},
        MachineRevoked {},
        TimeRanOut {},
    }
}

named_wire_record! {
    pub enum EndedSessionCause as "EndedSessionCause"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        EndedForGood {},
        GivenUp {},
        Revoked {},
        MachineRevoked {},
    }
}

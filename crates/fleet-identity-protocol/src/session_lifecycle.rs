//! The three session lifecycle calls on the authenticated link.

use crate::{CreateSessionAttempt, CreateSessionDigest, HarnessKind, SessionSecret, SessionTime};
use message_board::ProjectId;
use serde::{Deserialize, Deserializer, de::value::StrDeserializer};
use workspace_ids::{CentralSessionId, SessionId, SessionOperationId, SessionRef, WorkspaceId};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, schemars::JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateFleetSession {
    pub operation_id: SessionOperationId,
    pub attempt: CreateSessionAttempt,
    pub creator: SessionCreator,
    pub agent_program: HarnessKind,
    pub session_kind: CreateSessionKind,
    pub project: StartProject,
}

impl<'de> Deserialize<'de> for CreateFleetSession {
    fn deserialize<Decoder: Deserializer<'de>>(decoder: Decoder) -> Result<Self, Decoder::Error> {
        #[derive(Deserialize)]
        #[serde(
            remote = "CreateFleetSession",
            rename_all = "camelCase",
            deny_unknown_fields
        )]
        struct CreateFields {
            operation_id: SessionOperationId,
            attempt: CreateSessionAttempt,
            creator: SessionCreator,
            #[serde(deserialize_with = "deserialize_agent_program")]
            agent_program: HarnessKind,
            session_kind: CreateSessionKind,
            project: StartProject,
        }
        CreateFields::deserialize(crate::named_wire_record::NamedMap(decoder))
    }
}

fn deserialize_agent_program<'de, Decoder: Deserializer<'de>>(
    decoder: Decoder,
) -> Result<HarnessKind, Decoder::Error> {
    let spelling = String::deserialize(decoder)?;
    HarnessKind::deserialize(StrDeserializer::new(&spelling))
}

named_wire_record! {
    pub struct ProjectRef as "ProjectRef"
    [deny_unknown_fields] {
        pub workspace: WorkspaceId,
        pub project: ProjectId,
    }
}

named_wire_record! {
    pub enum StartProject as "StartProject"
    [tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields] {
        Named { project_ref: ProjectRef },
        SameAsParent {},
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
            workspace: WorkspaceId,
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
        ProjectRequired {},
        ProjectNotInWorkspace {},
        MachineNotAllowed {},
        HelperOutsideWorkspace {},
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
        Settled { settlement: NewEndSettlement },
        AlreadySettled { settlement: SessionSettlement },
        NoSuchStart {},
        NotYoursToEnd {},
    }
}

named_wire_record! {
    /// The settlement made by the End being answered.
    pub enum NewEndSettlement as "NewEndSettlement"
    [tag = "state", rename_all = "camelCase", deny_unknown_fields] {
        Abandoned { cause: NewAbandonmentCause, time: SessionTime },
        Ended { cause: RouterEndCause, time: SessionTime },
    }
}

named_wire_record! {
    pub enum NewAbandonmentCause as "NewAbandonmentCause"
    [tag = "kind", rename_all = "camelCase", deny_unknown_fields] {
        GivenUp {},
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

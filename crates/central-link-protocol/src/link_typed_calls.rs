//! Registered RPC calls carried by an authenticated machine connection.
use crate::{
    CorrelationId, FrameDirection, MachineLinkCloseCode,
    approval_rpc::{
        ApprovalNoticeRpc, DecideApprovalAsSessionRpc, DeliverEndingRpc,
        ListApprovalsForSessionRpc, PassOnApprovalForSessionRpc, RaiseApprovalRpc,
        ReadApprovalForSessionRpc, RegisterRunRpc, StopApprovalRpc,
    },
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};
use fleet_rpc::{Envelope, Rpc};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};
use std::{fmt, marker::PhantomData};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LinkCallRefusal {
    UnknownCall,
    WrongTarget,
    /// The call's cause is advisory. The link close and the next refresh carry enrollment authority.
    AuthorityLapsed {
        cause: MachineLinkCloseCode,
    },
    /// Central rolled back or declined to apply the call. The link remains open.
    CentralFault,
}

impl LinkCallRefusal {
    /// Only a lapsed link authority ends the physical socket after its refusal.
    #[must_use]
    pub const fn authority_close(self) -> Option<MachineLinkCloseCode> {
        match self {
            Self::AuthorityLapsed { cause } => Some(cause),
            Self::UnknownCall | Self::WrongTarget | Self::CentralFault => None,
        }
    }
}

impl<'de> Deserialize<'de> for LinkCallRefusal {
    fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
        enum NamedRefusal {
            UnknownCall {},
            WrongTarget {},
            AuthorityLapsed { cause: MachineLinkCloseCode },
            CentralFault {},
        }
        Ok(match named_map(input)? {
            NamedRefusal::UnknownCall {} => Self::UnknownCall,
            NamedRefusal::WrongTarget {} => Self::WrongTarget,
            NamedRefusal::CentralFault {} => Self::CentralFault,
            NamedRefusal::AuthorityLapsed { cause } => Self::AuthorityLapsed { cause },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[serde(bound(deserialize = "Reply: Deserialize<'de>"))]
pub enum TypedCallResult<Reply> {
    Answered {
        #[serde(deserialize_with = "named_map")]
        reply: Reply,
    },
    Refused {
        #[serde(deserialize_with = "named_map")]
        reason: LinkCallRefusal,
    },
}

mod registration {
    use super::{CentralCall, CentralCallReply, Envelope, MacCall, MacCallReply, Rpc};

    pub trait Binding<Marker: Rpc>: Sized {
        type ReplyFrame;

        fn wrap_request(envelope: Envelope<Marker::Target, Marker::Request>) -> Self;
        fn wrap_reply(envelope: Envelope<Marker::Target, Marker::Reply>) -> Self::ReplyFrame;
        fn take_reply(reply: Self::ReplyFrame) -> Option<Envelope<Marker::Target, Marker::Reply>>;
    }

    pub trait Central {}
    impl<Marker: Rpc> Central for Marker where
        CentralCall: Binding<Marker, ReplyFrame = CentralCallReply>
    {
    }

    pub trait Mac {}
    impl<Marker: Rpc> Mac for Marker where MacCall: Binding<Marker, ReplyFrame = MacCallReply> {}
}

/// A marker explicitly registered for calls from a Mac to Central.
///
/// Implementing the generic RPC trait alone does not allow a link call.
/// ```compile_fail,E0277
/// use central_link_protocol::CentralLinkRpc;
/// use fleet_rpc::{Envelope, Rpc};
/// struct Unlisted;
/// impl Rpc for Unlisted {
///     type Target = u32;
///     type Request = u32;
///     type Reply = u32;
/// }
/// let _ = <Unlisted as CentralLinkRpc>::request(Envelope { target: 1, body: 2 });
/// ```
pub trait CentralLinkRpc: Rpc + registration::Central {
    fn request(envelope: Envelope<Self::Target, Self::Request>) -> CentralCall;
    fn reply(envelope: Envelope<Self::Target, Self::Reply>) -> CentralCallReply;
    fn take_reply(reply: CentralCallReply) -> Option<Envelope<Self::Target, Self::Reply>>;
}

/// A marker explicitly registered for calls from Central to a Mac.
pub trait MacLinkRpc: Rpc + registration::Mac {
    fn request(envelope: Envelope<Self::Target, Self::Request>) -> MacCall;
    fn reply(envelope: Envelope<Self::Target, Self::Reply>) -> MacCallReply;
    fn take_reply(reply: MacCallReply) -> Option<Envelope<Self::Target, Self::Reply>>;
}

macro_rules! register_calls {
    ($request:ident, $request_name:literal, $request_reader:ident, $reply:ident, $reply_name:literal, $reply_reader:ident, $protocol:ident; $(
        $(#[$condition:meta])* $name:literal => $variant:ident($marker:path)
    ),* $(,)?) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
        #[serde(tag = "call", content = "envelope", deny_unknown_fields)]
        pub enum $request {
            $(
                $(#[$condition])*
                #[serde(rename = $name)]
                $variant(Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Request>),
            )*
        }

        #[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
        #[serde(tag = "call", content = "envelope", deny_unknown_fields)]
        pub enum $reply {
            $(
                $(#[$condition])*
                #[serde(rename = $name)]
                $variant(Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Reply>),
            )*
        }

        #[derive(Deserialize)]
        #[serde(remote = $request_name, tag = "call", content = "envelope", deny_unknown_fields)]
        enum $request_reader {
            $(
                $(#[$condition])*
                #[serde(rename = $name)]
                $variant(Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Request>),
            )*
        }

        #[derive(Deserialize)]
        #[serde(remote = $reply_name, tag = "call", content = "envelope", deny_unknown_fields)]
        enum $reply_reader {
            $(
                $(#[$condition])*
                #[serde(rename = $name)]
                $variant(Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Reply>),
            )*
        }

        impl<'de> Deserialize<'de> for $request {
            fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
                struct NamedCall;
                impl<'de> Visitor<'de> for NamedCall {
                    type Value = $request;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("named call fields") }
                    fn visit_map<Fields: MapAccess<'de>>(self, fields: Fields) -> Result<Self::Value, Fields::Error> {
                        $request_reader::deserialize(MapAccessDeserializer::new(fields))
                    }
                }
                input.deserialize_map(NamedCall)
            }
        }

        impl<'de> Deserialize<'de> for $reply {
            fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
                struct NamedReply;
                impl<'de> Visitor<'de> for NamedReply {
                    type Value = $reply;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("named reply fields") }
                    fn visit_map<Fields: MapAccess<'de>>(self, fields: Fields) -> Result<Self::Value, Fields::Error> {
                        $reply_reader::deserialize(MapAccessDeserializer::new(fields))
                    }
                }
                input.deserialize_map(NamedReply)
            }
        }

        impl $request {
            fn recognizes(name: &str) -> bool {
                const NAMES: &[&str] = &[$($(#[$condition])* $name,)*];
                NAMES.contains(&name)
            }
        }

        impl<Marker: Rpc> $protocol for Marker
        where
            $request: registration::Binding<Marker, ReplyFrame = $reply>,
        {
            fn request(envelope: Envelope<Marker::Target, Marker::Request>) -> $request {
                < $request as registration::Binding<Marker> >::wrap_request(envelope)
            }

            fn reply(envelope: Envelope<Marker::Target, Marker::Reply>) -> $reply {
                < $request as registration::Binding<Marker> >::wrap_reply(envelope)
            }

            fn take_reply(reply: $reply) -> Option<Envelope<Marker::Target, Marker::Reply>> {
                < $request as registration::Binding<Marker> >::take_reply(reply)
            }
        }

        $(
            $(#[$condition])*
            impl registration::Binding<$marker> for $request {
                type ReplyFrame = $reply;

                fn wrap_request(envelope: Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Request>) -> Self {
                    $request::$variant(envelope)
                }

                fn wrap_reply(envelope: Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Reply>) -> $reply {
                    $reply::$variant(envelope)
                }

                #[expect(clippy::allow_attributes, reason = "the fallback is generated for all registry sizes")]
                #[allow(
                    unreachable_patterns,
                    clippy::match_wildcard_for_single_variants,
                    reason = "the registry generates a shared projection for one or many reply variants"
                )]
                fn take_reply(reply: $reply) -> Option<Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Reply>> {
                    match reply {
                        $reply::$variant(envelope) => Some(envelope),
                        _ => None,
                    }
                }
            }
        )*
    };
}

register_calls! {
    CentralCall, "CentralCall", CentralCallReader, CentralCallReply, "CentralCallReply", CentralCallReplyReader, CentralLinkRpc;
    "registerRun" => RegisterRun(RegisterRunRpc),
    "raise" => Raise(RaiseApprovalRpc),
    "stop" => Stop(StopApprovalRpc),
    "listApprovalsForSession" => ListApprovalsForSession(ListApprovalsForSessionRpc),
    "readApprovalForSession" => ReadApprovalForSession(ReadApprovalForSessionRpc),
    "decideApprovalAsSession" => DecideApprovalAsSession(DecideApprovalAsSessionRpc),
    "passOnApproval" => PassOnApproval(PassOnApprovalForSessionRpc),
    "createFleetSession" => CreateFleetSession(CreateFleetSessionRpc),
    "bindFleetSession" => BindFleetSession(BindFleetSessionRpc),
    "endFleetSession" => EndFleetSession(EndFleetSessionRpc),
}

register_calls! {
    MacCall, "MacCall", MacCallReader, MacCallReply, "MacCallReply", MacCallReplyReader, MacLinkRpc;
    "deliverEnding" => DeliverEnding(DeliverEndingRpc),
    "approvalNotice" => ApprovalNotice(ApprovalNoticeRpc),
    #[cfg(any(test, feature = "test-support"))] "macProbe" => MacProbe(test_calls::MacProbe),
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_calls {
    use fleet_rpc::{IndependentReplyRpc, Rpc};
    use serde::{Deserialize, Deserializer, Serialize};
    use workspace_ids::MachineId;

    pub struct MacProbe;

    impl IndependentReplyRpc for MacProbe {}

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct ProbeRequest {
        pub sequence: u32,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct ProbeReply {
        pub echoed: u32,
    }

    impl<'de> Deserialize<'de> for ProbeRequest {
        fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct NamedRequest {
                sequence: u32,
            }
            let request: NamedRequest = super::named_map(input)?;
            Ok(Self {
                sequence: request.sequence,
            })
        }
    }

    impl<'de> Deserialize<'de> for ProbeReply {
        fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct NamedReply {
                echoed: u32,
            }
            let reply: NamedReply = super::named_map(input)?;
            Ok(Self {
                echoed: reply.echoed,
            })
        }
    }

    impl Rpc for MacProbe {
        type Target = MachineId;
        type Request = ProbeRequest;
        type Reply = ProbeReply;
    }
}

pub(crate) fn named_map<'de, Input: Deserializer<'de>, Value: Deserialize<'de>>(
    input: Input,
) -> Result<Value, Input::Error> {
    struct NamedMap<Value>(PhantomData<Value>);

    impl<'de, Value: Deserialize<'de>> Visitor<'de> for NamedMap<Value> {
        type Value = Value;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("named fields")
        }

        fn visit_map<Fields: MapAccess<'de>>(self, fields: Fields) -> Result<Value, Fields::Error> {
            Value::deserialize(MapAccessDeserializer::new(fields))
        }
    }

    input.deserialize_map(NamedMap(PhantomData))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceivedCallHeader {
    call: String,
    #[serde(rename = "envelope")]
    _envelope: Envelope<serde::de::IgnoredAny, serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum UnregisteredCallWire {
    CallCentral {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        request: ReceivedCallHeader,
    },
    CallMac {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        request: ReceivedCallHeader,
    },
}

/// Recover a correlation only from the exact, named call wrapper of an unknown call.
/// This never makes an unknown name available as a sendable frame.
pub(crate) fn unregistered_call(text: &str) -> Option<(FrameDirection, CorrelationId)> {
    let mut parser = serde_json::Deserializer::from_str(text);
    let call = named_map::<_, UnregisteredCallWire>(&mut parser).ok()?;
    parser.end().ok()?;
    match call {
        UnregisteredCallWire::CallCentral {
            correlation_id,
            request,
        } => (!CentralCall::recognizes(&request.call))
            .then_some((FrameDirection::ToCentral, correlation_id)),
        UnregisteredCallWire::CallMac {
            correlation_id,
            request,
        } => (!MacCall::recognizes(&request.call))
            .then_some((FrameDirection::ToRouter, correlation_id)),
    }
}

#[cfg(test)]
#[path = "link_typed_calls_test.rs"]
mod tests;

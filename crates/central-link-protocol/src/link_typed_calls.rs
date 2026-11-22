//! Registered RPC calls carried by an authenticated machine connection.
use crate::{
    CorrelationId, FrameDirection, MachineLinkCloseCode,
    approval_rpc::{DeliverEndingRpc, RaiseApprovalRpc, RegisterRunRpc, StopApprovalRpc},
};
use fleet_rpc::{Envelope, Rpc};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};
use std::{fmt, marker::PhantomData};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LinkCallRefusal {
    UnknownCall,
    WrongTarget,
    AuthorityLapsed { cause: MachineLinkCloseCode },
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
    pub trait Central {}
    pub trait Mac {}
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
    ($request:ident, $reply:ident, $protocol:ident, $seal:ident; $(
        $(#[$condition:meta])* $name:literal => $variant:ident($marker:path)
    ),* $(,)?) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
        #[serde(tag = "call", content = "envelope", deny_unknown_fields)]
        pub enum $request {
            $(
                $(#[$condition])*
                #[serde(rename = $name)]
                $variant(Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Request>),
            )*
        }

        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
        #[serde(tag = "call", content = "envelope", deny_unknown_fields)]
        pub enum $reply {
            $(
                $(#[$condition])*
                #[serde(rename = $name)]
                $variant(Envelope<<$marker as Rpc>::Target, <$marker as Rpc>::Reply>),
            )*
        }

        impl $request {
            fn recognizes(name: &str) -> bool {
                const NAMES: &[&str] = &[$($(#[$condition])* $name,)*];
                NAMES.contains(&name)
            }
        }

        $(
            $(#[$condition])*
            impl registration::$seal for $marker {}
            $(#[$condition])*
            impl $protocol for $marker {
                fn request(envelope: Envelope<Self::Target, Self::Request>) -> $request {
                    $request::$variant(envelope)
                }

                fn reply(envelope: Envelope<Self::Target, Self::Reply>) -> $reply {
                    $reply::$variant(envelope)
                }

                #[expect(clippy::allow_attributes, reason = "the fallback is generated for all registry sizes")]
                #[allow(
                    unreachable_patterns,
                    clippy::match_wildcard_for_single_variants,
                    reason = "the registry generates a shared projection for one or many reply variants"
                )]
                fn take_reply(reply: $reply) -> Option<Envelope<Self::Target, Self::Reply>> {
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
    CentralCall, CentralCallReply, CentralLinkRpc, Central;
    "registerRun" => RegisterRun(RegisterRunRpc),
    "raise" => Raise(RaiseApprovalRpc),
    "stop" => Stop(StopApprovalRpc),
    #[cfg(any(test, feature = "test-support"))] "centralProbe" => CentralProbe(test_calls::CentralProbe),
}

register_calls! {
    MacCall, MacCallReply, MacLinkRpc, Mac;
    "deliverEnding" => DeliverEnding(DeliverEndingRpc),
    #[cfg(any(test, feature = "test-support"))] "macProbe" => MacProbe(test_calls::MacProbe),
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_calls {
    use fleet_rpc::Rpc;
    use serde::{Deserialize, Serialize};
    use workspace_ids::MachineId;

    pub struct CentralProbe;
    pub struct MacProbe;

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct ProbeRequest {
        pub sequence: u32,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct ProbeReply {
        pub echoed: u32,
    }

    impl Rpc for CentralProbe {
        type Target = MachineId;
        type Request = ProbeRequest;
        type Reply = ProbeReply;
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

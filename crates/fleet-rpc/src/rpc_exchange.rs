use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, Deserializer, MapAccess, Visitor, value::MapAccessDeserializer},
};
use std::{fmt, future::Future, marker::PhantomData};

pub trait Rpc: Send + Sync {
    type Target: Serialize + DeserializeOwned + Clone + Eq + Send + Sync;
    type Request: Serialize + DeserializeOwned + Send;
    type Reply: Serialize + DeserializeOwned + Send;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Envelope<Target, Body> {
    pub target: Target,
    pub body: Body,
}

impl<'de, Target: Deserialize<'de>, Body: Deserialize<'de>> Deserialize<'de>
    for Envelope<Target, Body>
{
    fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
        struct NamedEnvelope<Target, Body>(PhantomData<(Target, Body)>);

        impl<'de, Target: Deserialize<'de>, Body: Deserialize<'de>> Visitor<'de>
            for NamedEnvelope<Target, Body>
        {
            type Value = Envelope<Target, Body>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an RPC envelope with named target and body fields")
            }

            fn visit_map<Fields: MapAccess<'de>>(
                self,
                fields: Fields,
            ) -> Result<Self::Value, Fields::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct EnvelopeFields<Target, Body> {
                    target: Target,
                    body: Body,
                }

                let envelope = EnvelopeFields::deserialize(MapAccessDeserializer::new(fields))?;
                Ok(Envelope {
                    target: envelope.target,
                    body: envelope.body,
                })
            }
        }

        input.deserialize_map(NamedEnvelope(PhantomData))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallFailure {
    /// The transport can establish that dispatch did not happen.
    NeverReached,
    /// Dispatch was attempted, but the caller does not have a valid reply.
    AnswerLost,
}

/// A typed call can fail at the service or while obtaining its reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallError<F> {
    Service(F),
    NeverReached,
    AnswerLost,
}

pub trait Transport<R: Rpc>: Send + Sync {
    fn exchange(
        &self,
        request: Envelope<R::Target, R::Request>,
    ) -> impl Future<Output = Result<Envelope<R::Target, R::Reply>, CallFailure>> + Send;
}

impl<R: Rpc, T: Transport<R> + ?Sized> Transport<R> for &T {
    fn exchange(
        &self,
        request: Envelope<R::Target, R::Request>,
    ) -> impl Future<Output = Result<Envelope<R::Target, R::Reply>, CallFailure>> + Send {
        (*self).exchange(request)
    }
}

pub struct RpcClient<R: Rpc, T: Transport<R>> {
    target: R::Target,
    transport: T,
    protocol: PhantomData<fn() -> R>,
}

impl<R: Rpc, T: Transport<R>> RpcClient<R, T> {
    pub fn new(target: R::Target, transport: T) -> Self {
        Self {
            target,
            transport,
            protocol: PhantomData,
        }
    }

    #[doc(hidden)]
    pub async fn exchange_message(&self, request: R::Request) -> Result<R::Reply, CallFailure> {
        let reply = self
            .transport
            .exchange(Envelope {
                target: self.target.clone(),
                body: request,
            })
            .await?;
        if reply.target != self.target {
            return Err(CallFailure::AnswerLost);
        }
        Ok(reply.body)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetMismatch;

/// Validate the owning object before the route opens storage or handles a call.
pub async fn dispatch<R, F, Fut>(
    expected_target: &R::Target,
    request: Envelope<R::Target, R::Request>,
    route: F,
) -> Result<Envelope<R::Target, R::Reply>, TargetMismatch>
where
    R: Rpc,
    F: FnOnce(R::Request) -> Fut,
    Fut: Future<Output = R::Reply>,
{
    if &request.target != expected_target {
        return Err(TargetMismatch);
    }
    Ok(Envelope {
        target: request.target,
        body: route(request.body).await,
    })
}

#[cfg(test)]
#[path = "rpc_exchange_test.rs"]
mod tests;

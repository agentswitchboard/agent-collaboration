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

/// An RPC whose replies need only the wire shape and addressed target checked.
pub trait IndependentReplyRpc: Rpc {}

/// An RPC whose legal replies depend on the originating request.
pub trait CorrelatedRpc: Rpc {
    type ReplyContext;
    type CheckedReply;
    type InvalidReply;

    fn reply_context(request: &Self::Request) -> Self::ReplyContext;
    fn check_reply(
        context: Self::ReplyContext,
        reply: Self::Reply,
    ) -> Result<Self::CheckedReply, Self::InvalidReply>;
    fn into_wire(checked: Self::CheckedReply) -> Self::Reply;
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

    async fn exchange_wire(&self, request: R::Request) -> Result<R::Reply, CallFailure> {
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

impl<R: IndependentReplyRpc, T: Transport<R>> RpcClient<R, T> {
    #[doc(hidden)]
    pub async fn exchange_message(&self, request: R::Request) -> Result<R::Reply, CallFailure> {
        self.exchange_wire(request).await
    }
}

impl<R: CorrelatedRpc, T: Transport<R>> RpcClient<R, T> {
    /// Exchange a request and return only a reply valid for that request.
    pub async fn exchange_correlated(
        &self,
        request: R::Request,
    ) -> Result<R::CheckedReply, CallFailure> {
        let context = R::reply_context(&request);
        let reply = self.exchange_wire(request).await?;
        R::check_reply(context, reply).map_err(|_| CallFailure::AnswerLost)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetMismatch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorrelatedDispatchError<InvalidReply> {
    TargetMismatch,
    InvalidReply(InvalidReply),
}

/// Validate the owning object before the route opens storage or handles a call.
pub async fn dispatch<R, F, Fut>(
    expected_target: &R::Target,
    request: Envelope<R::Target, R::Request>,
    route: F,
) -> Result<Envelope<R::Target, R::Reply>, TargetMismatch>
where
    R: IndependentReplyRpc,
    F: FnOnce(R::Request) -> Fut,
    Fut: Future<Output = R::Reply>,
{
    dispatch_wire::<R, _, _>(expected_target, request, route).await
}

async fn dispatch_wire<R, F, Fut>(
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

/// Validate the target before invoking the handler and the correlated reply before emitting it.
pub async fn dispatch_correlated<R, F, Fut>(
    expected_target: &R::Target,
    request: Envelope<R::Target, R::Request>,
    route: F,
) -> Result<Envelope<R::Target, R::Reply>, CorrelatedDispatchError<R::InvalidReply>>
where
    R: CorrelatedRpc,
    F: FnOnce(R::Request) -> Fut,
    Fut: Future<Output = R::Reply>,
{
    let context = R::reply_context(&request.body);
    let reply = dispatch_wire::<R, _, _>(expected_target, request, route)
        .await
        .map_err(|TargetMismatch| CorrelatedDispatchError::TargetMismatch)?;
    let checked =
        R::check_reply(context, reply.body).map_err(CorrelatedDispatchError::InvalidReply)?;
    Ok(Envelope {
        target: reply.target,
        body: R::into_wire(checked),
    })
}

#[cfg(test)]
#[path = "rpc_exchange_test.rs"]
mod tests;

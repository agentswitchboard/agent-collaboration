//! The shared HTTP and Unix MCP routes for a direct send and its status.
use collaboration_protocol::message_tools::{
    MESSAGE_SEND_DESCRIPTION, MESSAGE_STATUS_DESCRIPTION, MessageSendArguments, MessageSendOutcome,
    MessageStatusArguments, MessageToolRefusal, MessageToolResult, MessageWriteStatus, PostText,
    RequestId, SessionId, SessionRef, message_send_text, message_status_text,
};
use rmcp::{handler::server::tool::schema_for_type, model::ContentBlock, schemars};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{
    CallToolResult, CollaborationMcpServer, McpToolOutput, Parameters, tool, tool_router,
    validation_failure,
};

#[tool_router(router = message_tool_router, vis = "pub(super)")]
impl CollaborationMcpServer {
    #[tool(name = "message_send", description = MESSAGE_SEND_DESCRIPTION, output_schema = schema_for_type::<McpToolOutput<MessageToolResult<MessageSendOutcome>>>())]
    async fn message_send(
        &self,
        Parameters(input): Parameters<DecodedMessageInput<MessageSendArguments>>,
    ) -> CallToolResult {
        let result = async {
            let input = input.0?;
            let operations = self.application.messages();
            let caller = operations.admit_caller(input.caller_session_id).await?;
            operations
                .send(&caller, input.to, input.body, input.request_id)
                .await
        }
        .await
        .into();
        message_result(&result, message_send_text(&result))
    }

    #[tool(name = "message_status", description = MESSAGE_STATUS_DESCRIPTION, output_schema = schema_for_type::<McpToolOutput<MessageToolResult<MessageWriteStatus>>>())]
    async fn message_status(
        &self,
        Parameters(input): Parameters<DecodedMessageInput<MessageStatusArguments>>,
    ) -> CallToolResult {
        let result = async {
            let input = input.0?;
            let operations = self.application.messages();
            let caller = operations.admit_caller(input.caller_session_id).await?;
            operations.request_status(&caller, input.request_id).await
        }
        .await
        .into();
        message_result(&result, message_status_text(&result))
    }
}

fn message_result<T: Serialize>(value: &MessageToolResult<T>, text: String) -> CallToolResult {
    let Ok(structured) = serde_json::to_value(value) else {
        return validation_failure(&MessageToolRefusal::UnexpectedReply.to_string());
    };
    let mut result = if matches!(
        value,
        MessageToolResult::Refused(
            MessageToolRefusal::Front(_) | MessageToolRefusal::NoMessageLink
        )
    ) {
        CallToolResult::structured_error(structured)
    } else {
        CallToolResult::structured(structured)
    };
    result.content = vec![ContentBlock::text(text)];
    result
}

// Keep the advertised argument schema exact while returning a typed, useful error for older addresses.
struct DecodedMessageInput<Arguments>(Result<Arguments, MessageToolRefusal>);

impl<Arguments: schemars::JsonSchema> schemars::JsonSchema for DecodedMessageInput<Arguments> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        Arguments::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        Arguments::json_schema(generator)
    }
}

trait MessageArguments: DeserializeOwned {
    fn explain_invalid(value: &Value) -> MessageToolRefusal;
}

impl MessageArguments for MessageSendArguments {
    fn explain_invalid(value: &Value) -> MessageToolRefusal {
        if !value.get("to").is_some_and(valid::<SessionRef>) {
            return MessageToolRefusal::InvalidAddress;
        }
        if !value.get("body").is_some_and(valid::<PostText>) {
            return MessageToolRefusal::InvalidBody;
        }
        if value
            .get("requestId")
            .is_some_and(|id| !valid::<Option<RequestId>>(id))
        {
            return MessageToolRefusal::InvalidRequestId;
        }
        invalid_caller_or_arguments(value)
    }
}

impl MessageArguments for MessageStatusArguments {
    fn explain_invalid(value: &Value) -> MessageToolRefusal {
        if !value.get("requestId").is_some_and(valid::<RequestId>) {
            return MessageToolRefusal::InvalidRequestId;
        }
        invalid_caller_or_arguments(value)
    }
}

fn valid<T: DeserializeOwned>(value: &Value) -> bool {
    T::deserialize(value).is_ok()
}

fn invalid_caller_or_arguments(value: &Value) -> MessageToolRefusal {
    if value
        .get("callerSessionId")
        .is_some_and(|caller| !valid::<Option<SessionId>>(caller))
    {
        MessageToolRefusal::InvalidCaller
    } else {
        MessageToolRefusal::InvalidArguments
    }
}

impl<'de, Arguments: MessageArguments> Deserialize<'de> for DecodedMessageInput<Arguments> {
    fn deserialize<Decoder: Deserializer<'de>>(decoder: Decoder) -> Result<Self, Decoder::Error> {
        let value = Value::deserialize(decoder)?;
        Ok(Self(
            Arguments::deserialize(&value).map_err(|_| Arguments::explain_invalid(&value)),
        ))
    }
}

#[cfg(test)]
#[path = "message_tools_test.rs"]
mod tests;

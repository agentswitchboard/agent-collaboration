//! MCP transport for the Central-backed tools: decode the input, call the link family, and
//! convert its one result. Only a success is an MCP success; every other answer is a tool
//! error carrying the same typed output and text.
use collaboration_protocol::link_tool_result::{
    LinkRefusal, LinkToolArguments, LinkToolOutput, LinkToolResult, RemovedField,
};
use collaboration_protocol::workspace_tools::{THREAD_SHOW_DESCRIPTION, ThreadShowArguments};
use collaboration_service::collaboration_application::link_answers::link_tool_output;
use rmcp::{handler::server::tool::schema_for_type, model::ContentBlock, schemars};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::{
    CallToolResult, CollaborationMcpServer, McpToolOutput, Parameters, tool, tool_router,
    validation_failure,
};

#[tool_router(router = link_tool_router, vis = "pub(super)")]
impl CollaborationMcpServer {
    #[tool(name = "thread_show", description = THREAD_SHOW_DESCRIPTION, output_schema = schema_for_type::<McpToolOutput<LinkToolOutput>>())]
    async fn thread_show(
        &self,
        Parameters(input): Parameters<DecodedLinkInput<ThreadShowArguments>>,
    ) -> CallToolResult {
        let output = match input.0 {
            Ok(input) => self.application.link_tools().show_thread(input).await,
            Err(code) => link_tool_output(LinkToolResult::Refused { code }),
        };
        link_tool_call_result(&output)
    }
}

/// The one converter every Central-backed tool's answer goes through.
fn link_tool_call_result(output: &LinkToolOutput) -> CallToolResult {
    let Ok(structured) = serde_json::to_value(output) else {
        return validation_failure("Could not encode the tool's answer. Check the Router.");
    };
    let mut result = if output.result.succeeded() {
        CallToolResult::structured(structured)
    } else {
        CallToolResult::structured_error(structured)
    };
    result.content = vec![ContentBlock::text(output.text.clone())];
    result
}

/// Advertises the arguments' exact schema, and refuses an old input by name instead of as an
/// unknown field.
struct DecodedLinkInput<Arguments>(Result<Arguments, LinkRefusal>);

impl<Arguments: LinkToolArguments> schemars::JsonSchema for DecodedLinkInput<Arguments> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        Arguments::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        Arguments::json_schema(generator)
    }
}

impl<'de, Arguments: LinkToolArguments> Deserialize<'de> for DecodedLinkInput<Arguments> {
    fn deserialize<Decoder: Deserializer<'de>>(decoder: Decoder) -> Result<Self, Decoder::Error> {
        let value = Value::deserialize(decoder)?;
        Ok(Self(Arguments::deserialize(&value).map_err(|_| {
            removed_input::<Arguments>(&value).map_or(LinkRefusal::InvalidArguments, |field| {
                LinkRefusal::RemovedInput { field }
            })
        })))
    }
}

fn removed_input<Arguments: LinkToolArguments>(value: &Value) -> Option<RemovedField> {
    Arguments::REMOVED_INPUTS
        .iter()
        .find_map(|(name, field)| value.get(name).map(|_| *field))
}

#[cfg(test)]
#[path = "link_tools_test.rs"]
mod tests;

//! MCP transport for workspace reads.

use collaboration_protocol::workspace_tools::{
    RemovedThreadShowField, THREAD_SHOW_DESCRIPTION, ThreadShowArguments, ThreadShowOutput,
    ThreadShowRefusal, ThreadShowResult,
};
use rmcp::{handler::server::tool::schema_for_type, model::ContentBlock, schemars};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::{
    CallToolResult, CollaborationMcpServer, McpToolOutput, Parameters, tool, tool_router,
    validation_failure,
};

#[tool_router(router = workspace_tool_router, vis = "pub(super)")]
impl CollaborationMcpServer {
    #[tool(name = "thread_show", description = THREAD_SHOW_DESCRIPTION, output_schema = schema_for_type::<McpToolOutput<ThreadShowOutput>>())]
    async fn thread_show(
        &self,
        Parameters(input): Parameters<DecodedThreadShowInput>,
    ) -> CallToolResult {
        let output = match input.0 {
            Ok(input) => self.application.workspace().show_thread(input).await.into(),
            Err(refusal) => ThreadShowOutput::refused(refusal),
        };
        let Ok(structured) = serde_json::to_value(&output) else {
            return validation_failure("Could not encode the thread read. Reading again is safe.");
        };
        let mut result = match output.result {
            ThreadShowResult::Answered { .. } => CallToolResult::structured(structured),
            ThreadShowResult::ReadAnswerLost | ThreadShowResult::Refused { .. } => {
                CallToolResult::structured_error(structured)
            }
        };
        result.content = vec![ContentBlock::text(output.text)];
        result
    }
}

struct DecodedThreadShowInput(Result<ThreadShowArguments, ThreadShowRefusal>);

impl schemars::JsonSchema for DecodedThreadShowInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        ThreadShowArguments::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        ThreadShowArguments::json_schema(generator)
    }
}

impl<'de> Deserialize<'de> for DecodedThreadShowInput {
    fn deserialize<Decoder: Deserializer<'de>>(decoder: Decoder) -> Result<Self, Decoder::Error> {
        let value = Value::deserialize(decoder)?;
        let input = ThreadShowArguments::deserialize(&value).map_err(|_| {
            [
                ("includeRedacted", RemovedThreadShowField::IncludeRedacted),
                ("protocolVersion", RemovedThreadShowField::ProtocolVersion),
                ("projectId", RemovedThreadShowField::ProjectQualifier),
                ("project", RemovedThreadShowField::ProjectQualifier),
            ]
            .into_iter()
            .find_map(|(key, field)| {
                value
                    .get(key)
                    .map(|_| ThreadShowRefusal::RemovedInput { field })
            })
            .unwrap_or(ThreadShowRefusal::InvalidArguments)
        });
        Ok(Self(input))
    }
}

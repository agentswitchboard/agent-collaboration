//! The fixed caller-free message catalog for a managed ACP session.

use std::sync::Arc;

use collaboration_protocol::{
    ManagedToolResolution,
    message_tools::{
        ManagedMessageInboxArguments, ManagedMessageSendArguments, ManagedMessageStatusArguments,
        MessageInboxPage, MessageSendOutcome, MessageToolResult, MessageWriteStatus,
        message_inbox_text, message_send_text, message_status_text,
    },
};
use collaboration_service::{
    CollaborationApplication, collaboration_application::message_operations::ManagedMessageCaller,
};
use rmcp::{
    ErrorData, ServerHandler,
    handler::server::{
        common::Extension,
        router::tool::ToolRouter,
        tool::{ToolCallContext, schema_for_type},
        wrapper::Parameters,
    },
    model::{
        CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
        Implementation, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig,
        Tool,
    },
    service::{RequestContext, RoleServer},
    tool, tool_router,
};
use serde::Serialize;

use crate::{managed_tool_bindings::ManagedToolRequest, tool_call_registry::ToolCallRegistry};

#[derive(Clone)]
pub(crate) struct ManagedMessageServer {
    application: CollaborationApplication,
    tool_calls: ToolCallRegistry,
    router: Arc<ToolRouter<Self>>,
}

impl ManagedMessageServer {
    pub(crate) fn new(application: CollaborationApplication, tool_calls: ToolCallRegistry) -> Self {
        Self {
            application,
            tool_calls,
            router: Arc::new(Self::tool_router()),
        }
    }

    fn take_caller(&self, parts: &http::request::Parts) -> Result<ManagedMessageCaller, ErrorData> {
        let resolution = parts
            .extensions
            .get::<Arc<ManagedToolRequest>>()
            .and_then(|request| request.take_resolution());
        let (address, lease) = match resolution {
            Some(ManagedToolResolution::Open { address, lease }) => (address, lease),
            Some(ManagedToolResolution::Pending) => {
                return Err(ErrorData::invalid_request(
                    "The managed session is still starting",
                    None,
                ));
            }
            Some(ManagedToolResolution::Retired) | None => {
                return Err(ErrorData::resource_not_found(
                    "The managed tool binding is unavailable",
                    None,
                ));
            }
        };
        self.application
            .messages()
            .admit_managed_caller(address, lease)
            .map_err(|refusal| ErrorData::internal_error(refusal.to_string(), None))
    }
}

#[tool_router]
impl ManagedMessageServer {
    #[tool(name = "message_inbox", description = "Lists messages for this session. Pass the returned readToken as acknowledge to mark a page read.", output_schema = schema_for_type::<MessageToolResult<MessageInboxPage>>())]
    async fn message_inbox(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<ManagedMessageInboxArguments>,
    ) -> Result<CallToolResult, ErrorData> {
        let caller = self.take_caller(&parts)?;
        let result = self
            .application
            .messages()
            .inbox(caller.caller(), input.limit, input.acknowledge.as_deref())
            .await
            .into();
        message_result(&result, message_inbox_text(&result))
    }

    #[tool(name = "message_send", description = "Sends a direct message from this session to an exact machineId and sessionId.", output_schema = schema_for_type::<MessageToolResult<MessageSendOutcome>>())]
    async fn message_send(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<ManagedMessageSendArguments>,
    ) -> Result<CallToolResult, ErrorData> {
        let caller = self.take_caller(&parts)?;
        let result = self
            .application
            .messages()
            .send(caller.caller(), input.to, input.body, input.request_id)
            .await
            .into();
        message_result(&result, message_send_text(&result))
    }

    #[tool(name = "message_status", description = "Looks up a direct message this session sent, by requestId.", output_schema = schema_for_type::<MessageToolResult<MessageWriteStatus>>())]
    async fn message_status(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<ManagedMessageStatusArguments>,
    ) -> Result<CallToolResult, ErrorData> {
        let caller = self.take_caller(&parts)?;
        let result = self
            .application
            .messages()
            .request_status(caller.caller(), input.request_id)
            .await
            .into();
        message_result(&result, message_status_text(&result))
    }
}

fn message_result<T: Serialize>(
    value: &MessageToolResult<T>,
    text: String,
) -> Result<CallToolResult, ErrorData> {
    let structured = serde_json::to_value(value)
        .map_err(|_| ErrorData::internal_error("Could not encode the message result", None))?;
    let mut result = CallToolResult::structured(structured);
    result.content = vec![ContentBlock::text(text)];
    Ok(result)
}

impl ServerHandler for ManagedMessageServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("agent-router", env!("CARGO_PKG_VERSION")),
        )
    }

    async fn list_tools(
        &self,
        _parameters: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(self.router.list_all())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.router.get(name).cloned()
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let cancellation = context.ct.clone();
        self.tool_calls
            .run(
                self.router
                    .call(ToolCallContext::new(self, request, context)),
                cancellation,
                |_| {
                    Err(ErrorData::internal_error(
                        "The managed tool call ended early",
                        None,
                    ))
                },
            )
            .await
    }
}

//! Publish a Cursor plan before asking its Approver whether to proceed.

use std::sync::Arc;

use agent_client_protocol::{Agent, ConnectionTo, Dispatch, Error, HandleDispatchFrom, Handled};
use serde::Deserialize;
use serde_json::json;
use session_event_model::{
    ApprovalChoice, ApprovalEffect, ApprovalRequest, ApprovalScope, ApprovalSubject, OfferedOption,
    OfferedOptionId, OfferedOptions, OptionsOrigin, SessionEvent,
};
use tokio_util::sync::CancellationToken;

use super::OwnerApprovalBindings;
use crate::provider_cursor_plan_items::{CursorPlanDefinition, CursorPlanItems, CursorTodo};
use crate::{
    AcpRequestPayloads, AcpRequestPolicy, ApprovalPortOutcome, SessionEventSink,
    owner_callback_routing::OwnerCallbackRouting,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CursorCreatePlan {
    tool_call_id: String,
    name: Option<String>,
    overview: Option<String>,
    plan: String,
    #[serde(default)]
    todos: Vec<CursorTodo>,
}

pub(super) struct ProviderCursorCreatePlanHandler<P: AcpRequestPayloads> {
    tool_registry: Arc<OwnerCallbackRouting>,
    plan_items: Arc<CursorPlanItems>,
    event_sink: Arc<dyn SessionEventSink>,
    approval_bindings: OwnerApprovalBindings<P>,
    request_policy: Arc<AcpRequestPolicy<P>>,
    shutdown: CancellationToken,
    sink_closed: CancellationToken,
}

impl<P: AcpRequestPayloads> ProviderCursorCreatePlanHandler<P> {
    pub(super) fn new(
        tool_registry: Arc<OwnerCallbackRouting>,
        plan_items: Arc<CursorPlanItems>,
        event_sink: Arc<dyn SessionEventSink>,
        approval_bindings: OwnerApprovalBindings<P>,
        request_policy: Arc<AcpRequestPolicy<P>>,
        shutdown: CancellationToken,
        sink_closed: CancellationToken,
    ) -> Self {
        Self {
            tool_registry,
            plan_items,
            event_sink,
            approval_bindings,
            request_policy,
            shutdown,
            sink_closed,
        }
    }

    fn approval_request(
        session_id: &str,
        response_id: &impl serde::Serialize,
        plan: &CursorCreatePlan,
        plan_item_id: String,
    ) -> Result<ApprovalRequest, Error> {
        let request_id = serde_json::to_string(&(session_id, response_id))
            .map_err(|_| Error::internal_error())?;
        let options = OfferedOptions::new(vec![
            OfferedOption {
                option_id: OfferedOptionId::new("plan.accept")
                    .map_err(|_| Error::internal_error())?,
                label: "Accept plan".to_owned(),
                choice: ApprovalChoice::new(ApprovalEffect::Allow, ApprovalScope::Once),
            },
            OfferedOption {
                option_id: OfferedOptionId::new("plan.reject")
                    .map_err(|_| Error::internal_error())?,
                label: "Reject plan".to_owned(),
                choice: ApprovalChoice::new(ApprovalEffect::Decline, ApprovalScope::Once),
            },
        ])
        .map_err(|_| Error::internal_error())?;
        Ok(ApprovalRequest {
            request_id,
            title: plan
                .name
                .clone()
                .unwrap_or_else(|| "Approve plan".to_owned()),
            description: plan.overview.clone(),
            subject: Some(ApprovalSubject::Plan {
                tool_call_id: plan.tool_call_id.clone(),
                plan_item_id,
            }),
            options_origin: OptionsOrigin::RouterSynthesized,
            options,
        })
    }
}

fn cursor_plan_decision(outcome: ApprovalPortOutcome) -> serde_json::Value {
    match outcome {
        ApprovalPortOutcome::Selected { option_id, .. } if option_id == "plan.accept" => {
            json!({"outcome":"accepted"})
        }
        ApprovalPortOutcome::Selected { option_id, note } if option_id == "plan.reject" => {
            match note {
                Some(reason) => json!({"outcome":"rejected","reason":reason}),
                None => json!({"outcome":"rejected"}),
            }
        }
        ApprovalPortOutcome::Selected { .. }
        | ApprovalPortOutcome::Cancelled
        | ApprovalPortOutcome::Unavailable => json!({"outcome":"cancelled"}),
    }
}

impl<P: AcpRequestPayloads> HandleDispatchFrom<Agent> for ProviderCursorCreatePlanHandler<P> {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        connection: ConnectionTo<Agent>,
    ) -> Result<Handled<Dispatch>, Error> {
        let Dispatch::Request(request, responder) = message else {
            return Ok(Handled::No {
                message,
                retry: false,
            });
        };
        if request.method() != "cursor/create_plan" {
            return Ok(Handled::No {
                message: Dispatch::Request(request, responder),
                retry: false,
            });
        }
        let plan = match serde_json::from_value::<CursorCreatePlan>(request.params().clone()) {
            Ok(plan) => plan,
            Err(_) => {
                responder.respond_with_error(Error::invalid_params())?;
                return Ok(Handled::Yes);
            }
        };
        let Some(crate::owner_callback_routing::ToolCallOwner {
            session_id,
            run,
            turn,
        }) = self.tool_registry.resolve_session(&plan.tool_call_id)
        else {
            tracing::warn!("unresolved Cursor create_plan request");
            responder.respond(json!({"outcome":"cancelled"}))?;
            return Ok(Handled::Yes);
        };
        let context = self
            .approval_bindings
            .active
            .lock()
            .ok()
            .and_then(|contexts| {
                contexts
                    .get(&session_id)
                    .filter(|context| {
                        context.incarnation == run.incarnation
                            && context.generation == run.generation
                            && context.turn == Some(turn)
                    })
                    .cloned()
            });
        let Some(context) = context else {
            tracing::warn!("Cursor create_plan has no active Turn context");
            responder.respond(json!({"outcome":"cancelled"}))?;
            return Ok(Handled::Yes);
        };
        let (existed, item) = self.plan_items.set_plan(
            run,
            turn,
            CursorPlanDefinition {
                tool_call_id: plan.tool_call_id.clone(),
                name: plan.name.clone(),
                overview: plan.overview.clone(),
                markdown: plan.plan.clone(),
                todos: plan.todos.clone(),
            },
        );
        let approval =
            Self::approval_request(&session_id, responder.id(), &plan, item.item_id.clone())?;
        let event = if existed {
            SessionEvent::ItemUpdated { item }
        } else {
            SessionEvent::ItemStarted { item }
        };
        // In the run the plan arrived in (D1 R14).
        // In the run that announced the tool call (review F4).
        let publication =
            self.event_sink
                .publish_for_turn(&session_id, run.generation, turn, event);
        if publication.is_err() {
            self.sink_closed.cancel();
            self.shutdown.cancel();
        }
        if !matches!(publication, Ok(crate::GenerationPublication::Published)) {
            responder.respond(json!({"outcome":"cancelled"}))?;
            return Ok(Handled::Yes);
        }
        let port = Arc::clone(&self.request_policy);
        let request_cancellation = responder.cancellation();
        let Some(response_guard) = context.track_response("cursor/create_plan", responder.id())
        else {
            responder.respond(json!({"outcome":"cancelled"}))?;
            return Ok(Handled::Yes);
        };
        self.approval_bindings
            .spawn_callback(&connection, async move {
                if let Some(refusal) = context.managed_refusal(port.as_ref()) {
                    let (open, queued) = response_guard.commit(|open| {
                        (
                            open,
                            responder.respond(if open {
                                refusal.acp_cursor_plan()
                            } else {
                                json!({"outcome":"cancelled"})
                            }),
                        )
                    });
                    drop(response_guard);
                    if open && queued.is_ok() {
                        context.cancel_after_refusal(port, refusal).await;
                    }
                    return queued;
                }
                let outcome = context
                    .decide_approval(port.as_ref(), approval, request_cancellation)
                    .await;
                response_guard.commit(|open| {
                    responder.respond(cursor_plan_decision(if open {
                        outcome
                    } else {
                        ApprovalPortOutcome::Cancelled
                    }))
                })
            })?;
        Ok(Handled::Yes)
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "ProviderCursorCreatePlanHandler"
    }
}

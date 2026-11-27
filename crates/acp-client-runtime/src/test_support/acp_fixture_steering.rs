//! Steer through the fixture's actual owner and ACP port.

use super::{FixtureInput, FixtureResult, PolicyFixtureClient, fixture_error, lock};
use crate::{AcpRequestPayloads, ProviderSteeringOutcome, ProviderTurnInput};
use provider_session_machines::{
    session_owner::{
        OwnerInput, RemoteEffect, SessionOwnerCommand, SessionOwnerReply, SteerOrigin,
    },
    session_turn_machine::{SessionTurn, TurnId},
};
use session_event_model::InputId;
use uuid::Uuid;

impl<P: AcpRequestPayloads> PolicyFixtureClient<P> {
    pub async fn steer_contents_with_input(
        &self,
        id: String,
        input_id: InputId,
        submission: session_event_model::ProviderSubmission,
        origin: SteerOrigin,
    ) -> FixtureResult<ProviderSteeringOutcome<P::OperationId, P::Context>> {
        let session = self.session(&id)?;
        let capabilities = lock(&session.state).capabilities.clone();
        let content =
            self.prepare_owner_prompt(ProviderTurnInput::Prompt(submission), &capabilities)?;
        let operation = Uuid::now_v7();
        let turn = match session.owner.snapshots().borrow().turn {
            SessionTurn::Running { turn } => turn,
            _ => TurnId(Uuid::now_v7()),
        };
        let joined = lock(&session.state)
            .active_context
            .as_ref()
            .filter(|(active, _)| *active == turn)
            .map(|(_, context)| context.clone());
        let input = OwnerInput {
            generation: session.generation()?,
            operation,
            input: input_id,
            turn,
        };
        lock(&session.state).inputs.insert(
            input.input.clone(),
            FixtureInput {
                content,
                context: None,
                dispatch: None,
            },
        );
        let supported = capabilities.supports_steering;
        let command = if origin == SteerOrigin::Auto {
            SessionOwnerCommand::SteerExisting {
                input,
                supported,
                origin,
            }
        } else {
            SessionOwnerCommand::Steer {
                input,
                supported,
                origin,
            }
        };
        let reply = session.settle(operation, command).await?;
        Ok(match reply {
            SessionOwnerReply::InputFinished {
                remote: RemoteEffect::Applied,
                ..
            } => ProviderSteeringOutcome::Injected {
                running_operation_id: joined
                    .as_ref()
                    .map(|context| self.connection.policy_operation_id(context)),
                joined_turn: joined,
            },
            SessionOwnerReply::SteerStarted { .. } | SessionOwnerReply::PromptCompleted { .. } => {
                ProviderSteeringOutcome::StartedNewTurn
            }
            SessionOwnerReply::SteerInvalidReply { .. } => {
                return Err(fixture_error(
                    "provider returned an invalid steering result",
                ));
            }
            _ => ProviderSteeringOutcome::Failed,
        })
    }
}

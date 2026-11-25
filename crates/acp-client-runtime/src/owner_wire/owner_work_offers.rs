//! Frame construction and offer recording for the ordered ACP work calls.

use super::{
    AcpFrame, Error, OwnerAcpWire, OwnerWireRefusal, ResponseKind, SettingCorrelation, TransportRun,
};
use crate::{
    PermissionModePlan, RequestedPermissionMode,
    acp_transport::{TransportOfferRefusal, TransportRefusal},
    owner_session_setting::{ModeConfirmation, OwnerSettingWirePlan},
    provider_prompt_content::ProviderPromptContent,
    session_owner_task::OwnerCompletion,
};
use agent_client_protocol::{
    JsonRpcMessage as _, UntypedMessage,
    schema::v1::{CancelNotification, ContentBlock, PromptRequest},
};
use provider_session_machines::{
    ProviderSettingKind,
    session_owner::{DispatchAuthorization, DispatchResult},
    session_turn_machine::TurnId,
};
use switchboard_harness_contracts::{
    ReservedWorkReply, SettingOffer, SteerIdlePolicy, WorkOfferRefusal,
};

impl OwnerAcpWire {
    pub fn offer_prompt(
        &self,
        authorization: DispatchAuthorization,
        turn: TurnId,
        session: String,
        content: ProviderPromptContent,
        completion: OwnerCompletion,
    ) -> Result<(), OwnerWireRefusal> {
        let compact = content.is_compaction();
        self.offer_work_prompt(
            authorization,
            turn,
            session,
            content.into_blocks(),
            compact,
            Box::new(completion),
        )
        .map_err(legacy_work_refusal)
    }

    pub(crate) fn offer_work_prompt(
        &self,
        authorization: DispatchAuthorization,
        turn: TurnId,
        session: String,
        blocks: Vec<ContentBlock>,
        compact: bool,
        completion: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        let request = PromptRequest::new(session, blocks);
        let route = TransportRun {
            incarnation: authorization.incarnation,
            generation: authorization.run,
        };
        let reader = self
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&route)
            .cloned();
        self.offer_work_request(
            authorization.clone(),
            Some(turn),
            request.to_untyped_message(),
            ResponseKind::Prompt {
                authorization,
                turn,
                compact,
                reader,
                completion,
            },
        )
    }

    pub fn offer_steer(
        &self,
        authorization: DispatchAuthorization,
        session: String,
        content: ProviderPromptContent,
        completion: OwnerCompletion,
    ) -> Result<(), OwnerWireRefusal> {
        self.offer_work_steer(
            authorization,
            session,
            content.into_blocks(),
            SteerIdlePolicy::AllowOwnerPrompt,
            Box::new(completion),
        )
        .map_err(legacy_work_refusal)
    }

    /// Offers a steer which must leave the session untouched if its turn ended.
    pub fn offer_exact_steer(
        &self,
        authorization: DispatchAuthorization,
        session: String,
        content: ProviderPromptContent,
        completion: OwnerCompletion,
    ) -> Result<(), OwnerWireRefusal> {
        self.offer_work_steer(
            authorization,
            session,
            content.into_blocks(),
            SteerIdlePolicy::Refuse,
            Box::new(completion),
        )
        .map_err(legacy_work_refusal)
    }

    pub(crate) fn offer_work_steer(
        &self,
        authorization: DispatchAuthorization,
        session: String,
        blocks: Vec<ContentBlock>,
        idle: SteerIdlePolicy,
        completion: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        let request = UntypedMessage::new(
            "_session/steering",
            serde_json::json!({
                "sessionId": session,
                "prompt": blocks,
                "_meta": { "steering": { "idleBehavior": "promptRequired" } },
            }),
        );
        self.offer_work_request(
            authorization.clone(),
            None,
            request,
            ResponseKind::Steer {
                authorization,
                idle,
                completion,
            },
        )
    }

    fn offer_work_request(
        &self,
        authorization: DispatchAuthorization,
        new_turn: Option<TurnId>,
        message: Result<UntypedMessage, Error>,
        correlation: ResponseKind,
    ) -> Result<(), WorkOfferRefusal> {
        Ok(self.transport.offer_work(
            authorization,
            new_turn,
            AcpFrame::Request {
                message: message.map_err(|_| WorkOfferRefusal::PreparationFailed)?,
                correlation,
            },
        )?)
    }

    pub fn offer_cancel(
        &self,
        authorization: DispatchAuthorization,
        session: String,
    ) -> Result<(), TransportRefusal> {
        self.transport.offer(
            authorization,
            None,
            AcpFrame::Cancel(CancelNotification::new(session)),
        )
    }

    pub fn offer_explicit_setting(
        &self,
        authorization: DispatchAuthorization,
        session: String,
        kind: ProviderSettingKind,
        wanted: String,
        completion: OwnerCompletion,
    ) -> Result<Option<DispatchResult>, OwnerWireRefusal> {
        match self
            .offer_work_setting(authorization, session, kind, wanted, Box::new(completion))
            .map_err(legacy_work_refusal)?
        {
            SettingOffer::Offered => Ok(None),
            SettingOffer::Settled(outcome) => Ok(Some(outcome)),
        }
    }

    pub(crate) fn offer_work_setting(
        &self,
        authorization: DispatchAuthorization,
        session: String,
        kind: ProviderSettingKind,
        wanted: String,
        completion: ReservedWorkReply,
    ) -> Result<SettingOffer, WorkOfferRefusal> {
        let run = TransportRun {
            incarnation: authorization.incarnation,
            generation: authorization.run,
        };
        let catalog = self
            .session_catalog(run)
            .ok_or(WorkOfferRefusal::SettingCatalogueUnavailable)?;
        let (wire_value, fallback, mode_confirmation) = if kind == ProviderSettingKind::Mode {
            let plan = crate::plan_permission_mode(
                RequestedPermissionMode::from_request(Some(&wanted)),
                &catalog.advertised_values(kind),
                self.behavior.auto_permission_mode(),
            )
            .map_err(|_| WorkOfferRefusal::SettingValueNotOffered)?;
            match plan {
                PermissionModePlan::Keep { reason, .. } => {
                    return Ok(SettingOffer::Settled(DispatchResult::SettingKept {
                        authorization,
                        reason,
                    }));
                }
                PermissionModePlan::Set {
                    mode,
                    accepted_fallback,
                    reason,
                    ..
                } => (mode, accepted_fallback, Some(ModeConfirmation { reason })),
            }
        } else {
            (wanted, None, None)
        };
        let plan =
            OwnerSettingWirePlan::from_work_catalog(session, &catalog, kind, wire_value, fallback)?
                .with_mode_confirmation(mode_confirmation);
        let (message, setting) = plan.correlate(SettingCorrelation::Explicit {
            authorization: authorization.clone(),
            completion,
        });
        self.transport.offer_work(
            authorization,
            None,
            AcpFrame::Request {
                message,
                correlation: ResponseKind::Setting(Box::new(setting)),
            },
        )?;
        Ok(SettingOffer::Offered)
    }
}

impl From<TransportOfferRefusal> for WorkOfferRefusal {
    fn from(refusal: TransportOfferRefusal) -> Self {
        match refusal {
            TransportOfferRefusal::RouteUnavailable => Self::RouteUnavailable,
            TransportOfferRefusal::Closing => Self::RouteClosing,
            TransportOfferRefusal::WorkDoesNotFitAuthorization => Self::WorkDoesNotFitAuthorization,
            TransportOfferRefusal::OtherAuthorizationOffered => Self::OtherAuthorizationOffered,
            TransportOfferRefusal::AuthorizationSequenceOnRecord => {
                Self::AuthorizationSequenceOnRecord
            }
        }
    }
}

fn legacy_work_refusal(refusal: WorkOfferRefusal) -> OwnerWireRefusal {
    match refusal {
        WorkOfferRefusal::PreparationFailed
        | WorkOfferRefusal::SettingCatalogueUnavailable
        | WorkOfferRefusal::SettingValueNotOffered
        | WorkOfferRefusal::SettingSetterUnavailable => OwnerWireRefusal::Encoding,
        WorkOfferRefusal::RouteUnavailable => TransportRefusal::RouteUnavailable.into(),
        WorkOfferRefusal::RouteClosing => TransportRefusal::Closing.into(),
        WorkOfferRefusal::WorkDoesNotFitAuthorization
        | WorkOfferRefusal::OtherAuthorizationOffered
        | WorkOfferRefusal::AuthorizationSequenceOnRecord => {
            TransportRefusal::AuthorizationMismatch.into()
        }
    }
}

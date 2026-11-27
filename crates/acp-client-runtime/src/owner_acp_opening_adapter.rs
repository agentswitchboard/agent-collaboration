//! ACP's held session, settings plan and restore reply at the shared opening boundary.
use std::sync::Arc;

use provider_session_machines::ProviderSettingsCatalog;
use session_event_model::{CapabilityReport, SessionSettings, SkillView};
use switchboard_harness_contracts::{
    AcceptedRunSettings, LifecycleRefusal, SettingAttempt, SetupReport, WorkOfferRefusal,
};
use switchboard_harness_shared_mechanics::opening::{
    LifecycleRoute, OpeningAdapter, OpeningEffects, OpeningInstance, RestoreDecision,
    SendRestoreDecision, StepFaultReport,
};
use tokio::sync::oneshot;

use crate::{
    owner_session_open::OwnerOpenedSession, owner_session_setting::OwnerSettingWirePlan,
    owner_wire::OwnerAcpWire,
};

pub(super) struct AcpOpening;

impl OpeningAdapter for AcpOpening {
    type OpenedSession = OwnerOpenedSession;
    type SettingPlan = OwnerSettingWirePlan;
    type Capabilities = CapabilityReport;
    type Settings = SessionSettings;
    type Skills = SkillView;
    type RestoreSignal = RestoreSignal;

    fn accept_catalogue(
        session: &mut Self::OpenedSession,
        accepted: AcceptedRunSettings,
    ) -> SetupReport<CapabilityReport, SessionSettings> {
        session.accept_restore_catalogue(&ProviderSettingsCatalog::from_record(Arc::clone(
            &accepted.catalogue,
        )));
        SetupReport {
            capabilities: session.capabilities().to_session_model(),
            settings: session.settings(),
            observed: crate::owner_session_creation::catalogue_evidence(session.catalog()),
        }
    }
    fn plan_setting_put_back(
        session_name: String,
        attempt: SettingAttempt,
        accepted_mode_fallback: Option<String>,
    ) -> Result<Self::SettingPlan, LifecycleRefusal> {
        OwnerSettingWirePlan::from_work_catalog(
            session_name,
            &ProviderSettingsCatalog::from_record(attempt.catalogue),
            attempt.setting.kind,
            attempt.value.0,
            accepted_mode_fallback,
        )
        .map_err(|reason| match reason {
            WorkOfferRefusal::SettingValueNotOffered => LifecycleRefusal::SettingValueNotOffered,
            WorkOfferRefusal::SettingSetterUnavailable => {
                LifecycleRefusal::SettingSetterUnavailable
            }
            WorkOfferRefusal::PreparationFailed
            | WorkOfferRefusal::SettingCatalogueUnavailable
            | WorkOfferRefusal::RouteUnavailable
            | WorkOfferRefusal::RouteClosing
            | WorkOfferRefusal::WorkDoesNotFitAuthorization
            | WorkOfferRefusal::OtherAuthorizationOffered
            | WorkOfferRefusal::AuthorizationSequenceOnRecord => LifecycleRefusal::Encoding,
        })
    }
}

impl OpeningEffects for OwnerAcpWire {
    fn retire_opening_route(&self, route: &LifecycleRoute, instance: &OpeningInstance) {
        OwnerAcpWire::retire_opening_route(self, route, instance);
    }

    fn report_step_fault(&self, fault: &StepFaultReport) {
        tracing::error!(phase=?fault.phase, edge=?fault.edge, declared=?fault.declared,
            actual=?fault.actual, "ACP opening step returned an undeclared outcome");
    }
}
pub(super) struct RestoreSignal {
    sender: oneshot::Sender<RestoreDecision>,
    #[cfg(any(test, feature = "test-observation"))]
    pub(super) _disposal: Option<Box<crate::owner_opening_disposal::OpeningDisposalToken>>,
}

impl RestoreSignal {
    pub(super) fn new(sender: oneshot::Sender<RestoreDecision>) -> Self {
        Self {
            sender,
            #[cfg(any(test, feature = "test-observation"))]
            _disposal: None,
        }
    }
}

impl SendRestoreDecision for RestoreSignal {
    fn send(self, decision: RestoreDecision) {
        let _worker_gone = self.sender.send(decision);
    }
}

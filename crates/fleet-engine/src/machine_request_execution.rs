use crate::engine_host_binding::{EngineHost, FleetEngine};
use crate::{
    event_source_policy::RequestSource,
    machine_event_execution::MachineEventOutcome,
    machine_store_adapter::MachineStoreAdapter,
    request_receipt_store::{EngineReceipt, ReceiptClaim},
};
use central_engine_model::{InstanceKey, Machine};
use central_storage::StoreFailure;
use fleet_contracts::EngineSubject;
use fleet_runtime_ports::CentralStore as _;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use workspace_model::{ActorIdentityId, RequestId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "camelCase", deny_unknown_fields)]
pub enum EngineRequestOutcome<State, Error> {
    Answer {
        receipt: EngineReceipt<State, Error>,
    },
    Conflict,
    OutcomeUnknown,
    ItemFailed {
        failure: central_engine_model::ItemFailureClass,
    },
    CentralUnavailable,
    NotPermitted {
        reason: workspace_model::RefusalReason,
    },
}

impl<R: EngineHost> FleetEngine<'_, R> {
    pub async fn run_machine_request<M, Adapter>(
        &self,
        adapter: &Adapter,
        caller: ActorIdentityId,
        request: RequestId,
        key: InstanceKey,
        event: M::Event,
    ) -> Result<EngineRequestOutcome<M::State, M::Error>, StoreFailure>
    where
        M: Machine,
        M::State: Send + Sync + Serialize + DeserializeOwned,
        M::Event: Send + Sync + Serialize,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
        Adapter: MachineStoreAdapter<M>,
    {
        let answer = self
            .execute_engine_request::<M, Adapter>(adapter, caller, request, key, event)
            .await;
        let _ = self.after_event().await;
        answer
    }

    async fn execute_engine_request<M, Adapter>(
        &self,
        adapter: &Adapter,
        caller: ActorIdentityId,
        request: RequestId,
        key: InstanceKey,
        event: M::Event,
    ) -> Result<EngineRequestOutcome<M::State, M::Error>, StoreFailure>
    where
        M: Machine,
        M::State: Send + Sync + Serialize + DeserializeOwned,
        M::Event: Send + Sync + Serialize,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
        Adapter: MachineStoreAdapter<M>,
    {
        // The request entry checked the organization
        // once for this event, before any read (P3).
        let now = self.now()?;
        let cutoff = now
            .checked_sub_signed(chrono::Duration::hours(24))
            .ok_or_else(|| corrupt("runtime_request_log.cutoff"))?;
        let digest = fleet_runtime::workspace_request_log::digest(
            &serde_json::json!({"machine":M::KIND.0,"key":&key.0,"event":&event}),
        )?;
        let source = RequestSource(ReceiptClaim {
            request,
            caller,
            digest,
            started_at: now,
            cutoff,
        });
        let logged = self.host().read(source.0.lookup()).await?;
        if let Some(receipt) = source.0.replay(&logged)? {
            return Ok(EngineRequestOutcome::Answer { receipt });
        }
        #[cfg(feature = "test-support")]
        self.pauses()
            .reached_engine(
                fleet_runtime::PausePoint::AfterEngineAbsentLookup,
                &source.0.request.to_string(),
            )
            .await;
        let subject = EngineSubject {
            kind: M::KIND,
            key: key.clone(),
        };
        Ok(
            match self
                .handle::<M, Adapter, _>(adapter, key, event, &source)
                .await
            {
                MachineEventOutcome::TransitionCommitted(state) => EngineRequestOutcome::Answer {
                    receipt: EngineReceipt::Applied { state },
                },
                MachineEventOutcome::TransitionRefused(error) => EngineRequestOutcome::Answer {
                    receipt: EngineReceipt::Refused {
                        error: error.into(),
                    },
                },
                MachineEventOutcome::SubjectMissing => EngineRequestOutcome::Answer {
                    receipt: EngineReceipt::NotFound,
                },
                MachineEventOutcome::AttemptFailureRecorded(failure)
                | MachineEventOutcome::RequestItemFailed(failure) => {
                    EngineRequestOutcome::ItemFailed { failure }
                }
                MachineEventOutcome::StorageAnswerLost => EngineRequestOutcome::OutcomeUnknown,
                MachineEventOutcome::StorageFailed(failure) => {
                    self.observe_engine_store_down(&subject, failure);
                    EngineRequestOutcome::CentralUnavailable
                }
                MachineEventOutcome::EventSuperseded => {
                    let logged = self.host().read(source.0.lookup()).await?;
                    match source.0.replay(&logged)? {
                        Some(receipt) => EngineRequestOutcome::Answer { receipt },
                        None => EngineRequestOutcome::Conflict,
                    }
                }
            },
        )
    }
}

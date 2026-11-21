use super::engine_host_binding::{EngineHost, FleetEngine};
use super::{
    event_source_policy::RetainedRequestEventSource,
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

impl<Host: EngineHost> FleetEngine<'_, Host> {
    pub async fn run_machine_request<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        caller: ActorIdentityId,
        request: RequestId,
        key: InstanceKey,
        event: SubjectMachine::Event,
    ) -> Result<EngineRequestOutcome<SubjectMachine::State, SubjectMachine::Error>, StoreFailure>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Send + Sync + Serialize + DeserializeOwned,
        SubjectMachine::Event: Send + Sync + Serialize,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
    {
        let answer = self
            .execute_engine_request::<SubjectMachine, SubjectStore>(
                adapter, caller, request, key, event,
            )
            .await;
        let _ = self.complete_event_with_observation().await;
        answer
    }

    async fn execute_engine_request<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        caller: ActorIdentityId,
        request: RequestId,
        key: InstanceKey,
        event: SubjectMachine::Event,
    ) -> Result<EngineRequestOutcome<SubjectMachine::State, SubjectMachine::Error>, StoreFailure>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Send + Sync + Serialize + DeserializeOwned,
        SubjectMachine::Event: Send + Sync + Serialize,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
    {
        // The request entry checked the organization
        // once for this event, before any read (P3).
        let now = self.current_engine_time()?;
        let cutoff = now
            .checked_sub_signed(chrono::Duration::hours(24))
            .ok_or_else(|| corrupt("runtime_request_log.cutoff"))?;
        let digest = fleet_runtime::workspace_request_log::digest(
            &serde_json::json!({"machine":SubjectMachine::KIND.0,"key":&key.0,"event":&event}),
        )?;
        let source = RetainedRequestEventSource(ReceiptClaim {
            request_id: request,
            caller_actor_id: caller,
            request_content_digest: digest,
            request_started_at: now,
            receipt_retention_cutoff: cutoff,
        });
        let logged = self
            .central_store()
            .read(source.0.build_live_receipt_lookup())
            .await?;
        if let Some(receipt) = source.0.decode_retained_receipt(&logged)? {
            return Ok(EngineRequestOutcome::Answer { receipt });
        }
        #[cfg(feature = "test-support")]
        self.engine_test_pause_points()
            .reached_engine(
                fleet_runtime::PausePoint::AfterEngineAbsentLookup,
                &source.0.request_id.to_string(),
            )
            .await;
        let subject = EngineSubject {
            kind: SubjectMachine::KIND,
            key: key.clone(),
        };
        Ok(
            match self
                .apply_machine_event::<SubjectMachine, SubjectStore, _>(
                    adapter, key, event, &source,
                )
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
                    let logged = self
                        .central_store()
                        .read(source.0.build_live_receipt_lookup())
                        .await?;
                    match source.0.decode_retained_receipt(&logged)? {
                        Some(receipt) => EngineRequestOutcome::Answer { receipt },
                        None => EngineRequestOutcome::Conflict,
                    }
                }
            },
        )
    }
}

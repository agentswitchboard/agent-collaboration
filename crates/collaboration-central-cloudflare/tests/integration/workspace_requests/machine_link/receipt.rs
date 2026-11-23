use super::super::workerd_host::c2_controls::WitnessRecord;
use super::fixture;
use central_link_protocol::{CorrelationId, DataFrame};
use central_storage::ReadOutcome;
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use fleet_identity_protocol::OrganizationId;
use serde::Serialize;
use workspace_ids::MachineId;
use workspace_model::AreaId;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HibernationReceipt {
    pub kind: &'static str,
    pub organization: OrganizationId,
    pub machine_id: MachineId,
    pub welcome: DataFrame,
    pub baseline_request: DataFrame,
    pub baseline_reply: DataFrame,
    pub deciding_request: DataFrame,
    pub deciding_reply: DataFrame,
    pub read_request: DataFrame,
    pub read_reply: DataFrame,
    pub before_events: Vec<WitnessRecord>,
    pub heartbeat_events: Vec<Vec<WitnessRecord>>,
    pub deciding_events: Vec<WitnessRecord>,
    pub area: AreaReceipt,
    pub store: ReadOutcome,
    pub disposed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AreaReceipt {
    pub area_id: AreaId,
    pub name: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BatchHibernationReceipt {
    pub kind: &'static str,
    pub organization: OrganizationId,
    pub machine_id: MachineId,
    pub welcome: DataFrame,
    pub baseline_request: DataFrame,
    pub baseline_reply: DataFrame,
    pub batch_request: DataFrame,
    pub batch_replies: Vec<DataFrame>,
    pub read_requests: Vec<DataFrame>,
    pub read_replies: Vec<DataFrame>,
    pub before_events: Vec<WitnessRecord>,
    pub heartbeat_events: Vec<Vec<WitnessRecord>>,
    pub deciding_events: Vec<WitnessRecord>,
    pub areas: Vec<AreaReceipt>,
    pub store: ReadOutcome,
    pub wakes: ReadOutcome,
    pub disposed: bool,
}

pub(super) async fn receipt_result(
    socket: &mut reqwest_websocket::WebSocket,
    message_id: workspace_ids::MessageId,
    expected: central_link_protocol::ReceiptResult,
) -> Result<()> {
    let correlation_id: CorrelationId = "019f0000-0000-7000-8000-00000000c201".parse()?;
    fixture::send(
        socket,
        &DataFrame::Receipts {
            correlation_id: correlation_id.clone(),
            receipts: central_link_protocol::ReceiptBatch::new(vec![
                central_link_protocol::Receipt {
                    message_id,
                    kind: central_link_protocol::ReceiptKind::Delivered {
                        arrived_at: chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0)
                            .ok_or("fixture UTC")?,
                    },
                },
            ])?,
        },
    )
    .await?;
    if fixture::receive(socket).await?
        != (DataFrame::ReceiptResults {
            correlation_id,
            results: vec![expected],
        })
    {
        return Err("receipt result or correlation changed".into());
    }
    Ok(())
}

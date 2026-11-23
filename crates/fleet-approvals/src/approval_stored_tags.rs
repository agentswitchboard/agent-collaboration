use approval_request_protocol::{
    CallStopCause, CentralTime, Delivery, HolderStopCause, ProviderStopCause, RequestKind,
    WaitingStatus,
};
use central_storage::StoreFailure;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};

use crate::StopMarkerCause;

const PROVIDER_STOP_FAMILY: &str = "provider";
const UNPROVEN_CALL_STOP_FAMILY: &str = "unproven_call";

pub(crate) fn request_kind_tag(kind: RequestKind) -> &'static str {
    match kind {
        RequestKind::ProviderApproval => "provider_approval",
        RequestKind::ProviderQuestion => "provider_question",
        RequestKind::UnprovenCall => "unproven_call",
    }
}

pub(crate) fn waiting_status_tag(status: WaitingStatus) -> &'static str {
    match status {
        WaitingStatus::Pending => "pending",
        WaitingStatus::PassedOn => "passed_on",
    }
}

pub(crate) fn parse_waiting_status(stored: &str) -> Result<WaitingStatus, StoreFailure> {
    match stored {
        "pending" => Ok(WaitingStatus::Pending),
        "passed_on" => Ok(WaitingStatus::PassedOn),
        _ => Err(corrupt("approval_requests.waiting_status")),
    }
}

pub(crate) fn delivery_state_tag(delivery: Delivery) -> &'static str {
    match delivery {
        Delivery::Owed {} => "owed",
        Delivery::ReceivedByHolder {} => "received_by_holder",
        Delivery::ClosedWithoutReceipt {} => "closed_without_receipt",
    }
}

pub(crate) fn parse_delivery_state(stored: &str) -> Result<Delivery, StoreFailure> {
    match stored {
        "owed" => Ok(Delivery::Owed {}),
        "received_by_holder" => Ok(Delivery::ReceivedByHolder {}),
        "closed_without_receipt" => Ok(Delivery::ClosedWithoutReceipt {}),
        _ => Err(corrupt("approval_requests.delivery_state")),
    }
}

pub(crate) fn stop_marker_tags(stop_marker: StopMarkerCause) -> (&'static str, &'static str) {
    let family_tag = match stop_marker {
        StopMarkerCause::Provider(_) => PROVIDER_STOP_FAMILY,
        StopMarkerCause::UnprovenCall(_) => UNPROVEN_CALL_STOP_FAMILY,
    };
    let cause_tag = match stop_marker.holder_cause() {
        HolderStopCause::TurnStopped => "turn_stopped",
        HolderStopCause::AskReplaced => "ask_replaced",
        HolderStopCause::AskWithdrawn => "ask_withdrawn",
        HolderStopCause::SessionClosed => "session_closed",
        HolderStopCause::ProviderConnectionLost => "provider_connection_lost",
        HolderStopCause::CallWithdrawn => "call_withdrawn",
        HolderStopCause::HolderRefusedLocally => "holder_refused_locally",
    };
    (family_tag, cause_tag)
}

pub(crate) fn parse_stop_marker(
    stored_family: &str,
    stored_cause: &str,
) -> Result<StopMarkerCause, StoreFailure> {
    match stored_family {
        PROVIDER_STOP_FAMILY => {
            let stop_cause = match stored_cause {
                "turn_stopped" => ProviderStopCause::TurnStopped,
                "ask_replaced" => ProviderStopCause::AskReplaced,
                "ask_withdrawn" => ProviderStopCause::AskWithdrawn,
                "session_closed" => ProviderStopCause::SessionClosed,
                "provider_connection_lost" => ProviderStopCause::ProviderConnectionLost,
                "holder_refused_locally" => ProviderStopCause::HolderRefusedLocally,
                _ => return Err(corrupt("approval_requests.stop_cause")),
            };
            Ok(StopMarkerCause::Provider(stop_cause))
        }
        UNPROVEN_CALL_STOP_FAMILY => {
            let stop_cause = match stored_cause {
                "call_withdrawn" => CallStopCause::CallWithdrawn,
                "holder_refused_locally" => CallStopCause::HolderRefusedLocally,
                _ => return Err(corrupt("approval_requests.stop_cause")),
            };
            Ok(StopMarkerCause::UnprovenCall(stop_cause))
        }
        _ => Err(corrupt("approval_requests.stop_family")),
    }
}

pub(crate) fn parse_stored_time(value: &str, column: &str) -> Result<CentralTime, StoreFailure> {
    value
        .parse::<CentralTime>()
        .ok()
        .filter(|time| format_utc_millis(*time) == value)
        .ok_or_else(|| corrupt(column))
}

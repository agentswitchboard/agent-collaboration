use approval_request_protocol::{
    CentralTime, Delivery, HolderStopCause, RequestKind, WaitingStatus,
};
use central_storage::StoreFailure;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};

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

pub(crate) fn stop_cause_tag(cause: HolderStopCause) -> &'static str {
    match cause {
        HolderStopCause::TurnStopped => "turn_stopped",
        HolderStopCause::AskReplaced => "ask_replaced",
        HolderStopCause::AskWithdrawn => "ask_withdrawn",
        HolderStopCause::SessionClosed => "session_closed",
        HolderStopCause::ProviderConnectionLost => "provider_connection_lost",
        HolderStopCause::CallWithdrawn => "call_withdrawn",
    }
}

pub(crate) fn parse_stop_cause(stored: &str) -> Result<HolderStopCause, StoreFailure> {
    match stored {
        "turn_stopped" => Ok(HolderStopCause::TurnStopped),
        "ask_replaced" => Ok(HolderStopCause::AskReplaced),
        "ask_withdrawn" => Ok(HolderStopCause::AskWithdrawn),
        "session_closed" => Ok(HolderStopCause::SessionClosed),
        "provider_connection_lost" => Ok(HolderStopCause::ProviderConnectionLost),
        "call_withdrawn" => Ok(HolderStopCause::CallWithdrawn),
        _ => Err(corrupt("approval_requests.stop_cause")),
    }
}

pub(crate) fn parse_stored_time(value: &str, column: &str) -> Result<CentralTime, StoreFailure> {
    value
        .parse::<CentralTime>()
        .ok()
        .filter(|time| format_utc_millis(*time) == value)
        .ok_or_else(|| corrupt(column))
}

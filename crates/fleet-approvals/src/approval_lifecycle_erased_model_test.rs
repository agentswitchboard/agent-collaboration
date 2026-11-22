use approval_request_protocol::{Ending, RaiseReply, ReportDeliveryReply, StopReceiptEnding};

use crate::{ApprovalEffect, ApprovalReceipt, ApprovalRefusal};

use super::commands::{Command, command_name};
use super::{Rejected, domain, hidden, stopped_receipt};

pub(super) fn standing(ending: &Ending) -> StopReceiptEnding {
    match ending {
        Ending::Decided { .. } => StopReceiptEnding::DecidedEarlier {},
        Ending::Cancelled { reason } => StopReceiptEnding::Cancelled { reason: *reason },
        Ending::TimedOut {} => StopReceiptEnding::TimedOut {},
    }
}

pub(super) fn erased_effects(
    command: Command,
    ending: StopReceiptEnding,
) -> Result<Vec<ApprovalEffect>, Rejected> {
    let receipt = match command {
        Command::Raise { .. } => ApprovalReceipt::Raise(RaiseReply::AlreadyEnded {}),
        Command::HolderStops => stopped_receipt(ending),
        Command::Report {
            foreign_run: false,
            foreign_request: false,
            ..
        } => ApprovalReceipt::Report(ReportDeliveryReply::NoHandoverOwed {}),
        Command::Erase | Command::Report { .. } | Command::Offer { foreign: true } => {
            return Err(Rejected::Stale);
        }
        Command::Offer { foreign: false } => return Err(domain(ApprovalRefusal::NoHandoverOwed)),
        Command::ClockTicks | Command::RunEnds | Command::SessionEnds | Command::EnrollmentEnds => {
            return Err(Rejected::NotAllowed {
                state: "erased",
                event: command_name(command),
            });
        }
        _ => return Err(hidden()),
    };
    Ok(vec![ApprovalEffect::Reply(receipt)])
}

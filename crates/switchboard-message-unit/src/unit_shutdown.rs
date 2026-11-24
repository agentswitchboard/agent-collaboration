use std::time::Duration;

use router_clock::{RuntimeClock, timeout, timeout_at};
use switchboard_link_lease::LeaseJoin;
use switchboard_message_custody::custody_task::CustodyRunFailure;

use crate::{FrontRefusal, MessageUnit, message_unit::OwnedLinkTask};

const DRIVER_BUDGET: Duration = Duration::from_secs(5);
const CUSTODY_BUDGET: Duration = Duration::from_secs(5);
const SETTINGS_BUDGET: Duration = Duration::from_secs(2);
const LEASE_BUDGET: Duration = Duration::from_secs(5);

#[derive(Debug, Eq, PartialEq)]
pub enum PieceStop<Outcome> {
    Joined(Outcome),
    TimedOut,
    JoinUnconfirmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkTaskFailure {
    Panicked,
    Cancelled,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MessageUnitStopReport {
    pub driver: PieceStop<Result<(), LinkTaskFailure>>,
    pub custody: PieceStop<Result<(), CustodyRunFailure>>,
    pub settings: PieceStop<Result<(), LinkTaskFailure>>,
    pub lease: PieceStop<LeaseJoin>,
}

impl MessageUnit {
    /// Withdraws the front, then requests and joins each piece in dependency order.
    pub async fn stop(mut self) -> MessageUnitStopReport {
        let _ = self
            .publication
            .send_replace(Err(FrontRefusal::ShuttingDown));
        let clock = self.clock.as_ref();
        let driver = stop_link_task(&mut self.driver, clock, DRIVER_BUDGET).await;

        self.custody.request_stop();
        let custody = match timeout(clock, CUSTODY_BUDGET, self.custody.join()).await {
            Ok(outcome) => PieceStop::Joined(outcome),
            Err(_) => PieceStop::TimedOut,
        };

        let settings = stop_link_task(&mut self.settings, clock, SETTINGS_BUDGET).await;

        let lease_cutoff = clock.monotonic_now().after(LEASE_BUDGET);
        self.lease.request_stop_by(lease_cutoff);
        let lease = match timeout_at(clock, lease_cutoff, async {
            if matches!(driver, PieceStop::TimedOut) {
                // A timed-out driver may still report a definitive enrollment end.
                let _ = (&mut self.driver.join).await;
            }
            self.lease.seal_ending_reports();
            self.lease.join().await
        })
        .await
        {
            Ok(Ok(outcome)) => PieceStop::Joined(outcome),
            Ok(Err(_)) => PieceStop::JoinUnconfirmed,
            Err(_) => PieceStop::TimedOut,
        };

        MessageUnitStopReport {
            driver,
            custody,
            settings,
            lease,
        }
    }
}

async fn stop_link_task(
    task: &mut OwnedLinkTask,
    clock: &dyn RuntimeClock,
    budget: Duration,
) -> PieceStop<Result<(), LinkTaskFailure>> {
    task.stop.send_replace(true);
    match timeout(clock, budget, &mut task.join).await {
        Ok(result) => PieceStop::Joined(result.map_err(|error| {
            if error.is_panic() {
                LinkTaskFailure::Panicked
            } else {
                LinkTaskFailure::Cancelled
            }
        })),
        Err(_) => {
            task.join.abort();
            PieceStop::TimedOut
        }
    }
}

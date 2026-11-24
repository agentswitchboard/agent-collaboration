use std::{pin::pin, sync::Arc, time::Duration};

use futures_util::{FutureExt as _, future::BoxFuture};
use router_clock::{RuntimeClock, timeout_at};
use switchboard_machine_enrollment_store::MachineEnrollmentStore;
use tokio::{sync::mpsc, task::AbortHandle};

use crate::message_unit::{MessageUnit, OwnedLinkTask};
use crate::{Event, LeaseStopResult, StopReport, StopResult, StopTarget, UnitPiece};

pub(crate) fn stop_part(
    target: StopTarget,
    budget: Duration,
    unit: Option<&MessageUnit>,
    store: Option<&MachineEnrollmentStore>,
    clock: Arc<dyn RuntimeClock>,
    events: mpsc::UnboundedSender<Event>,
) -> BoxFuture<'static, ()> {
    let deadline = clock.monotonic_now().after(budget);
    let mut abort = None;
    let joined =
        match target {
            StopTarget::Unit { generation, piece } => match unit {
                Some(unit) => match piece {
                    UnitPiece::Driver => stop_task(&unit.driver, target, &mut abort),
                    UnitPiece::Settings => stop_task(&unit.settings, target, &mut abort),
                    UnitPiece::Custody => {
                        let custody = unit.custody.clone();
                        custody.request_stop();
                        async move {
                            let _joined = custody.join().await;
                            Some(bare_report(target, StopResult::Joined))
                        }
                        .boxed()
                    }
                    UnitPiece::LeaseAdapter => {
                        let lease = unit.lease.clone();
                        lease.request_stop_by(deadline);
                        async move {
                            let joined = lease.join().await.ok()?;
                            let unsettled_retirements = joined
                                .unsettled_retirements
                                .into_iter()
                                .map(|key| key.enrollment_id.to_string().parse())
                                .collect::<Result<_, _>>()
                                .ok()?;
                            Some(StopReport::LeaseAdapter {
                                generation,
                                result: LeaseStopResult::Joined {
                                    unsettled_retirements,
                                },
                            })
                        }
                        .boxed()
                    }
                },
                None => futures_util::future::ready(Some(bare_report(target, StopResult::Joined)))
                    .boxed(),
            },
            StopTarget::EnrollmentStore => match store {
                Some(store) => {
                    let store = store.clone();
                    store.request_close();
                    async move {
                        store.join().await.ok()?;
                        Some(StopReport::EnrollmentStore(StopResult::Joined))
                    }
                    .boxed()
                }
                None => futures_util::future::ready(Some(bare_report(target, StopResult::Joined)))
                    .boxed(),
            },
        };
    async move {
        let mut joined = pin!(joined);
        match timeout_at(clock.as_ref(), deadline, &mut joined).await {
            Ok(Some(report)) => send(&events, report),
            Ok(None) => {
                // An unconfirmed join cannot retire the owner. Report it at the
                // budget boundary, even when the observer disappears earlier.
                clock.sleep_until(deadline).await;
                send(&events, bare_report(target, StopResult::TimedOut));
            }
            Err(_) => {
                send(&events, bare_report(target, StopResult::TimedOut));
                if let Some(task) = abort {
                    task.abort();
                }
                if let Some(report) = joined.await {
                    send(&events, report);
                }
            }
        }
    }
    .boxed()
}

fn stop_task(
    task: &OwnedLinkTask,
    target: StopTarget,
    abort: &mut Option<AbortHandle>,
) -> BoxFuture<'static, Option<StopReport>> {
    task.stop.send_replace(true);
    *abort = Some(task.abort.clone());
    task.joined
        .clone()
        .map(move |()| Some(bare_report(target, StopResult::Joined)))
        .boxed()
}

fn send(events: &mpsc::UnboundedSender<Event>, report: StopReport) {
    let _ = events.send(Event::StopFinished(report));
}

fn bare_report(target: StopTarget, result: StopResult) -> StopReport {
    match target {
        StopTarget::EnrollmentStore => StopReport::EnrollmentStore(result),
        StopTarget::Unit { generation, piece } => match piece {
            UnitPiece::Driver => StopReport::Driver { generation, result },
            UnitPiece::Custody => StopReport::Custody { generation, result },
            UnitPiece::Settings => StopReport::Settings { generation, result },
            UnitPiece::LeaseAdapter => StopReport::LeaseAdapter {
                generation,
                result: match result {
                    StopResult::Joined => LeaseStopResult::Joined {
                        unsettled_retirements: Default::default(),
                    },
                    StopResult::TimedOut => LeaseStopResult::TimedOut,
                },
            },
        },
    }
}

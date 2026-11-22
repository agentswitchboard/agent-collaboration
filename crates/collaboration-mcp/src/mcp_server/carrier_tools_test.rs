use super::{observation_deadline, stream_observed_events};
use collaboration_protocol::ObservationEventNotification;
use router_clock::{ManualClock, RuntimeClock};
use serde_json::{Value, json};
use std::{cell::RefCell, future::pending, pin::pin, task::Poll, time::Duration};
use tokio::sync::mpsc;

const BOUND: Duration = Duration::from_secs(30);

fn event(index: u64) -> ObservationEventNotification {
    ObservationEventNotification::native_event(json!({ "index": index }))
}

// Rejects a blocked notification holding the result past the observation's bound.
#[tokio::test]
async fn a_blocked_notification_does_not_hold_the_observation_past_its_bound() {
    let clock = ManualClock::new(Default::default());
    let deadline = observation_deadline(&clock, BOUND.as_secs());
    let (observed, mut streamed) = mpsc::unbounded_channel();
    let observation = async {
        for index in 1..=2 {
            observed.send(event(index)).unwrap();
        }
        clock.sleep_until(deadline).await;
        "deadlineReached"
    };
    let attempted = RefCell::new(Vec::new());
    let blocked = |event: ObservationEventNotification| {
        attempted.borrow_mut().push(event.event);
        pending::<()>()
    };

    let mut call = pin!(stream_observed_events(
        &clock,
        deadline,
        &mut streamed,
        observation,
        blocked,
    ));
    assert_eq!(futures_util::poll!(&mut call), Poll::Pending);
    assert!(clock.has_not_due_check_now(deadline));
    assert_eq!(*attempted.borrow(), vec![event(1).event]);
    clock.advance(BOUND).unwrap();

    assert_eq!(
        futures_util::poll!(&mut call),
        Poll::Ready("deadlineReached")
    );
    assert_eq!(*attempted.borrow(), vec![event(1).event]);
}

// Rejects a drain that ends early or waits on a clock other than the Host's clock.
#[tokio::test]
async fn an_observation_that_ends_early_drains_until_the_injected_deadline() {
    let clock = ManualClock::new(Default::default());
    let deadline = observation_deadline(&clock, BOUND.as_secs());
    let (observed, mut streamed) = mpsc::unbounded_channel();
    let observation = async move {
        let events = (1..=3).map(event).collect::<Vec<_>>();
        for event in &events {
            observed.send(event.clone()).unwrap();
        }
        events
            .into_iter()
            .map(|event| event.event)
            .collect::<Vec<_>>()
    };
    let delivered = RefCell::new(Vec::new());
    let notify = |notification: ObservationEventNotification| {
        let delivered = &delivered;
        async move {
            if notification.event == event(1).event {
                delivered.borrow_mut().push(notification.event);
            } else {
                pending::<()>().await;
            }
        }
    };

    let mut call = pin!(stream_observed_events(
        &clock,
        deadline,
        &mut streamed,
        observation,
        notify,
    ));
    assert_eq!(futures_util::poll!(&mut call), Poll::Pending);
    assert!(clock.has_not_due_check_now(deadline));
    clock.advance(BOUND - Duration::from_secs(1)).unwrap();
    assert_eq!(futures_util::poll!(&mut call), Poll::Pending);
    clock.advance(Duration::from_secs(2)).unwrap();

    let Poll::Ready(events) = futures_util::poll!(&mut call) else {
        panic!("notification drain outlived its injected deadline");
    };
    assert_eq!(
        events,
        (1..=3).map(|index| event(index).event).collect::<Vec<_>>()
    );
    assert_eq!(*delivered.borrow(), events[..1]);
}

// Rejects a carrier that reorders or drops notifications while the reader keeps up.
#[tokio::test]
async fn a_reader_that_keeps_up_gets_every_notification_in_order_at_once() {
    let clock = ManualClock::new(Default::default());
    let deadline = observation_deadline(&clock, BOUND.as_secs());
    let (observed, mut streamed) = mpsc::unbounded_channel();
    let observation = async move {
        for index in 1..=3 {
            observed.send(event(index)).unwrap();
            tokio::task::yield_now().await;
        }
        "resultLimitReached"
    };
    let delivered = RefCell::new(Vec::<Value>::new());

    let result = stream_observed_events(&clock, deadline, &mut streamed, observation, |sent| {
        delivered.borrow_mut().push(sent.event);
        async {}
    })
    .await;

    assert_eq!(result, "resultLimitReached");
    assert_eq!(
        delivered.into_inner(),
        (1..=3).map(|index| event(index).event).collect::<Vec<_>>()
    );
    assert_eq!(clock.monotonic_now(), router_clock::ClockInstant::ZERO);
}

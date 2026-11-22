use super::*;
use hegel::generators as gs;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn poll_once<Output>(future: Pin<&mut (impl Future<Output = Output> + ?Sized)>) -> Poll<Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready_reply<Reply>(admission: ReplyAdmission<Reply>) -> Box<dyn ReservedReply<Reply>> {
    match admission {
        ReplyAdmission::Reserved(reply) => reply,
        ReplyAdmission::Refused(reason) => panic!("expected reserved reply, received {reason:?}"),
    }
}

fn refusal<Reply>(admission: ReplyAdmission<Reply>) -> Option<WriterState> {
    match admission {
        ReplyAdmission::Reserved(_) => None,
        ReplyAdmission::Refused(reason) => Some(reason),
    }
}

fn capacity(slots: usize) -> NonZeroUsize {
    NonZeroUsize::new(slots).expect("test capacity is positive")
}

fn received<Address, Message>(
    receiver: &mut CarrierReceiver<Address, Message>,
) -> Option<(Address, Message)> {
    let mut next = Box::pin(receiver.recv());
    match poll_once(next.as_mut()) {
        Poll::Ready(Some(message)) => Some(message.process(|address, message| (address, message))),
        Poll::Ready(None) | Poll::Pending => None,
    }
}

#[test]
fn reply_admission_distinguishes_full_from_stopped() {
    let (sender, receiver) = reply_carrier::<u8, u8>(NonZeroUsize::MIN);
    let held = ready_reply(sender.reserve(20, std::convert::identity));
    assert_eq!(
        refusal(sender.reserve(21, std::convert::identity)),
        Some(WriterState::Full),
        "a live writer with its slot reserved is full"
    );
    drop(held);
    drop(receiver);
    assert_eq!(
        refusal(sender.reserve(21, std::convert::identity)),
        Some(WriterState::Stopped),
        "a dropped receiver is stopped"
    );
}

#[test]
fn dropping_a_reserved_reply_releases_its_only_slot() {
    let (sender, mut receiver) = reply_carrier::<u8, u8>(NonZeroUsize::MIN);
    let reserved = ready_reply(sender.reserve(20, std::convert::identity));
    drop(reserved);
    assert!(
        matches!(
            sender.reserve(21, std::convert::identity),
            ReplyAdmission::Reserved(_)
        ),
        "dropping the exact unused reply must release its reservation"
    );
    assert_eq!(received(&mut receiver), None, "dropping emits no reply");
}

#[derive(Debug, Eq, PartialEq)]
enum IngressMessage {
    Settled { attempt: u8, payload: u8 },
    Observed(&'static str),
}

#[test]
fn typed_stamps_and_mapping_share_the_private_fifo() {
    let (sender, mut receiver) = reply_carrier(capacity(3));
    let retired = CancellationToken::new();
    let attempt = 37;
    let reserved = ready_reply(sender.reserve(13, move |payload| IngressMessage::Settled {
        attempt,
        payload,
    }));
    let events: Box<dyn EventPort<&'static str>> = {
        let binding = sender;
        let scope = retired;
        Box::new(binding.event_port(29, &scope, IngressMessage::Observed))
    };
    assert_eq!(
        poll_once(events.send("first").as_mut()),
        Poll::Ready(Ok(()))
    );
    reserved.send(63);
    assert_eq!(
        received(&mut receiver),
        Some((29, IngressMessage::Observed("first"))),
        "events retain the original scope even with a reply already reserved"
    );
    assert_eq!(
        received(&mut receiver),
        Some((
            13,
            IngressMessage::Settled {
                attempt: 37,
                payload: 63
            }
        )),
        "the reply must retain its reservation's scope"
    );
    assert_eq!(received(&mut receiver), None);
}

#[test]
fn one_streamed_event_can_occupy_the_only_slot() {
    let (sender, mut receiver) = reply_carrier(NonZeroUsize::MIN);
    let retired = CancellationToken::new();
    let events = sender.event_port(19, &retired, std::convert::identity);
    assert_eq!(
        poll_once(events.send(22).as_mut()),
        Poll::Ready(Ok(())),
        "capacity one still provides one stream credit"
    );
    assert_eq!(
        refusal(sender.reserve(16, std::convert::identity)),
        Some(WriterState::Full)
    );
    assert_eq!(received(&mut receiver), Some((19, 22)));
}

#[test]
fn stream_credit_is_held_through_receiver_processing_and_released_on_error() {
    let (sender, mut receiver) = reply_carrier(capacity(2));
    let retired = CancellationToken::new();
    let events = sender.event_port(5, &retired, std::convert::identity);
    assert_eq!(poll_once(events.send(11).as_mut()), Poll::Ready(Ok(())));
    let message = receiver
        .receiver
        .try_recv()
        .expect("first queued observation");
    let mut waiting = events.send(12);
    let mut while_processing = None;
    let result = message.process(|address, payload| {
        while_processing = Some((address, payload, poll_once(waiting.as_mut())));
        Err("outbox rejected the effect")
    });
    assert_eq!(result, Err::<(), _>("outbox rejected the effect"));
    assert_eq!(
        while_processing,
        Some((5, 11, Poll::Pending)),
        "stream credit is held until processing finishes even on error"
    );
    assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(Ok(())));
    assert_eq!(received(&mut receiver), Some((5, 12)));
}

#[test]
fn retirement_wins_an_enqueue_tie_and_keeps_an_already_enqueued_event() {
    let (sender, mut receiver) = reply_carrier(capacity(2));
    let retired = CancellationToken::new();
    let events = sender.event_port(20, &retired, std::convert::identity);
    assert_eq!(poll_once(events.send(30).as_mut()), Poll::Ready(Ok(())));
    let queued = receiver
        .receiver
        .try_recv()
        .expect("enqueued before retirement");

    let mut second = events.send(31);
    assert_eq!(poll_once(second.as_mut()), Poll::Pending, "credit is held");
    let already_enqueued = queued.process(|address, message| {
        retired.cancel();
        (address, message)
    });
    assert_eq!(
        poll_once(second.as_mut()),
        Poll::Ready(Err(EventRefusal::Retired)),
        "retirement wins even when credit and writer room become available"
    );
    assert_eq!(
        already_enqueued,
        (20, 30),
        "retirement does not recall ingress"
    );
    assert_eq!(received(&mut receiver), None);
}

#[test]
fn stopping_the_receiver_refuses_streams() {
    let (sender, receiver) = reply_carrier(NonZeroUsize::MIN);
    let retired = CancellationToken::new();
    let events = sender.event_port(11, &retired, std::convert::identity);
    drop(receiver);

    assert_eq!(
        poll_once(events.send(21).as_mut()),
        Poll::Ready(Err(EventRefusal::Stopped)),
        "a live scope cannot enqueue to a stopped writer"
    );
}

#[hegel::test(seed = Some(31337), test_cases = 100)]
fn generated_carrier_orders_match_a_slot_and_credit_model(case: hegel::TestCase) {
    let slots = case.draw(gs::integers::<usize>().min_value(1).max_value(6));
    let steps = case.draw(gs::integers::<u8>().min_value(1).max_value(40));
    let (sender, mut receiver) = reply_carrier(capacity(slots));
    let stop = CancellationToken::new();
    let stream = sender.event_port(100, &stop, std::convert::identity);
    let mut model_fifo = VecDeque::new();
    let mut reservations = VecDeque::new();
    let mut real_reservations = VecDeque::new();
    let mut retired = false;
    let stream_quota = if slots < 2 { 1 } else { slots / 2 };

    for stamp in 0..steps {
        let occupied = model_fifo.len() + reservations.len();
        match case.draw(gs::integers::<u8>().max_value(5)) {
            0 => {
                let reply = sender.reserve(stamp, std::convert::identity);
                if occupied < slots {
                    reservations.push_back(stamp);
                    real_reservations.push_back(ready_reply(reply));
                } else {
                    assert_eq!(refusal(reply), Some(WriterState::Full));
                }
            }
            1 => {
                if let Some(address) = reservations.pop_front() {
                    real_reservations
                        .pop_front()
                        .expect("reserved slot")
                        .send(stamp);
                    model_fifo.push_back((address, stamp));
                }
            }
            2 => {
                reservations.pop_front();
                drop(real_reservations.pop_front());
            }
            3 => {
                let streaming = model_fifo
                    .iter()
                    .filter(|(address, _)| *address == 100)
                    .count();
                let expected = if retired {
                    Poll::Ready(Err(EventRefusal::Retired))
                } else if occupied < slots && streaming < stream_quota {
                    model_fifo.push_back((100, stamp));
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                };
                assert_eq!(poll_once(stream.send(stamp).as_mut()), expected);
            }
            4 => assert_eq!(received(&mut receiver), model_fifo.pop_front()),
            _ => {
                retired = true;
                stop.cancel();
            }
        }
    }

    for message in model_fifo {
        assert_eq!(received(&mut receiver), Some(message));
    }
    assert_eq!(received(&mut receiver), None);
}

#[test]
fn retirement_does_not_recall_an_event_still_queued() {
    let (sender, mut receiver) = reply_carrier(NonZeroUsize::MIN);
    let retired = CancellationToken::new();
    let events = sender.event_port(11, &retired, std::convert::identity);
    assert_eq!(poll_once(events.send(32).as_mut()), Poll::Ready(Ok(())));
    retired.cancel();

    assert_eq!(
        received(&mut receiver),
        Some((11, 32)),
        "an event accepted by ingress survives retirement while queued"
    );
}

#[derive(Default)]
struct RecordedWake(std::sync::atomic::AtomicBool);

impl std::task::Wake for RecordedWake {
    fn wake(self: Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn receiver_stop_wakes_a_send_blocked_by_a_retained_message() {
    let (sender, mut receiver) = reply_carrier(NonZeroUsize::MIN);
    let retired = CancellationToken::new();
    let events = sender.event_port(17, &retired, std::convert::identity);
    assert_eq!(poll_once(events.send(31).as_mut()), Poll::Ready(Ok(())));
    let held = receiver
        .receiver
        .try_recv()
        .expect("extract the first observation but keep its credit");

    let wake = Arc::new(RecordedWake::default());
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut second = events.send(32);
    assert_eq!(second.as_mut().poll(&mut context), Poll::Pending);
    assert!(!wake.0.load(std::sync::atomic::Ordering::Relaxed));

    drop(receiver);
    let woke = wake.0.load(std::sync::atomic::Ordering::Relaxed);
    let stopped = second.as_mut().poll(&mut context);
    assert_eq!(
        (woke, stopped),
        (true, Poll::Ready(Err(EventRefusal::Stopped))),
        "receiver closure must wake and refuse a send while a retained message still owns all credit"
    );
    assert_eq!(
        held.process(|address, message| (address, message)),
        (17, 31)
    );
}

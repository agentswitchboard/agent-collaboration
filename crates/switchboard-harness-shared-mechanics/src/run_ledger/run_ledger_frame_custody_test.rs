use std::{cell::Cell, rc::Rc};

use super::test_fixtures::{authorization, close_attempt, test_run};
use super::*;

#[derive(Debug)]
struct CountedFrame {
    drops: Rc<Cell<usize>>,
    content: &'static str,
}

impl CountedFrame {
    fn new(content: &'static str) -> (Self, Rc<Cell<usize>>) {
        let drops = Rc::new(Cell::new(0));
        (
            Self {
                drops: Rc::clone(&drops),
                content,
            },
            drops,
        )
    }
}

impl Drop for CountedFrame {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

fn expect_frame_returned(
    ledger: &mut RunLedger<CountedFrame>,
    key: DispatchAuthorization,
    expected: OfferRefusal,
) {
    let (frame, drops) = CountedFrame::new("refused frame");
    let RefusedOffer { refusal, frame } = ledger.offer(key, None, frame).unwrap_err();

    assert_eq!(refusal, expected);
    assert_eq!(frame.content, "refused frame");
    assert!(Rc::ptr_eq(&frame.drops, &drops));
    assert_eq!(drops.get(), 0);
    drop(frame);
    assert_eq!(drops.get(), 1);
}

#[test]
fn each_offer_refusal_returns_its_original_frame_without_dropping_it() {
    let run = test_run(1, 1);
    let key = authorization(run, 1, 1, AuthorizationTarget::Session);
    let mut ledger = RunLedger::default();

    expect_frame_returned(&mut ledger, key.clone(), OfferRefusal::RouteUnavailable);
    assert_eq!(ledger.open_run(run, "session".into()), RunOpening::Opened);
    let attempt = close_attempt(1);
    ledger.begin_close(run).unwrap().mark(attempt);
    expect_frame_returned(&mut ledger, key.clone(), OfferRefusal::Closing);
    assert_eq!(ledger.readmit(run, attempt), ReadmitOutcome::Reopened);

    let missing_turn = authorization(run, 1, 1, AuthorizationTarget::NewTurn);
    expect_frame_returned(
        &mut ledger,
        missing_turn,
        OfferRefusal::WorkDoesNotFitAuthorization,
    );
    let (standing, drops) = CountedFrame::new("standing frame");
    ledger.offer(key.clone(), None, standing).unwrap();
    expect_frame_returned(
        &mut ledger,
        key,
        OfferRefusal::AuthorizationSequenceOnRecord,
    );
    let later = authorization(run, 1, 2, AuthorizationTarget::Session);
    expect_frame_returned(&mut ledger, later, OfferRefusal::OtherAuthorizationOffered);
    assert_eq!(
        drops.get(),
        0,
        "the refused frames cannot displace accepted work"
    );
}

#[test]
fn withdrawing_accepted_work_drops_the_frame_during_the_ledger_call() {
    #[derive(Clone, Copy, Debug)]
    enum Withdrawal {
        DirectRevoke,
        SettledCheck,
        CallerDeclined,
        Forget,
    }

    for withdrawal in [
        Withdrawal::DirectRevoke,
        Withdrawal::SettledCheck,
        Withdrawal::CallerDeclined,
        Withdrawal::Forget,
    ] {
        let run = test_run(1, 1);
        let key = authorization(run, 1, 1, AuthorizationTarget::Session);
        let mut ledger = RunLedger::default();
        assert_eq!(ledger.open_run(run, "session".into()), RunOpening::Opened);
        let (frame, drops) = CountedFrame::new("accepted");
        ledger.offer(key.clone(), None, frame).unwrap();
        assert_eq!(drops.get(), 0);

        match withdrawal {
            Withdrawal::DirectRevoke => {
                assert_eq!(
                    ledger.revoke(&key),
                    Some(DispatchResult::Revoked(key.clone()))
                );
            }
            Withdrawal::SettledCheck => {
                ledger.begin_close(run).unwrap().mark(close_attempt(1));
                assert!(matches!(
                    ledger.check_hand_over(&key),
                    Ok(HandOverCheck::Settled(DispatchResult::DefinitelyUnsent(authorization)))
                        if authorization == key
                ));
            }
            Withdrawal::CallerDeclined => {
                let Ok(HandOverCheck::Ready(ready)) = ledger.check_hand_over(&key) else {
                    panic!("the caller should be asked before the frame is taken");
                };
                assert!(matches!(
                    ready.commit(false),
                    HandOverCommit::Unsent(DispatchResult::DefinitelyUnsent(authorization))
                        if authorization == key
                ));
            }
            Withdrawal::Forget => ledger.forget(&key),
        }
        assert_eq!(
            drops.get(),
            1,
            "{withdrawal:?} drops the held frame before returning"
        );
        drop(ledger);
        assert_eq!(
            drops.get(),
            1,
            "{withdrawal:?} leaves no frame to drop later"
        );
    }
}

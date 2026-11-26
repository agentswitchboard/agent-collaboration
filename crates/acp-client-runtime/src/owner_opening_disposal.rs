//! Per-resource witnesses for disposal across the actual opening table lock.

use std::{
    cell::RefCell,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use switchboard_harness_contracts::{LifecycleReply, ReservedReply};

thread_local! {
    static HELD_TABLES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

pub(crate) struct OpeningTableScope(usize);

impl OpeningTableScope {
    pub(crate) fn enter<T>(table: &Mutex<T>) -> Self {
        let address = std::ptr::from_ref(table).addr();
        HELD_TABLES.with_borrow_mut(|held| held.push(address));
        Self(address)
    }
}

impl Drop for OpeningTableScope {
    fn drop(&mut self) {
        HELD_TABLES.with_borrow_mut(|held| {
            assert_eq!(held.pop(), Some(self.0), "opening table lock scope");
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OpeningDisposalKind {
    SdkSession,
    RestoreSignal,
    OpeningReply,
    ConfigureReply,
    CloseReply,
    SetterReply,
}

#[derive(Clone, Default)]
pub struct OpeningDisposalProbe(Arc<DisposalCounts>);

#[derive(Default)]
struct DisposalCounts {
    inside: AtomicUsize,
    outside: AtomicUsize,
}

impl OpeningDisposalProbe {
    pub(crate) fn bind<T>(&self, table: &Mutex<T>) -> OpeningDisposalToken {
        OpeningDisposalToken {
            table: std::ptr::from_ref(table).addr(),
            counts: Arc::clone(&self.0),
        }
    }

    pub fn assert_disposed_outside_table(&self) {
        assert_eq!(
            self.0.inside.load(Ordering::SeqCst),
            0,
            "owned opening resource was disposed while its table lock was held"
        );
        assert_eq!(
            self.0.outside.load(Ordering::SeqCst),
            1,
            "owned opening resource must be disposed exactly once outside its table lock"
        );
    }
}

pub(crate) struct OpeningDisposalToken {
    table: usize,
    counts: Arc<DisposalCounts>,
}

impl Drop for OpeningDisposalToken {
    fn drop(&mut self) {
        let inside = HELD_TABLES.with_borrow(|held| held.contains(&self.table));
        let counter = if inside {
            &self.counts.inside
        } else {
            &self.counts.outside
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) fn observe_reply<Fact: 'static>(
    reply: LifecycleReply<Fact>,
    witness: Option<OpeningDisposalToken>,
) -> LifecycleReply<Fact> {
    match witness {
        Some(witness) => Box::new(ObservedReply {
            reply,
            _witness: witness,
        }),
        None => reply,
    }
}

struct ObservedReply<Fact> {
    reply: LifecycleReply<Fact>,
    _witness: OpeningDisposalToken,
}

impl<Fact> ReservedReply<Fact> for ObservedReply<Fact> {
    fn send(self: Box<Self>, fact: Fact) {
        self.reply.send(fact);
    }
}

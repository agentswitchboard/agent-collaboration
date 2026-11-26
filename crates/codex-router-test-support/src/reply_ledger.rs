//! Check exact reserved reply custody across generated fixture schedules.

use std::{collections::BTreeMap, fmt::Debug};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplySite<Owner> {
    Reserved,
    Admitted(Owner),
    BufferedFromPeer(Owner),
    WorkerResult(Owner),
    Deferred(Owner),
    Delivered,
    Unoffered,
}

pub struct ReplyLedger<Id, Owner>(BTreeMap<Id, ReplySite<Owner>>);

impl<Id, Owner> Default for ReplyLedger<Id, Owner> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<Id: Copy + Ord + Debug, Owner: Copy + Eq + Debug> ReplyLedger<Id, Owner> {
    pub fn reserve(&mut self, id: Id) {
        assert_eq!(self.0.insert(id, ReplySite::Reserved), None);
    }

    pub fn move_to(&mut self, id: Id, before: ReplySite<Owner>, after: ReplySite<Owner>) {
        assert_eq!(
            self.0.insert(id, after),
            Some(before),
            "reply identity {id:?}"
        );
    }

    pub fn delivered(&mut self, reserved: Id, observed: Id, from: ReplySite<Owner>) {
        assert_eq!(
            observed, reserved,
            "reply envelope must settle its own reserved identity"
        );
        self.move_to(observed, from, ReplySite::Delivered);
    }

    pub fn owed(&self) -> BTreeMap<Id, Owner> {
        self.0
            .iter()
            .filter_map(|(id, site)| match site {
                ReplySite::Admitted(owner)
                | ReplySite::BufferedFromPeer(owner)
                | ReplySite::WorkerResult(owner)
                | ReplySite::Deferred(owner) => Some((*id, *owner)),
                ReplySite::Reserved | ReplySite::Delivered | ReplySite::Unoffered => None,
            })
            .collect()
    }

    pub fn assert_conserved(
        &self,
        mut owed_before: BTreeMap<Id, Owner>,
        newly_admitted: &[(Id, Owner)],
        held: &[(Id, Owner)],
        delivered: &[Id],
    ) {
        for (id, owner) in newly_admitted {
            assert!(
                owed_before.insert(*id, *owner).is_none(),
                "reply admitted twice"
            );
        }
        let accounted = self.owed();
        assert_eq!(accounted, held.iter().copied().collect::<BTreeMap<_, _>>());
        assert_eq!(accounted.len(), held.len(), "a holder was counted twice");
        for id in delivered {
            assert_eq!(self.0.get(id), Some(&ReplySite::Delivered));
            assert!(
                !accounted.contains_key(id),
                "a delivered reply is still held"
            );
            assert!(
                owed_before.remove(id).is_some(),
                "reply was unowed or delivered twice"
            );
        }
        assert_eq!(accounted, owed_before);
    }
}

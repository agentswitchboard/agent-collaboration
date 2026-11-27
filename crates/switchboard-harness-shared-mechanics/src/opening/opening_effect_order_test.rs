//! Observe the order in which real transition effects release owned values.
use super::*;
use crate::opening::{
    LifecycleRoute, OpeningEffects, StepFaultReport, opening_test_adapter::OrderWitness,
};
use switchboard_harness_contracts::RestoreVia;
use tokio::sync::oneshot;

struct RecordRetirement {
    order: OrderWitness,
    route: LifecycleRoute,
    instance: OpeningInstance,
}

impl OpeningEffects for RecordRetirement {
    fn retire_opening_route(&self, route: &LifecycleRoute, instance: &OpeningInstance) {
        assert!(self.route.is_same_as(route));
        assert!(self.instance.is_same_as(instance));
        self.order.record("retire route");
    }

    fn report_step_fault(&self, _: &StepFaultReport) {
        panic!("the declared restore and close effects should agree with their steps");
    }
}

#[tokio::test]
async fn resolving_restore_retires_before_sending_the_decision() {
    let fixture = Fixture::new();
    let instance = OpeningInstance::default();
    let order = OrderWitness::default();
    let (send, mut receive) = oneshot::channel();
    let state = OpeningState::RouteLost {
        run: fixture.run.clone(),
        receipt: LostReceipt::Open(RestoreReceiptStage::AwaitingPeer),
    };
    let command = Command::RestoreProgress(
        RestoreMilestone::held(
            TestOpenedSession::ordered(order.clone(), "drop milestone"),
            None,
            None,
            false,
        ),
        Some(TestRestoreSignal::ordered(send, order.clone())),
    );
    let Ok((_, effect @ Effect::DiscardAfter(_, _))) =
        step::<TestOpening>(state, command, &instance, false)
    else {
        panic!("the real late restore must resolve with a retiring release");
    };

    let Effect::DiscardAfter(_, next) = &effect else {
        panic!("the late milestone must be held until the effect runs");
    };
    assert!(matches!(
        next.as_ref(),
        Effect::ResolveRestore {
            release: Some(_),
            ..
        }
    ));
    assert!(effect.finish(&RecordRetirement {
        order: order.clone(),
        route: fixture.run.route,
        instance
    }));
    order.assert_order(&["drop milestone", "retire route", "send decision"]);
    assert!(matches!(
        receive.try_recv(),
        Ok(RestoreDecision::LocallyUnavailable(RestoreVia::Resume))
    ));
}

#[tokio::test]
async fn deferred_restore_drops_its_milestone_then_signals_then_retires() {
    let fixture = Fixture::new();
    let instance = OpeningInstance::default();
    let order = OrderWitness::default();
    let (send, mut receive) = oneshot::channel();
    let milestone = RestoreMilestone::held(
        TestOpenedSession::ordered(order.clone(), "drop milestone"),
        None,
        None,
        true,
    );
    let state = OpeningState::OfferingClose {
        origin: Box::new(OpeningState::AwaitingRestore(fixture.run.clone())),
        attempt: fixture.close.attempt,
        offer: OpeningOffer::default(),
        deferred: Some(DeferredRestore {
            milestone,
            outcome: TestRestoreSignal::ordered(send, order.clone()),
        }),
    };
    let command = Command::RouteRemoved(fixture.run.route.clone());
    let Ok((_, effect @ Effect::ResolveDeferredRestore { .. })) =
        step::<TestOpening>(state, command, &instance, false)
    else {
        panic!("the real route loss must settle the restore deferred under close");
    };

    assert!(effect.finish(&RecordRetirement {
        order: order.clone(),
        route: fixture.run.route,
        instance
    }));
    order.assert_order(&["drop milestone", "send decision", "retire route"]);
    assert!(matches!(
        receive.try_recv(),
        Ok(RestoreDecision::LocallyUnavailable(_))
    ));
}

#[tokio::test]
async fn confirmed_close_drops_the_held_origin_before_sending_superseded() {
    let fixture = Fixture::new();
    let instance = OpeningInstance::default();
    let order = OrderWitness::default();
    let (send, mut receive) = oneshot::channel();
    let origin = OpeningState::PublishingReplay(RoutedRestore {
        run: fixture.run.clone(),
        held: Box::new(HeldRestore {
            session: Box::new(TestOpenedSession::ordered(order.clone(), "drop origin")),
            accepted_mode_fallback: None,
            skills: None,
            setters: Vec::new(),
        }),
    });
    let state = OpeningState::OfferingClose {
        origin: Box::new(origin),
        attempt: fixture.close.attempt,
        offer: OpeningOffer::default(),
        deferred: Some(DeferredRestore {
            milestone: RestoreMilestone::ReplayAdmitted,
            outcome: TestRestoreSignal::ordered(send, order.clone()),
        }),
    };
    let Ok((_, effect @ Effect::CloseConfirmed(..))) = step::<TestOpening>(
        state,
        Command::CloseOffered(fixture.close, true),
        &instance,
        true,
    ) else {
        panic!("the real close confirmation must hold a non-retiring release");
    };

    assert!(effect.finish(&RecordRetirement {
        order: order.clone(),
        route: fixture.run.route,
        instance
    }));
    order.assert_order(&["drop origin", "send decision"]);
    assert!(matches!(
        receive.try_recv(),
        Ok(RestoreDecision::Superseded)
    ));
}

//! Exercise independently chosen payloads through the owned opening step.

use super::*;
use provider_session_machines::harness_lifecycle::LifecycleRefusal;

type Applied =
    Result<(Option<super::super::super::OpeningRow>, Effect), super::super::super::Refused>;

impl Fixture {
    fn apply_payload(
        &self,
        state: OpeningState,
        command: Command,
        instance: &OpeningInstance,
    ) -> Applied {
        let row = super::super::super::OpeningRow {
            attempt: self.attempt,
            instance: instance.clone(),
            state,
        };
        let _table = lock(&self.table);
        row.apply(instance, command)
    }
}

#[derive(Clone, Copy, Debug)]
enum ReaderOrigin {
    Active,
    UntakenClose,
    TakenClose,
    ClosingLive,
    CloseRefused,
    Readmitting,
}

#[tokio::test]
async fn every_reader_ending_obeys_its_reason_and_live_origin() {
    let fixture = Fixture::new().await;
    let reasons = [
        (ReaderEnd::Retired, false),
        (ReaderEnd::Finished, false),
        (ReaderEnd::Failed, true),
        (ReaderEnd::IngressClosed, false),
        (ReaderEnd::HandoffClosed, true),
    ];
    let origins = [
        (ReaderOrigin::Active, None, None),
        (ReaderOrigin::UntakenClose, Some(P::RouteLost), None),
        (
            ReaderOrigin::TakenClose,
            Some(P::RouteLost),
            Some(fixture.close.attempt),
        ),
        (
            ReaderOrigin::ClosingLive,
            Some(P::RouteLost),
            Some(fixture.close.attempt),
        ),
        (ReaderOrigin::CloseRefused, Some(P::RouteLost), None),
        (ReaderOrigin::Readmitting, Some(P::RouteLost), None),
    ];
    let mut visited = [[false; 5]; 6];
    for (origin_index, (origin, expected_phase, expected_close)) in origins.into_iter().enumerate()
    {
        for (reason_index, (reason, should_report)) in reasons.into_iter().enumerate() {
            let run = fixture.run.clone();
            let attempt = fixture.close.attempt;
            let state = match origin {
                ReaderOrigin::Active => OpeningState::Active(run),
                ReaderOrigin::UntakenClose | ReaderOrigin::TakenClose => {
                    let offer = OpeningOffer::default();
                    if let ReaderOrigin::TakenClose = origin {
                        assert!(offer.take());
                    }
                    OpeningState::OfferingClose {
                        origin: Box::new(OpeningState::Active(run)),
                        attempt,
                        offer,
                        deferred: None,
                    }
                }
                ReaderOrigin::ClosingLive => OpeningState::ClosingLive { run, attempt },
                ReaderOrigin::CloseRefused => OpeningState::CloseRefusedLive { run, attempt },
                ReaderOrigin::Readmitting => OpeningState::Readmitting { run, attempt },
            };
            let instance = OpeningInstance::default();
            let Ok((
                stored,
                Effect::ReaderEnded {
                    release,
                    report_loss,
                },
            )) = fixture.apply_payload(state, Command::ReaderStopped(reason), &instance)
            else {
                panic!("lost reader-ending payload for {origin:?}, reason index {reason_index}");
            };
            assert_eq!(
                report_loss, should_report,
                "reader ending must report the selected reason for {origin:?}, reason index {reason_index}"
            );
            assert!(release.retire);
            assert!(release.instance.is_same_as(&instance));
            assert!(
                release
                    .state
                    .run()
                    .expect("retiring exact live run")
                    .route
                    .is_same_as(&fixture.run.route)
            );
            assert_eq!(stored.as_ref().map(|row| row.phase()), expected_phase);
            if let Some(stored) = stored {
                assert_eq!(stored.attempt(), fixture.attempt);
                assert!(stored.instance().is_same_as(&instance));
                let OpeningState::RouteLost { run, receipt } = stored.state else {
                    panic!("reader loss must preserve route-loss evidence");
                };
                assert!(run.route.is_same_as(&fixture.run.route));
                let owed = match receipt {
                    LostReceipt::None => None,
                    LostReceipt::Close(close) => Some(close),
                    LostReceipt::Open(_) => panic!("a live reader cannot owe a restore receipt"),
                };
                assert_eq!(owed, expected_close);
            }
            visited[origin_index][reason_index] = true;
        }
    }
    assert_eq!(visited, [[true; 5]; 6]);
}

#[tokio::test]
async fn readmission_receipts_preserve_or_retire_only_their_exact_route() {
    let fixture = Fixture::new().await;
    let cases = [
        (ReadmitOutcome::Reopened, P::Active),
        (ReadmitOutcome::MarkerAbsent, P::Active),
        (ReadmitOutcome::RouteLost, P::RouteLost),
        (ReadmitOutcome::DifferentMarker, P::RouteLost),
    ];
    let mut visited = [[false; 2]; 4];
    for (index, (outcome, expected)) in cases.into_iter().enumerate() {
        for (identity, exact) in [false, true].into_iter().enumerate() {
            let instance = OpeningInstance::default();
            let attempt = fixture.close.attempt;
            let result = fixture.apply_payload(
                OpeningState::Readmitting {
                    run: fixture.run.clone(),
                    attempt,
                },
                Command::ReadmitResult(
                    if exact {
                        attempt
                    } else {
                        CloseAttempt(Uuid::now_v7())
                    },
                    outcome,
                ),
                &instance,
            );
            if exact {
                let Ok((Some(stored), effect)) = result else {
                    panic!("exact readmission receipt rejected");
                };
                assert_eq!(stored.phase(), expected);
                assert_eq!(stored.attempt(), fixture.attempt);
                assert!(stored.instance().is_same_as(&instance));
                assert!(
                    stored
                        .state
                        .run()
                        .expect("readmission keeps exact route evidence")
                        .route
                        .is_same_as(&fixture.run.route)
                );
                assert_eq!(
                    actual_actions(&effect, &fixture, &instance),
                    if expected == P::Active {
                        vec![Action::Hold]
                    } else {
                        vec![Action::Remove]
                    }
                );
            } else {
                let Err(refused) = result else {
                    panic!("stale readmission receipt admitted");
                };
                assert_eq!(refused.rejection.reason, LifecycleRefusal::RouteClosing);
                let OpeningState::Readmitting {
                    run,
                    attempt: exact_attempt,
                } = refused.row.state
                else {
                    panic!("stale receipt changed the live readmission");
                };
                assert!(run.route.is_same_as(&fixture.run.route));
                assert_eq!(exact_attempt, attempt);
            }
            visited[index][identity] = true;
        }
    }
    assert_eq!(visited, [[true; 2]; 4]);
}

#[derive(Clone, Copy, Debug)]
enum CloseOrigin {
    AwaitingPeer,
    PublishingReplay,
    RestoreHeld,
    RestorePrepared,
    Active,
}

impl CloseOrigin {
    fn row(self, fixture: &Fixture) -> OpeningState {
        match self {
            Self::AwaitingPeer => OpeningState::AwaitingRestore(fixture.run.clone()),
            Self::PublishingReplay => OpeningState::PublishingReplay(fixture.owned_restore()),
            Self::RestoreHeld => OpeningState::HeldRestore(fixture.owned_restore()),
            Self::RestorePrepared => OpeningState::PreparedRestore(fixture.owned_restore()),
            Self::Active => OpeningState::Active(fixture.run.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum DeferredPayload {
    Empty,
    Loaded,
    Resumed,
    ReplayAdmitted,
    ReplayRejected,
    Failed,
    WorkerExited,
}

impl DeferredPayload {
    fn milestone(self, fixture: &Fixture) -> Option<RestoreMilestone> {
        Some(match self {
            Self::Empty => return None,
            Self::Loaded => {
                RestoreMilestone::held(*fixture.session(), Some("load-fallback".into()), None, true)
            }
            Self::Resumed => RestoreMilestone::held(
                *fixture.session(),
                Some("resume-fallback".into()),
                None,
                false,
            ),
            Self::ReplayAdmitted => RestoreMilestone::ReplayAdmitted,
            Self::ReplayRejected => RestoreMilestone::ReplayRejected,
            Self::Failed => RestoreMilestone::Failed,
            Self::WorkerExited => RestoreMilestone::WorkerExited,
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum CloseReceiptOwner {
    ClosingRestore,
    ClosingLive,
    LostRoute,
    StoppedWriter,
}

#[tokio::test]
async fn close_receipts_check_kind_attempt_and_generation_before_retiring() {
    use CloseReceiptOwner as Owner;
    let fixture = Fixture::new().await;
    let attempts = [
        fixture.close,
        RunAttempt {
            attempt: CloseAttempt(Uuid::now_v7()),
            ..fixture.close
        },
        RunAttempt {
            generation: SessionGeneration::from_number(2),
            ..fixture.close
        },
    ];
    let owners = [
        (Owner::ClosingRestore, LifecycleRefusal::RouteClosing),
        (Owner::ClosingLive, LifecycleRefusal::RouteClosing),
        (Owner::LostRoute, LifecycleRefusal::BoundRouteUnavailable),
        (
            Owner::StoppedWriter,
            LifecycleRefusal::Writer(switchboard_harness_contracts::WriterState::Stopped),
        ),
    ];
    let mut visited = [[[false; 3]; 6]; 4];
    for (owner_index, (owner, wrong_receipt_refusal)) in owners.into_iter().enumerate() {
        for (identity, addressed) in attempts.into_iter().enumerate() {
            let facts = [
                CloseFact::Closed { run: addressed },
                CloseFact::RefusedStillLive { run: addressed },
                CloseFact::NotOffered { run: addressed },
                CloseFact::Unknown {
                    run: addressed,
                    cause: CloseUncertainty::EffectUnconfirmed,
                },
                CloseFact::Unknown {
                    run: addressed,
                    cause: CloseUncertainty::ReplyLost,
                },
                CloseFact::Unknown {
                    run: addressed,
                    cause: CloseUncertainty::InvalidReply,
                },
            ];
            for (fact_index, fact) in facts.into_iter().enumerate() {
                let run = fixture.run.clone();
                let attempt = fixture.close.attempt;
                let state = match owner {
                    Owner::ClosingRestore => OpeningState::ClosingRestore { run, attempt },
                    Owner::ClosingLive => OpeningState::ClosingLive { run, attempt },
                    Owner::LostRoute => OpeningState::RouteLost {
                        run,
                        receipt: LostReceipt::Close(attempt),
                    },
                    Owner::StoppedWriter => {
                        OpeningState::StoppedAwaitingReply(StoppedReceipt::Close(run, attempt))
                    }
                };
                let instance = OpeningInstance::default();
                let original_phase = state.phase();
                let result = fixture.apply_payload(state, Command::CloseSettled(fact), &instance);
                if identity != 0 {
                    let Err(refused) = result else {
                        panic!("a stale close took the exact reply");
                    };
                    assert_eq!(refused.rejection.reason, wrong_receipt_refusal);
                    assert_eq!(refused.row.phase(), original_phase);
                    assert_eq!(refused.row.attempt(), fixture.attempt);
                    assert!(refused.row.instance().is_same_as(&instance));
                    assert!(
                        refused
                            .row
                            .state
                            .run()
                            .expect("preserve the original close route")
                            .route
                            .is_same_as(&fixture.run.route)
                    );
                    assert!(
                        matches!(*refused.rejection._command, Command::CloseSettled(rejected) if rejected == fact)
                    );
                } else if matches!(owner, Owner::ClosingLive) && fact_index == 1 {
                    let Ok((Some(row), Effect::None)) = result else {
                        panic!("definite close refusal must keep the original live reader");
                    };
                    let OpeningState::CloseRefusedLive { run, attempt } = row.state else {
                        panic!("expected exact refused close");
                    };
                    assert!(run.route.is_same_as(&fixture.run.route));
                    assert_eq!(attempt, fixture.close.attempt);
                } else {
                    let Ok((None, Effect::Release(release))) = result else {
                        panic!("exact close reply must retire its own row");
                    };
                    assert!(release.instance.is_same_as(&instance));
                    assert!(release.retire);
                    assert!(
                        release
                            .state
                            .run()
                            .expect("close release owns the original route")
                            .route
                            .is_same_as(&fixture.run.route)
                    );
                }
                visited[owner_index][fact_index][identity] = true;
            }
        }
    }
    assert_eq!(visited, [[[true; 3]; 6]; 4]);
}

#[tokio::test]
async fn setter_reply_claim_must_retain_the_exact_plan_and_envelope() {
    let fixture = Fixture::new().await;
    let mut visited = [[false; 2]; 2];
    for (claim_index, exact) in [true, false].into_iter().enumerate() {
        for (plan_index, has_plan) in [true, false].into_iter().enumerate() {
            let mut restore = fixture.owned_restore();
            let expected_setting = restore.held.setters[0]
                .plan
                .as_ref()
                .expect("reserved setting plan")
                .setting
                .change;
            if !has_plan {
                drop(restore.held.setters[0].plan.take());
            }
            let claim = if exact {
                fixture.setter.clone()
            } else {
                OpeningOffer::default()
            };
            let reply = crate::owner_opening_disposal::observe_reply(
                Box::new(IgnoreSettingReply),
                Some(fixture.disposal_token()),
            );
            let instance = OpeningInstance::default();
            let result = fixture.apply_payload(
                OpeningState::HeldRestore(restore),
                Command::SetterReserved(claim, reply),
                &instance,
            );
            if exact && has_plan {
                let Ok((Some(row), Effect::WriteSetter(write))) = result else {
                    panic!("the exact reservation owns the setter plan");
                };
                let OpeningState::HeldRestore(restore) = row.state else {
                    panic!("setting stayed held");
                };
                assert!(write.route.is_same_as(&fixture.run.route));
                assert_eq!(write.restore, fixture.restore);
                assert_eq!(write.setting.change, expected_setting);
                assert!(restore.held.setters[0].plan.is_none());
                assert!(restore.held.setters[0].claim.is_same_as(&fixture.setter));
            } else {
                let Err(refused) = result else {
                    panic!("the wrong reservation cannot move the setter plan");
                };
                assert_eq!(
                    refused.rejection.reason,
                    LifecycleRefusal::OpeningAlreadyHeld(
                        switchboard_harness_contracts::HeldOpeningKind::Restore
                    )
                );
                let OpeningState::HeldRestore(restore) = refused.row.state else {
                    panic!("refused setting stayed held");
                };
                assert_eq!(restore.held.setters[0].plan.is_some(), has_plan);
                assert!(restore.held.setters[0].claim.is_same_as(&fixture.setter));
                drop(refused.rejection);
            }
            fixture.assert_disposals();
            visited[claim_index][plan_index] = true;
        }
    }
    assert_eq!(visited, [[true; 2]; 2]);
}

#[tokio::test]
async fn close_offer_outcomes_keep_each_origin_and_its_owned_deferred_payload() {
    use CloseOrigin as Origin;
    use DeferredPayload as Buffer;
    let cases = [
        (
            Origin::AwaitingPeer,
            Buffer::Empty,
            Some(P::AwaitingRestore),
        ),
        (
            Origin::AwaitingPeer,
            Buffer::Loaded,
            Some(P::CapturingReplay),
        ),
        (Origin::AwaitingPeer, Buffer::Resumed, Some(P::HeldRestore)),
        (Origin::AwaitingPeer, Buffer::Failed, None),
        (Origin::AwaitingPeer, Buffer::WorkerExited, None),
        (
            Origin::PublishingReplay,
            Buffer::Empty,
            Some(P::CapturingReplay),
        ),
        (
            Origin::PublishingReplay,
            Buffer::ReplayAdmitted,
            Some(P::HeldRestore),
        ),
        (Origin::PublishingReplay, Buffer::ReplayRejected, None),
        (Origin::PublishingReplay, Buffer::Failed, None),
        (Origin::PublishingReplay, Buffer::WorkerExited, None),
        (Origin::RestoreHeld, Buffer::Empty, Some(P::HeldRestore)),
        (
            Origin::RestorePrepared,
            Buffer::Empty,
            Some(P::PreparedRestore),
        ),
        (Origin::Active, Buffer::Empty, Some(P::Active)),
    ];
    let fixture = Fixture::new().await;
    let mut visited = [[false; 2]; 13];
    for (case, (origin, payload, rollback_phase)) in cases.into_iter().enumerate() {
        for (outcome, accepted) in [false, true].into_iter().enumerate() {
            let instance = OpeningInstance::default();
            let (outcome_send, _outcome_receive) = tokio::sync::oneshot::channel();
            let deferred = payload
                .milestone(&fixture)
                .map(|milestone| DeferredRestore {
                    milestone,
                    outcome: fixture.restore_signal(outcome_send),
                });
            let state = OpeningState::OfferingClose {
                origin: Box::new(origin.row(&fixture)),
                attempt: fixture.close.attempt,
                offer: OpeningOffer::default(),
                deferred,
            };
            let Ok((stored, effect)) = fixture.apply_payload(
                state,
                Command::CloseOffered(fixture.close, accepted),
                &instance,
            ) else {
                panic!("exact close must settle for {origin:?} / {payload:?}");
            };
            if let Some(stored) = &stored {
                assert_eq!(stored.attempt(), fixture.attempt);
                assert!(stored.instance().is_same_as(&instance));
                assert!(
                    stored
                        .state
                        .run()
                        .expect("the exact route is retained")
                        .route
                        .is_same_as(&fixture.run.route)
                );
            }
            match (accepted, payload, &effect) {
                (true, payload, Effect::CloseConfirmed(release, deferred)) => {
                    let expected = if let Origin::Active = origin {
                        P::ClosingLive
                    } else {
                        P::ClosingRestore
                    };
                    assert_eq!(stored.as_ref().map(|row| row.phase()), Some(expected));
                    assert!(release.instance.is_same_as(&instance));
                    assert!(!release.retire);
                    assert!(
                        release
                            .state
                            .run()
                            .expect("the close owns its exact origin")
                            .route
                            .is_same_as(&fixture.run.route)
                    );
                    assert_eq!(
                        deferred.as_ref().map(|deferred| deferred.milestone.event()),
                        payload.milestone_event()
                    );
                }
                (false, Buffer::Empty, Effect::Rollback(None)) => {
                    assert_eq!(stored.as_ref().map(|row| row.phase()), rollback_phase);
                }
                (
                    false,
                    payload,
                    Effect::ResolveRestore {
                        decision, release, ..
                    },
                ) => {
                    assert_eq!(stored.as_ref().map(|row| row.phase()), rollback_phase);
                    assert_eq!(*decision, RestoreDecision::Continue);
                    assert_eq!(release.is_some(), rollback_phase.is_none());
                    if let Some(release) = release {
                        assert!(release.instance.is_same_as(&instance));
                        assert!(release.retire);
                        assert!(
                            release
                                .state
                                .run()
                                .expect("release exact failed origin")
                                .route
                                .is_same_as(&fixture.run.route)
                        );
                    }
                    if let Buffer::Loaded | Buffer::Resumed = payload {
                        let row = stored
                            .as_ref()
                            .expect("positive peer reply keeps the original session");
                        let (OpeningState::PublishingReplay(restore)
                        | OpeningState::HeldRestore(restore)) = &row.state
                        else {
                            panic!(
                                "positive reply did not hold its SDK, phase {:?}",
                                row.phase()
                            );
                        };
                        let expected_fallback = if let Buffer::Loaded = payload {
                            "load-fallback"
                        } else {
                            "resume-fallback"
                        };
                        assert_eq!(
                            restore.held.accepted_mode_fallback.as_deref(),
                            Some(expected_fallback)
                        );
                        assert_eq!(restore.held.session.provider_id(), "matrix-session");
                    }
                }
                _ => panic!(
                    "wrong ordered effect for {origin:?} / {payload:?} / close accepted {accepted}"
                ),
            }
            drop((stored, effect));
            fixture.assert_disposals();
            visited[case][outcome] = true;
        }
    }
    assert_eq!(visited, [[true; 2]; 13]);
}

impl DeferredPayload {
    fn milestone_event(self) -> Option<E> {
        Some(match self {
            Self::Empty => return None,
            Self::Loaded => E::RestoreLoadReply,
            Self::Resumed => E::RestoreResumeReply,
            Self::ReplayAdmitted => E::ReplayAdmitted,
            Self::ReplayRejected => E::ReplayRejected,
            Self::Failed => E::OpenFailed,
            Self::WorkerExited => E::WorkerExited,
        })
    }
}

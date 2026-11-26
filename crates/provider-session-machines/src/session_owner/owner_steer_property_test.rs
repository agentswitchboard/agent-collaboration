use super::*;
use hegel::generators as gs;

#[derive(Clone, Copy)]
enum TurnEndWindow {
    BeforeMark,
    BeforeTake,
    AfterTake,
}

#[hegel::test]
fn steer_origin_and_input_survive_generated_turn_end_orders(case: hegel::TestCase) {
    let window = match case.draw(gs::integers::<u8>().max_value(2)) {
        0 => TurnEndWindow::BeforeMark,
        1 => TurnEndWindow::BeforeTake,
        _ => TurnEndWindow::AfterTake,
    };
    let serial = case.draw(gs::integers::<u64>().min_value(10).max_value(999));

    for origin in [SteerOrigin::Named, SteerOrigin::Auto, SteerOrigin::Notice] {
        let mut state = fresh();
        let run = restore_live(&mut state, 1);
        let foreground = input(run.generation, serial);
        let foreground_key = send_prompt(&mut state, &foreground);
        let steer = OwnerInput {
            turn: foreground.turn,
            ..input(run.generation, serial + 1)
        };
        let admission = match origin {
            SteerOrigin::Named => SessionOwnerCommand::Steer {
                input: steer.clone(),
                supported: true,
                origin,
            },
            SteerOrigin::Auto => SessionOwnerCommand::Prompt {
                input: steer.clone(),
                admission: PromptAdmission::Auto {
                    steering_supported: true,
                },
            },
            SteerOrigin::Notice => SessionOwnerCommand::SteerExisting {
                input: steer.clone(),
                supported: true,
                origin,
            },
        };
        let admitted = command(&mut state, admission);
        assert!(
            admitted.contains(&OwnerEffect::Store(SessionStoreRequest::MarkSteerDispatch(
                steer.clone()
            )))
        );
        let mark = |input: OwnerInput| {
            OwnerEvent::Store(SessionStoreResult::SteerDispatch {
                input,
                outcome: EffectOutcome::Applied,
            })
        };
        let unrelated = input(run.generation, serial + 2);
        assert_late(&state, mark(unrelated));
        let before_mark = matches!(window, TurnEndWindow::BeforeMark);
        if before_mark {
            end_turn(&mut state, foreground_key.clone());
        }
        let marked = apply(&mut state, mark(steer.clone()));
        let after_take = matches!(window, TurnEndWindow::AfterTake);
        let replacement = input(run.generation, serial + 3).turn;

        let (result, replay) = if before_mark {
            (marked, None)
        } else {
            let key = authorization(&marked);
            assert!(marked.contains(&OwnerEffect::Authorize {
                authorization: key.clone(),
                work: AuthorizedWork::Steer {
                    input: steer.clone(),
                    origin
                },
            }));
            if after_take {
                apply(
                    &mut state,
                    OwnerEvent::Dispatch(DispatchResult::Taken(key.clone())),
                );
            }
            end_turn(&mut state, foreground_key);
            let event = OwnerEvent::Dispatch(match (after_take, origin) {
                (true, SteerOrigin::Auto) => DispatchResult::PromptRequired {
                    authorization: key,
                    replacement_turn: replacement,
                },
                (true, _) => DispatchResult::SteerNeedsPrompt(key),
                (false, _) => DispatchResult::StaleTarget(key),
            });
            (apply(&mut state, event.clone()), Some(event))
        };

        let settled = if after_take && origin != SteerOrigin::Auto {
            result
        } else {
            assert!(
                result.contains(&OwnerEffect::Store(SessionStoreRequest::UndoDispatch(
                    steer.clone()
                )))
            );
            apply(
                &mut state,
                OwnerEvent::Store(SessionStoreResult::UndoDispatch {
                    input: steer.clone(),
                    outcome: EffectOutcome::Applied,
                }),
            )
        };
        if after_take && origin == SteerOrigin::Auto {
            assert!(settled.contains(&OwnerEffect::Store(
                SessionStoreRequest::MarkQueuedDispatch(OwnerInput {
                    turn: replacement,
                    ..steer.clone()
                })
            )));
        } else {
            let reason = match (window, origin) {
                (_, SteerOrigin::Named) => Some(RevokeReason::NoActiveTurn),
                (TurnEndWindow::AfterTake, SteerOrigin::Notice) => {
                    Some(RevokeReason::ProviderSteerNeedsPrompt)
                }
                (TurnEndWindow::BeforeTake, _) => Some(RevokeReason::TurnEnded),
                _ => None,
            };
            let reply = reason.map_or_else(
                || SessionOwnerReply::InputFinished {
                    input: steer.clone(),
                    remote: RemoteEffect::None,
                },
                |reason| SessionOwnerReply::InputUnsent {
                    input: steer.clone(),
                    reason,
                },
            );
            assert!(settled.contains(&OwnerEffect::Reply(reply)));
            assert!(!settled.iter().any(|effect| matches!(
                effect,
                OwnerEffect::Authorize { .. }
                    | OwnerEffect::Store(
                        SessionStoreRequest::MarkDispatch(_)
                            | SessionStoreRequest::MarkQueuedDispatch(_)
                    )
            )));
        }
        if let Some(replay) = replay {
            assert_late(&state, replay);
        }
        assert_eq!(state.snapshot().turn, SessionTurn::Idle);
    }
}

use central_storage::{CorruptReason, StoreFailure};
use fleet_actor_identity::{
    LiveSessionFacts, SessionAvailability, SessionCallOutcome, SessionServiceError,
};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSessionReply, CreateSessionKind,
    CreateSessionRefusal, EndFleetSession, EndFleetSessionReply, EndSessionTarget, EnrollmentId,
    NewAbandonmentCause, NewEndSettlement, PresentedSessionSecret, RouterEndCause, SessionCreator,
};

use super::fixtures::{Operation, Receipt, SessionStoreFixture, create, id, reservation, secret};

#[tokio::test]
async fn repeated_create_and_bind_reopen_one_immutable_identity() {
    let mut store = SessionStoreFixture::new().await;
    let first = create(5, 1);
    let (session_id, old_secret) = reservation(
        store
            .apply(Operation::Create(first.clone()), 0)
            .await
            .unwrap(),
    );
    assert_eq!(
        store.facts(&session_id).await,
        SessionAvailability::Unavailable
    );
    assert_eq!(
        store
            .apply(Operation::Create(first.clone()), 1)
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            CreateFleetSessionReply::Superseded {}
        ))
    );

    let (same_id, current_secret) = reservation(
        store
            .apply(Operation::Create(create(5, 3)), 2)
            .await
            .unwrap(),
    );
    assert_eq!(same_id, session_id);
    assert_ne!(old_secret, current_secret);
    assert_eq!(
        store
            .apply(Operation::Create(create(5, 2)), 3)
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            CreateFleetSessionReply::Superseded {}
        ))
    );
    let bind = |harness: &str, presented| {
        Operation::Bind(BindFleetSession {
            session_id: session_id.clone(),
            harness_session_id: harness.parse().unwrap(),
            secret: presented,
        })
    };
    assert_eq!(
        store
            .apply(bind("shared-address", old_secret), 4)
            .await
            .unwrap(),
        Receipt::Bind(SessionCallOutcome::Answered(
            BindFleetSessionReply::WrongSecret {}
        ))
    );
    let active = store
        .apply(bind("shared-address", current_secret.clone()), 5)
        .await
        .unwrap();
    assert!(matches!(
        active,
        Receipt::Bind(SessionCallOutcome::Answered(
            BindFleetSessionReply::Active { .. }
        ))
    ));
    assert_eq!(
        store
            .apply(bind("shared-address", current_secret.clone()), 700_000)
            .await
            .unwrap(),
        active
    );
    assert_eq!(
        store
            .apply(bind("different-address", current_secret.clone()), 700_001)
            .await
            .unwrap(),
        Receipt::Bind(SessionCallOutcome::Answered(
            BindFleetSessionReply::AlreadyBoundToDifferentId {}
        ))
    );
    let opened = store
        .apply(Operation::Create(first), 700_002)
        .await
        .unwrap();
    assert!(
        matches!(opened, Receipt::Create(SessionCallOutcome::Answered(CreateFleetSessionReply::Active { session_id: id, .. })) if id == session_id)
    );
    assert_eq!(
        store.facts(&session_id).await,
        SessionAvailability::Live(LiveSessionFacts::Owner)
    );
    assert_eq!(
        store.count("SELECT COUNT(*) FROM actor_identities").await,
        1
    );
    assert_eq!(store.count("SELECT COUNT(*) FROM actor_sessions").await, 1);
    let mut connection = store.open().await;
    let saved: String = sqlx::query_scalar("SELECT secret_hash FROM actor_sessions")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_ne!(saved, current_secret.expose_secret());
}

#[tokio::test]
async fn end_by_operation_requires_the_same_reservation_and_bound_addresses_are_never_released() {
    let mut store = SessionStoreFixture::new().await;
    let first = create(5, 1);
    let wrong = create(6, 1);
    let (session_id, first_secret) = reservation(
        store
            .apply(Operation::Create(first.clone()), 0)
            .await
            .unwrap(),
    );
    let bind = Operation::Bind(BindFleetSession {
        session_id: session_id.clone(),
        harness_session_id: "last-address".parse().unwrap(),
        secret: first_secret,
    });
    store.apply(bind, 1).await.unwrap();
    let by_operation = |body: &fleet_identity_protocol::CreateFleetSession| {
        Operation::End(EndFleetSession {
            target: EndSessionTarget::OperationId {
                operation_id: first.operation_id.clone(),
                contents_digest: body.contents_digest().unwrap(),
            },
            cause: RouterEndCause::GivenUp {},
        })
    };
    assert_eq!(
        store.apply(by_operation(&first), 2).await.unwrap(),
        Receipt::End(SessionCallOutcome::Answered(
            EndFleetSessionReply::NotYoursToEnd {}
        ))
    );
    let by_id = Operation::End(EndFleetSession {
        target: EndSessionTarget::CentralId { session_id },
        cause: RouterEndCause::EndedForGood {},
    });
    assert!(matches!(
        store.apply(by_id, 3).await.unwrap(),
        Receipt::End(SessionCallOutcome::Answered(
            EndFleetSessionReply::Settled {
                settlement: NewEndSettlement::Ended { .. }
            }
        ))
    ));
    let mut different_body = first.clone();
    different_body.agent_program = fleet_identity_protocol::HarnessKind::Claude;
    assert_eq!(
        store.apply(by_operation(&different_body), 4).await.unwrap(),
        Receipt::End(SessionCallOutcome::Answered(
            EndFleetSessionReply::NotYoursToEnd {}
        ))
    );

    let (other_session, other_secret) = reservation(
        store
            .apply(Operation::Create(wrong.clone()), 5)
            .await
            .unwrap(),
    );
    assert_eq!(
        store
            .apply(
                Operation::Bind(BindFleetSession {
                    session_id: other_session.clone(),
                    harness_session_id: "last-address".parse().unwrap(),
                    secret: other_secret,
                }),
                6
            )
            .await
            .unwrap(),
        Receipt::Bind(SessionCallOutcome::Answered(
            BindFleetSessionReply::AddressTaken {}
        ))
    );
    let target = EndSessionTarget::OperationId {
        operation_id: wrong.operation_id.clone(),
        contents_digest: wrong.contents_digest().unwrap(),
    };
    let end = EndFleetSession {
        target,
        cause: RouterEndCause::GivenUp {},
    };
    assert!(matches!(
        store.apply(Operation::End(end.clone()), 7).await.unwrap(),
        Receipt::End(SessionCallOutcome::Answered(
            EndFleetSessionReply::Settled {
                settlement: NewEndSettlement::Abandoned {
                    cause: NewAbandonmentCause::GivenUp {},
                    ..
                },
            }
        ))
    ));
    assert!(matches!(
        store.apply(Operation::End(end), 8).await.unwrap(),
        Receipt::End(SessionCallOutcome::Answered(
            EndFleetSessionReply::AlreadySettled { .. }
        ))
    ));
    assert_eq!(
        store.facts(&other_session).await,
        SessionAvailability::Unavailable
    );
}

#[tokio::test]
async fn authenticated_creator_is_checked_before_replay_and_only_that_session_can_be_the_parent() {
    let mut store = SessionStoreFixture::new().await;
    let (parent, parent_secret) = reservation(
        store
            .apply(Operation::Create(create(5, 1)), 0)
            .await
            .unwrap(),
    );
    store
        .apply(
            Operation::Bind(BindFleetSession {
                session_id: parent.clone(),
                harness_session_id: "parent".parse().unwrap(),
                secret: parent_secret.clone(),
            }),
            1,
        )
        .await
        .unwrap();
    let make_child = |presented, parent_id| {
        let mut child = create(6, 1);
        child.creator = SessionCreator::Session {
            session_id: parent.clone(),
            presented,
        };
        child.session_kind = CreateSessionKind::Delegate { parent: parent_id };
        Operation::Create(child)
    };
    let proof = PresentedSessionSecret::Secret {
        secret: parent_secret.clone(),
    };
    assert_eq!(
        store
            .apply(
                make_child(PresentedSessionSecret::Nothing {}, parent.clone()),
                2
            )
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            CreateFleetSessionReply::NoSecret {}
        ))
    );
    assert_eq!(
        store
            .apply(
                make_child(
                    PresentedSessionSecret::Secret {
                        secret: secret(201)
                    },
                    parent.clone()
                ),
                2
            )
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            CreateFleetSessionReply::WrongSecret {}
        ))
    );
    assert_eq!(
        store
            .apply(make_child(proof.clone(), id(200).parse().unwrap()), 2)
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            CreateFleetSessionReply::Refused {
                reason: CreateSessionRefusal::ParentNotValid {}
            }
        ))
    );
    let (child, child_secret) = reservation(
        store
            .apply(make_child(proof.clone(), parent.clone()), 3)
            .await
            .unwrap(),
    );
    store
        .apply(
            Operation::Bind(BindFleetSession {
                session_id: child.clone(),
                harness_session_id: "child".parse().unwrap(),
                secret: child_secret.clone(),
            }),
            4,
        )
        .await
        .unwrap();
    let SessionAvailability::Live(LiveSessionFacts::Delegate { parent: facts }) =
        store.facts(&child).await
    else {
        panic!("the bound delegate must expose its recorded parent");
    };
    assert_eq!(facts.session_id, parent);
    assert_eq!(facts.machine_id, store.caller.machine_id);
    assert_eq!(facts.enrollment_id, store.caller.enrollment_id);
    let organization = id(700).try_into().unwrap();
    let another_organization: fleet_identity_protocol::OrganizationId = id(701).try_into().unwrap();
    let mut foreign_store =
        SessionStoreFixture::for_organization(another_organization.clone()).await;
    let first_connection = store.capture_connection(4).await;
    assert_eq!(
        foreign_store
            .apply_with_connection(
                Operation::Create(create(478, 1)),
                4,
                Some(&first_connection)
            )
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Revoked)
    );
    assert!(foreign_store.dispatched_reads().is_empty());
    let presented = PresentedSessionSecret::Secret {
        secret: child_secret,
    };
    assert!(
        foreign_store
            .authenticate(&first_connection, &child, &presented)
            .await
            .is_none()
    );
    assert!(foreign_store.dispatched_reads().is_empty());
    assert_eq!(
        foreign_store.checked_facts(&child).await.unwrap(),
        (false, SessionAvailability::Unavailable),
    );
    let parent_route = SessionAvailability::Live(LiveSessionFacts::Delegate { parent: facts });
    assert_eq!(
        store.checked_facts(&parent).await.unwrap(),
        (true, SessionAvailability::Live(LiveSessionFacts::Owner))
    );
    assert_eq!(
        store.checked_facts(&child).await.unwrap(),
        (true, parent_route.clone())
    );
    assert!(
        store
            .authenticate(
                &first_connection,
                &child,
                &PresentedSessionSecret::Nothing {}
            )
            .await
            .is_none()
    );
    let wrong = PresentedSessionSecret::Secret { secret: secret(70) };
    assert!(
        store
            .authenticate(&first_connection, &child, &wrong)
            .await
            .is_none()
    );
    let admitted = store
        .authenticate(&first_connection, &child, &presented)
        .await
        .unwrap();
    assert_eq!(admitted.organization_id(), &organization);
    assert_eq!(
        admitted.scoped_session().machine_id,
        store.caller.machine_id
    );
    assert_eq!(
        admitted.scoped_session().enrollment_id,
        store.caller.enrollment_id
    );
    assert_eq!(admitted.scoped_session().session_id, child);

    store
        .apply(
            Operation::End(EndFleetSession {
                target: EndSessionTarget::CentralId {
                    session_id: parent.clone(),
                },
                cause: RouterEndCause::EndedForGood {},
            }),
            5,
        )
        .await
        .unwrap();
    assert_eq!(
        store.checked_facts(&parent).await.unwrap(),
        (false, SessionAvailability::Unavailable)
    );
    assert_eq!(
        store.checked_facts(&child).await.unwrap(),
        (true, parent_route)
    );
    assert_eq!(
        store
            .apply(make_child(proof, parent.clone()), 6)
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            CreateFleetSessionReply::SessionEnded {}
        ))
    );
    let mut connection = store.open().await;
    sqlx::query("UPDATE actor_sessions SET session_state='unrecognized' WHERE actor_id=?")
        .bind(parent.as_str())
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        store.checked_facts(&child).await,
        Err(central_storage::StoreFailure::Corrupt {
            reason: central_storage::CorruptReason::UnknownKind { column },
        }) if column == "session_state"
    ));
}

#[tokio::test]
async fn noncurrent_enrollment_and_corrupt_rows_fail_closed() {
    let mut store = SessionStoreFixture::new().await;
    let (_, _) = reservation(
        store
            .apply(Operation::Create(create(5, 1)), 0)
            .await
            .unwrap(),
    );
    let old = store.caller.enrollment_id.clone();
    store.caller.enrollment_id = EnrollmentId::try_from(id(97)).unwrap();
    assert_eq!(
        store
            .apply(Operation::Create(create(5, 2)), 1)
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Revoked)
    );
    store.caller.enrollment_id = old;
    let mut connection = store.open().await;
    sqlx::query("UPDATE actor_sessions SET session_state='future_state'")
        .execute(&mut connection)
        .await
        .unwrap();
    let result = store.apply(Operation::Create(create(5, 2)), 2).await;
    assert!(
        matches!(result, Err(SessionServiceError::Store(StoreFailure::Corrupt {
        reason: CorruptReason::UnknownKind { column },
    })) if column == "session_state")
    );
}

#[tokio::test]
async fn failure_after_actor_insert_rolls_back_before_retry() {
    let mut store = SessionStoreFixture::new().await;
    let mut connection = store.open().await;
    sqlx::raw_sql("CREATE TRIGGER fail_reserve BEFORE INSERT ON actor_sessions BEGIN SELECT RAISE(ABORT, 'fixture'); END;")
        .execute(&mut connection).await.unwrap();
    assert!(matches!(
        store.apply(Operation::Create(create(5, 1)), 0).await,
        Err(SessionServiceError::Store(StoreFailure::Unavailable))
    ));
    assert_eq!(
        store.count("SELECT COUNT(*) FROM actor_identities").await,
        0
    );
    assert_eq!(store.count("SELECT COUNT(*) FROM actor_sessions").await, 0);
    sqlx::raw_sql("DROP TRIGGER fail_reserve;")
        .execute(&mut connection)
        .await
        .unwrap();
    reservation(
        store
            .apply(Operation::Create(create(5, 2)), 1)
            .await
            .unwrap(),
    );
    assert_eq!(
        store.count("SELECT COUNT(*) FROM actor_identities").await,
        1
    );
    assert_eq!(store.count("SELECT COUNT(*) FROM actor_sessions").await, 1);
}

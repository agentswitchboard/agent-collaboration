use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use central_storage::StoreFailure;
use fleet_actor_identity::SessionCreateApplication;
use fleet_actor_identity::{SessionCallOutcome, SessionServiceError};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSessionReply, CreateSessionKind,
    PresentedSessionSecret, SessionCreator,
};
use sha2::{Digest, Sha256};
use sqlx::Connection;

use super::fixtures::{Operation, Receipt, SessionStoreFixture, create, reservation};

#[tokio::test]
async fn replies_without_a_new_secret_survive_entropy_failure() {
    let mut store = SessionStoreFixture::new().await;
    let (session_id, secret) = reservation(
        store
            .apply(Operation::Create(create(5, 1)), 0)
            .await
            .unwrap(),
    );
    let mut connection = store.open().await;
    let stored: String =
        sqlx::query_scalar("SELECT secret_hash FROM actor_sessions WHERE actor_id=?")
            .bind(session_id.as_str())
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(stored, URL_SAFE_NO_PAD.encode(Sha256::digest([1_u8; 32])));
    assert_ne!(stored, secret.expose_secret());
    connection.close().await.unwrap();
    store.refuse_future_secrets();

    assert_eq!(
        store
            .apply(Operation::Create(create(5, 1)), 1)
            .await
            .unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(
            SessionCreateApplication::ExistingOrRefused(CreateFleetSessionReply::Superseded {})
        )),
    );
    assert!(matches!(
        store.apply(Operation::Create(create(5, 2)), 2).await,
        Err(SessionServiceError::SecretUnavailable),
    ));
    assert!(matches!(
        store
            .apply(
                Operation::Bind(BindFleetSession {
                    session_id: session_id.clone(),
                    harness_session_id: "working-address".parse().unwrap(),
                    secret,
                }),
                3
            )
            .await
            .unwrap(),
        Receipt::Bind(SessionCallOutcome::Answered(
            BindFleetSessionReply::Active { .. }
        )),
    ));
    assert!(matches!(
        store.apply(Operation::Create(create(5, 2)), 4).await.unwrap(),
        Receipt::Create(SessionCallOutcome::Answered(SessionCreateApplication::ExistingOrRefused(CreateFleetSessionReply::Active {
            session_id: active, ..
        }))) if active == session_id,
    ));
}

#[tokio::test]
async fn a_delegate_insert_failure_rolls_back_both_preceding_rows() {
    let mut store = SessionStoreFixture::new().await;
    let (parent, secret) = reservation(
        store
            .apply(Operation::Create(create(5, 1)), 0)
            .await
            .unwrap(),
    );
    store
        .apply(
            Operation::Bind(BindFleetSession {
                session_id: parent.clone(),
                harness_session_id: "parent-address".parse().unwrap(),
                secret: secret.clone(),
            }),
            1,
        )
        .await
        .unwrap();
    let mut delegate = create(6, 1);
    delegate.creator = SessionCreator::Session {
        session_id: parent.clone(),
        presented: PresentedSessionSecret::Secret { secret },
    };
    delegate.session_kind = CreateSessionKind::Delegate { parent };

    let mut connection = store.open().await;
    sqlx::query("CREATE TRIGGER fail_delegate BEFORE INSERT ON actor_delegates BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        .execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
    assert!(matches!(
        store.apply(Operation::Create(delegate.clone()), 2).await,
        Err(SessionServiceError::Store(StoreFailure::Unavailable)),
    ));
    assert_eq!(
        store.count("SELECT COUNT(*) FROM actor_identities").await,
        1
    );
    assert_eq!(store.count("SELECT COUNT(*) FROM actor_sessions").await, 1);
    assert_eq!(store.count("SELECT COUNT(*) FROM actor_delegates").await, 0);

    let mut connection = store.open().await;
    sqlx::query("DROP TRIGGER fail_delegate")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    reservation(store.apply(Operation::Create(delegate), 3).await.unwrap());
    assert_eq!(store.count("SELECT COUNT(*) FROM actor_delegates").await, 1);
}

use super::support::{A, DIGEST, Fixture, PROVISION, Result, provision_body, signed};
use central_storage::StoreFailure;
use fleet_runtime::PausePoint;
use serde_json::json;

#[expect(
    clippy::panic_in_result_fn,
    reason = "A missing pause or a changed HTTP reply fails the test."
)]
#[tokio::test]
async fn reached_stamp_calls_keep_their_wire_answers_when_post_turn_publication_fails() -> Result {
    let fixture = Fixture::new().await?;
    let owner = fixture.runtime(A).await?;
    let runtime = owner.runtime();
    let first = signed::stamp(A, signed::NOW_SECONDS)?;
    let unrelated = signed::first_statement(A, signed::NOW_SECONDS, 1)?;
    let stale = signed::renewal(A, &unrelated, signed::NOW_SECONDS, 2)?;
    let provision = provision_body(A, &first);
    let digest_bytes = format!(
        r#"{{"digest":"{}","expiresAt":{},"signingKeyId":"stamp-a"}}"#,
        signed::digest(&first),
        signed::NOW_SECONDS - 60 + signed::LIFETIME_SECONDS,
    );
    for (path, body, status, bytes) in [
        (
            PROVISION,
            provision.clone(),
            200,
            br#"{"result":"provisioned"}"#.as_slice(),
        ),
        (
            PROVISION,
            provision,
            200,
            br#"{"result":"provisioned"}"#.as_slice(),
        ),
        (
            DIGEST,
            json!({"organizationId": A}),
            200,
            digest_bytes.as_bytes(),
        ),
        (PROVISION, provision_body(A, &stale), 409, b""),
    ] {
        let attempts = runtime.completion_attempts();
        let entries = runtime.engine_completion_entries();
        let mut held = runtime
            .pause_points()
            .hold_central(PausePoint::EngineCompletionEntered);
        let reply = fixture.post(path, &body);
        tokio::pin!(reply);
        tokio::select! {
            () = held.until_held() => {}
            received = &mut reply => panic!("primary reply escaped the post-turn hook: {:?}", received.map(|reply| reply.status)),
        }

        assert!(runtime.try_central_turn().is_some());
        assert_eq!(runtime.completion_attempts(), attempts + 1);
        let retained = fixture.snapshot().await?;
        assert_eq!((retained.0.len(), retained.1.len()), (1, 1));
        runtime
            .statement_host()
            .fail_next_alarm_batch(StoreFailure::Unavailable);
        held.release();

        let reply = reply.await?;
        assert_eq!((reply.status, reply.body.as_slice()), (status, bytes));
        assert_eq!(runtime.engine_completion_entries(), entries + 1);
        assert!(
            runtime
                .statement_host()
                .failed_alarm_publication_at()
                .is_some()
        );
        assert_eq!(fixture.snapshot().await?, retained);
    }
    fixture.close().await
}

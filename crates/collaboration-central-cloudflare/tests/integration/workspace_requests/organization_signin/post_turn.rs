use super::support::{DIGEST, Fixture, ORG, PROVISION};
use central_storage::StoreFailure;
use collaboration_central_conformance::signin_fixtures as signed;
use serde_json::json;

#[tokio::test]
async fn reached_stamp_calls_keep_their_wire_answers_when_post_turn_publication_fails() {
    let fixture = Fixture::new().await;
    let runtime = fixture.runtime().await;
    let point = "engineCompletionEntered";
    let first = signed::stamp(ORG, signed::NOW_SECONDS).unwrap();
    let unrelated = signed::first_statement(ORG, signed::NOW_SECONDS, 1).unwrap();
    let stale = signed::renewal(ORG, &unrelated, signed::NOW_SECONDS, 2).unwrap();
    let provision = json!({"organizationId": ORG, "organizationStatement": first});
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
            json!({"organizationId": ORG}),
            200,
            digest_bytes.as_bytes(),
        ),
        (
            PROVISION,
            json!({"organizationId": ORG, "organizationStatement": stale}),
            409,
            b"",
        ),
    ] {
        let attempts: u64 = runtime.control("completion-attempts", &()).await.unwrap();
        let entries: u64 = runtime
            .control("engine-completion-entries", &())
            .await
            .unwrap();
        assert!(
            runtime
                .control::<bool>("hold-central", &point)
                .await
                .unwrap()
        );
        let reply = fixture.post_bytes(path, &body, &[]);
        tokio::pin!(reply);
        tokio::select! {
            reached = runtime.control::<bool>("until-held-central", &point) => assert!(reached.unwrap()),
            (status, _) = &mut reply => panic!("primary reply escaped the post-turn hook: {status}"),
        }

        let workspace = "019f0000-0000-7000-8000-00000000d252";
        let [central_free, _]: [bool; 2] = runtime
            .control("try-completion-turns", &workspace)
            .await
            .unwrap();
        assert!(central_free);
        assert_eq!(
            runtime
                .control::<u64>("completion-attempts", &())
                .await
                .unwrap(),
            attempts + 1
        );
        let retained = fixture.counts().await;
        assert!(
            runtime
                .control::<bool>("fail-alarm-batch", &StoreFailure::Unavailable)
                .await
                .unwrap()
        );
        assert!(
            runtime
                .control::<bool>("release-central", &point)
                .await
                .unwrap()
        );

        let (actual_status, actual_bytes) = reply.await;
        assert_eq!((actual_status, actual_bytes.as_slice()), (status, bytes));
        assert_eq!(
            runtime
                .control::<u64>("engine-completion-entries", &())
                .await
                .unwrap(),
            entries + 1
        );
        assert!(
            !runtime
                .control::<bool>("alarm-batch-fault-pending", &())
                .await
                .unwrap()
        );
        assert_eq!(fixture.counts().await, retained);
    }
    fixture.close().await;
}

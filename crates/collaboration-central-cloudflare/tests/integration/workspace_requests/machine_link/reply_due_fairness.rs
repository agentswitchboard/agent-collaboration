use super::*;

#[tokio::test]
async fn a_ready_public_reply_uses_the_host_turn_before_pass_two() -> Result<()> {
    for skip_yield in [false, true] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime();
        let mut link = ConnectedLink::open(&fixture).await?;
        let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
        let group: uuid::Uuid = "019f0000-0000-7000-8000-00000000dd22".parse()?;
        for number in 0..=250 {
            let key: uuid::Uuid = format!("019f0000-0000-7000-8000-{number:012}").parse()?;
            assert!(matches!(
                runtime
                    .seed_probe(SeedProbe {
                        key,
                        group,
                        due: Some(due),
                        claim: key.to_string()
                    })
                    .await?,
                BatchOutcome::Applied { .. }
            ));
        }

        let last_first_pass = format!("019f0000-0000-7000-8000-{:012}", 249);
        let first_pass = runtime
            .hold_engine("afterEngineCommit", &last_first_pass)
            .await;
        sign_in_digest(&fixture).await?;
        runtime.until_held_engine(first_pass).await;
        let writer_point = "beforeLinkReplyWrite";
        let _: bool = runtime.control("hold-central", &writer_point).await?;
        {
            let reply = request(DueReplyRoute::WorkspaceLink, &fixture, &mut link);
            tokio::pin!(reply);
            tokio::select! {
                reached = runtime.control::<bool>("until-held-central", &writer_point) => { assert!(reached?); }
                result = &mut reply => {
                    return Err(format!("the public request bypassed its writer hold: {result:?}").into());
                }
            }
            if skip_yield {
                let _: bool = runtime.control("skip-next-due-host-yield", &()).await?;
            }
            // The release is synchronous at the end of pass one. Pass two has
            // no test hold. A skipped host turn must start discovery before this
            // already prepared public reply can use the real writer.
            let _: bool = runtime
                .control("release-writer-when-due-pass-finishes", &())
                .await?;
            runtime.release_engine(first_pass).await;
            reply.await?;
        }
        let observed: Vec<usize> = runtime.control("discoveries-at-each-reply", &()).await?;
        assert_eq!(
            observed,
            if skip_yield { vec![0, 2] } else { vec![0, 1] },
            "skip host yield: {skip_yield}"
        );
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn the_worker_http_drain_waits_while_the_reply_pipe_is_backpressured() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime();
    let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
    let key: uuid::Uuid = "019f0000-0000-7000-8000-00000000dd23".parse()?;
    runtime
        .seed_probe(SeedProbe {
            key,
            group: key,
            due: Some(due),
            claim: "Worker HTTP pipe".into(),
        })
        .await?;
    let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    let point = "beforePrivateReplySinkReads";
    let _: bool = runtime.control("hold-central", &point).await?;
    {
        let reply = sign_in_digest(&fixture);
        tokio::pin!(reply);
        tokio::select! {
            held = runtime.control::<bool>("until-held-central", &point) => assert!(held?),
            result = &mut reply => return Err(format!("reply bypassed its held sink: {result:?}").into()),
        }
        assert_eq!(
            runtime
                .control::<(u64, u64)>("raw-reply-due-activity", &())
                .await?,
            activity
        );
        let facts = runtime
            .read(Statement {
                sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                args: vec![SqlValue::Text(key.to_string())],
            })
            .await?
            .rows;
        assert_eq!(facts, [vec![SqlValue::Integer(0.try_into()?)]]);
        let _: bool = runtime.control("release-central", &point).await?;
        reply.await?;
    }
    assert!(
        observe_event(|| async {
            Ok(runtime
                .control::<(u64, u64)>("raw-reply-due-activity", &())
                .await?
                == (activity.0 + 1, 0))
        })
        .await?
    );
    assert_eq!(
        runtime
            .control::<Option<i64>>("raw-alarm-time", &())
            .await?,
        None
    );
    fixture.close().await
}

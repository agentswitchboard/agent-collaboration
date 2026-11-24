use super::*;

#[tokio::test]
async fn a_ready_public_reply_uses_the_host_turn_before_pass_two() -> Result<()> {
    for skip_yield in [false, true] {
        let fixture = start_fixture().await?;
        let runtime = fixture.host.central(&fixture.organization_id).await?;
        let (mut client, attempt, mut link, _) = connect_client(&fixture, &fixture.token).await?;
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
        let pauses = runtime.runtime().pause_points();
        let last_first_pass = format!("019f0000-0000-7000-8000-{:012}", 249);
        let mut first_pass = pauses.hold_engine(PausePoint::AfterEngineCommit, &last_first_pass);
        sign_in_digest(&fixture).await?;
        first_pass.until_held().await;
        let mut waiting_writer = pauses.hold_central(PausePoint::BeforeLinkReplyWrite);
        {
            let reply = request(
                DueReplyRoute::WorkspaceLink,
                &fixture,
                &mut client,
                &attempt,
                &mut link,
            );
            tokio::pin!(reply);
            tokio::select! {
                reached = waiting_writer.until_held() => { let _: () = reached; }
                result = &mut reply => {
                    return Err(format!("the public request bypassed its writer hold: {result:?}").into());
                }
            }
            if skip_yield {
                runtime.runtime().test_faults().skip_next_due_host_yield();
            }
            // The release is synchronous at the end of pass one. Pass two has
            // no test hold. A skipped host turn must start discovery before this
            // already prepared public reply can use the real writer.
            pauses.release_reply_when_pass_finishes(waiting_writer);
            first_pass.release();
            reply.await?;
        }
        let observed: Vec<usize> = runtime.runtime().due_batches().discoveries_at_each_reply();
        assert_eq!(
            observed,
            if skip_yield { vec![0, 2] } else { vec![0, 1] },
            "skip host yield: {skip_yield}"
        );
        fixture.close().await?;
    }
    Ok(())
}

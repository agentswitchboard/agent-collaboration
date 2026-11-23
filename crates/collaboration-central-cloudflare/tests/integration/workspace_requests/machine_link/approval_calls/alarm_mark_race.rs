use super::*;

#[tokio::test]
async fn workerd_approval_mark_survives_request_before_capped_drive() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result =
        collaboration_central_conformance::approval_provider_scenarios::approval_alarm_mark_race(
            &fixture,
        )
        .await;
    fixture.close().await?;
    result
}

impl collaboration_central_conformance::approval_provider_scenarios::ApprovalAlarmRaceFixture
    for Fixture
{
    async fn seed_alarm_race_probe(&self, ordinal: u16, due_at_ms: i64) -> Result<()> {
        let seed = collaboration_central_core::engine_probe::SeedProbe {
            key: format!("019f0000-0000-7000-8000-00000005{ordinal:04x}").parse()?,
            group: "019f0000-0000-7000-8000-000000040000".parse()?,
            due: Some(chrono::DateTime::from_timestamp_millis(due_at_ms).ok_or("manual due time")?),
            claim: format!("approval-race-{ordinal}"),
        };
        let result = self.runtime().seed_probe(seed).await?;
        if !matches!(result, central_storage::BatchOutcome::Applied { .. }) {
            return Err("approval alarm race probe seed did not apply".into());
        }
        Ok(())
    }

    async fn ordinary_request_at_retained_mark(
        &self,
        socket: &mut WebSocket,
        mark: i64,
    ) -> Result<Option<i64>> {
        self.advance(u64::try_from(mark - self.now_seconds())?)
            .await?;
        collaboration_central_conformance::approval_provider_scenarios::publish_ordinary_alarm_request(
            socket, &self.target()?, 2,
        ).await?;
        let retained = self.retained_alarm_ms().await?;

        Ok(retained)
    }

    async fn retained_alarm_ms(&self) -> Result<Option<i64>> {
        Ok(self.runtime().control("raw-alarm-time", &()).await?)
    }
}

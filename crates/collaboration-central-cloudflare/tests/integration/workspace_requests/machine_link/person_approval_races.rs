use collaboration_central_conformance::{
    ConformanceResult as Result,
    signin_fixtures::person_approval_races::{
        DecisionCompetitor, DecisionRaceOrder, PersonApprovalPause, PersonApprovalRaceFixture,
        RacePauseStep, check_decision_race,
    },
};
use serde_json::Value;

use super::Fixture;

impl PersonApprovalRaceFixture for Fixture {
    type Link = Self;

    fn link(&self) -> &Self::Link {
        self
    }

    async fn post_private(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> Result<(u16, Vec<u8>)> {
        let mut request = self
            .client
            .post(format!("{}{path}", self.host.signin_origin()))
            .json(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await?;
        if response
            .headers()
            .get("x-collaboration-central-entry")
            .is_none_or(|value| value != "collaboration-central-link-entry")
        {
            return Err("person Decide did not reach the Workerd sign-in entrypoint".into());
        }
        Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
    }

    async fn control_pause(&self, point: PersonApprovalPause, step: RacePauseStep) -> Result<()> {
        let point = match point {
            PersonApprovalPause::BeforeTurn => "beforePersonApprovalTurn",
            PersonApprovalPause::AfterCommit => "afterPersonApprovalCommit",
        };
        let operation = match step {
            RacePauseStep::Arm => "hold-central",
            RacePauseStep::Wait => "until-held-central",
            RacePauseStep::Release => "release-central",
        };
        if !self.runtime().control::<bool>(operation, &point).await? {
            return Err(format!("Workerd did not {operation} at {point}").into());
        }
        Ok(())
    }

    async fn redeliver_alarm(&self) -> Result<()> {
        self.runtime().after_event().await?;
        Ok(())
    }
}

async fn check(competitor: DecisionCompetitor, order: DecisionRaceOrder) -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = check_decision_race(&fixture, competitor, order).await;
    fixture.close().await?;
    observed
}

macro_rules! race_tests {
    ($competitor:ident, $competitor_first:ident, $person_first:ident) => {
        #[tokio::test]
        async fn $competitor_first() -> Result<()> {
            check(
                DecisionCompetitor::$competitor,
                DecisionRaceOrder::CompetitorFirst,
            )
            .await
        }

        #[tokio::test]
        async fn $person_first() -> Result<()> {
            check(
                DecisionCompetitor::$competitor,
                DecisionRaceOrder::PersonFirst,
            )
            .await
        }
    };
}

race_tests!(Stop, stop_before_person_decide, person_decide_before_stop);
race_tests!(
    Deadline,
    deadline_before_person_decide,
    person_decide_before_deadline
);
race_tests!(
    EnrollmentEnd,
    enrollment_end_before_person_decide,
    person_decide_before_enrollment_end
);

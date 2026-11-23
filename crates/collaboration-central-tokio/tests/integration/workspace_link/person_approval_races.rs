use std::cell::{Cell, RefCell};

use collaboration_central_conformance::{
    ConformanceResult as Result,
    signin_fixtures::person_approval_races::{
        DecisionCompetitor, DecisionRaceOrder, PersonApprovalPause, PersonApprovalRaceFixture,
        RacePauseStep, check_decision_race, check_person_session_race,
    },
};
use fleet_runtime::{HeldPause, PausePoint};
use serde_json::Value;

use super::{NativeApprovalFixture, signed_clock, start_session_link_fixture};

struct RaceFixture<'fixture> {
    link: &'fixture NativeApprovalFixture,
    client: reqwest::Client,
    pause: RefCell<Option<HeldPause>>,
}

impl PersonApprovalRaceFixture for RaceFixture<'_> {
    type Link = NativeApprovalFixture;

    fn link(&self) -> &Self::Link {
        self.link
    }

    async fn post_private(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> Result<(u16, Vec<u8>)> {
        let mut request = self
            .client
            .post(format!("http://central.local{path}"))
            .json(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await?;
        Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
    }

    async fn control_pause(&self, point: PersonApprovalPause, step: RacePauseStep) -> Result<()> {
        match step {
            RacePauseStep::Arm => {
                let organization = &self.link.link.organization_id;
                let runtime = self.link.link.host.central(organization).await?;
                let point = match point {
                    PersonApprovalPause::BeforeTurn => PausePoint::BeforePersonApprovalTurn,
                    PersonApprovalPause::AfterCommit => PausePoint::AfterPersonApprovalCommit,
                };
                self.pause
                    .replace(Some(runtime.runtime().pause_points().hold_central(point)));
            }
            RacePauseStep::Wait => {
                let mut pause = self.pause.take().ok_or("person approval pause not armed")?;
                pause.until_held().await;
                self.pause.replace(Some(pause));
            }
            RacePauseStep::Release => {
                self.pause
                    .take()
                    .ok_or("person approval pause not armed")?
                    .release();
            }
        }
        Ok(())
    }

    async fn redeliver_alarm(&self) -> Result<()> {
        let link = &self.link.link;
        let runtime = link.host.central(&link.organization_id).await?;
        runtime.after_event().await.map_err(|error| error.failure)?;
        Ok(())
    }
}

async fn check(competitor: DecisionCompetitor, order: DecisionRaceOrder) -> Result<()> {
    let link = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(link.link.signin_socket.clone())
            .build()?;
        let fixture = RaceFixture {
            link: &link,
            client,
            pause: RefCell::default(),
        };
        check_decision_race(&fixture, competitor, order).await
    }
    .await;
    link.link.close().await?;
    observed
}

async fn check_person_session(order: DecisionRaceOrder) -> Result<()> {
    let link = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(link.link.signin_socket.clone())
            .build()?;
        let fixture = RaceFixture {
            link: &link,
            client,
            pause: RefCell::default(),
        };
        check_person_session_race(&fixture, order).await
    }
    .await;
    link.link.close().await?;
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

#[tokio::test]
async fn session_decide_before_signed_person_decide() -> Result<()> {
    check_person_session(DecisionRaceOrder::CompetitorFirst).await
}

#[tokio::test]
async fn signed_person_decide_before_session_decide() -> Result<()> {
    check_person_session(DecisionRaceOrder::PersonFirst).await
}

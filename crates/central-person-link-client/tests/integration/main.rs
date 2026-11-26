#![expect(
    clippy::unwrap_used,
    reason = "fixture setup and required test receipts"
)]

use central_person_link_client::test_support::fixture_authority::FixturePerson;
use collaboration_central_conformance::signin_fixtures::{
    ORGANIZATION, OWNER, OWNER_SUBJECT, POLICY, SESSION,
};

const PERSON: FixturePerson = FixturePerson {
    organization: ORGANIZATION,
    person: OWNER,
    subject: OWNER_SUBJECT,
    policy: POLICY,
    session: SESSION,
};

mod login_and_refresh;
mod person_approvals;
mod workspace_reconnect;

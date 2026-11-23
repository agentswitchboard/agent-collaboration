use super::{NativeSessionTransactions, NativeTransaction, ObjectStorage, fill_session_secret};
use crate::test_support::{MachineLinkFixture, start_session_link_fixture};
use central_link_call_composition::{
    OrganizationSessionTransactions, SessionApplyFailure, SessionLinkDispatcher, SessionWiringFault,
};
use central_link_protocol::{CentralLinkRpc, session_lifecycle_rpc::CreateFleetSessionRpc};
use central_storage::{CorruptReason, StoreFailure};
use collaboration_central_conformance::{
    ConformanceResult as Result,
    session_link_scenarios::{session_identity_records, session_mutation_records},
    signin_fixtures as signed,
};
use collaboration_central_core::{CentralLinkDispatcher, recheck_lent_link_authority};
use fleet_identity_protocol::{
    CreateFleetSession, CreateSessionKind, HarnessKind, OrganizationId, SessionCreator,
};
use fleet_runtime::ClockIds;
use fleet_runtime_ports::{Clock, SyncTime};
use router_clock::ManualClock;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

const OTHER_ORGANIZATION: &str = "019f0000-0000-7000-8000-0000000053b0";

#[derive(Clone)]
struct FixtureClock(Arc<ManualClock>);
impl Clock for FixtureClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(runtime_primitives::Clock::now(self.0.as_ref()).unix_millis())
    }
}

#[tokio::test]
async fn admitted_machine_cannot_apply_to_a_foreign_stamped_organization_with_the_same_enrollment()
-> Result<()> {
    let clock = ManualClock::starting_at(runtime_primitives::SyncTime::from_unix_millis(
        signed::NOW_SECONDS * 1000,
    ))?;
    let fixture = start_session_link_fixture(Arc::new(clock)).await?;
    let observed = reject_foreign_executor(&fixture).await;
    fixture.close().await?;
    observed
}

async fn reject_foreign_executor(fixture: &MachineLinkFixture) -> Result<()> {
    let admitted_organization = fixture.organization_id.clone();
    let owner = fixture.host.central(&admitted_organization).await?;
    let Ok(machine) = owner.admit_machine_connection(&fixture.token).await? else {
        return Err("the original organization did not admit the signed bearer".into());
    };

    // Clone the live SQLite snapshot so both stores have the same current machine and enrollment.
    let source = owner.runtime().statement_host();
    let foreign_path = fixture.root_path().join("foreign-organization.sqlite");
    sqlx::query("VACUUM INTO ?")
        .bind(
            foreign_path
                .to_str()
                .ok_or("foreign fixture path is not UTF-8")?,
        )
        .execute(source.opened().await?.pool())
        .await?;
    let foreign = Arc::new(ObjectStorage::new(
        foreign_path,
        Arc::new(fleet_storage_schema::workspace_migrations::central_migrator()),
    ));
    let foreign_organization: OrganizationId = OTHER_ORGANIZATION.to_owned().try_into()?;
    let updated =
        sqlx::query("UPDATE signin_organization_identity SET id=?,organization_statement=?")
            .bind(foreign_organization.as_str())
            .bind(signed::stamp(
                foreign_organization.as_str(),
                signed::NOW_SECONDS,
            )?)
            .execute(foreign.opened().await?.pool())
            .await?;
    if updated.rows_affected() != 1 {
        return Err("the foreign fixture did not have exactly one stored stamp".into());
    }

    let clock = FixtureClock(fixture.clock.clone());
    let mut connection = foreign.opened().await?.pool().acquire().await?;
    let (_, lapse) = recheck_lent_link_authority(
        &mut NativeTransaction(&mut connection),
        machine.attachment_ref(),
        &clock,
    )
    .await?;
    drop(connection);
    if lapse.is_some() {
        return Err("the authority rows in the foreign fixture would mask a miswired loan".into());
    }

    let before = foreign.read(session_mutation_records()).await?;
    let identities = foreign.read(session_identity_records()).await?;
    let transactions = NativeSessionTransactions {
        storage: foreign.clone(),
    };
    let entered = Arc::new(AtomicBool::new(false));
    let did_enter = entered.clone();
    let wrong_loan = transactions
        .apply(admitted_organization.clone(), move |_| {
            did_enter.store(true, Ordering::Relaxed);
            Box::pin(async { Err(StoreFailure::Unavailable.into()) })
        })
        .await;
    if !matches!(
        wrong_loan,
        Err(SessionApplyFailure::Wiring(
            SessionWiringFault::ForeignOrganizationStore
        ))
    ) || entered.load(Ordering::Relaxed)
    {
        return Err(
            format!("foreign executor was lent or lost its typed fault: {wrong_loan:?}").into(),
        );
    }

    let dispatcher = SessionLinkDispatcher::new(
        transactions,
        Arc::new(clock.clone()),
        Arc::new(ClockIds::new(clock)),
        fill_session_secret,
    );
    let call = CreateFleetSessionRpc::request(fleet_rpc::Envelope {
        target: admitted_organization,
        body: CreateFleetSession {
            operation_id: "019f0000-0000-7000-8000-0000000053b1".parse()?,
            attempt: 1_u64.try_into()?,
            creator: SessionCreator::RouterOperator {},
            agent_program: HarnessKind::Codex,
            session_kind: CreateSessionKind::Ordinary {},
        },
    });
    let result = dispatcher.dispatch(&machine, call).await;
    if !matches!(result, Err(StoreFailure::Corrupt { reason: CorruptReason::BadValue { ref column } }) if column == "signin_organization_identity.id")
    {
        return Err(format!("the foreign executor produced a session answer: {result:?}").into());
    }
    if foreign.read(session_mutation_records()).await?.rows != before.rows
        || foreign.read(session_identity_records()).await?.rows != identities.rows
    {
        return Err("a miswired executor changed foreign session or actor rows".into());
    }
    Ok(())
}

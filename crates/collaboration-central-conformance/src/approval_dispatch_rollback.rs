//! Rollback through the production approval dispatcher on each owning host.
#[path = "approval_dispatch_corrupt_attempt.rs"]
mod corrupt_attempt;
#[path = "approval_dispatch_holder_offer.rs"]
mod holder_offer;
use std::{future::Future, num::NonZeroUsize};

use approval_request_protocol::{
    ApprovalDecideReply, ApprovalSessionCall, Raise, ReadApprovalForSessionReply,
    ReadApprovalForSessionRequest, RegisterRunReply, RegisterRunRequest,
};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkFrame, MacCallKind, TypedCallResult,
    approval_rpc::{
        ApprovalHolderTarget, ApprovalServiceTarget, ReadApprovalForSessionRpc, RegisterRunRpc,
    },
};
use central_storage::{ReadOutcome, SqlValue, Statement};
use fleet_identity_protocol::{OrganizationId, SessionSecret};
use fleet_rpc::Envelope;
use reqwest_websocket::{CloseCode, Message, WebSocket};
use serde_json::json;
use workspace_ids::CentralSessionId;

use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        announce_mac_calls, central_call, raise, register_run, require_receipt, send_frame,
    },
    approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise},
    fixture_link_reader::read_fixture_frame,
    signin_fixtures::{
        self as signed, Person,
        approval_socket::{DECIDE, PersonApprovalSocket, SocketApprovalReply},
    },
};

pub type ApprovalWriteTrace = Vec<(usize, String)>;
pub type ApprovalSnapshot = Vec<Vec<Vec<SqlValue>>>;

pub trait ApprovalDispatchRollbackHost: Sized {
    fn target(&self) -> Result<ApprovalHolderTarget>;
    fn now_ms(&self) -> i64;
    fn set_time(&self, millis: i64) -> impl Future<Output = Result<()>>;
    fn connect_machine(&self) -> impl Future<Output = Result<WebSocket>>;
    fn connect_person(&self, person: &Person)
    -> impl Future<Output = Result<PersonApprovalSocket>>;
    fn arm(&self, cut: Option<NonZeroUsize>) -> impl Future<Output = Result<()>>;
    fn trace(&self) -> impl Future<Output = Result<ApprovalWriteTrace>>;
    fn prepare_notice_recovery(&self) -> impl Future<Output = Result<()>>;
    fn finish_notice_recovery(&self) -> impl Future<Output = Result<()>>;
    fn ending_recovery_count(&self) -> impl Future<Output = Result<u64>>;
    fn wait_ending_recovery(&self, minimum: u64) -> impl Future<Output = Result<()>>;
    fn scheduled_delivery_count(&self) -> impl Future<Output = Result<u64>>;
    fn drive_due(&self) -> impl Future<Output = Result<()>>;
    fn read(&self, statement: Statement) -> impl Future<Output = Result<ReadOutcome>>;
    fn write(&self, statement: Statement) -> impl Future<Output = Result<()>>;
    fn snapshot(&self, reopen: bool) -> impl Future<Output = Result<ApprovalSnapshot>>;
    fn close(self) -> impl Future<Output = Result<()>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDispatchCase {
    CorruptAttempt,
    FirstPersonSaved,
    FirstPersonLate,
    FirstPersonInvalid,
    HolderFloor,
    SessionReadFloor,
    NoticeRecoveryFloor,
    DeadlineFloor,
}

pub async fn verify_approval_dispatch_rollback<Host: ApprovalDispatchRollbackHost>(
    open: impl AsyncFn() -> Result<Host>,
    case: ApprovalDispatchCase,
) -> Result<()> {
    let control = open().await?;
    let prepared = prepare(&control, case).await?;
    apply(&control, &prepared, Some(None), false).await?;
    let successful_trace = control
        .trace()
        .await
        .map_err(|error| format!("{case:?} successful trace: {error}"))?;
    assert_write_order(case, &successful_trace);
    require_committed(&control, &prepared).await?;
    control.close().await?;

    let host = open().await?;
    let mut prepared = prepare(&host, case).await?;
    let before = host.snapshot(false).await?;
    for write in 1..=successful_trace.len() {
        let ordinal = NonZeroUsize::new(write).ok_or("invalid cut ordinal")?;
        holder_offer::apply_cut(&host, &prepared, ordinal).await?;
        assert_eq!(
            host.trace()
                .await
                .map_err(|error| format!("{case:?} cut {write}: {error}"))?,
            successful_trace
                .get(..write)
                .ok_or("invalid write prefix")?,
            "{case:?} cut {write}"
        );
        assert_eq!(
            host.snapshot(true).await?,
            before,
            "{case:?} rollback after write {write}"
        );
        if matches!(
            case,
            ApprovalDispatchCase::CorruptAttempt | ApprovalDispatchCase::DeadlineFloor
        ) {
            // A failed dispatch still claims the current process's due batch.
            prepared.at += chrono::Duration::minutes(1).num_milliseconds();
        }
    }
    holder_offer::apply_unarmed(&host, &prepared).await?;
    require_committed(&host, &prepared).await?;
    verify_no_write_control(&host, &prepared).await?;
    println!(
        "{case:?} cut {} writes in order: {successful_trace:?}",
        successful_trace.len()
    );
    host.close().await
}

struct Prepared {
    case: ApprovalDispatchCase,
    at: i64,
    request: Option<Raise>,
    parent: Option<(CentralSessionId, SessionSecret)>,
    due_subject: Option<corrupt_attempt::DueSubject>,
}

impl Prepared {
    fn request(&self) -> Result<&Raise> {
        self.request
            .as_ref()
            .ok_or_else(|| "approval action has no prepared request".into())
    }
}

async fn prepare(
    host: &impl ApprovalDispatchRollbackHost,
    case: ApprovalDispatchCase,
) -> Result<Prepared> {
    let target = host.target()?;
    let at = host.now_ms() + 1_000;
    if case == ApprovalDispatchCase::HolderFloor {
        return Ok(Prepared {
            case,
            at,
            request: None,
            parent: None,
            due_subject: None,
        });
    }
    let mut socket = host.connect_machine().await?;
    assert_eq!(
        register_run(&mut socket, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (first, secret) = create_bound(&mut socket, &target.organization_id, 301, None).await?;
    let (requester, parent) = match case {
        ApprovalDispatchCase::SessionReadFloor | ApprovalDispatchCase::NoticeRecoveryFloor => {
            let (delegate, _) = create_bound(
                &mut socket,
                &target.organization_id,
                302,
                Some((&first, secret.clone())),
            )
            .await?;
            (delegate, Some((first, secret)))
        }
        _ => (first, None),
    };
    let request = provider_raise(&target, &requester, 303, ProviderAsk::Approval)?;
    let receipt =
        require_receipt(raise(&mut socket, &target.organization_id, request.clone()).await?)?;
    socket.close(CloseCode::Normal, None).await?;
    let deadline = receipt.window.deadline().timestamp_millis();
    let mut due_subject = None;
    let at = match case {
        ApprovalDispatchCase::CorruptAttempt => {
            due_subject = Some(corrupt_attempt::read_due_subject(host, &request, deadline).await?);
            host.write(Statement {
                sql: "INSERT INTO engine_work_attempts
                    (attempt_id, machine_kind, subject_key, state, failure_count, next_try_at,
                     last_failure, first_failed_at, last_failed_at, created_at, updated_at)
                    SELECT '019f0000-0000-7000-8000-0000000058a1', 'approval_request',
                        approval_row_id, 'failing', 1, first_saved_at, 'fixture_unknown_failure',
                        first_saved_at, first_saved_at, first_saved_at, first_saved_at
                    FROM approval_requests WHERE request_id=?1",
                args: vec![text(request.request_id().as_str())],
            })
            .await?;
            effective_due(host).await?
        }
        ApprovalDispatchCase::DeadlineFloor => effective_due(host).await?,
        ApprovalDispatchCase::FirstPersonLate | ApprovalDispatchCase::NoticeRecoveryFloor => {
            // These entries settle the deadline themselves. Keep the alarm's
            // projected wake later than the applying sample.
            host.write(Statement {
                sql: "UPDATE engine_due_entries SET effective_due_ms=?1 WHERE work_kind='approval_request'",
                args: vec![SqlValue::Integer(central_storage::SafeInteger::new(deadline + 300_000)?)],
            }).await?;
            deadline
        }
        _ => at,
    };
    if matches!(
        case,
        ApprovalDispatchCase::FirstPersonSaved
            | ApprovalDispatchCase::FirstPersonLate
            | ApprovalDispatchCase::FirstPersonInvalid
    ) {
        seed_person_authorization(host, &target).await?;
        assert!(
            host.read(query("SELECT actor_id FROM actor_persons"))
                .await?
                .rows
                .is_empty()
        );
        if case == ApprovalDispatchCase::FirstPersonLate {
            holder_offer::registered_before_decision(host, at)
                .await?
                .close(CloseCode::Normal, None)
                .await?;
        }
    }
    Ok(Prepared {
        case,
        at,
        request: Some(request),
        parent,
        due_subject,
    })
}

async fn effective_due(host: &impl ApprovalDispatchRollbackHost) -> Result<i64> {
    let outcome = host
        .read(query(
            "SELECT effective_due_ms FROM engine_due_entries WHERE work_kind='approval_request'",
        ))
        .await?;
    let [row] = outcome.rows.as_slice() else {
        return Err("expected one approval due".into());
    };
    let [SqlValue::Integer(at)] = row.as_slice() else {
        return Err("expected an integer approval due".into());
    };
    Ok(at.get())
}

async fn seed_person_authorization(
    host: &impl ApprovalDispatchRollbackHost,
    target: &ApprovalHolderTarget,
) -> Result<()> {
    host.write(Statement {
        sql: "INSERT INTO signin_authorization_floors
              VALUES (?1, 3, 'active', ?2, 0, 1, 1)
              ON CONFLICT(id) DO UPDATE SET minimum_revision=3, state='active', permission_policy_digest=?2",
        args: vec![text(target.organization_id.as_str()), text(signed::POLICY)],
    }).await?;
    host.write(Statement {
        sql: "INSERT INTO signin_person_authorization_floors
              VALUES (?1, ?2, ?3, 4, 'active', 1, 1)",
        args: vec![
            text(signed::OWNER_SUBJECT),
            text(target.organization_id.as_str()),
            text(signed::OWNER),
        ],
    })
    .await
}

async fn apply(
    host: &impl ApprovalDispatchRollbackHost,
    prepared: &Prepared,
    probe: Option<Option<NonZeroUsize>>,
    rejected: bool,
) -> Result<()> {
    let target = host.target()?;
    let organization = &target.organization_id;
    host.set_time(prepared.at).await?;
    match prepared.case {
        ApprovalDispatchCase::CorruptAttempt | ApprovalDispatchCase::DeadlineFloor => {
            arm_when_requested(host, probe).await?;
            host.drive_due().await?;
        }
        ApprovalDispatchCase::FirstPersonSaved
        | ApprovalDispatchCase::FirstPersonLate
        | ApprovalDispatchCase::FirstPersonInvalid => {
            let person = Person::owner(organization.as_str(), prepared.at.div_euclid(1_000))?;
            let mut socket = host.connect_person(&person).await?;
            arm_when_requested(host, probe).await?;
            let option = if prepared.case == ApprovalDispatchCase::FirstPersonInvalid {
                2
            } else {
                1
            };
            let reply = socket
                .call(
                    DECIDE,
                    organization.as_str(),
                    &json!({
                        "requestId":prepared.request()?.request_id(),
                        "operationId":"019f0000-0000-7000-8000-0000000058a2",
                        "answer":{"kind":"approval","optionId":option}
                    }),
                )
                .await?;
            if rejected {
                assert_eq!(
                    reply,
                    SocketApprovalReply::Refused {
                        reason: "centralFault".to_owned()
                    }
                );
            } else {
                let reply: ApprovalDecideReply = serde_json::from_slice(&reply.answer()?)?;
                match prepared.case {
                    ApprovalDispatchCase::FirstPersonSaved => {
                        assert!(matches!(reply, ApprovalDecideReply::SavedDecision { .. }))
                    }
                    ApprovalDispatchCase::FirstPersonLate => {
                        assert_eq!(reply, ApprovalDecideReply::TooLate {})
                    }
                    ApprovalDispatchCase::FirstPersonInvalid => {
                        assert!(matches!(reply, ApprovalDecideReply::Invalid { .. }))
                    }
                    unexpected => {
                        return Err(format!("unexpected person case {unexpected:?}").into());
                    }
                }
            }
            socket.socket.close(CloseCode::Normal, None).await?;
        }
        ApprovalDispatchCase::HolderFloor => {
            let socket = host.connect_machine().await?;
            arm_when_requested(host, probe).await?;
            check_machine_call::<RegisterRunRpc>(
                socket,
                organization,
                RegisterRunRequest {
                    router_run: target.router_run,
                },
                rejected,
                |reply| *reply == RegisterRunReply::Registered {},
            )
            .await?;
        }
        ApprovalDispatchCase::SessionReadFloor => {
            let (session, secret) = prepared
                .parent
                .as_ref()
                .ok_or("missing authenticated parent")?;
            let socket = host.connect_machine().await?;
            arm_when_requested(host, probe).await?;
            check_machine_call::<ReadApprovalForSessionRpc>(
                socket,
                organization,
                ApprovalSessionCall {
                    session_id: session.clone(),
                    secret: secret.clone(),
                    input: ReadApprovalForSessionRequest {
                        request_id: prepared.request()?.request_id().clone(),
                    },
                },
                rejected,
                |reply| matches!(reply, ReadApprovalForSessionReply::Request { .. }),
            )
            .await?;
        }
        ApprovalDispatchCase::NoticeRecoveryFloor => {
            let mut socket = host.connect_machine().await?;
            host.prepare_notice_recovery().await?;
            arm_when_requested(host, probe).await?;
            announce_mac_calls(&mut socket, vec![MacCallKind::ApprovalNotice]).await?;
            host.finish_notice_recovery().await?;
            socket.close(CloseCode::Normal, None).await?;
        }
    }
    Ok(())
}

async fn arm_when_requested(
    host: &impl ApprovalDispatchRollbackHost,
    probe: Option<Option<NonZeroUsize>>,
) -> Result<()> {
    if let Some(cut) = probe {
        host.arm(cut).await?;
    }
    Ok(())
}

async fn check_machine_call<Rpc: CentralLinkRpc<Target = ApprovalServiceTarget>>(
    mut socket: WebSocket,
    organization: &OrganizationId,
    body: Rpc::Request,
    rejected: bool,
    passed: impl FnOnce(&Rpc::Reply) -> bool,
) -> Result<()> {
    if !rejected {
        let reply = central_call::<Rpc>(&mut socket, organization, body).await?;
        assert!(matches!(&reply, TypedCallResult::Answered { reply } if passed(reply)));
        socket.close(CloseCode::Normal, None).await?;
        return Ok(());
    }
    send_frame(
        &mut socket,
        LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: "019f0000-0000-7000-8000-0000000058a3".parse()?,
            request: Box::new(Rpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body,
            })),
        }),
    )
    .await?;
    let closed = read_fixture_frame(&mut socket, "approval write failure close").await?;
    assert!(
        matches!(&closed, Some(Ok(Message::Close { code: CloseCode::Error, reason })) if reason == "central unavailable"),
        "{closed:?}"
    );
    Ok(())
}

fn assert_write_order(case: ApprovalDispatchCase, writes: &ApprovalWriteTrace) {
    let mut expected = Vec::new();
    let timeout = [
        "UPDATE approval_requests",
        "DELETE FROM engine_work_attempts",
        "INSERT INTO engine_due_entries",
    ];
    let first_person = [
        "INSERT INTO actor_identities",
        "INSERT INTO actor_persons",
        "INSERT INTO runtime_request_log",
        "DELETE FROM runtime_request_log",
        "UPDATE runtime_request_log",
    ];
    let floor = "INSERT INTO approval_applying_time";
    match case {
        ApprovalDispatchCase::CorruptAttempt => expected.extend([
            floor,
            "INSERT INTO engine_work_attempts",
            "UPDATE engine_due_entries",
        ]),
        ApprovalDispatchCase::DeadlineFloor => {
            expected.push(floor);
            expected.extend(timeout);
        }
        ApprovalDispatchCase::FirstPersonSaved | ApprovalDispatchCase::FirstPersonLate => {
            expected.extend(timeout);
            expected.extend(first_person);
            expected.push(floor);
        }
        ApprovalDispatchCase::FirstPersonInvalid => expected.extend(first_person),
        ApprovalDispatchCase::HolderFloor => {
            expected.extend([floor, "INSERT INTO approval_router_runs"])
        }
        ApprovalDispatchCase::SessionReadFloor => {
            expected.extend(["UPDATE approval_request_destinations", floor])
        }
        ApprovalDispatchCase::NoticeRecoveryFloor => {
            expected.extend(timeout);
            expected.push(floor);
        }
    }
    assert_eq!(writes.len(), expected.len(), "{case:?} successful trace");
    for ((ordinal, label), (index, expected)) in writes.iter().zip(expected.into_iter().enumerate())
    {
        assert_eq!(*ordinal, index + 1);
        assert!(
            label.starts_with(expected),
            "statement {ordinal}: expected {expected}, got {label}"
        );
    }
}

async fn require_committed(
    host: &impl ApprovalDispatchRollbackHost,
    prepared: &Prepared,
) -> Result<()> {
    let rows = |sql| async move {
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(host.read(query(sql)).await?.rows)
    };
    if prepared.case != ApprovalDispatchCase::FirstPersonInvalid {
        assert_eq!(
            rows("SELECT observed_ms FROM approval_applying_time").await?,
            [vec![SqlValue::Integer(central_storage::SafeInteger::new(
                prepared.at
            )?)]]
        );
    } else {
        assert_eq!(
            rows("SELECT observed_ms FROM approval_applying_time").await?,
            [vec![SqlValue::Integer(central_storage::SafeInteger::new(
                prepared.at - 1_000
            )?)]]
        );
    }
    match prepared.case {
        ApprovalDispatchCase::CorruptAttempt => {
            corrupt_attempt::require_repaired(host, prepared).await?;
        }
        ApprovalDispatchCase::FirstPersonSaved
        | ApprovalDispatchCase::FirstPersonLate
        | ApprovalDispatchCase::FirstPersonInvalid => {
            assert_eq!(rows("SELECT person.person_id FROM actor_persons person
                             JOIN actor_identities identity ON identity.actor_id=person.actor_id
                             JOIN runtime_request_log log ON log.caller=identity.actor_id
                             JOIN approval_requests request ON request.approval_row_id=log.approval_row_id
                             WHERE identity.actor_kind='person'").await?,
                [vec![text(signed::OWNER)]]);
            let status = if prepared.case == ApprovalDispatchCase::FirstPersonInvalid {
                "waiting"
            } else {
                "ended"
            };
            assert_eq!(
                rows("SELECT record_state FROM approval_requests").await?,
                [vec![text(status)]]
            );
        }
        ApprovalDispatchCase::HolderFloor => {
            assert_eq!(
                rows("SELECT router_run_id FROM approval_router_runs").await?,
                [vec![text(host.target()?.router_run.as_str())]]
            );
        }
        ApprovalDispatchCase::SessionReadFloor => {
            assert_eq!(rows("SELECT request_id FROM approval_request_destinations WHERE first_read_at IS NOT NULL").await?,
                [vec![text(prepared.request()?.request_id().as_str())]]);
        }
        ApprovalDispatchCase::NoticeRecoveryFloor | ApprovalDispatchCase::DeadlineFloor => {
            assert_eq!(
                rows("SELECT record_state, delivery_state FROM approval_requests").await?,
                [vec![text("ended"), text("owed")]]
            );
            assert!(effective_due(host).await? > prepared.at);
        }
    }
    Ok(())
}

async fn verify_no_write_control(
    host: &impl ApprovalDispatchRollbackHost,
    prepared: &Prepared,
) -> Result<()> {
    match prepared.case {
        ApprovalDispatchCase::HolderFloor => {
            let before = host.snapshot(false).await?;
            let older = Prepared {
                case: prepared.case,
                at: prepared.at - 1,
                request: None,
                parent: None,
                due_subject: None,
            };
            apply(host, &older, Some(None), false).await?;
            assert!(host.trace().await?.is_empty());
            assert_eq!(host.snapshot(true).await?, before);
        }
        ApprovalDispatchCase::NoticeRecoveryFloor => {
            let before = host.snapshot(false).await?;
            let empty = Prepared {
                case: prepared.case,
                at: prepared.at + 1,
                request: None,
                parent: None,
                due_subject: None,
            };
            apply(host, &empty, Some(None), false).await?;
            assert!(host.trace().await?.is_empty());
            assert_eq!(host.snapshot(true).await?, before);
        }
        _ => {}
    }
    Ok(())
}

pub async fn approval_snapshot(
    read: impl AsyncFn(Statement) -> Result<ReadOutcome>,
) -> Result<ApprovalSnapshot> {
    let mut snapshot = Vec::new();
    for sql in [
        "SELECT * FROM approval_requests ORDER BY approval_row_id",
        "SELECT * FROM approval_request_destinations ORDER BY approval_destination_id",
        "SELECT * FROM approval_router_runs ORDER BY router_run_id",
        "SELECT * FROM approval_applying_time ORDER BY organization_id",
        "SELECT * FROM engine_work_attempts ORDER BY attempt_id",
        "SELECT * FROM engine_due_entries ORDER BY due_entry_id",
        "SELECT * FROM runtime_request_log ORDER BY request_id",
        "SELECT * FROM actor_identities ORDER BY actor_id",
        "SELECT * FROM actor_persons ORDER BY actor_id",
    ] {
        snapshot.push(read(query(sql)).await?.rows);
    }
    Ok(snapshot)
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

fn query(sql: &'static str) -> Statement {
    Statement {
        sql,
        args: Vec::new(),
    }
}

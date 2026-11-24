//! Controls the actual modern CentralObject only in test-support bundles.
use super::{CentralObject, ObjectAddress, OrganizationId, statement_store};
use crate::store_wire::{OwnedStatement, OwnedStoreCall};
use fleet_runtime::{HeldPause, PausePoint};
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};
use worker::{Env, Request, Response, Result};
use worker::{Headers, Method, RequestInit, js_sys};
#[path = "engine_due_migration_fixture.rs"]
mod engine_due_migration_fixture;
#[path = "workspace_fixture_admission.rs"]
mod fixture_admission;
#[path = "workspace_store_cases.rs"]
mod workspace_store_cases;
#[path = "workspace_turn_cases.rs"]
mod workspace_turn_cases;

#[derive(Default)]
pub(crate) struct Controls {
    pub(super) fail_approval_after_write: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(super) fail_reserved_session_reply: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(super) fail_typed_refusal_send: std::cell::Cell<bool>,
    pub(super) faults: std::sync::Arc<fleet_runtime::TestFaults>,
    pub(super) zero_next_write_alarm: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(super) lose_read: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(super) legacy_workspace_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub(super) workspace_page_reads: std::sync::Arc<std::sync::Mutex<Option<Vec<u64>>>>,
    pub(super) retired_page_reads: std::sync::Arc<std::sync::Mutex<Option<Vec<u64>>>>,
    pub(super) direct_mark_guard_reads: std::sync::Arc<std::sync::Mutex<Option<Vec<u64>>>>,
    observations: RefCell<BTreeMap<workspace_ids::WorkspaceId, HeldPause>>,
    holds: RefCell<BTreeMap<(workspace_ids::WorkspaceId, Point), HeldPause>>,
    central_holds: RefCell<BTreeMap<Point, HeldPause>>,
    engine_holds: RefCell<BTreeMap<u64, HeldPause>>,
    next_engine_hold: std::cell::Cell<u64>,
    /// A probe caller admitted once, ahead of a held turn, so a request can
    /// run without its own admission.
    admitted_probe_caller: RefCell<Option<collaboration_central_core::WorkspaceCaller>>,
    alarm_waiters: RefCell<Vec<futures_channel::oneshot::Sender<()>>>,
}
#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
enum Point {
    AfterReads,
    AfterStoreApplied,
    BeforeWorkspaceRequestApplying,
    AfterWorkspaceExecute,
    EngineCompletionEntered,
    BeforeDrainCommit,
    AfterDrainCommit,
    BeforeTimerCommit,
    BeforeWakeRequestCommit,
    BeforeReceiptTransaction,
    BeforeMachineLinkWelcome,
    AfterMachineLinkWelcome,
    BeforeSessionTransaction,
    BeforeApprovalCommit,
    ApprovalEndingNeverReached,
    ApprovalEndingAnswerApplied,
    BeforeApprovalEndingWrite,
    BeforeApprovalEndingEnqueue,
    BeforeApprovalCarrierTurn,
    BeforeParentApprovalTransaction,
    BeforeParentApprovalReply,
    BeforeApprovalNoticeWrite,
    AfterApprovalNoticeAttempt,
    AfterApprovalNoticeRecovery,
    BeforeMacEndingOffer,
    AfterMacEndingOffer,
    BeforeMacNoticeOffer,
    AfterMacNoticeOffer,
    MacEndingRecoveryStarted,
    MacEndingRecoveryFinished,
    MacEndingRegistrationRecoveryScheduled,
    BeforePersonApprovalTurn,
    AfterPersonApprovalCommit,
    AfterIdleExpiryCommit,
    AfterIdleRenewalPlatformSendError,
    BeforePrivateEnrollmentInvalidation,
    BeforeIdleEnrollmentCleanup,
    AfterIdleEnrollmentCleanup,
    AfterTimerCommit,
    AfterEngineAttemptGate,
    AfterEngineLoad,
    AfterEngineCommit,
    BeforeEngineFailureWrite,
    AfterEngineAbsentLookup,
    BeforeControlClear,
    BeforeRenewalApplyingSample,
    BeforeMachineAuthorityCloseWrite,
    AfterMachineFrameHandled,
    AfterMachineMailOffer,
    BeforeSignInSample,
    AfterSignInSample,
}
impl From<Point> for PausePoint {
    fn from(point: Point) -> Self {
        match point {
            Point::AfterReads => Self::AfterReads,
            Point::AfterStoreApplied => Self::AfterStoreApplied,
            Point::BeforeWorkspaceRequestApplying => Self::BeforeWorkspaceRequestApplying,
            Point::AfterWorkspaceExecute => Self::AfterWorkspaceExecute,
            Point::EngineCompletionEntered => Self::EngineCompletionEntered,
            Point::BeforeDrainCommit => Self::BeforeDrainCommit,
            Point::AfterDrainCommit => Self::AfterDrainCommit,
            Point::BeforeTimerCommit => Self::BeforeTimerCommit,
            Point::BeforeWakeRequestCommit => Self::BeforeWakeRequestCommit,
            Point::BeforeReceiptTransaction => Self::BeforeReceiptTransaction,
            Point::BeforeMachineLinkWelcome => Self::BeforeMachineLinkWelcome,
            Point::AfterMachineLinkWelcome => Self::AfterMachineLinkWelcome,
            Point::BeforeSessionTransaction => Self::BeforeSessionTransaction,
            Point::BeforeApprovalCommit => Self::BeforeApprovalCommit,
            Point::ApprovalEndingNeverReached => Self::ApprovalEndingNeverReached,
            Point::ApprovalEndingAnswerApplied => Self::ApprovalEndingAnswerApplied,
            Point::BeforeApprovalEndingWrite => Self::BeforeApprovalEndingWrite,
            Point::BeforeApprovalEndingEnqueue => Self::BeforeApprovalEndingEnqueue,
            Point::BeforeApprovalCarrierTurn => Self::BeforeApprovalCarrierTurn,
            Point::BeforeParentApprovalTransaction => Self::BeforeParentApprovalTransaction,
            Point::BeforeParentApprovalReply => Self::BeforeParentApprovalReply,
            Point::BeforeApprovalNoticeWrite => Self::BeforeApprovalNoticeWrite,
            Point::AfterApprovalNoticeAttempt => Self::AfterApprovalNoticeAttempt,
            Point::AfterApprovalNoticeRecovery => Self::AfterApprovalNoticeRecovery,
            Point::BeforeMacEndingOffer => Self::BeforeMacEndingOffer,
            Point::AfterMacEndingOffer => Self::AfterMacEndingOffer,
            Point::BeforeMacNoticeOffer => Self::BeforeMacNoticeOffer,
            Point::AfterMacNoticeOffer => Self::AfterMacNoticeOffer,
            Point::MacEndingRecoveryStarted => Self::MacEndingRecoveryStarted,
            Point::MacEndingRecoveryFinished => Self::MacEndingRecoveryFinished,
            Point::MacEndingRegistrationRecoveryScheduled => {
                Self::MacEndingRegistrationRecoveryScheduled
            }
            Point::BeforePersonApprovalTurn => Self::BeforePersonApprovalTurn,
            Point::AfterPersonApprovalCommit => Self::AfterPersonApprovalCommit,
            Point::AfterIdleExpiryCommit => Self::AfterIdleExpiryCommit,
            Point::AfterIdleRenewalPlatformSendError => Self::AfterIdleRenewalPlatformSendError,
            Point::BeforePrivateEnrollmentInvalidation => Self::BeforePrivateEnrollmentInvalidation,
            Point::BeforeIdleEnrollmentCleanup => Self::BeforeIdleEnrollmentCleanup,
            Point::AfterIdleEnrollmentCleanup => Self::AfterIdleEnrollmentCleanup,
            Point::AfterTimerCommit => Self::AfterTimerCommit,
            Point::AfterEngineAttemptGate => Self::AfterEngineAttemptGate,
            Point::AfterEngineLoad => Self::AfterEngineLoad,
            Point::AfterEngineCommit => Self::AfterEngineCommit,
            Point::BeforeEngineFailureWrite => Self::BeforeEngineFailureWrite,
            Point::AfterEngineAbsentLookup => Self::AfterEngineAbsentLookup,
            Point::BeforeControlClear => Self::BeforeControlClear,
            Point::BeforeRenewalApplyingSample => Self::BeforeRenewalApplyingSample,
            Point::BeforeMachineAuthorityCloseWrite => Self::BeforeMachineAuthorityCloseWrite,
            Point::AfterMachineFrameHandled => Self::AfterMachineFrameHandled,
            Point::AfterMachineMailOffer => Self::AfterMachineMailOffer,
            Point::BeforeSignInSample => Self::BeforeSignInSample,
            Point::AfterSignInSample => Self::AfterSignInSample,
        }
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EnginePause {
    point: Point,
    key: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Pause {
    workspace: workspace_ids::WorkspaceId,
    point: Point,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Wait {
    workspace: workspace_ids::WorkspaceId,
    count: usize,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Store {
    workspace: workspace_ids::WorkspaceId,
    call: OwnedStoreCall,
}
fn error(message: impl ToString) -> worker::Error {
    worker::Error::RustError(message.to_string())
}

pub(crate) async fn route(
    mut request: Request,
    env: &Env,
) -> Result<std::result::Result<Response, Request>> {
    let path = request.path();
    let Some(rest) = path.strip_prefix("/test-support/workspace/") else {
        return Ok(Err(request));
    };
    let Some((organization, operation)) = rest.split_once('/') else {
        return Ok(Ok(Response::error("missing operation", 400)?));
    };
    let organization: OrganizationId =
        serde_json::from_value(serde_json::Value::String(organization.to_owned()))?;
    if operation == "typed-mac-call" {
        return Ok(Ok(super::workspace_typed_link::probe(
            env,
            organization,
            request,
        )
        .await?));
    }
    let ending_path = match operation {
        "typed-ending-call" => Some(super::workspace_typed_link::FIXTURE_ENDING_CALL_PATH),
        "typed-ending-production-ingress" => Some(super::workspace_typed_link::MACHINE_CALL_PATH),
        _ => None,
    };
    if let Some(path) = ending_path {
        return Ok(Ok(super::workspace_typed_link::probe_ending(
            env,
            organization,
            request,
            path,
        )
        .await?));
    }
    let address = ObjectAddress::Central(organization);

    let headers = Headers::new();
    crate::cloudflare_transport::address_header(&headers, &address)?;
    let body = request.bytes().await?;
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(js_sys::Uint8Array::from(body.as_slice()).into()));
    let path = format!("/test/{operation}");
    let forwarded = Request::new_with_init(&format!("https://central.internal{path}"), &init)?;
    Ok(Ok(crate::cloudflare_transport::stub(env, &address)?
        .fetch_with_request(forwarded)
        .await?))
}
pub(super) async fn control(
    object: &CentralObject,
    organization: OrganizationId,
    mut request: Request,
) -> Result<Response> {
    let path = request.path();
    if path == "/test/typed-mac-waiting" {
        let correlation = request
            .json::<central_link_protocol::CorrelationId>()
            .await?;
        return Response::from_json(&object.pending_mac_calls.is_waiting(&correlation));
    }
    if path == "/test/drop-approval-before-write" {
        return Response::from_json(&object.pending_mac_calls.drop_approval_before_write());
    }
    if path == "/test/approval-fixture-decide" {
        let runtime = object.runtime(organization).await?;
        let completion = runtime
            .fixture_decide_approval(request.json().await?)
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        if let Some(completion) = completion {
            object.state.wait_until(completion);
        }
        return Response::from_json(&true);
    }
    if path == "/test/fail-approval-after-write" {
        object.controls.fail_approval_after_write.store(
            request.json::<bool>().await?,
            std::sync::atomic::Ordering::Release,
        );
        return Response::from_json(&true);
    }
    if path == "/test/fail-reserved-session-reply" {
        object.controls.fail_reserved_session_reply.store(
            request.json::<bool>().await?,
            std::sync::atomic::Ordering::Release,
        );
        return Response::from_json(&true);
    }
    if path == "/test/fail-typed-refusal-send" {
        object
            .controls
            .fail_typed_refusal_send
            .set(request.json::<bool>().await?);
        return Response::from_json(&true);
    }
    if path == "/test/machine-login-revoke" {
        let control: collaboration_central_core::MachineLoginControl = request.json().await?;
        let runtime = object.runtime(organization).await?;
        return Response::from_json(
            &collaboration_central_core::machine_login_control(runtime.as_ref(), control).await,
        );
    }
    if matches!(path.as_str(), "/test/request" | "/test/mail") {
        return fixture_admission::dispatch(object, organization, request).await;
    }
    if path == "/test/receipt-clock-after-enrollment-read" {
        object
            .clock
            .move_after_receipt_enrollment_read(request.json::<i64>().await?);
        return Response::from_json(&true);
    }
    // Holding time before initialization also pins the first minted identity.
    if path == "/test/clock" {
        object.clock.hold(request.json::<i64>().await?);
        return Response::from_json(&true);
    }
    let runtime = object.runtime(organization.clone()).await?;
    match path.as_str() {
        "/test/direct-receipt-fixture" => {
            let (authority, message, receipt): (
                fleet_contracts::MachineConnectionAttachment,
                workspace_ids::MessageId,
                workspace_model::central_mail::ReceiptKind,
            ) = request.json().await?;
            Response::from_json(
                &runtime
                    .apply_delivery_receipt(&authority, &message, receipt)
                    .await,
            )
        }
        "/test/workspace-page-read-observations" => {
            let arm = request.json::<bool>().await?;
            let mut observations = object
                .controls
                .workspace_page_reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let observed = observations.take();
            if arm {
                *observations = Some(Vec::new());
            }
            Response::from_json(&observed)
        }
        "/test/retired-page-read-observations" => {
            let arm = request.json::<bool>().await?;
            let mut observations = object
                .controls
                .retired_page_reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Response::from_json(&std::mem::replace(&mut *observations, arm.then(Vec::new)))
        }
        "/test/direct-mark-guard-read-observations" => {
            let arm = request.json::<bool>().await?;
            let mut observations = object
                .controls
                .direct_mark_guard_reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Response::from_json(&std::mem::replace(&mut *observations, arm.then(Vec::new)))
        }
        "/test/completion-attempts" => {
            Response::from_json(&runtime.runtime().completion_attempts())
        }
        "/test/engine-admin-control" => {
            let control = request.json().await?;
            Response::from_json(&runtime.fixture_engine_admin(control).await)
        }
        "/test/engine-alarm-inputs" => {
            use fleet_runtime_ports::CentralStore as _;

            let inputs = runtime
                .runtime()
                .statement_host()
                .engine_alarm_inputs()
                .await;
            Response::from_json(&inputs.map(|times| {
                [
                    times.earliest_on_grid_due_unix_millis,
                    times.earliest_legacy_due_unix_millis,
                ]
            }))
        }
        "/test/engine-completion-entries" => {
            Response::from_json(&runtime.runtime().engine_completion_entries())
        }
        "/test/try-completion-turns" => {
            let workspace = request.json::<workspace_ids::WorkspaceId>().await?;
            let central_free = runtime.runtime().try_central_turn().is_some();
            let workspace_free = runtime
                .runtime()
                .workspace_turns()
                .try_take(&workspace)
                .is_some();
            Response::from_json(&[central_free, workspace_free])
        }
        "/test/zero-next-write-alarm" => {
            object
                .controls
                .zero_next_write_alarm
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Response::from_json(&true)
        }
        "/test/legacy-unit-invocations" => {
            Response::from_json(&runtime.runtime().legacy_unit_invocations())
        }
        "/test/due-unit-starts" => Response::from_json(&runtime.runtime().due_unit_starts()),
        "/test/engine-commit-fault-pending" => {
            Response::from_json(&object.controls.faults.commit_pending())
        }
        "/test/engine-commit-fault" => {
            let fault = request.json().await?;
            object.controls.faults.arm_commit(fault);
            Response::from_json(&true)
        }
        "/test/engine-read-fault" => {
            let fault = request.json().await?;
            object.controls.faults.arm_read(fault);
            Response::from_json(&true)
        }
        "/test/machine-restore-failure" => {
            let (skip, failure) = request.json().await?;
            runtime.fail_machine_restore_after(skip, failure);
            Response::from_json(&true)
        }
        "/test/fail-mail-offer" => {
            runtime.fail_next_mail_offer(request.json().await?);
            Response::from_json(&true)
        }
        "/test/fail-idle-renewal-send" => {
            runtime.fail_next_idle_renewal_send();
            Response::from_json(&true)
        }
        "/test/engine-probe-seed" => {
            Response::from_json(&runtime.seed_probe(request.json().await?).await)
        }
        "/test/engine-probe-request" => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct ProbeCall {
                caller: workspace_model::Caller,
                token: String,
                request: collaboration_central_core::engine_probe::ProbeRequest,
            }
            let call: ProbeCall = request.json().await?;
            let Some(caller) =
                fixture_admission::admitted_caller(&runtime, &call.token, call.caller).await?
            else {
                return Response::error("probe caller not admitted", 401);
            };
            Response::from_json(&runtime.request_probe(&caller, call.request).await)
        }
        "/test/engine-owed-seed" => {
            Response::from_json(&runtime.seed_owed_reply(request.json().await?).await)
        }
        "/test/engine-owed-request" => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct OwedCall {
                caller: workspace_model::Caller,
                token: String,
                request: collaboration_central_core::engine_owed_probe::OwedReplyRequest,
            }
            let call: OwedCall = request.json().await?;
            let Some(caller) =
                fixture_admission::admitted_caller(&runtime, &call.token, call.caller).await?
            else {
                return Response::error("probe caller not admitted", 401);
            };
            Response::from_json(&runtime.request_owed_reply(&caller, call.request).await)
        }
        "/test/lose-provisioning-reply" => {
            runtime.lose_next_provisioning_reply();
            Response::from_json(&true)
        }
        "/test/alarm-batch-fault-pending" => {
            Response::from_json(&object.controls.faults.alarm_batch_pending())
        }
        "/test/fail-issuer-cache" => {
            Response::from_json(&runtime.fail_next_issuer_cache(request.json().await?))
        }
        "/test/mac-delivery-count" => {
            let point: Point = request.json().await?;
            Response::from_json(&runtime.runtime().pause_points().central_count(point.into()))
        }
        "/test/until-mac-delivery-count" => {
            let (point, minimum): (Point, u64) = request.json().await?;
            runtime
                .runtime()
                .pause_points()
                .until_central_count(point.into(), minimum)
                .await;
            Response::from_json(&true)
        }
        "/test/mac-call-waits" => Response::from_json(&object.clock.mac_call_waits()),
        "/test/fail-alarm-batch" => {
            let failure = request.json().await?;
            object.controls.faults.arm_alarm_batch(failure);
            Response::from_json(&true)
        }
        "/test/drain" => {
            let workspace = request.json().await?;
            Response::from_json(&runtime.drain_workspace(&workspace).await)
        }
        "/test/queue" => {
            let session: workspace_ids::SessionRef = request.json().await?;
            Response::from_json(&runtime.queued_for_session(&session).await)
        }
        "/test/alarm" => Response::from_json(&object.state.storage().get_alarm().await?),
        "/test/hold-central"
        | "/test/hold-engine"
        | "/test/until-held-engine"
        | "/test/release-engine"
        | "/test/until-central-waiting"
        | "/test/until-admission-waiting"
        | "/test/admission-waiting"
        | "/test/engine-probe-admit"
        | "/test/engine-probe-request-admitted"
        | "/test/until-held-central"
        | "/test/release-central" => {
            workspace_turn_cases::control(object, &runtime, &path, request).await
        }
        "/test/central-batch"
        | "/test/central-read"
        | "/test/engine-due-schedules"
        | "/test/engine-due-system-expiries"
        | "/test/store"
        | "/test/store-failing-upgrade"
        | "/test/reset-store"
        | "/test/central-failing-upgrade"
        | "/test/lose-read"
        | "/test/lose-reply"
        | "/test/hold"
        | "/test/until-held"
        | "/test/guard-next-read"
        | "/test/waiting-before-read"
        | "/test/crash"
        | "/test/until-waiting"
        | "/test/release" => workspace_store_cases::control(object, &runtime, &path, request).await,
        _ => Response::error("unknown workspace control", 404),
    }
}

#[path = "workspace_raw_controls.rs"]
mod raw_controls;
pub(super) use raw_controls::{alarm_with_receipt, raw_control};

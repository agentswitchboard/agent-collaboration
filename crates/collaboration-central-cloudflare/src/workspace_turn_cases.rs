//! Central and Engine turn controls for the test-support bundle.
use super::super::OrgRuntime;
use super::{CentralObject, EnginePause, Point, error, fixture_admission};
use serde::Deserialize;
use worker::{Request, Response, Result};

pub(super) async fn control(
    object: &CentralObject,
    runtime: &OrgRuntime,
    path: &str,
    mut request: Request,
) -> Result<Response> {
    match path {
        "/test/hold-central" => {
            let point: Point = request.json().await?;
            if !matches!(
                point,
                Point::BeforeTimerCommit
                    | Point::BeforeWakeRequestCommit
                    | Point::BeforeReceiptTransaction
                    | Point::BeforeSessionTransaction
                    | Point::ApprovalEndingNeverReached
                    | Point::ApprovalEndingAnswerApplied
                    | Point::BeforeApprovalEndingWrite
                    | Point::BeforeApprovalCarrierTurn
                    | Point::BeforeParentApprovalTransaction
                    | Point::BeforeParentApprovalReply
                    | Point::BeforeApprovalNoticeWrite
                    | Point::AfterApprovalNoticeAttempt
                    | Point::AfterApprovalNoticeRecovery
                    | Point::BeforeMacEndingOffer
                    | Point::AfterMacEndingOffer
                    | Point::BeforeMacNoticeOffer
                    | Point::AfterMacNoticeOffer
                    | Point::MacEndingRecoveryStarted
                    | Point::MacEndingRecoveryFinished
                    | Point::BeforePersonApprovalTurn
                    | Point::AfterPersonApprovalCommit
                    | Point::AfterIdleExpiryCommit
                    | Point::AfterIdleRenewalPlatformSendError
                    | Point::BeforePrivateEnrollmentInvalidation
                    | Point::BeforeIdleEnrollmentCleanup
                    | Point::AfterIdleEnrollmentCleanup
                    | Point::AfterTimerCommit
                    | Point::AfterEngineLoad
                    | Point::BeforeRenewalApplyingSample
                    | Point::BeforeSignInSample
                    | Point::AfterSignInSample
                    | Point::BeforeWorkspaceRequestApplying
                    | Point::AfterWorkspaceExecute
                    | Point::EngineCompletionEntered
            ) {
                return Err(error("not a timer point"));
            }
            object.controls.central_holds.borrow_mut().insert(
                point,
                runtime.runtime().pause_points().hold_central(point.into()),
            );
            Response::from_json(&true)
        }
        "/test/hold-engine" => {
            let pause: EnginePause = request.json().await?;
            let id = object.controls.next_engine_hold.get();
            object.controls.next_engine_hold.set(id + 1);
            let held = runtime
                .runtime()
                .pause_points()
                .hold_engine(pause.point.into(), &pause.key);
            object.controls.engine_holds.borrow_mut().insert(id, held);
            Response::from_json(&id)
        }
        "/test/until-held-engine" => {
            let id: u64 = request.json().await?;
            let mut held = object
                .controls
                .engine_holds
                .borrow_mut()
                .remove(&id)
                .ok_or_else(|| error("engine hold not armed"))?;
            held.until_held().await;
            object.controls.engine_holds.borrow_mut().insert(id, held);
            Response::from_json(&true)
        }
        "/test/release-engine" => {
            let id: u64 = request.json().await?;
            if let Some(held) = object.controls.engine_holds.borrow_mut().remove(&id) {
                held.release();
            }
            Response::from_json(&true)
        }
        "/test/until-central-waiting" => {
            let count: usize = request.json().await?;
            runtime.runtime().until_central_waiting(count).await;
            Response::from_json(&true)
        }
        "/test/until-admission-waiting" => {
            let count: usize = request.json().await?;
            runtime.runtime().until_admission_waiting(count).await;
            Response::from_json(&true)
        }
        "/test/admission-waiting" => {
            Response::from_json(&runtime.runtime().admission_waiting_now())
        }
        "/test/engine-probe-admit" => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct AdmitCall {
                caller: workspace_model::Caller,
                token: String,
            }
            let call: AdmitCall = request.json().await?;
            let admitted =
                fixture_admission::admitted_caller(runtime, &call.token, call.caller).await?;
            let accepted = admitted.is_some();
            *object.controls.admitted_probe_caller.borrow_mut() = admitted;
            Response::from_json(&accepted)
        }
        "/test/engine-probe-request-admitted" => {
            let probe: collaboration_central_core::engine_probe::ProbeRequest =
                request.json().await?;
            let admitted = object.controls.admitted_probe_caller.borrow().clone();
            let Some(caller) = admitted else {
                return Response::error("no admitted probe caller", 409);
            };
            Response::from_json(&runtime.request_probe(&caller, probe).await)
        }
        "/test/until-held-central" => {
            let point: Point = request.json().await?;
            let mut held = object
                .controls
                .central_holds
                .borrow_mut()
                .remove(&point)
                .ok_or_else(|| error("point not armed"))?;
            held.until_held().await;
            object
                .controls
                .central_holds
                .borrow_mut()
                .insert(point, held);
            Response::from_json(&true)
        }
        "/test/release-central" => {
            let point: Point = request.json().await?;
            if let Some(held) = object.controls.central_holds.borrow_mut().remove(&point) {
                held.release();
            }
            Response::from_json(&true)
        }
        _ => Response::error("unknown workspace control", 404),
    }
}

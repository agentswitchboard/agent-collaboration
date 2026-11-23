//! The C3 response table on the existing organization sign-in link.

use axum::{
    Json,
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_signin::{
    ApprovalPersonVerificationInput,
    approval_person_wire::{
        ApprovalDecideReply, ApprovalListReply, ApprovalPersonRoute, ApprovalReadReply,
    },
    person_proof::AdmissionRefusal,
};
use serde::Deserialize;

use crate::{OrganizationAdminResult, OrganizationSignInPort, PersonApprovalAnswer};

/// Used only to address the private organization runtime. The verifier reads the full request.
pub fn person_approval_organization(body: &[u8]) -> Option<OrganizationId> {
    #[derive(Deserialize)]
    struct OrganizationAddress {
        organization: OrganizationId,
    }

    serde_json::from_slice::<OrganizationAddress>(body)
        .ok()
        .map(|address| address.organization)
}

pub(super) async fn forward<Port: OrganizationSignInPort>(
    port: &Port,
    route: ApprovalPersonRoute,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let (Some(authorization), Some(proof)) = (header("authorization"), header("dpop")) else {
        return respond(ApprovalHttpResult::Answered(
            PersonApprovalAnswer::Unauthenticated,
        ));
    };
    let Some(organization) = person_approval_organization(&body) else {
        return respond(ApprovalHttpResult::BadAddress);
    };
    let checked_reply = port
        .person_approval(
            organization,
            ApprovalPersonVerificationInput {
                route,
                method: "POST",
                authorization,
                proof,
                body: &body,
            },
        )
        .await;
    respond(match checked_reply {
        Ok(reply) => ApprovalHttpResult::Answered(reply),
        Err(failure) => ApprovalHttpResult::Unavailable(failure),
    })
}

enum ApprovalHttpResult {
    BadAddress,
    Unavailable(StoreFailure),
    Answered(PersonApprovalAnswer),
}

/// Each classified result keeps its exact response body and has an explicit status.
fn respond(result: ApprovalHttpResult) -> Response {
    match result {
        ApprovalHttpResult::BadAddress => StatusCode::BAD_REQUEST.into_response(),
        ApprovalHttpResult::Unavailable(_failure) => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        ApprovalHttpResult::Answered(answer) => match answer {
            PersonApprovalAnswer::Unauthenticated => StatusCode::UNAUTHORIZED.into_response(),
            PersonApprovalAnswer::Refused(
                reason @ (AdmissionRefusal::NotPermitted
                | AdmissionRefusal::SecondFactorRequired
                | AdmissionRefusal::RefreshRequired
                | AdmissionRefusal::AuthorizationSyncRequired
                | AdmissionRefusal::Expired
                | AdmissionRefusal::RequestTime
                | AdmissionRefusal::RequestReused),
            ) => (
                StatusCode::OK,
                Json(OrganizationAdminResult::Refused {
                    reason: reason.into(),
                }),
            )
                .into_response(),
            PersonApprovalAnswer::List(reply @ ApprovalListReply::Page { .. }) => {
                (StatusCode::OK, Json(reply)).into_response()
            }
            PersonApprovalAnswer::Read(
                reply @ (ApprovalReadReply::Request { .. } | ApprovalReadReply::NotVisible {}),
            ) => (StatusCode::OK, Json(reply)).into_response(),
            PersonApprovalAnswer::Decide(
                reply @ (ApprovalDecideReply::SavedDecision { .. }
                | ApprovalDecideReply::AlreadyDecided { .. }
                | ApprovalDecideReply::TooLate {}
                | ApprovalDecideReply::FactorRequired { .. }
                | ApprovalDecideReply::Invalid { .. }
                | ApprovalDecideReply::NotVisible {}
                | ApprovalDecideReply::OperationConflict {}),
            ) => (StatusCode::OK, Json(reply)).into_response(),
        },
    }
}

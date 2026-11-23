//! The C3 response table on the existing organization sign-in link.

use axum::{
    Json,
    body::Bytes,
    http::{HeaderMap, Method, StatusCode, header::ALLOW},
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

/// Address and raw credentials carried to the private organization runtime.
pub struct PersonApprovalPreflight<'request> {
    organization: OrganizationId,
    authorization: &'request str,
    proof: &'request str,
}

impl PersonApprovalPreflight<'_> {
    pub fn organization(&self) -> &OrganizationId {
        &self.organization
    }
}

pub enum PersonApprovalPreflightRefusal {
    WrongMethod,
    MissingProofHeaders,
    BadAddress,
}

impl IntoResponse for PersonApprovalPreflightRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::WrongMethod => {
                (StatusCode::METHOD_NOT_ALLOWED, [(ALLOW, "POST")]).into_response()
            }
            Self::MissingProofHeaders => StatusCode::UNAUTHORIZED.into_response(),
            Self::BadAddress => StatusCode::BAD_REQUEST.into_response(),
        }
    }
}

/// The private entry checks method and proof headers before addressing the organization.
pub fn person_approval_preflight<'request>(
    method: &Method,
    headers: &'request HeaderMap,
    body: &[u8],
) -> Result<PersonApprovalPreflight<'request>, PersonApprovalPreflightRefusal> {
    if *method != Method::POST {
        return Err(PersonApprovalPreflightRefusal::WrongMethod);
    }
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let (Some(authorization), Some(proof)) = (header("authorization"), header("dpop")) else {
        return Err(PersonApprovalPreflightRefusal::MissingProofHeaders);
    };

    #[derive(Deserialize)]
    struct OrganizationAddress {
        organization: OrganizationId,
    }

    let OrganizationAddress { organization } =
        serde_json::from_slice(body).map_err(|_| PersonApprovalPreflightRefusal::BadAddress)?;
    Ok(PersonApprovalPreflight {
        organization,
        authorization,
        proof,
    })
}

pub(super) async fn forward<Port: OrganizationSignInPort>(
    port: &Port,
    route: ApprovalPersonRoute,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let PersonApprovalPreflight {
        organization,
        authorization,
        proof,
    } = match person_approval_preflight(&method, &headers, &body) {
        Ok(preflight) => preflight,
        Err(refusal) => return refusal.into_response(),
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
    Unavailable(StoreFailure),
    Answered(PersonApprovalAnswer),
}

/// Each classified result keeps its exact response body and has an explicit status.
fn respond(result: ApprovalHttpResult) -> Response {
    match result {
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
                | ApprovalDecideReply::Invalid { .. }
                | ApprovalDecideReply::NotVisible {}
                | ApprovalDecideReply::OperationConflict {}),
            ) => (StatusCode::OK, Json(reply)).into_response(),
        },
    }
}

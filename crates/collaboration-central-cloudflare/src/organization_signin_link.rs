//! Private Worker entry and routing to the organization's modern central object.

use collaboration_central_core::{
    identity_contract::AUTHORIZATION_REVISIONS_ROUTE, person_request::AdminRoute,
};
use fleet_contracts::{ORGANIZATION_STATEMENT_DIGEST_ROUTE, PROVISION_ORGANIZATION_ROUTE};

pub(crate) fn is_signin_path(path: &str) -> bool {
    [
        PROVISION_ORGANIZATION_ROUTE,
        ORGANIZATION_STATEMENT_DIGEST_ROUTE,
        AUTHORIZATION_REVISIONS_ROUTE,
        "/internal/device-login-decisions",
        "/internal/machine-token-decisions",
        AdminRoute::CreateWorkspace.path(),
        AdminRoute::WorkspaceLookup.path(),
        AdminRoute::ListWorkspaces.path(),
        AdminRoute::ReadChangeRecords.path(),
        AdminRoute::RevokeMachineEnrollment.path(),
    ]
    .contains(&path)
}

#[cfg(feature = "link")]
mod private_entry {
    use super::is_signin_path;
    use crate::object_location::ObjectAddress;
    use crate::{cloudflare_transport, http_body};
    use fleet_identity_protocol::OrganizationId;
    use serde::Deserialize;
    use worker::{Context, Env, Request, Response, Result, event};

    const ENTRY_HEADER: &str = "x-collaboration-central-entry";
    const LINK_ENTRY: &str = "collaboration-central-link-entry";

    /// Used only to address the object. Its runtime parses the whole request.
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Addressed {
        organization_id: OrganizationId,
    }

    #[event(fetch)]
    pub async fn main(request: Request, env: Env, _context: Context) -> Result<Response> {
        let response = forward(request, &env).await?;
        // A fetched response may carry an immutable header guard.
        let headers = response.headers().clone();
        headers.set(ENTRY_HEADER, LINK_ENTRY)?;
        Ok(response.with_headers(headers))
    }

    async fn forward(request: Request, env: &Env) -> Result<Response> {
        let request: worker::HttpRequest = request.try_into()?;
        let (mut parts, body) = request.into_parts();
        let body = match http_body::complete(body).await? {
            http_body::BodyCompletion::Complete(body) => body,
            http_body::BodyCompletion::OverLimit => return http_body::over_limit(),
        };
        if !is_signin_path(parts.uri.path()) {
            return Response::empty().map(|response| response.with_status(404));
        }
        // Completion above has already enforced the upload bound. Preserve these
        // exact bytes when forwarding DPoP's signed request intent.
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|_| worker::Error::RustError("sign-in body unavailable".into()))?;
        let Ok(Addressed { organization_id }) = serde_json::from_slice(&bytes) else {
            return Response::empty().map(|response| response.with_status(400));
        };
        let address = ObjectAddress::Central(organization_id);
        let stub = match cloudflare_transport::stub(env, &address) {
            Ok(stub) => stub,
            Err(_) => return Response::empty().map(|response| response.with_status(503)),
        };
        parts.headers.insert(
            "x-central-object-address",
            http::HeaderValue::from_str(&serde_json::to_string(&address)?)
                .map_err(|_| worker::Error::RustError("central address unavailable".into()))?,
        );
        let request = http::Request::from_parts(parts, axum::body::Body::from(bytes));
        match stub.fetch_with_request(request.try_into()?).await {
            Ok(response) => Ok(response),
            Err(_) => Response::empty().map(|response| response.with_status(503)),
        }
    }
}

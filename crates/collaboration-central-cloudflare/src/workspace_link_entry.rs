//! Link paths select an organization. The owning object checks the bearer.
use crate::object_location::ObjectAddress;
use fleet_identity_protocol::OrganizationId;
use worker::{Env, Method, Request, Response, Result};

pub(crate) enum LinkRoute {
    Machine(std::result::Result<OrganizationId, ()>),
    Person(std::result::Result<OrganizationId, ()>),
}

pub(crate) fn link_route(path: &str) -> Option<LinkRoute> {
    let rest = path.strip_prefix("/v1/organizations/")?;
    if let Some(id) = rest.strip_suffix("/person-link") {
        Some(LinkRoute::Person(organization(id)))
    } else {
        rest.strip_suffix("/link")
            .map(|id| LinkRoute::Machine(organization(id)))
    }
}

fn organization(id: &str) -> std::result::Result<OrganizationId, ()> {
    if id.contains('/') {
        return Err(());
    }
    OrganizationId::try_from(id.to_owned()).map_err(|_| ())
}

pub(crate) fn bearer(request: &Request) -> Result<Option<String>> {
    let Some(value) = request.headers().get("authorization")? else {
        return Ok(None);
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return Ok(None);
    };
    Ok((scheme.eq_ignore_ascii_case("bearer")
        && !token.is_empty()
        && !token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == b','))
    .then(|| token.to_owned()))
}

pub(crate) fn is_upgrade(request: &Request) -> Result<bool> {
    Ok(request.method() == Method::Get
        && request
            .headers()
            .get("upgrade")?
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket")))
}

pub(crate) async fn forward(request: Request, env: &Env, route: LinkRoute) -> Result<Response> {
    let (LinkRoute::Machine(Ok(organization)) | LinkRoute::Person(Ok(organization))) = &route
    else {
        return Response::error("invalid organization", 400);
    };
    if !is_upgrade(&request)? {
        return Response::error("websocket upgrade required", 426);
    }
    if bearer(&request)?.is_none() {
        return match route {
            LinkRoute::Machine(_) => Response::error("machine token required", 401),
            LinkRoute::Person(_) => Response::empty().map(|response| response.with_status(401)),
        };
    }
    let address = ObjectAddress::Central(organization.clone());
    let mut forwarded = request.clone_mut()?;
    crate::cloudflare_transport::address_header(forwarded.headers_mut()?, &address)?;
    crate::cloudflare_transport::stub(env, &address)?
        .fetch_with_request(forwarded)
        .await
}

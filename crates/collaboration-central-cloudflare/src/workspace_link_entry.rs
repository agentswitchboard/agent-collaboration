//! The public machine-link address selects an organization, never its authority.
use crate::object_location::ObjectAddress;
use fleet_identity_protocol::OrganizationId;
use worker::{Env, Method, Request, Response, Result};

pub(crate) enum LinkRoute {
    Machine(OrganizationId),
    Person(OrganizationId),
}

pub(crate) fn link_route(path: &str) -> Option<std::result::Result<LinkRoute, ()>> {
    let rest = path.strip_prefix("/v1/organizations/")?;
    let (id, constructor): (_, fn(OrganizationId) -> LinkRoute) =
        if let Some(id) = rest.strip_suffix("/person-link") {
            (id, LinkRoute::Person)
        } else {
            (rest.strip_suffix("/link")?, LinkRoute::Machine)
        };
    Some(if id.contains('/') {
        Err(())
    } else {
        OrganizationId::try_from(id.to_owned())
            .map(constructor)
            .map_err(|_| ())
    })
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

pub(crate) async fn forward(request: Request, env: &Env) -> Result<Response> {
    let Some(Ok(route)) = link_route(&request.path()) else {
        return Response::error("invalid organization", 400);
    };
    if !is_upgrade(&request)? {
        return Response::error("websocket upgrade required", 426);
    }
    if bearer(&request)?.is_none() {
        return Response::error("machine token required", 401);
    }
    let organization = match route {
        LinkRoute::Machine(organization) | LinkRoute::Person(organization) => organization,
    };
    let address = ObjectAddress::Central(organization);
    let mut forwarded = request.clone_mut()?;
    crate::cloudflare_transport::address_header(forwarded.headers_mut()?, &address)?;
    crate::cloudflare_transport::stub(env, &address)?
        .fetch_with_request(forwarded)
        .await
}

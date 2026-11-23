use serde::Deserialize;
use url::Url;

use crate::{DecodeAnswerError, OrganizationId, RouterClientId};

const ORGANIZATION_MARKER: &str = "{organization_id}";
const ENCODED_ORGANIZATION_MARKER: &str = "%7Borganization_id%7D";

/// Sign-in provides the public OAuth client and the central dial template for its deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryAnswer {
    pub client_id: RouterClientId,
    pub link_url: CentralLinkTemplate,
}

/// The discovery decoder validates this template before a link driver can use it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CentralLinkTemplate(String);

impl CentralLinkTemplate {
    fn new(template: String) -> Result<Self, DecodeAnswerError> {
        let invalid = DecodeAnswerError::Malformed;
        let (scheme, address) = template.split_once("://").ok_or(invalid)?;
        let (authority, path_and_query) = address.split_once('/').ok_or(invalid)?;
        let path = path_and_query
            .split_once('?')
            .map_or(path_and_query, |(path, _)| path);
        if !scheme.eq_ignore_ascii_case("https")
            || authority.is_empty()
            || authority.contains(['?', '@'])
            || template.contains(['\\', '#'])
            || template
                .chars()
                .any(|ch| ch.is_whitespace() || ch.is_control() || ch == '\u{feff}')
            || template.matches(ORGANIZATION_MARKER).count() != 1
            || !path
                .split('/')
                .any(|segment| segment == ORGANIZATION_MARKER)
        {
            return Err(invalid);
        }
        let url = Url::parse(&template).map_err(|_| invalid)?;
        if !url.path_segments().is_some_and(|mut segments| {
            segments.any(|segment| segment == ENCODED_ORGANIZATION_MARKER)
        }) {
            return Err(invalid);
        }
        Ok(Self(template))
    }

    pub fn for_organization(&self, organization: OrganizationId) -> Result<Url, DecodeAnswerError> {
        Url::parse(
            &self
                .0
                .replace(ORGANIZATION_MARKER, &organization.to_string()),
        )
        .map_err(|_| DecodeAnswerError::Malformed)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryBody {
    client_id: RouterClientId,
    link_url: String,
}

pub fn decode_discovery(body: &[u8]) -> Result<DiscoveryAnswer, DecodeAnswerError> {
    let decoded: DiscoveryBody = crate::response_object::from_json(body)?;
    Ok(DiscoveryAnswer {
        client_id: decoded.client_id,
        link_url: CentralLinkTemplate::new(decoded.link_url)?,
    })
}

#[cfg(test)]
#[path = "discovery_answer_test.rs"]
mod tests;

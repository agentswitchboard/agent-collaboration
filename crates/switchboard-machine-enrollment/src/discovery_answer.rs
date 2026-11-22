use serde::Deserialize;
use url::Url;

use crate::{DecodeAnswerError, OrganizationId, RouterClientId};

const ORGANIZATION_MARKER: &str = "{organization_id}";
const TEST_ORGANIZATION: &str = "00000000-0000-7000-8000-000000000000";

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
        let (prefix, suffix) = template.split_once(ORGANIZATION_MARKER).ok_or(invalid)?;
        if !prefix.ends_with('/')
            || (!suffix.is_empty() && !suffix.starts_with('/'))
            || suffix.contains(ORGANIZATION_MARKER)
            || template.contains(TEST_ORGANIZATION)
        {
            return Err(invalid);
        }
        let probe =
            Url::parse(&format!("{prefix}{TEST_ORGANIZATION}{suffix}")).map_err(|_| invalid)?;
        if probe.scheme() != "https"
            || !probe.has_host()
            || !probe.username().is_empty()
            || probe.password().is_some()
            || probe.query().is_some()
            || probe.fragment().is_some()
            || !probe
                .path_segments()
                .is_some_and(|mut pieces| pieces.any(|piece| piece == TEST_ORGANIZATION))
        {
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

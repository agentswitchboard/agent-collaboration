use central_link_protocol::LinkCallRefusal;
use reqwest::StatusCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FleetTargetError {
    #[error("could not read the Fleet target file")]
    Read,
    #[error("invalid Fleet target JSON or organization ID")]
    InvalidDocument,
    #[error(
        "the Fleet target issuer must be an HTTPS URL without credentials, a query, or a fragment"
    )]
    InvalidIssuer,
    #[error("the Fleet target needs a Central admin audience and Owner Fleet CLI client ID")]
    MissingFleetSettings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FleetCredentialError {
    #[error("Fleet credentials require the macOS login Keychain")]
    UnsupportedPlatform,
    #[error(
        "Fleet credential missing from the login Keychain. Run the Node owner CLI's fleet-login first"
    )]
    Missing,
    #[error("could not read the Fleet credential from the login Keychain")]
    Read,
    #[error("the stored Fleet credential is invalid")]
    InvalidStoredCredential,
    #[error(
        "could not save the rotated Fleet credential to the login Keychain. Run fleet-login again"
    )]
    Save,
    #[error("could not sign the Fleet DPoP proof")]
    Proof,
    #[error("the Sign-in token endpoint could not be reached")]
    TokenEndpoint,
    #[error("Sign-in refused the Fleet refresh with HTTP {0}")]
    RefreshRefused(StatusCode),
    #[error("Sign-in returned an invalid Fleet token response")]
    InvalidTokenResponse,
    #[error("the refreshed Fleet token does not match this audience, client and proof key")]
    TokenBinding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PersonLinkError {
    #[error(transparent)]
    Credential(#[from] FleetCredentialError),
    #[error("could not configure the Sign-in HTTP client")]
    HttpClient,
    #[error("could not open the Sign-in person WebSocket")]
    Upgrade,
    #[error("Sign-in or Central refused the person WebSocket with HTTP {0}")]
    UpgradeRefused(StatusCode),
    #[error("the person WebSocket closed or failed during the workspace read")]
    Socket,
    #[error("the person WebSocket closed with code {0}")]
    Closed(u16),
    #[error("the person WebSocket returned an unexpected protocol frame")]
    Protocol,
    #[error("Central refused the workspace read ({0:?})")]
    WorkspaceRefused(LinkCallRefusal),
    #[error("Central returned an invalid workspace page cursor")]
    WorkspaceCursor,
    #[error("the workspace read reached its deadline")]
    Deadline,
    #[error("could not allocate a workspace request ID")]
    RequestId,
    #[error("the person connection could not advance")]
    ConnectionState,
}

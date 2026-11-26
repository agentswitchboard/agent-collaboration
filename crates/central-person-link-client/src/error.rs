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
    #[error("the Fleet target needs an HTTP loopback ownerCliRedirectUri for central login")]
    InvalidRedirect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FleetCredentialError {
    #[error("Fleet credentials require the macOS login Keychain")]
    UnsupportedPlatform,
    #[error(
        "Central login missing from the Keychain. Run codex-router central login --target <file>"
    )]
    Missing,
    #[error("could not read the Fleet credential from the login Keychain")]
    Read,
    #[error("the stored Fleet credential is invalid")]
    InvalidStoredCredential,
    #[error("could not save the Central login to the Keychain")]
    Save,
    #[error(
        "the Central login changed underneath this command. Run it again to read the current item"
    )]
    Changed,
    #[error(
        "the saved Central credential may have been spent ({0}). Run codex-router central login --target <file> again"
    )]
    SignInAgain(RefreshUncertainty),
    #[error("could not sign the Fleet DPoP proof")]
    Proof,
    #[error(transparent)]
    Exchange(#[from] TokenExchangeError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RefreshUncertainty {
    #[error("a prior refresh has no saved successor")]
    NoSavedSuccessor,
    #[error("the refresh reached its deadline")]
    Deadline,
    #[error(transparent)]
    Exchange(TokenExchangeError),
    #[error("the successor could not be saved")]
    Save,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TokenExchangeError {
    #[error("the Sign-in token endpoint could not be reached")]
    Transport,
    #[error("Sign-in refused the Fleet token exchange with HTTP {0}")]
    Refused(StatusCode),
    #[error("Sign-in returned an invalid Fleet token response")]
    InvalidResponse,
    #[error("the Fleet token does not match this audience, client and proof key")]
    Binding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FleetLoginError {
    #[error(transparent)]
    Target(#[from] FleetTargetError),
    #[error(transparent)]
    Credential(#[from] FleetCredentialError),
    #[error("could not start the owner sign-in loopback listener")]
    Listener,
    #[error("could not show the owner sign-in address")]
    ShowAddress,
    #[error("owner sign-in refused")]
    Refused,
    #[error("owner sign-in reached its deadline")]
    Deadline,
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
    #[error("the person WebSocket closed or failed during the Central call")]
    Socket,
    #[error("the person WebSocket closed with code {0}")]
    Closed(u16),
    #[error("the person WebSocket returned an unexpected protocol frame")]
    Protocol,
    #[error("could not encode the person call")]
    CallEncoding,
    #[error("Central refused the person call ({0:?})")]
    CallRefused(LinkCallRefusal),
    #[error("refused on replay: {0:?}")]
    ReplayRefused(LinkCallRefusal),
    #[error("Central returned an invalid workspace page cursor")]
    WorkspaceCursor,
    #[error("Central returned a person approval cursor that did not advance")]
    ApprovalCursor,
    #[error("the person call reached its deadline")]
    Deadline,
    #[error("could not allocate a workspace request ID")]
    RequestId,
    #[error("the person connection could not advance")]
    ConnectionState,
}

use approval_request_protocol::{
    ApprovalOffer, HolderEnding, OfferedOptionId, PrivateEnding, ProviderCancellation,
    QuestionAnswer, QuestionOffer,
};

/// The checked provider offer the harness supplies, narrowing central's OfferView to ACP asks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderAskOffer {
    ProviderApproval { offer: ApprovalOffer },
    ProviderQuestion { offer: QuestionOffer },
}

/// The provider-only PrivateEnding that custody prepares before starting a harness reply.
///
/// Central's ProviderEndingView is a recipient projection and ProviderAnswer only carries
/// decisions. This type preserves the private holder's decision, cancellation and timeout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderAskEnding {
    ProviderApproval {
        ending: HolderEnding<OfferedOptionId, ProviderCancellation>,
    },
    ProviderQuestion {
        ending: HolderEnding<QuestionAnswer, ProviderCancellation>,
    },
}

/// The custody conversion rejects private endings outside the provider ask domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidProviderAskEnding {
    #[error("the ending belongs to a held session call rather than a provider ask")]
    HeldSessionCall,
}

impl TryFrom<PrivateEnding> for ProviderAskEnding {
    type Error = InvalidProviderAskEnding;

    fn try_from(ending: PrivateEnding) -> Result<Self, Self::Error> {
        match ending {
            PrivateEnding::ProviderApproval { ending } => Ok(Self::ProviderApproval { ending }),
            PrivateEnding::ProviderQuestion { ending } => Ok(Self::ProviderQuestion { ending }),
            PrivateEnding::UnprovenCall { .. } => Err(InvalidProviderAskEnding::HeldSessionCall),
        }
    }
}

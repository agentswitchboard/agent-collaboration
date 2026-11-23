//! Only the verified C3 request can carry person authority into an applying turn.

use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::{Ed25519Verification, Es256Verification, IssuerKeySource};
use workspace_ids::PersonId;

use crate::{
    FleetPermission,
    approval_person_wire::{ApprovalDecideRequest, ApprovalPersonOperation, ApprovalPersonRoute},
    person_proof::{
        AdmissionRefusal, FactorStanding, OrganizationFloor, PersonFloor, PersonProofFacts,
        PersonProofVerifier, PersonRequestRefusal, decide_admission,
        decide_retained_approval_admission,
    },
};

/// Person authority from the signed C3 request and its organization applying checks.
///
/// ```compile_fail,E0451
/// use fleet_signin::{ApprovalPersonAdmission, FactorStanding};
/// let _ = ApprovalPersonAdmission {
///     person: "019a3dce-2000-7000-8000-000000000001".parse().unwrap(),
///     organization: "019a3dce-2000-7000-8000-000000000002".to_owned().try_into().unwrap(),
///     factor: FactorStanding::NotFresh,
/// };
/// ```
///
/// ```compile_fail,E0277
/// let _: fleet_signin::ApprovalPersonAdmission = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Debug)]
pub struct ApprovalPersonAdmission {
    person: PersonId,
    organization: OrganizationId,
    factor: FactorStanding,
}

impl ApprovalPersonAdmission {
    #[must_use]
    pub const fn person(&self) -> &PersonId {
        &self.person
    }

    #[must_use]
    pub const fn organization(&self) -> &OrganizationId {
        &self.organization
    }

    #[must_use]
    pub const fn factor(&self) -> FactorStanding {
        self.factor
    }
}

/// The checked proof, paired with precisely the signed approval operation.
#[derive(Debug)]
pub struct VerifiedApprovalPersonRequest {
    proof: PersonProofFacts,
    operation: ApprovalPersonOperation,
    person: PersonId,
}

/// Values read in the applying organization turn. They carry no authority by themselves.
#[derive(Clone, Copy)]
pub struct ApprovalApplyingContext<'turn> {
    pub organization: &'turn OrganizationId,
    pub now_ms: i64,
    pub organization_floor: Option<&'turn OrganizationFloor>,
    pub person_floor: Option<&'turn PersonFloor>,
    pub proof_used: bool,
}

impl VerifiedApprovalPersonRequest {
    #[must_use]
    pub const fn proof(&self) -> &PersonProofFacts {
        &self.proof
    }

    #[must_use]
    pub const fn operation(&self) -> &ApprovalPersonOperation {
        &self.operation
    }

    /// Authenticate a retry without granting permission to apply a new decision.
    pub fn authorize_retained_decision(
        &self,
        applying: ApprovalApplyingContext<'_>,
    ) -> Result<Option<VerifiedApprovalDecideRetry<'_>>, StoreFailure> {
        let ApprovalPersonOperation::Decide(request) = &self.operation else {
            return Ok(None);
        };
        if applying.organization != &self.proof.organization_id
            || decide_retained_approval_admission(
                &(&self.proof).into(),
                applying.now_ms,
                applying.organization_floor,
                applying.person_floor,
                applying.proof_used,
            )?
            .is_err()
        {
            return Ok(None);
        }
        Ok(Some(VerifiedApprovalDecideRetry {
            person: &self.person,
            request,
            intent_digest: &self.proof.intent_digest,
        }))
    }

    /// The caller still has to reserve the proof and commit it with any effect.
    pub fn admit(
        self,
        applying: ApprovalApplyingContext<'_>,
    ) -> Result<Result<AdmittedApprovalPersonOperation, AdmissionRefusal>, StoreFailure> {
        if applying.organization != &self.proof.organization_id {
            return Ok(Err(AdmissionRefusal::NotPermitted));
        }
        if let Err(refusal) = decide_admission(
            &(&self.proof).into(),
            applying.now_ms,
            applying.organization_floor,
            applying.person_floor,
            FleetPermission::DecideApproval.as_str(),
            false,
            applying.proof_used,
        )? {
            return Ok(Err(refusal));
        }
        Ok(Ok(AdmittedApprovalPersonOperation {
            approval: ApprovalPersonAdmission {
                person: self.person,
                organization: self.proof.organization_id.clone(),
                factor: FactorStanding::at(self.proof.second_factor_at, applying.now_ms),
            },
            operation: self.operation,
            proof: self.proof,
        }))
    }
}

/// This checked identity may retrieve its own exact retained decision.
pub struct VerifiedApprovalDecideRetry<'request> {
    person: &'request PersonId,
    request: &'request ApprovalDecideRequest,
    intent_digest: &'request str,
}

impl<'request> VerifiedApprovalDecideRetry<'request> {
    #[must_use]
    pub const fn person(&self) -> &'request PersonId {
        self.person
    }

    #[must_use]
    pub const fn request(&self) -> &'request ApprovalDecideRequest {
        self.request
    }

    #[must_use]
    pub const fn intent_digest(&self) -> &'request str {
        self.intent_digest
    }
}

/// The matched operation and proof accompany the admission until the applying batch is prepared.
#[derive(Debug)]
pub struct AdmittedApprovalPersonOperation {
    approval: ApprovalPersonAdmission,
    operation: ApprovalPersonOperation,
    proof: PersonProofFacts,
}

impl AdmittedApprovalPersonOperation {
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApprovalPersonAdmission,
        ApprovalPersonOperation,
        PersonProofFacts,
    ) {
        (self.approval, self.operation, self.proof)
    }
}

/// The raw request never chooses its own route.
pub struct ApprovalPersonVerificationInput<'wire> {
    pub route: ApprovalPersonRoute,
    pub method: &'wire str,
    pub authorization: &'wire str,
    pub proof: &'wire str,
    pub body: &'wire [u8],
}

impl ApprovalPersonVerificationInput<'_> {
    pub async fn verify_with<
        Ed: Ed25519Verification,
        Es: Es256Verification,
        Keys: IssuerKeySource,
    >(
        self,
        verifier: &PersonProofVerifier<Ed, Es, Keys>,
    ) -> Result<VerifiedApprovalPersonRequest, PersonRequestRefusal> {
        let checked = verifier
            .verify(
                self.route.path(),
                self.method,
                self.authorization,
                self.proof,
            )
            .await?;
        let operation = self.route.read(self.body)?;
        let proof = checked.bind_intent(operation.organization(), &operation.intent().digest()?)?;
        let person = proof
            .person_id
            .parse()
            .map_err(|_| PersonRequestRefusal::PersonClaims)?;
        Ok(VerifiedApprovalPersonRequest {
            proof,
            operation,
            person,
        })
    }
}

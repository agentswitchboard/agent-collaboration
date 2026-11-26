//! Person approval authority is minted and consumed in one owning transaction.

use crate::FleetPermission;
use central_storage::{ReadOutcome, Statement, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::OrganizationTransaction;
use fleet_runtime_ports::SyncTime;
use std::marker::PhantomData;
use workspace_ids::PersonId;

use crate::{
    approval_person_commands::{
        ApprovalDecideRequest, ApprovalListRequest, ApprovalPersonOperation, ApprovalReadRequest,
    },
    authorization_floor_store::{
        decode_organization_floor, decode_person_floor, organization_floor_statement,
        person_floor_statement,
    },
    person_proof::{AdmissionRefusal, PersonSocketAttachment},
};

/// The stamped organization transaction can run the Sign-in owner's checked reads.
/// It supplies an executor, never decoded authorization facts.
pub trait ApprovalPersonTransaction: OrganizationTransaction + Send {
    fn sample_person_authorization_time(&self) -> Result<SyncTime, StoreFailure>;

    fn read_person_authorization(
        &mut self,
        statement: Statement,
    ) -> impl std::future::Future<Output = Result<ReadOutcome, StoreFailure>> + Send;
}

/// Read-only actor facts, minted by Sign-in. Store commands require the whole bound operation.
///
/// ```compile_fail
/// use fleet_signin::ApprovalPersonAdmission;
/// fn build() -> ApprovalPersonAdmission {
///     ApprovalPersonAdmission {
///         person: "019a3dce-2000-7000-8000-000000000001".parse().unwrap(),
///         organization: "019a3dce-2000-7000-8000-000000000002".to_owned().try_into().unwrap(),
///     }
/// }
/// ```
///
/// ```compile_fail
/// let _: fleet_signin::ApprovalPersonAdmission = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Debug)]
pub struct ApprovalPersonAdmission {
    person: PersonId,
    organization: OrganizationId,
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
}

/// Recheck the socket's applying authority inside the transaction. The authenticated
/// connection owns the person and target.
pub async fn admit_person_link_approval<'loan, Transaction: ApprovalPersonTransaction>(
    attachment: &PersonSocketAttachment,
    operation: ApprovalPersonOperation,
    transaction: &'loan mut Transaction,
) -> Result<
    Result<AdmittedApprovalPersonOperation<'loan, Transaction>, AdmissionRefusal>,
    StoreFailure,
> {
    let organization = attachment.organization();
    let person = attachment.person();
    if transaction.organization_id() != organization || operation.organization() != organization {
        return Ok(Err(AdmissionRefusal::NotPermitted));
    }
    let sampled = transaction.sample_person_authorization_time()?;
    if !attachment.is_current(sampled.unix_millis()) {
        return Ok(Err(AdmissionRefusal::Expired));
    }
    let organization_rows = transaction
        .read_person_authorization(organization_floor_statement(organization.as_str()))
        .await?;
    let organization_floor = decode_organization_floor(&organization_rows, organization.as_str())?;
    let person_rows = transaction
        .read_person_authorization(person_floor_statement(
            organization.as_str(),
            person.as_str(),
        ))
        .await?;
    let person_floor = decode_person_floor(&person_rows, organization.as_str(), person.as_str())?;
    if !organization_floor
        .zip(person_floor)
        .is_some_and(|(organization, person)| attachment.still_matches(&organization, &person))
    {
        return Ok(Err(AdmissionRefusal::RefreshRequired));
    }

    let access = if attachment.permits(FleetPermission::DecideApproval) {
        ApprovalPersonAccess::Granted
    } else if let ApprovalPersonOperation::Decide(request) = &operation {
        let intent_digest = request
            .intent_digest()
            .map_err(|_| StoreFailure::Unavailable)?;
        ApprovalPersonAccess::RetainedOnly { intent_digest }
    } else {
        return Ok(Err(AdmissionRefusal::NotPermitted));
    };
    let identity = ApprovalPersonAdmission {
        person: person.clone(),
        organization: organization.clone(),
    };
    Ok(bind_approval_command(
        transaction,
        identity,
        operation,
        access,
        sampled,
    ))
}

enum ApprovalPersonAccess {
    Granted,
    RetainedOnly { intent_digest: String },
}

fn bind_approval_command<'loan, Transaction>(
    transaction: &'loan mut Transaction,
    identity: ApprovalPersonAdmission,
    operation: ApprovalPersonOperation,
    access: ApprovalPersonAccess,
    sampled: SyncTime,
) -> Result<AdmittedApprovalPersonOperation<'loan, Transaction>, AdmissionRefusal> {
    let command = match (access, operation) {
        (ApprovalPersonAccess::Granted, ApprovalPersonOperation::List(request)) => {
            AdmittedApprovalPersonOperation::List(AdmittedApprovalCommand::new(
                transaction,
                identity,
                sampled,
                request,
            ))
        }
        (ApprovalPersonAccess::Granted, ApprovalPersonOperation::Read(request)) => {
            AdmittedApprovalPersonOperation::Read(AdmittedApprovalCommand::new(
                transaction,
                identity,
                sampled,
                request,
            ))
        }
        (ApprovalPersonAccess::Granted, ApprovalPersonOperation::Decide(request)) => {
            AdmittedApprovalPersonOperation::Decide(AdmittedApprovalCommand::new(
                transaction,
                identity,
                sampled,
                request,
            ))
        }
        (
            ApprovalPersonAccess::RetainedOnly { intent_digest },
            ApprovalPersonOperation::Decide(request),
        ) => AdmittedApprovalPersonOperation::Retry(AdmittedApprovalCommand::new(
            transaction,
            identity,
            sampled,
            RetainedDecisionRequest {
                request,
                intent_digest,
            },
        )),
        (
            ApprovalPersonAccess::RetainedOnly { .. },
            ApprovalPersonOperation::List(_) | ApprovalPersonOperation::Read(_),
        ) => {
            return Err(AdmissionRefusal::NotPermitted);
        }
    };
    Ok(command)
}

pub enum AdmittedApprovalPersonOperation<'loan, Transaction> {
    List(AdmittedApprovalCommand<'loan, Transaction, ApprovalListRequest>),
    Read(AdmittedApprovalCommand<'loan, Transaction, ApprovalReadRequest>),
    Decide(AdmittedApprovalCommand<'loan, Transaction, ApprovalDecideRequest>),
    Retry(AdmittedApprovalCommand<'loan, Transaction, RetainedDecisionRequest>),
}

/// An exact retry can read the retained result, without authority to apply a decision.
pub struct RetainedDecisionRequest {
    request: ApprovalDecideRequest,
    intent_digest: String,
}

/// The operation, person authority and applying loan are consumed together.
///
/// ```compile_fail,E0382
/// use fleet_signin::AdmittedApprovalCommand;
/// fn replay<T, R>(command: AdmittedApprovalCommand<'_, T, R>) {
///     let _ = command.into_application();
///     let _ = command.into_application();
/// }
/// ```
///
/// ```compile_fail
/// use fleet_signin::AdmittedApprovalCommand;
/// fn escape<'loan, T: 'static, R>(command: AdmittedApprovalCommand<'loan, T, R>)
///     -> AdmittedApprovalCommand<'static, T, R> { command }
/// ```
///
/// ```compile_fail
/// use fleet_signin::{AdmittedApprovalCommand, approval_person_commands::{ApprovalReadRequest, ApprovalDecideRequest}};
/// fn substitute<T>(command: AdmittedApprovalCommand<'_, T, ApprovalReadRequest>)
///     -> AdmittedApprovalCommand<'_, T, ApprovalDecideRequest> { command }
/// ```
pub struct AdmittedApprovalCommand<'loan, Transaction, Request> {
    transaction: &'loan mut Transaction,
    admitted: AdmittedApprovalFacts<'loan, Request>,
}

impl<'loan, Transaction, Request> AdmittedApprovalCommand<'loan, Transaction, Request> {
    fn new(
        transaction: &'loan mut Transaction,
        identity: ApprovalPersonAdmission,
        sampled: SyncTime,
        request: Request,
    ) -> Self {
        Self {
            transaction,
            admitted: AdmittedApprovalFacts {
                identity,
                sampled,
                request,
                loan: PhantomData,
            },
        }
    }

    /// The store can unpack the verified facts once it owns the command. The
    /// facts alone cannot start another store command or swap its loan.
    #[must_use]
    pub fn into_application(
        self,
    ) -> (
        &'loan mut Transaction,
        AdmittedApprovalFacts<'loan, Request>,
    ) {
        (self.transaction, self.admitted)
    }
}

pub struct AdmittedApprovalFacts<'loan, Request> {
    identity: ApprovalPersonAdmission,
    sampled: SyncTime,
    request: Request,
    loan: PhantomData<&'loan mut ()>,
}

impl<Request> AdmittedApprovalFacts<'_, Request> {
    #[must_use]
    pub const fn identity(&self) -> &ApprovalPersonAdmission {
        &self.identity
    }

    #[must_use]
    pub const fn request(&self) -> &Request {
        &self.request
    }

    #[must_use]
    pub const fn sampled(&self) -> SyncTime {
        self.sampled
    }
}

impl AdmittedApprovalFacts<'_, RetainedDecisionRequest> {
    #[must_use]
    pub const fn retained_decision(&self) -> VerifiedApprovalDecideRetry<'_> {
        VerifiedApprovalDecideRetry {
            person: self.identity.person(),
            request: &self.request.request,
            intent_digest: self.request.intent_digest.as_str(),
        }
    }
}

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

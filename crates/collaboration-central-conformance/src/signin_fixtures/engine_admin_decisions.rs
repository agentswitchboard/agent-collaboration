//! The engine admin decision rows shared by the Tokio and Workerd HTTP suites.

use super::{NOW_SECONDS, ORGANIZATION, Person, fresh_request_id};
use crate::ConformanceResult;
use central_storage::SqlValue;
use serde_json::{Value, json};
use std::future::Future;

mod attempt_fixtures;
mod decision_registry;

use attempt_fixtures::{check_domain_writes, read_admin_facts, seed_attempt};

#[derive(Clone, Copy)]
pub enum EngineAdminRoute {
    Health,
    Retired,
    Retry,
    Discard,
}

impl EngineAdminRoute {
    fn path(self) -> &'static str {
        match self {
            Self::Health => "/organization-admin/v1/engine-health",
            Self::Retired => "/organization-admin/v1/engine-retired",
            Self::Retry => "/organization-admin/v1/engine-retry",
            Self::Discard => "/organization-admin/v1/engine-discard",
        }
    }

    fn operation(self) -> &'static str {
        match self {
            Self::Health => "EngineHealth",
            Self::Retired => "EngineListRetired",
            Self::Retry => "EngineRetry",
            Self::Discard => "EngineDiscard",
        }
    }

    fn permission(self) -> &'static str {
        match self {
            Self::Health | Self::Retired => "organization:readAudit",
            Self::Retry | Self::Discard => "organization:repairWork",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum EngineAdminCase {
    HealthSnapshot,
    RetiredPage,
    RetiredEmptyContinuation,
    RetiredEnd,
    Retried,
    Discarded,
    NotPermitted,
    SecondFactorRequired,
    RefreshRequired,
    AuthorizationSyncRequired,
    Expired,
    RequestTime,
    RequestReused,
    GuardChanged,
    OperationConflict,
    RetainedReplay,
    NotFound,
    NotRetired,
    DiscardUnsupported,
    Stale,
    SubjectUnreadable,
    WrongMethod,
    MissingProof,
    BadAddress,
    InvalidIntent,
    Unavailable,
}

pub enum ApplyingControl {
    Immediate,
    AdvanceAfterVerification,
    MoveFloorAfterSampling,
}

pub struct EngineAdminRequest {
    pub method: reqwest::Method,
    pub path: &'static str,
    pub body: Value,
    pub headers: Vec<(&'static str, String)>,
}

pub trait EngineAdminHarness {
    fn send_admin(
        &self,
        request: &EngineAdminRequest,
        control: ApplyingControl,
    ) -> impl Future<Output = ConformanceResult<(u16, Vec<u8>)>>;
    fn read_admin_rows(
        &self,
        statement: &'static str,
        args: Vec<SqlValue>,
    ) -> impl Future<Output = ConformanceResult<Vec<Vec<SqlValue>>>>;
    fn apply_admin_fixture(
        &self,
        statement: &'static str,
        args: Vec<SqlValue>,
    ) -> impl Future<Output = ConformanceResult<()>>;
    fn create_wake_for_discard(&self) -> impl Future<Output = ConformanceResult<String>>;
    fn published_alarm_millis(&self) -> impl Future<Output = Option<i64>>;
}

const ENGINE_ATTEMPT: &str = "019f0000-0000-7000-8000-000000000101";
const OTHER_ATTEMPT: &str = "019f0000-0000-7000-8000-000000000102";
const ENGINE_OPERATION: &str = "019f0000-0000-7000-8000-000000000103";

impl EngineAdminRequest {
    fn signed(
        route: EngineAdminRoute,
        case: EngineAdminCase,
        person: &Person,
        proof_id: &str,
        attempt_id: &str,
    ) -> ConformanceResult<Self> {
        use EngineAdminCase as Case;
        let path = route.path();
        let (params, operation_id) = match route {
            EngineAdminRoute::Health => (json!({}), None),
            EngineAdminRoute::Retired => (json!({"after":null,"limit":1}), None),
            EngineAdminRoute::Retry | EngineAdminRoute::Discard => {
                (json!({"attemptId":attempt_id}), Some(ENGINE_OPERATION))
            }
        };
        let signed_target = if matches!(case, Case::InvalidIntent) {
            OTHER_ATTEMPT
        } else {
            ORGANIZATION
        };
        let intent = json!({
            "v":1,"operation":route.operation(),"organization":ORGANIZATION,
            "target":signed_target,"operationId":operation_id,"params":params
        });
        let mut body = params;
        let body_fields = body
            .as_object_mut()
            .ok_or("engine fixture parameters must be an object")?;
        body_fields.insert("organizationId".to_owned(), json!(ORGANIZATION));
        if let Some(operation_id) = operation_id {
            body_fields.insert("operationId".to_owned(), json!(operation_id));
        }
        let mut headers = person.headers_with(path, &intent, proof_id, NOW_SECONDS, &|claims| {
            if matches!(case, Case::Expired)
                && let Some(expiry) = claims.get_mut("exp")
            {
                *expiry = json!(NOW_SECONDS + 1);
            }
        })?;
        let method = if matches!(case, Case::WrongMethod) {
            reqwest::Method::PUT
        } else {
            reqwest::Method::POST
        };
        match case {
            Case::MissingProof => headers.retain(|(name, _)| *name != "dpop"),
            Case::BadAddress => body = json!({}),
            Case::InvalidIntent
            | Case::HealthSnapshot
            | Case::RetiredPage
            | Case::RetiredEmptyContinuation
            | Case::RetiredEnd
            | Case::Retried
            | Case::Discarded
            | Case::NotPermitted
            | Case::SecondFactorRequired
            | Case::RefreshRequired
            | Case::AuthorizationSyncRequired
            | Case::Expired
            | Case::RequestTime
            | Case::RequestReused
            | Case::GuardChanged
            | Case::OperationConflict
            | Case::RetainedReplay
            | Case::NotFound
            | Case::NotRetired
            | Case::DiscardUnsupported
            | Case::Stale
            | Case::SubjectUnreadable
            | Case::WrongMethod
            | Case::Unavailable => {}
        }
        Ok(Self {
            method,
            path,
            body,
            headers,
        })
    }
}

pub async fn check_engine_admin_case(
    host: &impl EngineAdminHarness,
    route: EngineAdminRoute,
    case: EngineAdminCase,
) -> ConformanceResult<()> {
    use EngineAdminCase as Case;
    let mut person = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    person.permissions = vec![route.permission().to_owned()];
    if matches!(route, EngineAdminRoute::Health | EngineAdminRoute::Retired) {
        person.second_factor_at = None;
    }
    match case {
        Case::NotPermitted => person.permissions = vec!["organization:view".to_owned()],
        Case::SecondFactorRequired => person.second_factor_at = Some(NOW_SECONDS - 901),
        Case::RefreshRequired => person.organization_revision = 2,
        Case::AuthorizationSyncRequired => person.organization_revision = 4,
        Case::RequestTime => {
            host.apply_admin_fixture(
                "UPDATE signin_authorization_floors SET request_reject_through_ms=?1",
                vec![SqlValue::Integer((NOW_SECONDS * 1000).try_into()?)],
            )
            .await?;
        }
        Case::Unavailable => {
            host.apply_admin_fixture(
                "UPDATE signin_authorization_floors SET minimum_revision=0",
                vec![],
            )
            .await?;
        }
        Case::HealthSnapshot
        | Case::RetiredPage
        | Case::RetiredEmptyContinuation
        | Case::RetiredEnd
        | Case::Retried
        | Case::Discarded
        | Case::Expired
        | Case::RequestReused
        | Case::GuardChanged
        | Case::OperationConflict
        | Case::RetainedReplay
        | Case::NotFound
        | Case::NotRetired
        | Case::DiscardUnsupported
        | Case::Stale
        | Case::SubjectUnreadable
        | Case::WrongMethod
        | Case::MissingProof
        | Case::BadAddress
        | Case::InvalidIntent => {}
    }

    let subject = seed_attempt(host, case).await?;
    let proof_id = fresh_request_id(NOW_SECONDS)?;
    let request = EngineAdminRequest::signed(route, case, &person, &proof_id, ENGINE_ATTEMPT)?;
    let initial = match case {
        Case::RequestReused => Some(
            host.send_admin(&request, ApplyingControl::Immediate)
                .await?,
        ),
        Case::RetainedReplay | Case::OperationConflict => {
            let initial_request = EngineAdminRequest::signed(
                route,
                case,
                &person,
                &fresh_request_id(NOW_SECONDS)?,
                if matches!(case, Case::OperationConflict) {
                    OTHER_ATTEMPT
                } else {
                    ENGINE_ATTEMPT
                },
            )?;
            Some(
                host.send_admin(&initial_request, ApplyingControl::Immediate)
                    .await?,
            )
        }
        Case::HealthSnapshot
        | Case::RetiredPage
        | Case::RetiredEmptyContinuation
        | Case::RetiredEnd
        | Case::Retried
        | Case::Discarded
        | Case::NotPermitted
        | Case::SecondFactorRequired
        | Case::RefreshRequired
        | Case::AuthorizationSyncRequired
        | Case::Expired
        | Case::RequestTime
        | Case::GuardChanged
        | Case::NotFound
        | Case::NotRetired
        | Case::DiscardUnsupported
        | Case::Stale
        | Case::SubjectUnreadable
        | Case::WrongMethod
        | Case::MissingProof
        | Case::BadAddress
        | Case::InvalidIntent
        | Case::Unavailable => None,
    };
    if let Some(initial) = &initial {
        assert_eq!(initial.0, 200);
        if matches!(case, Case::OperationConflict | Case::RetainedReplay) {
            assert_eq!(initial.1, br#"{"result":"refused","reason":"notFound"}"#);
        }
    }

    let before = read_admin_facts(host).await?;
    let control = match case {
        Case::Expired => ApplyingControl::AdvanceAfterVerification,
        Case::GuardChanged => ApplyingControl::MoveFloorAfterSampling,
        Case::HealthSnapshot
        | Case::RetiredPage
        | Case::RetiredEmptyContinuation
        | Case::RetiredEnd
        | Case::Retried
        | Case::Discarded
        | Case::NotPermitted
        | Case::SecondFactorRequired
        | Case::RefreshRequired
        | Case::AuthorizationSyncRequired
        | Case::RequestTime
        | Case::RequestReused
        | Case::OperationConflict
        | Case::RetainedReplay
        | Case::NotFound
        | Case::NotRetired
        | Case::DiscardUnsupported
        | Case::Stale
        | Case::SubjectUnreadable
        | Case::WrongMethod
        | Case::MissingProof
        | Case::BadAddress
        | Case::InvalidIntent
        | Case::Unavailable => ApplyingControl::Immediate,
    };
    let (status, bytes) = host.send_admin(&request, control).await?;
    let expected = expected_reply(route, case);
    assert_eq!(status, expected.status, "{case:?}, {}", request.path);
    if let Some(body) = &expected.body {
        assert_eq!(serde_json::from_slice::<Value>(&bytes)?, *body, "{case:?}");
    } else {
        assert!(bytes.is_empty(), "{case:?} must have an empty response");
    }

    let after = read_admin_facts(host).await?;
    check_domain_writes(case, &subject, &before, &after);
    assert_effects(
        case,
        &proof_id,
        &expected,
        std::str::from_utf8(&bytes)?,
        &before,
        &after,
    );
    if matches!(case, Case::RetainedReplay) {
        assert_eq!(
            initial.ok_or("setup produced no retained response")?.1,
            bytes
        );
    }
    Ok(())
}

struct ExpectedReply {
    status: u16,
    body: Option<Value>,
    writes: AdminWrites,
}

enum AdminWrites {
    NoWrites,
    ProofOnly,
    RetainResult,
}

impl ExpectedReply {
    fn answered(body: Value, writes: AdminWrites) -> Self {
        Self {
            status: 200,
            body: Some(body),
            writes,
        }
    }

    fn refused(reason: &str, writes: AdminWrites) -> Self {
        Self::answered(json!({"result":"refused","reason":reason}), writes)
    }

    fn empty(status: u16) -> Self {
        Self {
            status,
            body: None,
            writes: AdminWrites::NoWrites,
        }
    }
}

fn expected_reply(route: EngineAdminRoute, case: EngineAdminCase) -> ExpectedReply {
    use AdminWrites::{NoWrites, ProofOnly, RetainResult};
    use EngineAdminCase as Case;
    use ExpectedReply as Reply;
    match case {
        Case::HealthSnapshot => Reply::answered(attempt_fixtures::expected_health(), ProofOnly),
        Case::RetiredPage => Reply::answered(attempt_fixtures::expected_retired_page(), ProofOnly),
        Case::RetiredEmptyContinuation => Reply::answered(
            json!({"result":"engineRetired","items":[],"next":attempt_fixtures::LAST_HIDDEN_ATTEMPT}),
            ProofOnly,
        ),
        Case::RetiredEnd => Reply::answered(
            json!({"result":"engineRetired","items":[],"next":null}),
            ProofOnly,
        ),
        Case::Retried => Reply::answered(json!({"result":"engineRetried"}), RetainResult),
        Case::Discarded => Reply::answered(json!({"result":"engineDiscarded"}), RetainResult),
        Case::NotPermitted => Reply::refused("notPermitted", NoWrites),
        Case::SecondFactorRequired => Reply::refused("secondFactorRequired", NoWrites),
        Case::RefreshRequired => Reply::refused("refreshRequired", NoWrites),
        Case::AuthorizationSyncRequired => Reply::refused("authorizationSyncRequired", NoWrites),
        Case::Expired => Reply::refused("expired", NoWrites),
        Case::RequestTime => Reply::refused("requestTime", NoWrites),
        Case::RequestReused => Reply::refused("requestReused", NoWrites),
        Case::GuardChanged => Reply::refused(
            match route {
                EngineAdminRoute::Retry => "notFound",
                EngineAdminRoute::Health
                | EngineAdminRoute::Retired
                | EngineAdminRoute::Discard => "conflict",
            },
            NoWrites,
        ),
        Case::OperationConflict => Reply::refused("conflict", ProofOnly),
        Case::RetainedReplay => Reply::refused("notFound", ProofOnly),
        Case::NotFound => Reply::refused("notFound", RetainResult),
        Case::NotRetired => Reply::refused("notRetired", RetainResult),
        Case::DiscardUnsupported => Reply::refused("discardUnsupported", RetainResult),
        Case::Stale => Reply::refused("stale", RetainResult),
        Case::SubjectUnreadable => Reply::refused("subjectUnreadable", RetainResult),
        Case::WrongMethod => Reply::empty(405),
        Case::MissingProof | Case::InvalidIntent => Reply::empty(401),
        Case::BadAddress => Reply::empty(400),
        Case::Unavailable => Reply::empty(503),
    }
}

fn assert_effects(
    case: EngineAdminCase,
    proof_id: &str,
    expected: &ExpectedReply,
    reply_bytes: &str,
    before: &attempt_fixtures::AdminFacts,
    after: &attempt_fixtures::AdminFacts,
) {
    match expected.writes {
        AdminWrites::NoWrites => {
            assert_eq!(after.proofs, before.proofs, "{case:?} proof reservation");
            assert_eq!(after.receipts, before.receipts, "{case:?} operation log");
            assert_eq!(after.actors, before.actors, "{case:?} actors");
        }
        AdminWrites::ProofOnly => {
            assert_eq!(after.proofs.len(), before.proofs.len() + 1);
            assert!(
                after
                    .proofs
                    .contains(&vec![SqlValue::Text(proof_id.to_owned())])
            );
            assert_eq!(after.receipts, before.receipts, "{case:?} operation log");
            assert_eq!(after.actors, before.actors, "{case:?} actors");
        }
        AdminWrites::RetainResult => {
            assert_eq!(after.proofs.len(), before.proofs.len() + 1);
            assert!(
                after
                    .proofs
                    .contains(&vec![SqlValue::Text(proof_id.to_owned())])
            );
            assert_eq!(after.receipts.len(), before.receipts.len() + 1);
            assert_eq!(after.actors.len(), before.actors.len() + 1);
            assert!(after.receipts.contains(&vec![
                SqlValue::Text(ENGINE_OPERATION.to_owned()),
                SqlValue::Text(reply_bytes.to_owned()),
            ]));
        }
    }
}

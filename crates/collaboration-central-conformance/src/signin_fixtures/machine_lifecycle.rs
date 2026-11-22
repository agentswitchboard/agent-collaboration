//! A route-level oracle shared by the Tokio and workerd Hegel suites.
use super::{ConformanceResult, OWNER, Person, SESSION, fresh_request_id};
use serde_json::{Value, json};

mod owning_rows;
use owning_rows::{AuditChange, EnrollmentStatus, ExpectedRows};

const DEVICE: &str = "/internal/device-login-decisions";
const TOKEN: &str = "/internal/machine-token-decisions";
const REVOKE: &str = super::revocation_journey::REVOKE_MACHINE;
const REVISIONS: &str = "/internal/organization-authorization-revisions";
const WRONG_CLIENT: &str = "019f0000-0000-7000-8000-00000000ee00";
const WORKING_REQUEST: &str = "019f0000-0000-7000-8000-00000000ee01";
const STALE_REQUEST: &str = "019f0000-0000-7000-8000-00000000ee02";
const SUCCESSOR_REQUEST: &str = "019f0000-0000-7000-8000-00000000ee03";
const EXPIRING_REQUEST: &str = "019f0000-0000-7000-8000-00000000ee04";

#[expect(
    async_fn_in_trait,
    reason = "Test hosts are driven on their owning current-thread runtime."
)]
pub trait MachineLifecycleHost {
    fn now_millis(&self) -> i64;
    async fn post(
        &self,
        path: &'static str,
        body: &Value,
        headers: &[(&'static str, String)],
    ) -> ConformanceResult<(u16, Value)>;
    /// Every query projects each SQLite row as one JSON array.
    async fn rows(&self, sql: &'static str) -> ConformanceResult<Vec<Value>>;
    async fn advance(&self, millis: u64) -> ConformanceResult<()>;
    async fn reopen(&mut self) -> ConformanceResult<()>;
    async fn revoke_and_observe_close(
        &self,
        machine: &str,
        bearer: &str,
        path: &'static str,
        body: &Value,
        headers: &[(&'static str, String)],
    ) -> ConformanceResult<(u16, Value)>;
}

#[derive(Clone, Copy, Debug)]
pub enum GrantRevokeOrder {
    GrantThenRevoke,
    RevokeThenGrant,
}

#[derive(Clone, Copy, Debug)]
pub enum ReplayProbe {
    Equal,
    ChangedLabel,
    ChangedWorkspaceSet,
    ChangedDeadline,
    Reopen,
}

#[derive(Debug)]
pub struct MachineLifecycleTrace {
    pub organization: &'static str,
    pub machine: &'static str,
    pub workspaces: [&'static str; 2],
    pub lapse_millis_after_deadline: u64,
    pub stale_client_at_deadline: bool,
    pub first_grant_elapsed_seconds: u64,
    pub replay_probes: Vec<ReplayProbe>,
}

struct Approval {
    request_id: &'static str,
    organization: &'static str,
    machine: &'static str,
    label: &'static str,
    workspaces: Vec<&'static str>,
    deadline: i64,
    request: Value,
    enrollment: String,
    reply: Value,
}

impl Approval {
    fn token(&self, use_correct_client: bool) -> Value {
        let client = if use_correct_client {
            &self.enrollment
        } else {
            WRONG_CLIENT
        };
        json!({
            "organizationId": self.organization,
            "enrollmentId": self.enrollment, "clientId": client
        })
    }
}

impl MachineLifecycleTrace {
    pub async fn exercise(
        &self,
        host: &mut impl MachineLifecycleHost,
        order: GrantRevokeOrder,
    ) -> ConformanceResult<()> {
        let [first, second] = self.workspaces;
        let approval_time = host.now_millis();
        let start_seconds = approval_time.div_euclid(1000);
        let deadline_seconds = start_seconds + 30;
        let (working, stale, successor, expiring, mut expected) = self
            .prepare_approvals(host, start_seconds, deadline_seconds)
            .await?;

        for &probe in &self.replay_probes {
            self.probe(host, &successor, &expected, probe).await?;
        }
        self.probe(host, &successor, &expected, ReplayProbe::Equal)
            .await?;
        self.probe(host, &successor, &expected, ReplayProbe::ChangedLabel)
            .await?;

        let elapsed =
            30_000 + self.lapse_millis_after_deadline - self.first_grant_elapsed_seconds * 1000;
        host.advance(elapsed).await?;
        let now = host.now_millis();
        assert_eq!(
            host.post(TOKEN, &stale.token(!self.stale_client_at_deadline), &[])
                .await?,
            token_refusal("enrollmentNotActive"),
            "expiry must win even for an approval behind the generation fence"
        );
        expected.observe_approval(&stale, 0, EnrollmentStatus::Lapsed(now));
        expected.assert_stored(host).await?;
        assert_eq!(
            host.post(TOKEN, &expiring.token(false), &[]).await?,
            token_refusal("enrollmentNotActive"),
            "expiry must win over wrong client on a current first grant"
        );
        expected.observe_approval(&expiring, 1, EnrollmentStatus::Lapsed(now));
        expected.assert_stored(host).await?;
        assert_eq!(
            host.post(TOKEN, &expiring.token(true), &[]).await?,
            token_refusal("enrollmentNotActive")
        );
        expected.assert_stored(host).await?;

        let (revoked, stale_jti) = match order {
            GrantRevokeOrder::GrantThenRevoke => {
                self.activate(host, &successor, Some(&working)).await?;
                expected.machine = json!([[self.machine, "enrolled", 2, "Expiring"]]);
                expected.observe_approval(
                    &working,
                    0,
                    EnrollmentStatus::Revoked {
                        cause: "superseded",
                        ended_at: now,
                    },
                );
                expected.observe_approval(
                    &successor,
                    1,
                    EnrollmentStatus::Enrolled(now.div_euclid(1000) * 1000),
                );
                expected.record(
                    SUCCESSOR_REQUEST,
                    AuditChange::Revoked(&working.enrollment),
                    "machine:enroll",
                    now,
                );
                expected.record(
                    SUCCESSOR_REQUEST,
                    AuditChange::Enrolled(&successor.enrollment),
                    "machine:enroll",
                    now,
                );
                expected.assert_stored(host).await?;

                let stale_revoke = self.signed_revocation(host, &working)?;
                stale_revoke.reserve_on(&mut expected);
                stale_revoke.send(host, "alreadyEnded").await?;
                expected.assert_stored(host).await?;
                (&successor, Some(stale_revoke))
            }
            GrantRevokeOrder::RevokeThenGrant => (&working, None),
        };

        let revoke = self.signed_revocation(host, revoked)?;
        revoke.reserve_on(&mut expected);
        let granted_at = match order {
            GrantRevokeOrder::GrantThenRevoke => now.div_euclid(1000),
            GrantRevokeOrder::RevokeThenGrant => {
                start_seconds + i64::try_from(self.first_grant_elapsed_seconds)?
            }
        };
        let bearer = super::machine_token(
            self.organization,
            self.machine,
            &revoked.enrollment,
            granted_at,
            granted_at + 600,
        )?;
        let applied = host
            .revoke_and_observe_close(self.machine, &bearer, REVOKE, &revoke.body, &revoke.headers)
            .await?;
        assert_eq!(applied, (200, json!({"result":"revoked"})));
        let (generation, base) = match order {
            GrantRevokeOrder::GrantThenRevoke => (3, 1),
            GrantRevokeOrder::RevokeThenGrant => (2, 0),
        };
        expected.machine = json!([[self.machine, "revoked", generation, "Expiring"]]);
        expected.observe_approval(
            revoked,
            base,
            EnrollmentStatus::Revoked {
                cause: "owner",
                ended_at: now,
            },
        );
        expected.record(
            &revoke.jti,
            AuditChange::Revoked(&revoked.enrollment),
            "machine:revoke",
            now,
        );
        // Withdrawal records follow the original insertion order; allowance
        // rows themselves have no order beyond their machine/workspace key.
        for workspace in [first, second] {
            expected.record(
                &revoke.jti,
                AuditChange::Allowance {
                    machine: self.machine,
                    workspace,
                    state: "withdrawn",
                },
                "machine:revoke",
                now,
            );
        }
        expected.allowances = self.allowance_rows("withdrawn", &revoke.jti, &revoke.jti);
        expected.assert_stored(host).await?;

        if matches!(order, GrantRevokeOrder::RevokeThenGrant) {
            assert_eq!(
                host.post(TOKEN, &successor.token(true), &[]).await?,
                token_refusal("superseded"),
                "an owner revoke fences a still-fresh pending grant"
            );
        }
        assert_eq!(
            host.post(TOKEN, &revoked.token(true), &[]).await?,
            token_refusal("enrollmentNotActive")
        );

        self.probe(host, &successor, &expected, ReplayProbe::Reopen)
            .await?;
        revoke.assert_reused(host).await?;
        if let Some(stale_revoke) = stale_jti {
            stale_revoke.assert_reused(host).await?;
        }
        for &probe in self.replay_probes.iter().rev() {
            self.probe(host, &successor, &expected, probe).await?;
        }
        self.probe(host, &successor, &expected, ReplayProbe::Equal)
            .await?;
        self.probe(
            host,
            &successor,
            &expected,
            ReplayProbe::ChangedWorkspaceSet,
        )
        .await
    }

    async fn prepare_approvals(
        &self,
        host: &impl MachineLifecycleHost,
        start: i64,
        deadline: i64,
    ) -> ConformanceResult<(Approval, Approval, Approval, Approval, ExpectedRows)> {
        let [first, second] = self.workspaces;
        let mut expected = ExpectedRows {
            machine: json!([[self.machine, "revoked", 0, "Working"]]),
            approvals: std::collections::BTreeMap::new(),
            allowances: vec![json!([
                self.machine,
                first,
                "active",
                allowance_request(WORKING_REQUEST, first),
                "person"
            ])],
            audit: Vec::new(),
            proofs: Vec::new(),
        };
        let working = self
            .approve(host, WORKING_REQUEST, "Working", &[first], start + 600)
            .await?;
        expected.observe_approval(&working, 0, EnrollmentStatus::Approved);
        expected.record(
            WORKING_REQUEST,
            AuditChange::Allowance {
                machine: self.machine,
                workspace: first,
                state: "active",
            },
            "machine:enroll",
            host.now_millis(),
        );
        expected.assert_stored(host).await?;

        let stale = self
            .approve(host, STALE_REQUEST, "Stale", &[first], deadline)
            .await?;
        expected.machine = json!([[self.machine, "revoked", 0, "Stale"]]);
        expected.observe_approval(&stale, 0, EnrollmentStatus::Approved);
        expected.assert_stored(host).await?;
        host.advance(self.first_grant_elapsed_seconds * 1000)
            .await?;
        let now = host.now_millis();
        self.activate(host, &working, None).await?;
        expected.machine = json!([[self.machine, "enrolled", 1, "Stale"]]);
        expected.observe_approval(
            &working,
            0,
            EnrollmentStatus::Enrolled(now.div_euclid(1000) * 1000),
        );
        expected.record(
            WORKING_REQUEST,
            AuditChange::Enrolled(&working.enrollment),
            "machine:enroll",
            now,
        );

        assert_eq!(
            host.post(TOKEN, &stale.token(true), &[]).await?,
            token_refusal("superseded")
        );
        expected.assert_stored(host).await?;
        let successor = self
            .approve(
                host,
                SUCCESSOR_REQUEST,
                "Successor",
                &[second, first, first],
                start + 600,
            )
            .await?;
        expected.machine = json!([[self.machine, "enrolled", 1, "Successor"]]);
        expected.observe_approval(&successor, 1, EnrollmentStatus::Approved);
        expected.allowances = self.allowance_rows("active", WORKING_REQUEST, SUCCESSOR_REQUEST);
        expected.record(
            SUCCESSOR_REQUEST,
            AuditChange::Allowance {
                machine: self.machine,
                workspace: second,
                state: "active",
            },
            "machine:enroll",
            now,
        );
        assert_eq!(
            host.post(TOKEN, &successor.token(false), &[]).await?,
            token_refusal("wrongClient")
        );
        expected.assert_stored(host).await?;
        let expiring = self
            .approve(host, EXPIRING_REQUEST, "Expiring", &[second], deadline)
            .await?;
        expected.machine = json!([[self.machine, "enrolled", 1, "Expiring"]]);
        expected.observe_approval(&expiring, 1, EnrollmentStatus::Approved);
        expected.assert_stored(host).await?;
        Ok((working, stale, successor, expiring, expected))
    }

    fn allowance_rows(&self, state: &str, first_request: &str, second_request: &str) -> Vec<Value> {
        let [first, second] = self.workspaces;
        let mut pairs = [(first, first_request), (second, second_request)];
        pairs.sort_unstable();
        pairs
            .into_iter()
            .map(|(workspace, request)| {
                json!([
                    self.machine,
                    workspace,
                    state,
                    allowance_request(request, workspace),
                    "person"
                ])
            })
            .collect()
    }

    async fn approve(
        &self,
        host: &impl MachineLifecycleHost,
        request_id: &'static str,
        label: &'static str,
        workspaces: &[&'static str],
        deadline: i64,
    ) -> ConformanceResult<Approval> {
        let request = json!({
            "organizationId": self.organization, "approver": OWNER,
            "permissionChecked": "machine:enroll", "requestId": request_id, "machineId": self.machine,
            "label": label, "allowedWorkspaces": workspaces,
            "approverSecondFactorAt": host.now_millis().div_euclid(1000) - 60, "codeExpiresAt": deadline
        });
        let (status, reply) = host.post(DEVICE, &request, &[]).await?;
        let enrollment = reply
            .get("enrollmentId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("approval {request_id} did not return an enrollment: {status} {reply}")
            })?
            .to_owned();
        assert_eq!(
            (status, &reply),
            (
                200,
                &json!({
                    "result": "approved", "enrollmentId": enrollment, "label": label,
                    "allowedWorkspaces": workspaces
                })
            )
        );
        Ok(Approval {
            request_id,
            organization: self.organization,
            machine: self.machine,
            label,
            workspaces: workspaces.to_vec(),
            deadline,
            request,
            enrollment,
            reply,
        })
    }

    async fn activate(
        &self,
        host: &impl MachineLifecycleHost,
        approval: &Approval,
        superseded: Option<&Approval>,
    ) -> ConformanceResult<()> {
        let decided_at = host.now_millis().div_euclid(1000);
        let mut expected = serde_json::Map::from_iter([
            (String::from("result"), json!("grant")),
            (String::from("machineId"), json!(self.machine)),
            (String::from("enrollmentId"), json!(approval.enrollment)),
            (String::from("decidedAt"), json!(decided_at)),
            (String::from("grantExpiresAt"), json!(decided_at + 600)),
        ]);
        if let Some(previous) = superseded {
            expected.insert(
                "superseded".to_owned(),
                json!({
                    "enrollmentId": previous.enrollment, "clientId": previous.enrollment
                }),
            );
        }
        assert_eq!(
            host.post(TOKEN, &approval.token(true), &[]).await?,
            (200, Value::Object(expected))
        );
        Ok(())
    }

    async fn probe(
        &self,
        host: &mut impl MachineLifecycleHost,
        approval: &Approval,
        expected: &ExpectedRows,
        probe: ReplayProbe,
    ) -> ConformanceResult<()> {
        let [first, second] = self.workspaces;
        let mut request = approval.request.clone();
        let fields = request
            .as_object_mut()
            .ok_or("approval request must be an object")?;
        let reply = match probe {
            ReplayProbe::Equal => {
                fields.insert("allowedWorkspaces".to_owned(), json!([first, second]));
                fields.insert("approverSecondFactorAt".to_owned(), Value::Null);
                approval.reply.clone()
            }
            ReplayProbe::ChangedLabel => {
                fields.insert("label".to_owned(), json!("Conflicting label"));
                request_conflict()
            }
            ReplayProbe::ChangedWorkspaceSet => {
                fields.insert("allowedWorkspaces".to_owned(), json!([first]));
                request_conflict()
            }
            ReplayProbe::ChangedDeadline => {
                fields.insert(
                    "codeExpiresAt".to_owned(),
                    json!(host.now_millis().div_euclid(1000) + 601),
                );
                request_conflict()
            }
            ReplayProbe::Reopen => {
                host.reopen().await?;
                return expected.assert_stored(host).await;
            }
        };
        assert_eq!(
            host.post(DEVICE, &request, &[]).await?,
            (200, reply),
            "{probe:?}"
        );
        expected.assert_stored(host).await
    }

    pub async fn prepare_revoker(&self, host: &impl MachineLifecycleHost) -> ConformanceResult<()> {
        for body in [
            json!({"kind":"organization","organizationId":self.organization,"revision":3,"state":"active","permissionPolicyDigest":super::POLICY}),
            json!({"kind":"person","organizationId":self.organization,"personId":OWNER,"subjectId":super::OWNER_SUBJECT,"revision":4,"state":"active"}),
        ] {
            assert_eq!(host.post(REVISIONS, &body, &[]).await?.0, 200);
        }
        Ok(())
    }

    fn signed_revocation(
        &self,
        host: &impl MachineLifecycleHost,
        approval: &Approval,
    ) -> ConformanceResult<SignedRevocation> {
        let now = host.now_millis().div_euclid(1000);
        let mut owner = Person::owner(self.organization, now)?;
        owner.permissions = vec!["machine:revoke".to_owned()];
        let jti = fresh_request_id(now)?;
        let intent = json!({
            "v": 1, "operation": "RevokeMachineEnrollment", "organization": self.organization,
            "target": approval.enrollment, "operationId": null,
            "params": {"actor": OWNER, "secondFactorAt": owner.second_factor_at},
        });
        let headers = owner.headers(REVOKE, &intent, &jti, now)?;
        let proof_row = json!([
            jti,
            self.organization,
            (now - 1) * 1000,
            null,
            OWNER,
            SESSION,
            super::sha256_url(super::canonical(&intent).as_bytes()),
        ]);
        Ok(SignedRevocation {
            body: json!({"organizationId": self.organization, "enrollmentId": approval.enrollment}),
            headers,
            jti,
            proof_row,
        })
    }
}

struct SignedRevocation {
    body: Value,
    headers: Vec<(&'static str, String)>,
    jti: String,
    proof_row: Value,
}

impl SignedRevocation {
    fn reserve_on(&self, expected: &mut ExpectedRows) {
        expected.proofs.push(self.proof_row.clone());
    }

    async fn send(
        &self,
        host: &impl MachineLifecycleHost,
        expected: &str,
    ) -> ConformanceResult<()> {
        assert_eq!(
            host.post(REVOKE, &self.body, &self.headers).await?,
            (200, json!({"result":expected}))
        );
        Ok(())
    }

    async fn assert_reused(&self, host: &impl MachineLifecycleHost) -> ConformanceResult<()> {
        assert_eq!(
            host.post(REVOKE, &self.body, &self.headers).await?,
            (401, Value::Null),
            "a byte-identical proof may not revoke again, including after owning reopen"
        );
        Ok(())
    }
}

fn request_conflict() -> Value {
    json!({"result":"refused","reason":{"kind":"requestConflict"}})
}

fn token_refusal(kind: &str) -> (u16, Value) {
    (200, json!({"result":"refuse","reason":{"kind":kind}}))
}

fn allowance_request(request: &str, workspace: &str) -> String {
    format!("{request}:machineAllowanceChanged:{workspace}")
}

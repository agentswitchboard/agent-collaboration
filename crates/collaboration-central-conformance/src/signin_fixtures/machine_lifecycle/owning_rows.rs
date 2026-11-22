//! Expected storage projections derived from the test actions and their known inputs.
use super::{Approval, ConformanceResult, MachineLifecycleHost, OWNER, Value, json};

use std::collections::BTreeMap;

impl Approval {
    fn owning_row(&self, generation: i64, status: EnrollmentStatus) -> Value {
        let (state, reason, client, last_grant, ended_at) = match status {
            EnrollmentStatus::Approved => ("approved", None, None, None, None),
            EnrollmentStatus::Enrolled(last_grant) => (
                "enrolled",
                None,
                Some(self.enrollment.as_str()),
                Some(last_grant),
                None,
            ),
            EnrollmentStatus::Lapsed(ended_at) => {
                ("ended", Some("lapsed"), None, None, Some(ended_at))
            }
            EnrollmentStatus::Revoked { cause, ended_at } => (
                "ended",
                Some(cause),
                Some(self.enrollment.as_str()),
                None,
                Some(ended_at),
            ),
        };
        json!([
            self.enrollment,
            self.request_id,
            self.machine,
            self.label,
            self.workspaces,
            OWNER,
            "machine:enroll",
            generation,
            self.deadline,
            state,
            reason,
            client,
            last_grant,
            ended_at
        ])
    }
}

pub(super) enum EnrollmentStatus {
    Approved,
    Enrolled(i64),
    Lapsed(i64),
    Revoked { cause: &'static str, ended_at: i64 },
}

#[derive(Debug)]
pub(super) struct ExpectedRows {
    pub(super) machine: Value,
    pub(super) approvals: BTreeMap<&'static str, Value>,
    pub(super) allowances: Vec<Value>,
    pub(super) audit: Vec<Value>,
    pub(super) proofs: Vec<Value>,
}

impl ExpectedRows {
    pub(super) async fn assert_stored(
        &self,
        host: &impl MachineLifecycleHost,
    ) -> ConformanceResult<()> {
        let approvals = host.rows("SELECT json_array(approval_id,request_id,machine_id,label,json(allowed_workspaces),approved_by_person_id,permission_checked,base_generation,unixepoch(code_expires_at),state,end_reason,machine_client_id,unixepoch(last_grant_at)*1000,CAST(round(unixepoch(ended_at,'subsec')*1000) AS INTEGER)) FROM login_machine_approvals ORDER BY request_id").await?;
        assert!(
            approvals.iter().eq(self.approvals.values()),
            "approval history: expected {:?}, found {approvals:?}",
            self.approvals
        );
        let machine = host.rows("SELECT json_array(machine_id,state,enrollment_generation,label) FROM login_machines ORDER BY machine_id").await?;
        assert_eq!(
            Value::Array(machine),
            self.machine,
            "machine state, generation and label"
        );
        let allowances = host.rows("SELECT json_array(machine_id,workspace_id,state,change_request_id,change_origin) FROM login_machine_allowances ORDER BY workspace_id").await?;
        assert_eq!(
            allowances, self.allowances,
            "exact allowance identities and transition origins"
        );
        let audit = host.rows("SELECT json_array(request_id,kind,origin,actor_person_id,permission_checked,target_id,workspace_id,allowance_state,created_at) FROM signin_change_records WHERE kind IN ('machineEnrolled','machineRevoked','machineExpired','machineAllowanceChanged','allowanceChanged') ORDER BY id").await?;
        assert_eq!(audit, self.audit, "exact ordered machine audit effects");
        let proofs = host.rows("SELECT json_array(id,organization_id,request_time_ms,operation_id,actor_person_id,actor_session_id,intent_digest) FROM admin_used_requests ORDER BY id").await?;
        assert_eq!(proofs, self.proofs, "proof reservations");
        Ok(())
    }

    pub(super) fn observe_approval(
        &mut self,
        approval: &Approval,
        generation: i64,
        status: EnrollmentStatus,
    ) {
        self.approvals
            .insert(approval.request_id, approval.owning_row(generation, status));
    }

    pub(super) fn record(
        &mut self,
        request_id: &str,
        change: AuditChange<'_>,
        permission: &str,
        now_ms: i64,
    ) {
        let (kind, target, workspace, state) = match change {
            AuditChange::Enrolled(enrollment) => ("machineEnrolled", enrollment, None, None),
            AuditChange::Revoked(enrollment) => ("machineRevoked", enrollment, None, None),
            AuditChange::Allowance {
                machine,
                workspace,
                state,
            } => (
                "machineAllowanceChanged",
                machine,
                Some(workspace),
                Some(state),
            ),
        };
        let suffix = workspace.unwrap_or(target);
        self.audit.push(json!([
            format!("{request_id}:{kind}:{suffix}"),
            kind,
            "person",
            OWNER,
            permission,
            target,
            workspace,
            state,
            now_ms
        ]));
    }
}

pub(super) enum AuditChange<'approval> {
    Enrolled(&'approval str),
    Revoked(&'approval str),
    Allowance {
        machine: &'approval str,
        workspace: &'approval str,
        state: &'static str,
    },
}

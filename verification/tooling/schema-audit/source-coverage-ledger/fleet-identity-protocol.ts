import type { LedgerSection } from "../schema-audit-types.ts";

const R = "crates/fleet-identity-protocol/src/";
const fleetWire =
  "central fleet connection contract between a machine's Router and central (lib.rs:1), not one of the six outside edges; collaboration-protocol uses only RemoteLaunchPolicy/RemoteLaunchAccess from this crate";

export const section: LedgerSection = {
  root: "crates/fleet-identity-protocol",
  files: [
    {
      file: `${R}identity_values.rs`,
      disposition: { kind: "defines", owners: ["router-vocabulary"] },
      note: "central fleet identity values OrganizationId, EnrollmentId, RemoteStartId, IsolationInstanceId, InstallationId (l39-43), RepositoryLocator, ModelName, CommitId, UnixSeconds l90, UnixMillis l102, SequenceNumber l115; AbsolutePath l70 (the same value and schema as ProviderWorkingDirectory) is Fleet AbsolutePath, outside Project 2's protocols, no inventory row",
    },
    {
      file: `${R}lib.rs`,
      disposition: { kind: "none" },
      note: "module declarations and re-exports only",
    },
    {
      file: `${R}machine_renewal.rs`,
      disposition: {
        kind: "excluded",
        reason: `${fleetWire}: RenewMachineToken l6`,
      },
    },
    {
      file: `${R}remote_start.rs`,
      disposition: { kind: "defines", owners: ["router-vocabulary"] },
      note: "Fleet remote-start wire includes RemoteLaunch, RemoteStartAdmission, ReportRemoteStart and RemoteStartResult with the board SessionRef. RemoteLaunchAccess duplicates collaboration-protocol RouterAccess. HarnessKind is also used in the session lifecycle contract. These Fleet messages are outside Project 2's protocols.",
    },
    {
      file: `${R}session_activity.rs`,
      disposition: {
        kind: "excluded",
        reason: `${fleetWire}: Heartbeat l8, MachineCapability l18, ReportSessionActivity l23, SessionActivityEntry l30 (board SessionRef, no conversion), Activity l36, ReportSessionActivityResult l47, ActivityRefusal l59`,
      },
    },
    {
      file: `${R}session_registration.rs`,
      disposition: {
        kind: "excluded",
        reason: `${fleetWire}: RegisterFleetSession l11, StartedBy l26, RegisterFleetSessionResult l43, SessionRefusal l59, AgentIsolation l66, IsolationKind l72, MachineEnvironment l80, LaunchContext l86, EndFleetSession l97, SessionEndCause l104 (board SessionRef fields, no conversion)`,
      },
    },
  ],
  sendSites: [],
};

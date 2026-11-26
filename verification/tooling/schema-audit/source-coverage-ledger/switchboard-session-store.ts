import type { LedgerSection } from "../schema-audit-types.ts";

export const section: LedgerSection = {
  root: "crates/switchboard-session-store",
  files: [
    {
      file: "crates/switchboard-session-store/src/conversation_operation_projection.rs",
      disposition: { kind: "none" },
      note: "conversation_operation_snapshot l5: typed field copy ProviderOperationRecord to ConversationOperationSnapshot (queue_state and input_id set None l17-18); no serde round trip",
    },
    {
      file: "crates/switchboard-session-store/src/provider_operation_store.rs",
      disposition: { kind: "converts", owners: ["router-vocabulary"] },
      note: "provider operation metadata store: Router vocabulary enums stored as their serde strings and records as closed JSON text (:800-820), legacy ProviderBindingIdentity binding decoded (:822-828), target SessionRef rebuilt from three columns (:759-780); storage format, not an outside edge",
    },
    {
      file: "crates/switchboard-session-store/src/provider_session_record.rs",
      disposition: { kind: "converts", owners: ["router-vocabulary"] },
      note: "durable provider session record: requested_policy, created_by, approver stored and read as JSON text columns (:205-212, :305-310, :361-366); endpoint ids claude-local|cursor-local (:90); storage format, not an outside edge",
    },
  ],
  sendSites: [],
};

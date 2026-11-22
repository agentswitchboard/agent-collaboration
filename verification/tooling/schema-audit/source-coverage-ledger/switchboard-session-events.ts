import type { LedgerSection } from "../schema-audit-types.ts";

export const section: LedgerSection = {
  root: "crates/switchboard-session-events",
  files: [
    {
      file: "crates/switchboard-session-events/src/provider_session_event_hub.rs",
      disposition: { kind: "converts", owners: ["router-vocabulary"] },
      note: "N5 hub: board and protocol SessionRef conversions (:543-587), TurnOutcome to SessionState (:120-126); no external wire",
    },
    {
      file: "crates/switchboard-session-events/src/provider_session_inventory_metadata.rs",
      disposition: { kind: "reads", owners: ["router-vocabulary"] },
      note: "parses SessionTitleMetadata from item text JSON (:36, :65-82); JSON-in-string reader",
    },
  ],
  sendSites: [],
};

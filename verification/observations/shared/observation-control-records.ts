import { z } from "zod";

// An oversized event is replaced in place by this marker (event_value_bounded,
// provider_observation_operations.rs:267-292).
export const eventTooLargeSchema = z.strictObject({
  kind: z.literal("eventTooLarge"),
  sequence: z.bigint(),
  itemId: z.string().nullable().optional(),
});
// A stale epoch or a lagging reader yields this unnumbered marker as the call's last event
// (provider_observation_operations.rs:89-101,133-136); a hub reset yields a numbered
// `resyncRequired` event instead. Either way the history must be attached again.
export const bareResyncMarkerSchema = z.strictObject({ kind: z.literal("resyncRequired") });

export const observationCursorSchema = z.strictObject({
  epoch: z.bigint(),
  afterSequence: z.bigint(),
});
export type ObservationCursor = z.infer<typeof observationCursorSchema>;

export const snapshotEndSchema = z.strictObject({
  kind: z.literal("snapshotEnd"),
  cursor: observationCursorSchema,
});

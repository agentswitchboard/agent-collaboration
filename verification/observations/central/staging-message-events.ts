import { z } from "zod";
import { sessionSchema, type Session } from "../../declaration.ts";

const noticeText = "You have 1 new message. Read them with message_inbox.";
const eventSchema = z.looseObject({
  kind: z.string(),
  turnId: z.string().optional(),
  outcome: z.looseObject({ stop_reason: z.string().optional() }).optional(),
  item: z
    .object({
      itemId: z.string(),
      text: z.string().nullable(),
      kind: z.looseObject({ kind: z.string() }),
    })
    .optional(),
});
const eventLine = z.object({
  kind: z.enum(["listenerReady", "providerSessionEvent"]),
  target: sessionSchema,
  generation: z.object({ serviceEpoch: z.string(), generation: z.number() }),
  message: z
    .object({
      kind: z.string().optional(),
      sequence: z.number().int().nonnegative().optional(),
      cursor: z.object({ epoch: z.number().int(), afterSequence: z.number().int() }).optional(),
      event: eventSchema.optional(),
    })
    .optional(),
});

type Event = z.infer<typeof eventSchema>;
export interface NoticeObservation {
  readonly turnId: string | null;
  readonly itemId: string;
  readonly sequence: number;
}

interface TurnTiming {
  readonly startedAt: number;
  readonly startedMonotonic: number;
  endedAt?: number;
  durationMs?: number;
  stopReason?: string;
  noticeAt?: number;
  answerUpdates: number;
  answerUpdatesAfterNotice: number;
  longestAnswerChars: number;
}

/** Correlate only live events after the initial snapshot. Never retain arbitrary item text. */
export class StagingMessageEvents {
  ready = false;
  activeTurn: string | null = null;
  readonly starts: string[] = [];
  readonly ends: string[] = [];
  readonly endObservedAt = new Map<string, number>();
  readonly notices: NoticeObservation[] = [];
  readonly answeringTurns = new Set<string>();
  readonly toolTurns = new Set<string>();
  readonly timings = new Map<string, TurnTiming>();
  readonly target: Session;
  private owner: string | undefined;
  private epoch: number | undefined;
  private sequence: number | undefined;

  constructor(target: Session) {
    this.target = target;
  }

  ingest(line: string): void {
    let decoded: unknown;
    try {
      decoded = JSON.parse(line);
    } catch {
      throw new Error("recipient stream returned invalid JSON");
    }
    const value = eventLine.safeParse(decoded);
    if (!value.success) throw new Error("recipient stream closed or returned an unknown envelope");
    const { target, generation, message, kind } = value.data;
    if (
      target.sessionId !== this.target.sessionId ||
      target.endpoint.serviceId !== this.target.endpoint.serviceId ||
      target.endpoint.endpointId !== this.target.endpoint.endpointId
    )
      throw new Error("recipient stream target changed");
    const owner = `${generation.serviceEpoch}/${generation.generation}`;
    if (kind === "listenerReady" && this.owner === undefined) {
      this.owner = owner;
      return;
    }
    if (kind !== "providerSessionEvent" || this.owner !== owner || message === undefined)
      throw new Error("recipient stream owner changed");
    if (message.kind === "snapshotEnd" && message.cursor !== undefined) {
      const { epoch, afterSequence } = message.cursor;
      if (this.ready && (this.epoch !== epoch || afterSequence < (this.sequence ?? 0)))
        throw new Error("recipient snapshot continuity lost");
      this.epoch = epoch;
      this.sequence = afterSequence;
      this.ready = true;
      return;
    }
    const { sequence, event } = message;
    if (
      sequence === undefined ||
      event === undefined ||
      ["resyncRequired", "eventTooLarge"].includes(event.kind)
    )
      throw new Error("recipient stream cannot prove contiguous coverage");
    if (!this.ready) return;
    // Hub snapshots retain the latest representation of each item. A reset or
    // overrun is a separate, explicit resync event.
    if (sequence <= (this.sequence ?? 0))
      throw new Error("recipient stream reordered or replayed an event");
    this.sequence = sequence;
    this.observe(event, sequence);
  }

  private observe(event: Event, sequence: number): void {
    const { kind, item, turnId } = event;
    if (kind === "turnStarted" && turnId !== undefined) {
      if (this.activeTurn !== null) throw new Error("recipient has overlapping turns");
      this.activeTurn = turnId;
      this.starts.push(turnId);
      this.timings.set(turnId, {
        startedAt: Date.now(),
        startedMonotonic: performance.now(),
        answerUpdates: 0,
        answerUpdatesAfterNotice: 0,
        longestAnswerChars: 0,
      });
    }
    if (kind === "turnEnded" && turnId !== undefined) {
      if (this.activeTurn !== turnId) throw new Error("recipient turn end has no matching start");
      this.activeTurn = null;
      this.ends.push(turnId);
      this.endObservedAt.set(turnId, Date.now());
      const timing = this.timings.get(turnId);
      if (timing !== undefined) {
        timing.endedAt = Date.now();
        timing.durationMs = performance.now() - timing.startedMonotonic;
        if (event.outcome?.stop_reason !== undefined) timing.stopReason = event.outcome.stop_reason;
      }
    }
    if (kind === "interactionRequested")
      throw new Error("recipient requires approval, sign-in, or an answer");
    if (
      (kind === "itemStarted" || kind === "itemUpdated") &&
      item?.kind.kind === "agentMessage" &&
      item.text?.trim() &&
      this.activeTurn !== null
    ) {
      this.answeringTurns.add(this.activeTurn);
      const timing = this.timings.get(this.activeTurn);
      if (timing !== undefined) {
        timing.answerUpdates++;
        if (timing.noticeAt !== undefined) timing.answerUpdatesAfterNotice++;
        timing.longestAnswerChars = Math.max(timing.longestAnswerChars, item.text.length);
      }
    }
    if (item?.kind.kind === "toolCall" && this.activeTurn !== null)
      this.toolTurns.add(this.activeTurn);
    if (kind === "itemStarted" && item?.kind.kind === "userMessage" && item.text === noticeText) {
      this.notices.push({ turnId: this.activeTurn, itemId: item.itemId, sequence });
      const timing = this.activeTurn === null ? undefined : this.timings.get(this.activeTurn);
      if (timing !== undefined) timing.noticeAt ??= Date.now();
    }
  }
}

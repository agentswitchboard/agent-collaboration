import { z } from "zod";
import { ObservationFailure, type Receipt } from "../../receipt.ts";
import type { NoticeObservation, StagingMessageEvents } from "./staging-message-events.ts";

export const noticeSeenToleranceMs = 5;
const publicationPeriodMs = 60_000;
const publicationGraceMs = 5_000;

const statusSchema = z.object({
  kind: z.string(),
  value: z
    .object({
      kind: z.string(),
      messageId: z.uuid().optional(),
      delivery: z
        .object({
          kind: z.string(),
          value: z
            .object({
              kind: z.string(),
              arrivedAt: z.iso.datetime().optional(),
              completedAt: z.iso.datetime().optional(),
              deadlinePassed: z.boolean().optional(),
            })
            .optional(),
        })
        .optional(),
    })
    .optional(),
});

interface ObservationClock {
  monotonic(): number;
  utc(): number;
  poll(): Promise<void>;
}

export interface DeliveryCheck {
  readonly receipt: Pick<Receipt, "record">;
  readonly view: StagingMessageEvents;
  readonly requestId: string;
  readonly checkpoint: number;
  readonly answeredId: string | undefined;
  readonly runningTurn: string | undefined;
  readonly startCount: number;
  readonly seenWindowMs: number;
  readonly assertStream: () => void;
  readonly readStatus: () => Promise<
    { exit: number | null; output: unknown; observedAt?: string } | undefined
  >;
  readonly clock: ObservationClock;
}

class ObservationGap extends ObservationFailure {}

function assertContinuous(check: DeliveryCheck): void {
  try {
    check.assertStream();
  } catch (error) {
    throw new ObservationGap(error instanceof Error ? error.message : "recipient observation lost");
  }
}

export function sendCheckpoint(view: StagingMessageEvents): number {
  const sequence = view.checkpoint();
  requireFreshNotice(view, sequence);
  return sequence;
}

function requireFreshNotice(
  view: StagingMessageEvents,
  checkpoint: number,
): NoticeObservation | undefined {
  if (
    view.historyNoticeIds.size > 0 ||
    view.notices.some((notice) => notice.sequence <= checkpoint)
  )
    throw new ObservationFailure("recipient notice predates the send checkpoint");
  if (view.notices.length > 1) throw new ObservationFailure("recipient received multiple notices");
  const notice = view.notices[0];
  if (notice !== undefined && notice.messageCount !== 1)
    throw new ObservationFailure("recipient notice did not report exactly one message");
  return notice;
}

interface SeenEvidence {
  readonly messageId: string;
  readonly completedAt: string;
  readonly duplicateUntil: number;
}

function matchNoticeTime(notice: NoticeObservation, seen: SeenEvidence): string {
  if (!z.uuidv7().safeParse(notice.itemId).success)
    throw new ObservationFailure("recipient notice item has no UUIDv7 creation time");
  const noticeMs = Number.parseInt(notice.itemId.replaceAll("-", "").slice(0, 12), 16);
  if (Math.abs(noticeMs - Date.parse(seen.completedAt)) > noticeSeenToleranceMs)
    throw new ObservationFailure("recipient notice time does not match sender Seen");
  return new Date(noticeMs).toISOString();
}

function requireTurnPlacement(
  check: DeliveryCheck,
  notice: NoticeObservation | undefined,
  arrivedAt: number | undefined,
  seen: SeenEvidence | undefined,
): void {
  const { view, runningTurn, startCount } = check;
  if (runningTurn === undefined) {
    if (view.starts.length > 1 || (notice !== undefined && notice.turnId !== view.starts[0]))
      throw new ObservationFailure("idle recipient did not receive the notice in one new turn");
    return;
  }
  if (notice === undefined && view.activeTurn === runningTurn && view.toolTurns.has(runningTurn))
    throw new Error("the held turn used a tool");
  if (view.ends.includes(runningTurn) && notice?.turnId !== runningTurn) {
    const end = view.endObservedAt.get(runningTurn);
    if (arrivedAt !== undefined && end !== undefined && end - arrivedAt >= 65_000)
      throw new ObservationFailure("accepted message did not enter the already-running turn");
    if (arrivedAt !== undefined || seen !== undefined)
      throw new Error("hold ended before a full notice window after recipient Mac custody");
  } else if (
    view.starts.length !== startCount ||
    (notice !== undefined && notice.turnId !== runningTurn)
  ) {
    throw new ObservationFailure("notice was placed outside the active test turn");
  }
}

export async function checkDelivery(check: DeliveryCheck): Promise<void> {
  try {
    await pollDelivery(check);
  } catch (error) {
    if (error instanceof ObservationGap)
      await check.receipt.record("continuity", { passed: false, reason: error.message });
    throw error;
  }
}

async function pollDelivery(check: DeliveryCheck): Promise<void> {
  const { clock, receipt, requestId, view } = check;
  let expires = clock.monotonic() + 275_000;
  let firstNoticeAt: number | undefined;
  let seen: SeenEvidence | undefined;
  let messageId = check.answeredId;
  let arrivedAt: number | undefined;
  let previousStatus: string | undefined;
  for (;;) {
    assertContinuous(check);
    let notice = requireFreshNotice(view, check.checkpoint);
    const now = clock.monotonic();
    if (notice !== undefined && firstNoticeAt === undefined) {
      firstNoticeAt = now;
      expires = Math.max(expires, now + check.seenWindowMs);
    }
    if (now >= expires) break;

    const result = await check.readStatus();
    assertContinuous(check);
    if (result !== undefined) {
      if (result.exit !== 0) throw new Error("sender status failed; do not replay the send");
      const status = statusSchema.parse(result.output);
      const encoded = JSON.stringify(status);
      if (encoded !== previousStatus) {
        await receipt.record("sender-status", {
          requestId,
          status,
          exit: result.exit,
          observedAt: result.observedAt,
        });
        previousStatus = encoded;
      }
      const delivery = status.value?.delivery?.value;
      const id = status.value?.messageId;
      if (id !== undefined) {
        if (messageId !== undefined && messageId !== id)
          throw new ObservationFailure("sender reported inconsistent Central message identities");
        messageId = id;
      }
      if (
        status.kind !== "returned" ||
        ["refused", "notSent", "notConfirmed", "notFound"].includes(status.value?.kind ?? "") ||
        delivery?.kind === "refused"
      )
        throw new Error("sender reported refusal or broken custody");
      if (delivery?.kind === "delivered" && delivery.arrivedAt !== undefined)
        arrivedAt ??= Date.parse(delivery.arrivedAt);
      if (delivery?.kind === "seen") {
        if (id === undefined || delivery.completedAt === undefined)
          throw new ObservationFailure(
            "sender Seen requires a message identity and completion time",
          );
        if (seen !== undefined && seen.completedAt !== delivery.completedAt)
          throw new ObservationFailure("sender Seen completion time changed");
        if (seen === undefined) {
          const observedAt = clock.utc();
          const throughMark =
            (Math.floor(observedAt / publicationPeriodMs) + 1) * publicationPeriodMs +
            publicationGraceMs -
            observedAt;
          seen = {
            messageId: id,
            completedAt: delivery.completedAt,
            duplicateUntil: clock.monotonic() + throughMark,
          };
          expires = Math.max(expires, seen.duplicateUntil + 20_000);
          await receipt.record("duplicate-window", {
            requestId,
            observedAt: new Date(observedAt).toISOString(),
            through: new Date(observedAt + throughMark).toISOString(),
          });
        }
      }
    }

    notice = requireFreshNotice(view, check.checkpoint);
    requireTurnPlacement(check, notice, arrivedAt, seen);
    if (seen !== undefined && notice?.turnId != null) {
      const noticeCreatedAt = matchNoticeTime(notice, seen);
      if (clock.monotonic() >= seen.duplicateUntil) {
        assertContinuous(check);
        await receipt.record("pass", {
          requestId,
          messageId: seen.messageId,
          notice,
          starts: view.starts.length,
          noticeCreatedAt,
          seenCompletedAt: seen.completedAt,
          noticeSeenToleranceMs,
          continuity: "continuous",
        });
        return;
      }
    }
    await clock.poll();
  }
  if (seen !== undefined && view.notices.length === 0)
    throw new ObservationFailure("Seen without a matching visible notice");
  if (seen !== undefined) throw new Error("observation ended during the notice window");
  throw new Error("the observation bound ended without a sender Seen receipt");
}

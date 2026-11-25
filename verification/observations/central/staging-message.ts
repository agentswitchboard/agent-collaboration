import { spawn } from "node:child_process";
import { randomUUIDv7 } from "node:crypto";
import { mkdir, realpath } from "node:fs/promises";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout } from "node:timers/promises";
import { z } from "zod";
import { privateDirectory } from "../../machines/owned-process.ts";
import { ObservationFailure, Receipt } from "../../receipt.ts";
import { StagingMessageEvents } from "./staging-message-events.ts";
import { awaitRemoteSend, readRemoteReceipt } from "./staging-message-sender.ts";
import {
  cli,
  inspect,
  parseCommand,
  prepare,
  readPlan,
  serviceDirectory,
  type CommandResult,
  type Plan,
} from "./staging-message-plan.ts";

const sendSchema = z.object({
  kind: z.literal("returned"),
  value: z.object({
    kind: z.enum(["inOutbox", "answered"]),
    requestId: z.uuid(),
    flushAt: z.string().optional(),
    messageId: z.uuid().optional(),
  }),
});
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
function holdPrompt(answerLines: number): string {
  return (
    "This is a bounded verification fixture. Your answer must contain the decimal numbers " +
    `1 through ${answerLines} inclusive, in order, one number per line. ` +
    "Write every number yourself without tools. Do not use ellipses or ranges. " +
    "If an unrelated notice arrives, continue the numbered answer without tools."
  );
}

interface RecipientStream {
  readonly view: StagingMessageEvents;
  assertContinuous(): void;
  close(): Promise<void>;
}

function attachRecipient(plan: Plan): RecipientStream {
  const view = new StagingMessageEvents(plan.recipient);
  const child = spawn(
    plan.collaboration.path,
    [
      "events",
      "listen",
      "--endpoint",
      "claude-local",
      "--session",
      plan.recipient.sessionId,
      "--attach",
      "--service-directory",
      serviceDirectory(plan),
      "--timeout-seconds",
      plan.senderHandoff === undefined ? "540" : "1320",
    ],
    { stdio: ["ignore", "pipe", "ignore"] },
  );
  let streamError: Error | undefined;
  const exited = new Promise<void>((resolve) => {
    child.once("close", () => {
      streamError ??= new Error("recipient observation ended");
      resolve();
    });
  });
  child.once("error", () => {
    streamError = new Error("failed to start recipient observation");
  });
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  lines.on("line", (line: string): void => {
    if (streamError !== undefined) return;
    try {
      view.ingest(line);
    } catch (error: unknown) {
      streamError = error instanceof Error ? error : new Error("invalid recipient event");
    }
  });
  return {
    view,
    assertContinuous(): void {
      if (streamError !== undefined) throw streamError;
    },
    async close(): Promise<void> {
      child.kill("SIGTERM");
      await exited;
      lines.close();
    },
  };
}

async function observe(plan: Plan, receipt: Receipt): Promise<void> {
  const stream = attachRecipient(plan);
  const { view, assertContinuous: assertStream, close } = stream;
  let hold: Promise<CommandResult | undefined> | undefined;
  let holdOperationId: string | undefined;
  let runningTurn: string | undefined;
  try {
    await requireIdleObservation(plan, stream);
    if (plan.journey === "running") {
      const holdSeconds = plan.timing === undefined ? 320 : 420;
      holdOperationId = randomUUIDv7();
      await receipt.record("hold-command", {
        operationId: holdOperationId,
        lines: plan.answerLines,
      });
      hold = cli(
        plan,
        [
          "conversation",
          "prompt",
          "--to",
          JSON.stringify(plan.recipient),
          "--operation-id",
          holdOperationId,
          "--text",
          holdPrompt(plan.answerLines),
          "--timeout-seconds",
          String(holdSeconds),
          "--json",
        ],
        holdSeconds + 5,
      ).catch(() => undefined);
      for (let attempt = 0; attempt < 180; attempt++) {
        assertStream();
        if (view.activeTurn !== null && view.answeringTurns.has(view.activeTurn)) {
          runningTurn = view.activeTurn;
          break;
        }
        if (view.ends.length !== 0) break;
        await setTimeout(250);
      }
      if (
        runningTurn === undefined ||
        (await inspect(plan)).state !== "running" ||
        view.activeTurn !== runningTurn ||
        view.toolTurns.has(runningTurn)
      )
        throw new Error("the bounded no-tools answer did not produce an observable running turn");
      await receipt.record("held-recipient-turn", { turnId: runningTurn });
    }
    if (plan.timing !== "control") await driveNotice(plan, receipt, stream, runningTurn);
    if (plan.timing !== undefined && runningTurn !== undefined)
      await recordNaturalTurn(plan, receipt, stream, runningTurn);
  } finally {
    const runTurn = runningTurn ?? (hold === undefined ? undefined : view.starts[0]);
    if (runTurn !== undefined && view.activeTurn === runTurn) {
      const cleanup = await cli(plan, [
        "turn",
        "interrupt",
        "--to",
        JSON.stringify(plan.recipient),
        "--turn",
        runTurn,
        "--json",
      ]).catch(() => undefined);
      await receipt.record("interrupt-own-hold", { turnId: runTurn, exit: cleanup?.exit ?? null });
    }
    if (hold !== undefined) {
      const settled = await hold;
      await receipt.record("hold-command-settlement", {
        exit: settled?.exit ?? null,
        permissionRejected: await holdPermissionRejected(plan, holdOperationId, settled),
      });
    }
    await close();
    await receipt.record("recipient-events", {
      starts: view.starts,
      ends: view.ends,
      notices: view.notices,
    });
  }
}

async function driveNotice(
  plan: Plan,
  receipt: Receipt,
  stream: RecipientStream,
  runningTurn: string | undefined,
): Promise<void> {
  const { view, assertContinuous: assertStream } = stream;
  const startCount = view.starts.length;
  const requestId = randomUUIDv7();
  await receipt.record("send-attempt", { requestId, runningTurn });
  assertStream();
  await sendMessage(plan, receipt, requestId, runningTurn);
  await checkDelivery({ plan, receipt, view, requestId, runningTurn, startCount, assertStream });
}

async function recordNaturalTurn(
  plan: Plan,
  receipt: Receipt,
  stream: RecipientStream,
  turnId: string,
): Promise<void> {
  const { view, assertContinuous } = stream;
  const expires = performance.now() + 425_000;
  while (!view.ends.includes(turnId) && performance.now() < expires) {
    assertContinuous();
    await setTimeout(250);
  }
  assertContinuous();
  const timing = view.timings.get(turnId);
  if (
    timing?.endedAt === undefined ||
    timing.durationMs === undefined ||
    !["endTurn", "end_turn", "maxTokens", "max_tokens"].includes(timing.stopReason ?? "") ||
    view.starts.length !== 1 ||
    (plan.timing === "control" && (view.notices.length !== 0 || view.toolTurns.has(turnId)))
  )
    throw new Error("timing specimen did not complete its expected no-tools turn naturally");
  await receipt.record("turn-timing", {
    turnId,
    condition: plan.timing,
    requestedAnswerLines: plan.answerLines,
    startedAt: new Date(timing.startedAt).toISOString(),
    endedAt: new Date(timing.endedAt).toISOString(),
    noticeAt: timing.noticeAt === undefined ? null : new Date(timing.noticeAt).toISOString(),
    durationMs: Math.round(timing.durationMs),
    stopReason: timing.stopReason,
    answerUpdates: timing.answerUpdates,
    answerUpdatesAfterNotice: timing.answerUpdatesAfterNotice,
    longestAnswerChars: timing.longestAnswerChars,
    toolsObserved: view.toolTurns.has(turnId),
  });
}

async function sendMessage(
  plan: Plan,
  receipt: Receipt,
  requestId: string,
  runningTurn: string | undefined,
): Promise<void> {
  const to = { machineId: plan.machineId, sessionId: plan.recipient.sessionId };
  const body = `Staging notice verification ${requestId}`;
  let output: unknown;
  if (plan.senderHandoff === undefined) {
    output = parseCommand(
      await cli(plan, [
        "message",
        "send",
        "--to",
        JSON.stringify(to),
        "--request-id",
        requestId,
        "--body",
        body,
        "--json",
      ]),
      "send",
    );
  } else {
    const armedAt = Date.now();
    const sendBefore = new Date(armedAt + 60_000).toISOString();
    const relayed = await awaitRemoteSend({
      handoff: plan.senderHandoff,
      sender: plan.sender,
      requestId,
      receipt,
      intent: { requestId, to, body, runningTurn, source: plan.source, sendBefore },
    });
    if (relayed.exit !== 0 && relayed.exit !== null)
      throw new Error("remote sender reported a failed send; do not replay");
    if (Date.parse(relayed.observedAt) < armedAt || relayed.observedAt > sendBefore)
      throw new Error("remote send receipt is outside the armed window; do not replay");
    await receipt.record("remote-send-readback", {
      requestId,
      observedAt: relayed.observedAt,
      exit: relayed.exit,
    });
    output = relayed.output;
  }
  const send = sendSchema.parse(output);
  if (send.value.requestId !== requestId) throw new Error("send returned another request ID");
  await receipt.record("send", send);
}

async function requireIdleObservation(plan: Plan, stream: RecipientStream): Promise<void> {
  for (let attempt = 0; !stream.view.ready && attempt < 80; attempt++) {
    stream.assertContinuous();
    await setTimeout(250);
  }
  stream.assertContinuous();
  if (
    !stream.view.ready ||
    (await inspect(plan)).state !== "idle" ||
    stream.view.starts.length !== 0
  )
    throw new Error("recipient did not provide an idle, continuous pre-send observation");
}

async function holdPermissionRejected(
  plan: Plan,
  operationId: string | undefined,
  settled: CommandResult | undefined,
): Promise<boolean> {
  if (operationId === undefined || settled?.exit !== 4) return false;
  const waited = await cli(plan, [
    "conversation",
    "operation",
    "wait",
    "--operation-id",
    operationId,
    "--timeout-seconds",
    "2",
    "--json",
  ]).catch(() => undefined);
  if (waited === undefined) return false;
  try {
    const result = z
      .object({
        kind: z.literal("error"),
        error: z.object({ kind: z.literal("permissionRejected") }),
      })
      .safeParse(JSON.parse(waited.stdout));
    return result.success;
  } catch {
    return false;
  }
}

interface DeliveryCheck {
  readonly plan: Plan;
  readonly receipt: Receipt;
  readonly view: StagingMessageEvents;
  readonly requestId: string;
  readonly runningTurn: string | undefined;
  readonly startCount: number;
  readonly assertStream: () => void;
}

// After the notice, allow the next minute mark, then receipt publication.
async function checkDelivery(check: DeliveryCheck): Promise<void> {
  const { plan, receipt, view, requestId, runningTurn, startCount, assertStream } = check;
  let expires = performance.now() + 275_000;
  const seenWindow = plan.senderHandoff === undefined ? 125_000 : 600_000;
  let seen: string | undefined;
  let messageId: string | undefined;
  let arrivedAt: number | undefined;
  let firstNoticeAt: number | undefined;
  let previousStatus: string | undefined;
  for (let elapsed = performance.now(); ; elapsed = performance.now()) {
    if (view.notices.length > 0 && firstNoticeAt === undefined) {
      firstNoticeAt = elapsed;
      expires = Math.max(expires, firstNoticeAt + seenWindow);
    }
    if (elapsed >= expires) break;
    assertStream();
    const result = await senderStatus(plan, requestId);
    if (result === undefined) {
      await setTimeout(2500);
      continue;
    }
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
    const observationId = status.value?.messageId;
    if (observationId !== undefined) {
      if (messageId !== undefined && messageId !== observationId)
        throw new Error("sender reported inconsistent Central message identities");
      messageId = observationId;
    }
    if (
      status.kind !== "returned" ||
      ["refused", "notSent", "notConfirmed", "notFound"].includes(status.value?.kind ?? "") ||
      delivery?.kind === "refused"
    )
      throw new Error("sender reported refusal or broken custody");
    if (delivery?.kind === "seen") seen = status.value?.messageId;
    if (view.notices.length > 1)
      throw new ObservationFailure("recipient received multiple notices");
    const notice = view.notices[0];
    if (runningTurn !== undefined) {
      if (
        notice === undefined &&
        view.activeTurn === runningTurn &&
        view.toolTurns.has(runningTurn)
      )
        throw new Error("the held turn used a tool");
      if (delivery?.kind === "delivered" && delivery.arrivedAt !== undefined)
        arrivedAt ??= Date.parse(delivery.arrivedAt);
      if (view.ends.includes(runningTurn) && notice?.turnId !== runningTurn) {
        const endObservedAt = view.endObservedAt.get(runningTurn);
        if (
          arrivedAt !== undefined &&
          endObservedAt !== undefined &&
          endObservedAt - arrivedAt >= 65_000
        )
          throw new ObservationFailure("accepted message did not enter the already-running turn");
        if (arrivedAt !== undefined || seen !== undefined)
          throw new Error("hold ended before a full notice window after recipient Mac custody");
      } else if (
        view.starts.length !== startCount ||
        (notice !== undefined && notice.turnId !== runningTurn)
      ) {
        throw new ObservationFailure("notice was placed outside the active test turn");
      }
    } else if (
      view.starts.length > 1 ||
      (notice !== undefined && notice.turnId !== view.starts[0])
    ) {
      throw new ObservationFailure("idle recipient did not receive the notice in one new turn");
    }
    const noticeObserved =
      notice?.turnId != null &&
      (view.ends.includes(notice.turnId) ||
        (firstNoticeAt !== undefined && performance.now() - firstNoticeAt >= 65_000));
    if (seen !== undefined && noticeObserved) {
      await receipt.record("pass", {
        requestId,
        messageId: seen,
        notice,
        starts: view.starts.length,
      });
      return;
    }
    await setTimeout(2500);
  }
  reportExpiredObservation(seen !== undefined, view.notices.length);
}

function reportExpiredObservation(seen: boolean, noticeCount: number): never {
  if (seen && noticeCount === 0) throw new ObservationFailure("Seen without notice");
  if (seen) throw new Error("observation ended during the notice window");
  throw new Error("the observation bound ended without a sender Seen receipt");
}

async function senderStatus(
  plan: Plan,
  requestId: string,
): Promise<{ exit: number | null; output: unknown; observedAt?: string } | undefined> {
  if (plan.senderHandoff !== undefined)
    return await readRemoteReceipt({
      handoff: plan.senderHandoff,
      sender: plan.sender,
      requestId,
      operation: "status",
    });
  const result = await cli(plan, ["message", "status", "--request-id", requestId, "--json"]);
  return { exit: result.exit, output: parseCommand(result, "sender status") };
}

export async function runStagingMessage(file: string): Promise<void> {
  const plan = await readPlan(file);
  await privateDirectory(await realpath(join(plan.evidence, "..")));
  await mkdir(plan.evidence, { mode: 0o700 });
  await privateDirectory(plan.evidence);
  const receipt = new Receipt(plan.evidence);
  await receipt.save("target.json", plan);
  try {
    await prepare(plan, receipt);
    await observe(plan, receipt);
    await receipt.save("outcome.json", { kind: "passed", journey: plan.journey });
    const observed = plan.timing === "control" ? "no-notice hold" : `${plan.journey} notice`;
    process.stdout.write(`Staging ${observed} observed. Receipt: ${plan.evidence}\n`);
  } catch (error: unknown) {
    const failed = error instanceof ObservationFailure;
    const reason =
      error instanceof z.ZodError
        ? "product or declaration shape changed"
        : error instanceof Error
          ? error.message
          : "unknown setup failure";
    await receipt.save("outcome.json", { kind: failed ? "failed" : "blocked", reason });
    process.stderr.write(`${reason}. Receipt: ${plan.evidence}\n`);
    process.exitCode = failed ? 1 : 2;
  }
}

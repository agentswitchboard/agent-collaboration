import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { z } from "zod";
import type {
  Raise_Serialize,
  StopRequest,
} from "../../crates/approval-request-protocol/contracts/approval-requests.ts";
import type { WorkerdStorage } from "./workerd-storage.ts";

const replySchema = z.object({
  reply: z.unknown(),
  error: z.string().nullable(),
  writes: z.number().int().nonnegative(),
  rowsChanged: z.number().int().nonnegative(),
  committed: z.boolean(),
});
export const appliedSchema = z.object({
  result: z.unknown(),
  offers: z.array(z.object({ requestId: z.string() }).passthrough()),
});
export const dueSchema = z.object({
  rows: z.array(
    z.object({ subject: z.string(), occurrenceMs: z.number(), effectiveDueMs: z.number() }),
  ),
  rowsRead: z.array(z.number().int().nonnegative()),
});

type WitnessReply = z.infer<typeof replySchema>;

export const hourMs = 3_600_000;
export const uuid = (suffix: number): string =>
  `019ac19f-aa00-7000-8000-${suffix.toString(16).padStart(12, "0")}`;
export const holder = {
  organization: uuid(5101),
  machine: uuid(5102),
  enrollment: uuid(5103),
  runId: uuid(5104),
};
const sameEnrollment = { machine: holder.machine, enrollment: holder.enrollment };
export const requestingSession = { ...sameEnrollment, sessionId: uuid(5105) };
export const parentSession = { ...sameEnrollment, sessionId: uuid(5106) };
export const unrelatedSession = { ...sameEnrollment, sessionId: uuid(5107) };
export const personId = uuid(5201);

export function raiseRequest(
  ordinal: number,
  title: string,
  sessionId = requestingSession.sessionId,
): Extract<Raise_Serialize, { kind: "providerApproval" }> {
  return {
    kind: "providerApproval",
    requestId: uuid(ordinal),
    holder: { routerRun: holder.runId, sessionId },
    offer: {
      title,
      description: null,
      subject: null,
      options: [
        { id: 1, label: "Allow", effect: "allow", lasts: { kind: "thisRequest" } },
        { id: 2, label: "Decline", effect: "decline", lasts: { kind: "thisRequest" } },
      ],
    },
  };
}

export function stopRequest(request: Raise_Serialize): StopRequest {
  assert.equal(request.kind, "providerApproval");
  // The fixture uses ASCII keys and ordinary integers. Sort each object's keys
  // by UTF-16 order in independently supplied wire bytes before calculating its digest.
  const canonical = JSON.stringify(request, (_key: string, value: unknown): unknown => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return value;
    return Object.fromEntries(
      Object.entries(value).sort(([left], [right]) => (left < right ? -1 : Number(left > right))),
    );
  });
  return {
    kind: "provider",
    requestId: request.requestId,
    routerRun: request.holder.routerRun,
    contentsDigest: createHash("sha256").update(canonical).digest("base64url"),
    cause: "askReplaced",
  };
}

export function commandPath(database: string): string {
  return `/test-support/storage/${database}/approval-store`;
}

interface InvokeApprovalWitness {
  readonly central: WorkerdStorage;
  readonly database: string;
  readonly atMs?: number;
  readonly failAfterWrite?: number;
  readonly command: Record<string, unknown>;
}

export async function invokeApprovalWitness(props: InvokeApprovalWitness): Promise<WitnessReply> {
  return replySchema.parse(
    await props.central.dispatchJson(commandPath(props.database), {
      atMs: props.atMs ?? 0,
      command: props.command,
      failAfterWrite: props.failAfterWrite,
    }),
  );
}

export function committedResult(reply: WitnessReply): unknown {
  assert.equal(reply.error, null);
  assert.equal(reply.committed, true);
  return reply.reply;
}

export function typedResult(reply: WitnessReply): unknown {
  return appliedSchema.parse(committedResult(reply)).result;
}

export async function initializeApprovals(
  central: WorkerdStorage,
  database: string,
  delegated = false,
): Promise<void> {
  committedResult(
    await invokeApprovalWitness({
      central,
      database,
      command: { kind: "storeEnrollment", holder, live: true },
    }),
  );
  const enrolled = await invokeApprovalWitness({
    central,
    database,
    command: { kind: "register", holder },
  });
  assert.deepEqual(typedResult(enrolled), { kind: "registered" });
  for (const session of [parentSession, unrelatedSession, requestingSession]) {
    committedResult(
      await invokeApprovalWitness({
        central,
        database,
        command: {
          kind: "storeSession",
          organization: holder.organization,
          session,
          live: true,
          parent: delegated && session === requestingSession ? parentSession : null,
        },
      }),
    );
  }
}

export async function snapshot(central: WorkerdStorage, database: string): Promise<string[]> {
  const inspected = await invokeApprovalWitness({
    central,
    database,
    command: { kind: "inspect" },
  });
  return z.array(z.string()).parse(committedResult(inspected));
}

export function personDecision(
  requestId: string,
  operationId: string,
  optionId: number,
): Record<string, unknown> {
  return {
    kind: "decide",
    organization: holder.organization,
    actor: { kind: "person", personId, freshFactor: true },
    requestId,
    operationId,
    value: { kind: "approval", optionId },
  };
}

export async function assertRollbackAtEachWrite(
  props: InvokeApprovalWitness & { readonly writes: number },
): Promise<WitnessReply> {
  const before = await snapshot(props.central, props.database);
  for (let write = 1; write <= props.writes; write += 1) {
    const failed = await invokeApprovalWitness({ ...props, failAfterWrite: write });
    assert.equal(failed.committed, false);
    assert.match(failed.error ?? "", /Store\(Unavailable\)/);
    assert.equal(failed.writes, write);
    assert.deepEqual(await snapshot(props.central, props.database), before);
  }
  const result = await invokeApprovalWitness(props);
  committedResult(result);
  assert.equal(result.writes, props.writes);
  return result;
}

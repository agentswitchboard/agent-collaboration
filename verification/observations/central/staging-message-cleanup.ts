import { randomUUIDv7 } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { z } from "zod";
import { sessionSchema } from "../../declaration.ts";
import { type Receipt } from "../../receipt.ts";
import type { StagingMessageEvents } from "./staging-message-events.ts";
import { cli, type Plan } from "./staging-message-plan.ts";

const operationSchema = z.object({
  result: z.object({
    record: z.object({
      operationId: z.uuid(),
      target: sessionSchema,
      stage: z.string(),
      reconciliation: z.string(),
    }),
  }),
});

async function holdOperation(
  plan: Plan,
  operationId: string,
): Promise<z.infer<typeof operationSchema>["result"]["record"] | undefined> {
  try {
    const read = await cli(
      plan,
      ["conversation", "operation", "show", "--operation-id", operationId, "--json"],
      5,
    );
    if (read.exit !== 0) return undefined;
    const { record } = operationSchema.parse(JSON.parse(read.stdout)).result;
    if (
      record.operationId !== operationId ||
      record.target.sessionId !== plan.recipient.sessionId ||
      record.target.endpoint.serviceId !== plan.recipient.endpoint.serviceId ||
      record.target.endpoint.endpointId !== plan.recipient.endpoint.endpointId
    )
      return undefined;
    return record;
  } catch {
    return undefined;
  }
}

export async function cleanupManagedHold(
  plan: Plan,
  receipt: Receipt,
  operationId: string,
  view: StagingMessageEvents,
  runningTurn: string | undefined,
): Promise<void> {
  const settled = async (): Promise<boolean> => {
    const record = await holdOperation(plan, operationId);
    return (
      record?.stage === "terminal" &&
      record.reconciliation === "confirmed" &&
      (runningTurn === undefined || view.ends.includes(runningTurn))
    );
  };
  if (await settled()) {
    await receipt.record("hold-cleanup", { operationId, kind: "alreadySettled" });
    return;
  }
  const cancelId = randomUUIDv7();
  await receipt.record("hold-cancel-intent", { operationId, cancelId });
  const cancel = await cli(
    plan,
    [
      "conversation",
      "cancel",
      "--target",
      JSON.stringify(plan.recipient),
      "--target-operation-id",
      operationId,
      "--operation-id",
      cancelId,
      "--json",
    ],
    15,
  ).catch(() => undefined);
  for (let attempt = 0; attempt < 5; attempt++) {
    if (await settled()) {
      await receipt.record("hold-cleanup", {
        operationId,
        cancelId,
        cancelExit: cancel?.exit ?? null,
        kind: "settled",
      });
      return;
    }
    await setTimeout(500);
  }
  await receipt.record("hold-cleanup", {
    operationId,
    cancelId,
    cancelExit: cancel?.exit ?? null,
    kind: "failed",
    owner: "allocator",
  });
  process.stderr.write("Managed hold settlement unconfirmed; cleanup belongs to the allocator.\n");
}

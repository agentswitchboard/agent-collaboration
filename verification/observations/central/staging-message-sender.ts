import { lstat, readFile } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { setTimeout } from "node:timers/promises";
import { z } from "zod";
import { sessionSchema, type Session } from "../../declaration.ts";
import { privateDirectory } from "../../machines/owned-process.ts";
import type { Receipt } from "../../receipt.ts";

export const senderHandoffSchema = z.strictObject({
  machineId: z.uuid(),
  directory: z.string().refine(isAbsolute),
});
export type SenderHandoff = z.infer<typeof senderHandoffSchema>;

const qualifiedSchema = z.strictObject({
  source: z.string().regex(/^[a-f0-9]{40}$/u),
  machineId: z.uuid(),
  sender: sessionSchema,
  debugOnly: z.literal(true),
  enrolledConnected: z.literal(true),
  routerSha256: z.string().regex(/^[a-f0-9]{64}$/u),
  cliSha256: z.string().regex(/^[a-f0-9]{64}$/u),
});
const relaySchema = z.strictObject({
  requestId: z.uuid(),
  machineId: z.uuid(),
  sender: sessionSchema,
  observedAt: z.iso.datetime(),
  exit: z.number().int(),
  output: z.unknown(),
});

function sameSession(left: Session, right: Session): boolean {
  return (
    left.sessionId === right.sessionId &&
    left.endpoint.serviceId === right.endpoint.serviceId &&
    left.endpoint.endpointId === right.endpoint.endpointId
  );
}

async function privateJson(file: string): Promise<unknown> {
  try {
    const stat = await lstat(file);
    if (!stat.isFile() || stat.uid !== process.getuid?.() || (stat.mode & 0o077) !== 0)
      throw new Error("sender handoff receipt must be a private regular file");
    const contents = await readFile(file, "utf8");
    try {
      return JSON.parse(contents);
    } catch {
      throw new Error("sender handoff receipt is not valid JSON");
    }
  } catch (error: unknown) {
    if (z.object({ code: z.literal("ENOENT") }).safeParse(error).success) return undefined;
    throw error;
  }
}

export async function qualifyRemoteSender(
  handoff: SenderHandoff,
  expected: { source: string; receiverMachine: string; sender: Session },
): Promise<z.infer<typeof qualifiedSchema>> {
  await privateDirectory(handoff.directory);
  const qualified = qualifiedSchema.parse(
    await privateJson(join(handoff.directory, "sender-qualified.json")),
  );
  if (
    qualified.source !== expected.source ||
    qualified.machineId !== handoff.machineId ||
    qualified.machineId === expected.receiverMachine ||
    !sameSession(qualified.sender, expected.sender)
  )
    throw new Error(
      "sender handoff does not identify the distinct qualified staging Mac and session",
    );
  return qualified;
}

export async function readRemoteReceipt(input: {
  handoff: SenderHandoff;
  sender: Session;
  requestId: string;
  operation: "send" | "status";
}): Promise<z.infer<typeof relaySchema> | undefined> {
  const { handoff, sender, requestId, operation } = input;
  const result = await privateJson(join(handoff.directory, `${requestId}-${operation}.json`));
  if (result === undefined) return undefined;
  const envelope = relaySchema.parse(result);
  if (
    envelope.requestId !== requestId ||
    envelope.machineId !== handoff.machineId ||
    !sameSession(envelope.sender, sender)
  )
    throw new Error("remote product receipt belongs to another request, machine, or session");
  return envelope;
}

export async function awaitRemoteSend(input: {
  handoff: SenderHandoff;
  sender: Session;
  requestId: string;
  intent: unknown;
  receipt: Receipt;
}): Promise<z.infer<typeof relaySchema>> {
  await input.receipt.save("sender-intent.json", input.intent);
  process.stdout.write(
    `Receiver armed. Give the declared sender operator the private intent at ${input.receipt.directory}/sender-intent.json\n`,
  );
  for (let poll = 0; poll < 120; poll++) {
    const result = await readRemoteReceipt({ ...input, operation: "send" });
    if (result !== undefined) return result;
    await setTimeout(500);
  }
  throw new Error(
    "no exact remote send receipt arrived within 60 seconds; do not replay the request",
  );
}

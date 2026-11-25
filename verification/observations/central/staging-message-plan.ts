import { lstat, readFile, realpath } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, relative } from "node:path";
import { pathToFileURL } from "node:url";
import { z } from "zod";
import { artifactSchema, fileSchema, sessionSchema } from "../../declaration.ts";
import { command, fingerprint, qualifyArtifact } from "../../machines/owned-process.ts";
import type { Receipt } from "../../receipt.ts";
import { qualifyRemoteSender, senderHandoffSchema } from "./staging-message-sender.ts";

const absolutePath = z.string().refine(isAbsolute);
const declarationSchema = z
  .strictObject({
    journey: z.enum(["idle", "running"]),
    timing: z.enum(["notice", "control"]).optional(),
    answerLines: z.number().int().min(4000).max(20000).default(8000),
    source: z.string().regex(/^[a-f0-9]{40}$/u),
    signInOrigin: z.url().refine((value): boolean => {
      const url = URL.parse(value);
      return url?.protocol === "https:" && url.href === `${url.origin}/`;
    }, "expected an HTTPS origin"),
    router: artifactSchema,
    collaboration: artifactSchema,
    root: absolutePath,
    port: z.number().int().min(1024).max(65535),
    machineId: z.uuid(),
    hostPid: z.number().int().positive(),
    claudeAdapter: fileSchema,
    claudeAdapterPid: z.number().int().positive(),
    sender: sessionSchema,
    senderHandoff: senderHandoffSchema.optional(),
    recipient: sessionSchema,
    evidence: absolutePath,
  })
  .refine(
    (plan) =>
      plan.timing === undefined ||
      (plan.journey === "running" &&
        (plan.timing === "notice" || plan.senderHandoff === undefined)),
  );
export type Plan = z.infer<typeof declarationSchema>;
export type CommandResult = Awaited<ReturnType<typeof command>>;

const inspectSchema = z.object({
  result: z.object({
    record: z.object({
      target: sessionSchema,
      state: z.string(),
      capabilities: z.object({
        steer: z.boolean(),
        authStatus: z.object({ kind: z.string() }).optional(),
      }),
      settingsCatalog: z.object({
        configOptions: z.array(
          z.object({
            id: z.string(),
            currentValue: z.object({ value: z.string() }).optional(),
          }),
        ),
      }),
    }),
  }),
});
const senderSchema = z.object({
  result: z.object({
    record: z.object({ session: sessionSchema, endpointRegistered: z.literal(true) }),
  }),
});
const quotaSchema = z.object({
  accounts: z.array(
    z.object({
      provider: z.string(),
      credential_store_status: z.string(),
      oauth_maintenance_state: z.string(),
    }),
  ),
});
function firstLine(value: string): string {
  return value.split("\n", 1)[0] ?? "";
}

export function serviceDirectory(plan: Plan): string {
  return join(plan.root, "agent-communication");
}

export function cli(
  plan: Plan,
  args: readonly string[],
  timeoutSeconds = 20,
): Promise<CommandResult> {
  return command(
    plan.collaboration.path,
    [...args, "--service-directory", serviceDirectory(plan)],
    {
      timeoutSeconds,
    },
  );
}

export function parseCommand(result: CommandResult, label: string): unknown {
  if (result.exit !== 0) throw new Error(`${label} failed with exit ${String(result.exit)}`);
  try {
    return JSON.parse(result.stdout);
  } catch {
    throw new Error(`${label} returned invalid JSON`);
  }
}

export async function readPlan(file: string): Promise<Plan> {
  if (!isAbsolute(file) || (await realpath(file)) !== file)
    throw new Error("pass a canonical absolute path to the private declaration");
  const info = await lstat(file);
  if (!info.isFile() || info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0)
    throw new Error("the declaration must be an owner-private file");
  const outside = relative(process.cwd(), file);
  if (!outside.startsWith("../") && !isAbsolute(outside))
    throw new Error("keep the private declaration outside the checkout");
  return declarationSchema.parse(JSON.parse(await readFile(file, "utf8")));
}

async function requireLocalClaudeRoute(plan: Plan): Promise<void> {
  const host = await command("/bin/ps", ["-ww", "-p", String(plan.hostPid), "-o", "command="]);
  if (
    host.exit !== 0 ||
    !host.stdout.startsWith(`${plan.router.path} host `) ||
    !host.stdout.includes(`--router-root ${plan.root} `) ||
    !host.stdout.includes("--require-debug-isolation") ||
    !host.stdout.includes(`--port ${plan.port} `)
  )
    throw new Error("declared Host PID is not the signed debug Host on this root and port");
  const relation = await command("/bin/ps", ["-p", String(plan.claudeAdapterPid), "-o", "ppid="]);
  if (relation.exit !== 0 || Number(relation.stdout.trim()) !== plan.hostPid)
    throw new Error("declared Claude adapter is not a child of the declared Host");
  // Only the expected non-secret route and script path are compared. Do not save ps environment output.
  const adapter = await command("/bin/ps", [
    "eww",
    "-p",
    String(plan.claudeAdapterPid),
    "-o",
    "command=",
  ]);
  const assigned = adapter.stdout.match(/(?:^|\s)ANTHROPIC_BASE_URL=([^\s]*)/gu) ?? [];
  if (
    adapter.exit !== 0 ||
    !adapter.stdout.includes(pathToFileURL(plan.claudeAdapter.path).href) ||
    assigned.length !== 1 ||
    assigned[0]?.trim() !== `ANTHROPIC_BASE_URL=http://127.0.0.1:${plan.port}/anthropic`
  )
    throw new Error("cannot confirm the managed Claude adapter's actual debug proxy route");
}

export async function prepare(plan: Plan, receipt: Receipt): Promise<void> {
  const production = join(homedir(), ".codex-router");
  if (
    (await realpath(plan.root)) !== plan.root ||
    plan.root === production ||
    plan.root.startsWith(`${production}/`) ||
    [8787, 8788, 19741, 19742, 19743, 43127].includes(plan.port) ||
    !plan.router.identifier.endsWith(".codex-router.debug") ||
    !plan.collaboration.identifier.endsWith(".agent-collaboration.debug") ||
    plan.router.team !== plan.collaboration.team
  )
    throw new Error("a non-production root, debug port and signed debug artifacts are required");
  await qualifyArtifact(plan.router);
  await qualifyArtifact(plan.collaboration);
  if ((await fingerprint(plan.claudeAdapter.path)) !== plan.claudeAdapter.sha256)
    throw new Error("Claude adapter fingerprint changed");
  await requireLocalClaudeRoute(plan);
  const environment = { ...process.env, CODEX_ROUTER_DEBUG_ROUTER_ROOT: plan.root };
  Reflect.deleteProperty(environment, "CODEX_ROUTER_USE_HOME_DEFAULT");
  const enrollment = await command(plan.router.path, ["enrollment", "status"], { environment });
  const enrolled =
    enrollment.exit === 0 &&
    firstLine(enrollment.stdout) === "enrolled, connected" &&
    enrollment.stdout.includes(`Machine: ${plan.machineId}\n`) &&
    enrollment.stdout.split("\n").includes(`Sign-in: ${new URL(plan.signInOrigin).href}`);
  await receipt.record("readiness", { enrolled, exit: enrollment.exit });
  if (!enrolled) throw new Error("declared debug Host is not enrolled and connected to staging");
  const quota = await command(
    plan.router.path,
    ["quota", "--router-root", plan.root, "--format", "json", "--no-refresh"],
    { environment },
  );
  const { accounts } = quotaSchema.parse(parseCommand(quota, "debug Claude credential readiness"));
  const readyClaude = accounts.some(
    (account) =>
      account.provider === "claude" &&
      account.credential_store_status === "ready" &&
      account.oauth_maintenance_state === "healthy",
  );
  await receipt.record("debug-claude-credentials", { ready: readyClaude, exit: quota.exit });
  if (!readyClaude) throw new Error("no already authenticated Claude account on the debug root");
  const identity = senderSchema.parse(
    parseCommand(await cli(plan, ["whoami", "--json"]), "sender identity"),
  );
  const operator = identity.result.record.session;
  if (
    operator.endpoint.serviceId !== plan.recipient.endpoint.serviceId ||
    plan.sender.sessionId === plan.recipient.sessionId ||
    plan.recipient.endpoint.endpointId !== "claude-local"
  )
    throw new Error("declared recipient is not distinct and local to the operator");
  if (plan.senderHandoff === undefined) {
    if (
      operator.sessionId !== plan.sender.sessionId ||
      operator.endpoint.endpointId !== plan.sender.endpoint.endpointId ||
      operator.endpoint.serviceId !== plan.sender.endpoint.serviceId
    )
      throw new Error("the same-Mac sender identity does not match the declaration");
  } else {
    const qualified = await qualifyRemoteSender(plan.senderHandoff, {
      source: plan.source,
      receiverMachine: plan.machineId,
      sender: plan.sender,
    });
    await receipt.save("separate-mac-sender.json", qualified);
  }
  const inspected = await inspect(plan);
  if (inspected.state !== "idle" || (plan.journey === "running" && !inspected.capabilities.steer))
    throw new Error(
      "recipient must be open and idle, with steering advertised for the running case",
    );
  await receipt.record("recipient-ready", {
    model: "haiku",
    steer: inspected.capabilities.steer,
    adapterAuth: inspected.capabilities.authStatus?.kind,
    proxy: `http://127.0.0.1:${plan.port}/anthropic`,
  });
}

export async function inspect(
  plan: Plan,
): Promise<z.infer<typeof inspectSchema>["result"]["record"]> {
  const result = await cli(plan, [
    "session",
    "inspect",
    "--to",
    JSON.stringify(plan.recipient),
    "--json",
  ]);
  const { record } = inspectSchema.parse(parseCommand(result, "recipient inspect")).result;
  const model = record.settingsCatalog.configOptions.find(({ id }) => id === "model")?.currentValue
    ?.value;
  if (
    record.target.sessionId !== plan.recipient.sessionId ||
    record.target.endpoint.serviceId !== plan.recipient.endpoint.serviceId ||
    record.target.endpoint.endpointId !== "claude-local" ||
    model !== "haiku"
  )
    throw new Error(
      "exact declared Claude recipient or effective Haiku setting cannot be confirmed",
    );
  return record;
}

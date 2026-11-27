import { readFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { decode, encode, objectSchema } from "../../../exact-json.ts";

const rpcSchema = z
  .looseObject({
    jsonrpc: z.literal("2.0"),
    id: z.union([z.string(), z.bigint(), z.null()]).optional(),
    method: z.string().optional(),
  })
  .refine((message) => message.method !== undefined || message.id !== undefined);
const entrySchema = z.strictObject({
  seq: z.bigint().nonnegative(),
  connection: z.string().min(1),
  direction: z.enum(["clientToServer", "serverToClient"]),
  message: rpcSchema,
});
type Entry = z.infer<typeof entrySchema>;
type JsonObject = Record<string, unknown>;

const fieldNames = [
  "Command",
  "Working folder",
  "Output",
  "Standard error",
  "Exit code",
  "File path",
  "Old and new text",
  "Line number",
] as const;
type FieldName = (typeof fieldNames)[number];

interface FieldPath {
  readonly field: FieldName;
  readonly path: string;
}

interface ToolFrame {
  readonly entry: Entry;
  readonly kind: "tool_call" | "tool_call_update" | "session/request_permission";
  readonly tool: JsonObject;
  readonly path: string;
  readonly session: unknown;
  readonly prompt: number | undefined;
}

interface Prompt {
  readonly session: unknown;
  readonly number: number;
}

function object(value: unknown): JsonObject | undefined {
  const result = objectSchema.safeParse(value);
  return result.success ? result.data : undefined;
}

function parseCapture(input: string): Entry[] {
  const lines = input.split("\n");
  if (lines.pop() !== "" || lines.length === 0)
    throw new Error("Capture is empty or ends without a complete line");
  const entries = lines.map((line, index) => {
    try {
      const entry = entrySchema.parse(decode(line));
      if (entry.seq !== BigInt(index)) throw new Error("Sequence mismatch");
      return entry;
    } catch {
      throw new Error(`Invalid or incomplete ACP capture at line ${index + 1}`);
    }
  });
  if (new Set(entries.map(({ connection }) => connection)).size !== 1)
    throw new Error("Expected one ACP connection per capture file");
  return entries;
}

function observeFrames(entries: readonly Entry[]): {
  readonly tools: readonly ToolFrame[];
  readonly messageIds: ReadonlyMap<string, ReadonlySet<number>>;
  readonly prompts: number;
  readonly unansweredPrompts: number;
} {
  const outstanding = new Map<unknown, Prompt>();
  const prompts = new Map<unknown, Prompt>();
  const messageIds = new Map<string, Set<number>>();
  let promptNumber = 0;
  const frames: ToolFrame[] = [];
  for (const entry of entries) {
    const { direction, message } = entry;
    const params = object(message.params);
    const session = params?.sessionId;
    if (
      direction === "clientToServer" &&
      message.method === "session/prompt" &&
      typeof session === "string"
    ) {
      const prompt = { session, number: ++promptNumber };
      outstanding.set(message.id, prompt);
      prompts.set(session, prompt);
    } else if (direction === "serverToClient" && message.method === undefined) {
      const completed = outstanding.get(message.id);
      if (completed !== undefined) {
        outstanding.delete(message.id);
        if (prompts.get(completed.session) === completed) prompts.delete(completed.session);
      }
    }
    const prompt = prompts.get(session)?.number;
    const update = object(params?.update);
    const kind = update?.sessionUpdate;
    const messageId = update?.messageId ?? params?.messageId;
    if (typeof messageId === "string") {
      const turns = messageIds.get(messageId) ?? new Set();
      if (prompt !== undefined) turns.add(prompt);
      messageIds.set(messageId, turns);
    }
    if (
      message.method === "session/update" &&
      update !== undefined &&
      (kind === "tool_call" || kind === "tool_call_update")
    ) {
      frames.push({ entry, kind, tool: update, path: "$.params.update", session, prompt });
    } else if (message.method === "session/request_permission") {
      const tool = object(params?.toolCall);
      if (tool !== undefined)
        frames.push({
          entry,
          kind: message.method,
          tool,
          path: "$.params.toolCall",
          session,
          prompt,
        });
    }
  }
  return { tools: frames, messageIds, prompts: promptNumber, unansweredPrompts: outstanding.size };
}

function keyPath(path: string, key: string): string {
  return /^[a-zA-Z_][a-zA-Z_0-9]*$/.test(key)
    ? `${path}.${key}`
    : `${path}[${JSON.stringify(key)}]`;
}

function inventory(value: unknown, path: string): string[] {
  if (Array.isArray(value)) {
    return [
      `${path} [${value.length}]`,
      ...value.flatMap((entry: unknown, index: number) => inventory(entry, `${path}[${index}]`)),
    ];
  }
  const record = object(value);
  if (record !== undefined)
    return [
      `${path} {${Object.keys(record)
        .map((key) => JSON.stringify(key))
        .join(", ")}}`,
      ...Object.entries(record).flatMap(([key, entry]) => inventory(entry, keyPath(path, key))),
    ];
  return [];
}

// Paths based on explicit field names or typed ACP content. Titles and prose never
// imply an exit code, a location, a separate stderr stream or a previous value.
function fieldPaths({ tool, path }: ToolFrame): FieldPath[] {
  const observed: FieldPath[] = [];
  function match(
    record: JsonObject,
    prefix: string,
    field: FieldName,
    keys: readonly string[],
  ): void {
    for (const key of keys) {
      if (Object.hasOwn(record, key)) observed.push({ field, path: keyPath(prefix, key) });
    }
  }
  function commandFields(record: JsonObject, prefix: string): void {
    match(record, prefix, "Command", ["command", "cmd", "commandLine", "command_line"]);
    match(record, prefix, "Working folder", [
      "cwd",
      "workdir",
      "workingDirectory",
      "working_directory",
    ]);
  }
  function fileFields(record: JsonObject, prefix: string): void {
    match(record, prefix, "File path", ["path", "filePath", "file_path", "notebook_path"]);
    match(record, prefix, "Old and new text", [
      "oldText",
      "newText",
      "old_string",
      "new_string",
      "old_text",
      "new_text",
    ]);
    match(record, prefix, "Line number", [
      "line",
      "lineNumber",
      "line_number",
      "startLine",
      "start_line",
    ]);
  }
  commandFields(tool, path);
  const input = object(tool.rawInput);
  if (input !== undefined) {
    commandFields(input, `${path}.rawInput`);
    fileFields(input, `${path}.rawInput`);
  }
  const output = object(tool.rawOutput);
  if (typeof tool.rawOutput === "string")
    observed.push({ field: "Output", path: `${path}.rawOutput` });
  for (const [record, prefix] of [
    [tool, path],
    [output, `${path}.rawOutput`],
  ] as const) {
    if (record === undefined) continue;
    match(record, prefix, "Output", ["output", "stdout"]);
    match(record, prefix, "Standard error", ["stderr", "stdErr", "standardError"]);
    match(record, prefix, "Exit code", ["exitCode", "exit_code", "return_code"]);
    fileFields(record, prefix);
  }
  if (Array.isArray(tool.rawOutput)) {
    tool.rawOutput.forEach((item: unknown, index: number) => {
      const block = object(item);
      if (block !== undefined && typeof block.text === "string")
        match(block, `${path}.rawOutput[${index}]`, "Output", ["text"]);
    });
  }
  const metadata = object(tool._meta);
  for (const key of ["terminal_output", "terminal_output_delta"] as const) {
    const terminal = object(metadata?.[key]);
    if (terminal !== undefined) match(terminal, `${path}._meta.${key}`, "Output", ["data"]);
  }
  const terminalExit = object(metadata?.terminal_exit);
  if (terminalExit !== undefined)
    match(terminalExit, `${path}._meta.terminal_exit`, "Exit code", ["exit_code"]);
  if (Array.isArray(tool.locations)) {
    tool.locations.forEach((location: unknown, index: number) => {
      const record = object(location);
      if (record !== undefined) fileFields(record, `${path}.locations[${index}]`);
    });
  }
  if (Array.isArray(tool.content)) {
    tool.content.forEach((item: unknown, index: number) => {
      const record = object(item);
      const prefix = `${path}.content[${index}]`;
      if (record?.type === "diff") fileFields(record, prefix);
      const block = object(record?.content);
      if (record?.type === "content" && block?.type === "text" && Object.hasOwn(block, "text"))
        observed.push({ field: "Output", path: `${prefix}.content.text` });
    });
  }
  return observed;
}

function inline(value: string): string {
  const fence = "`".repeat(
    Math.max(0, ...[...value.matchAll(/`+/g)].map(([run]) => run.length)) + 1,
  );
  return `${fence} ${value.replaceAll("|", "&#124;").replaceAll("\n", "\\n")} ${fence}`;
}

function idForm(value: unknown): string {
  if (typeof value === "bigint") return "integer";
  if (typeof value !== "string") return "absent or non-string";
  if (/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value)) return "UUID string";
  if (/^toolu_[a-z0-9_]+$/i.test(value)) return "toolu_ string";
  return `string (${Buffer.byteLength(value)} bytes)`;
}

interface CaptureSummary {
  readonly lines: readonly string[];
  readonly paths: ReadonlyMap<FieldName, ReadonlySet<string>>;
  readonly toolFramesSeen: boolean;
}

function summarize(label: string, entries: readonly Entry[]): CaptureSummary {
  const initialize = entries.find(
    ({ direction, message }) => direction === "clientToServer" && message.method === "initialize",
  );
  const initialized = entries.find(
    ({ direction, message, seq }) =>
      initialize !== undefined &&
      seq > initialize.seq &&
      direction === "serverToClient" &&
      message.method === undefined &&
      message.id === initialize.message.id,
  );
  const offered = object(initialize?.message.params)?.clientCapabilities;
  const agent = object(object(initialized?.message.result)?.agentInfo);
  const lines = [
    `## ${label}`,
    "",
    `Agent name and version: ${inline(encode([agent?.name ?? null, agent?.version ?? null]))}`,
    `Client capabilities at initialize: ${offered === undefined ? "unobserved" : inline(encode(offered))}`,
    `Capture: ${entries.length} frames, ${entries.filter(({ direction }) => direction === "clientToServer").length} client to provider.`,
  ];
  const paths = new Map<FieldName, Set<string>>();
  for (const field of fieldNames) paths.set(field, new Set());
  const sizes = { rawInput: 0, rawOutput: 0, content: 0 };
  const maximumFields = ["rawInput", "rawOutput", "content"] as const;
  const { tools: frames, messageIds, prompts, unansweredPrompts } = observeFrames(entries);
  const toolIds = new Map<unknown, Map<string, number>>();
  const turnsByTool = new Map<string, Set<number>>();
  const rpcIds = new Set<unknown>();
  for (const { message } of entries) {
    if (message.id !== undefined && message.id !== null) rpcIds.add(message.id);
  }
  const toolValues = new Set<string>();
  const idForms = new Set<string>();
  const previous = new Map<number, JsonObject>();
  let nextToolNumber = 1;
  for (const frame of frames) {
    const id = frame.tool.toolCallId;
    let ids = toolIds.get(frame.session);
    if (ids === undefined) {
      ids = new Map();
      toolIds.set(frame.session, ids);
    }
    let number: number | undefined;
    if (typeof id === "string") {
      toolValues.add(id);
      idForms.add(idForm(id));
      number = ids.get(id) ?? nextToolNumber++;
      ids.set(id, number);
      const turns = turnsByTool.get(id) ?? new Set();
      if (frame.prompt !== undefined) turns.add(frame.prompt);
      turnsByTool.set(id, turns);
    }
    const keys = Object.keys(frame.tool);
    const seen = number === undefined ? undefined : previous.get(number);
    if (number !== undefined && frame.kind !== "session/request_permission")
      previous.set(number, { ...seen, ...frame.tool });
    const repeated = keys.filter((key) => seen !== undefined && Object.hasOwn(seen, key));
    const found = fieldPaths(frame);
    for (const { field, path } of found) paths.get(field)?.add(path);
    for (const field of maximumFields) {
      if (Object.hasOwn(frame.tool, field))
        sizes[field] = Math.max(sizes[field], Buffer.byteLength(encode(frame.tool[field])));
    }
    lines.push(
      "",
      `### Frame ${frame.entry.seq} · ${frame.entry.direction} · ${frame.kind}`,
      "",
      `Prompt ${frame.prompt ?? "unattributed"}. Tool ${number ?? "unidentified"}. ID form ${idForm(id)}.`,
      `New tool keys: ${inline(encode(keys.filter((key) => !repeated.includes(key))))}`,
      `Repeated tool keys with unchanged values: ${inline(encode(repeated.filter((key) => isDeepStrictEqual(seen?.[key], frame.tool[key]))))}`,
      `Repeated tool keys with changed values: ${inline(encode(repeated.filter((key) => !isDeepStrictEqual(seen?.[key], frame.tool[key]))))}`,
      `Fields: ${found.map(({ field, path }) => `${field} ${inline(path)}`).join(", ") || "none matched"}.`,
      "",
      ...inventory(frame.entry.message, "$").map((line) => `- ${inline(line)}`),
    );
  }
  lines.push(
    "",
    `Prompt requests: ${prompts}. Without a matching reply: ${unansweredPrompts}.`,
    `Tool frames: ${frames.length}. Largest compact JSON UTF-8 values: rawInput ${sizes.rawInput}, rawOutput ${sizes.rawOutput}, content ${sizes.content} bytes (0 means absent).`,
    `Tool ID forms: ${[...idForms].join(", ") || "unobserved"}. Tool IDs repeated across prompts: ${[...turnsByTool.values()].filter((turns) => turns.size > 1).length}.`,
    `Message ID forms: ${[...new Set([...messageIds.keys()].map(idForm))].join(", ") || "unobserved"}. Message IDs repeated across prompts: ${[...messageIds.values()].filter((turns) => turns.size > 1).length}.`,
    `JSON-RPC envelope ID forms: ${[...new Set([...rpcIds].map(idForm))].join(", ") || "unobserved"}.`,
    `Message/tool ID overlaps: ${[...messageIds.keys()].filter((id) => toolValues.has(id)).length}. Envelope/tool ID overlaps: ${[...toolValues].filter((id) => rpcIds.has(id)).length}.`,
  );
  return { lines, paths, toolFramesSeen: frames.length > 0 };
}

function compare(
  left: ReadonlySet<string> | undefined,
  right: ReadonlySet<string> | undefined,
): string {
  if (left === undefined || right === undefined) return "Undetermined";
  if (left.size === 0 && right.size === 0) return "Both lack recognized paths";
  return left.size === right.size && [...left].every((path) => right.has(path))
    ? "Same paths"
    : "Different paths";
}

function showPaths(paths: ReadonlySet<string> | undefined): string {
  if (paths === undefined) return "Unobserved";
  return [...paths].map(inline).join(", ") || "Absent from recognized fields";
}

interface Captures {
  readonly claude?: string;
  readonly cursor?: string;
}

export function renderToolFrameReport(captures: Captures): string {
  const claude =
    captures.claude === undefined ? undefined : summarize("Claude", parseCapture(captures.claude));
  const cursor =
    captures.cursor === undefined ? undefined : summarize("Cursor", parseCapture(captures.cursor));
  return [
    "# ACP tool frame report",
    "",
    "The inventory lists every object key and array index in each tool frame. The table compares recognized wire paths. Review the inventory for provider-specific fields. No value or effect is inferred from titles or prose.",
    "",
    "| Field | Claude JSON paths | Cursor JSON paths | Comparison |",
    "|---|---|---|---|",
    ...fieldNames.map((field) => {
      const left = claude?.toolFramesSeen ? claude.paths.get(field) : undefined;
      const right = cursor?.toolFramesSeen ? cursor.paths.get(field) : undefined;
      return `| ${field} | ${showPaths(left)} | ${showPaths(right)} | ${compare(left, right)} |`;
    }),
    "",
    ...(claude?.lines ?? ["## Claude", "", "No capture supplied."]),
    "",
    ...(cursor?.lines ?? ["## Cursor", "", "No capture supplied."]),
    "",
  ].join("\n");
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2);
    const captures: Partial<Record<"--claude" | "--cursor", string>> = {};
    if (args.length === 0 || args.length % 2 !== 0) throw new Error("Supply one or two captures");
    for (let index = 0; index < args.length; index += 2) {
      const provider = args[index];
      const path = args[index + 1];
      if (
        (provider !== "--claude" && provider !== "--cursor") ||
        path === undefined ||
        provider in captures
      )
        throw new Error("Supply at most one capture per provider");
      try {
        captures[provider] = readFileSync(path, "utf8");
      } catch {
        throw new Error(`Cannot read ${provider} capture`);
      }
    }
    const claude = captures["--claude"];
    const cursor = captures["--cursor"];
    process.stdout.write(
      renderToolFrameReport({
        ...(claude === undefined ? {} : { claude }),
        ...(cursor === undefined ? {} : { cursor }),
      }),
    );
  } catch (error) {
    process.stderr.write(
      `Usage: node acp-tool-frame-report.ts [--claude CAPTURE] [--cursor CAPTURE]\n${error instanceof Error ? error.message : "Invalid capture"}\n`,
    );
    process.exitCode = 2;
  }
}

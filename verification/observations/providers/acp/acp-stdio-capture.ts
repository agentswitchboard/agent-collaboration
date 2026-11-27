import { spawn } from "node:child_process";
import { once } from "node:events";
import { closeSync, lstatSync, openSync, realpathSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { decode, objectSchema } from "../../../exact-json.ts";

type Direction = "clientToServer" | "serverToClient";

/** Record complete lines as they arrive while relaying the original bytes. */
function captureStream(
  direction: Direction,
  record: (direction: Direction, line: Buffer, terminated: boolean) => void,
): Transform {
  let fragments: Buffer[] = [];
  let pendingBytes = 0;
  function append(fragment: Buffer): void {
    pendingBytes += fragment.length;
    // Match the product's ACP line ceiling without buffering an unlimited stream.
    if (pendingBytes > 64 * 1024 * 1024) throw new Error("ACP line exceeds capture limit");
    if (fragment.length > 0) fragments.push(fragment);
  }
  function finish(): void {
    if (pendingBytes > 0) record(direction, Buffer.concat(fragments, pendingBytes), false);
    fragments = [];
    pendingBytes = 0;
  }
  return new Transform({
    transform(chunk: Buffer, _encoding, callback): void {
      try {
        let start = 0;
        for (let newline = chunk.indexOf(10); newline >= 0; newline = chunk.indexOf(10, start)) {
          append(chunk.subarray(start, newline));
          record(direction, Buffer.concat(fragments, pendingBytes), true);
          fragments = [];
          pendingBytes = 0;
          start = newline + 1;
        }
        append(chunk.subarray(start));
        callback(null, chunk);
      } catch {
        callback(new Error("ACP recording failed"));
      }
    },
    flush(callback): void {
      try {
        finish();
        callback();
      } catch {
        callback(new Error("ACP recording failed"));
      }
    },
    destroy(error, callback): void {
      try {
        finish();
        callback(error);
      } catch {
        callback(new Error("ACP recording failed"));
      }
    },
  });
}

function frameText(line: Buffer): string | undefined {
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(line);
    objectSchema.parse(decode(text));
    return text;
  } catch {
    return undefined;
  }
}

async function relay(
  directory: string,
  executable: string,
  providerArguments: readonly string[],
): Promise<number> {
  const status = lstatSync(directory);
  if (
    !isAbsolute(directory) ||
    realpathSync(directory) !== directory ||
    !status.isDirectory() ||
    status.uid !== process.getuid?.() ||
    (status.mode & 0o777) !== 0o700
  )
    throw new Error("ACP capture requires a canonical owner-private directory");

  const output = openSync(join(directory, `acp-${process.pid}.jsonl`), "wx", 0o600);
  let seq = 0;
  function record(direction: Direction, line: Buffer, terminated: boolean): void {
    if (terminated && /^[\t\r ]*$/.test(line.toString("latin1"))) return;
    const envelope = JSON.stringify({ seq, connection: `agent-${process.pid}`, direction });
    const message = terminated ? frameText(line) : undefined;
    // Embed the wire text directly to keep number spelling and every provider key.
    const entry =
      message === undefined
        ? `"unparsed":${JSON.stringify(line.toString("base64"))},"encoding":"base64","unterminated":${!terminated}`
        : `"message":${message}`;
    writeFileSync(output, `${envelope.slice(0, -1)},${entry}}\n`);
    seq++;
  }

  // Provider stderr may contain account diagnostics. Only the ACP pipes are captured.
  const agent = spawn(executable, providerArguments, { stdio: ["pipe", "pipe", "ignore"] });
  const forwardTerm = (): void => {
    agent.kill("SIGTERM");
  };
  const forwardInterrupt = (): void => {
    agent.kill("SIGINT");
  };
  process.on("SIGTERM", forwardTerm);
  process.on("SIGINT", forwardInterrupt);
  let providerExited = false;
  const closed = once(agent, "close").then(([exit]: unknown[]): number => {
    providerExited = true;
    process.stdin.destroy();
    return typeof exit === "number" ? exit : 1;
  });
  const input = pipeline(process.stdin, captureStream("clientToServer", record), agent.stdin).catch(
    (error: unknown): void => {
      if (
        !(
          providerExited &&
          error instanceof Error &&
          "code" in error &&
          error.code === "ERR_STREAM_PREMATURE_CLOSE"
        )
      )
        throw error;
    },
  );
  const outputPipe = pipeline(
    agent.stdout,
    captureStream("serverToClient", record),
    process.stdout,
  );
  try {
    const [exit] = await Promise.all([closed, input, outputPipe]);
    return exit;
  } catch {
    process.stdin.destroy();
    agent.kill("SIGKILL");
    await Promise.allSettled([closed, input, outputPipe]);
    throw new Error("ACP capture relay failed");
  } finally {
    process.stdin.destroy();
    process.off("SIGTERM", forwardTerm);
    process.off("SIGINT", forwardInterrupt);
    closeSync(output);
  }
}

try {
  const [directory, separator, executable, ...args] = process.argv.slice(2);
  if (directory === undefined || separator !== "--" || executable === undefined) {
    process.stderr.write(
      "Usage: node acp-stdio-capture.ts PRIVATE_DIRECTORY -- EXECUTABLE [ARG ...]\n",
    );
    process.exitCode = 2;
  } else {
    process.exitCode = await relay(directory, executable, args);
  }
} catch {
  process.stderr.write(
    "ACP capture failed. Check the private directory and the provider launch.\n",
  );
  process.exitCode = 1;
}

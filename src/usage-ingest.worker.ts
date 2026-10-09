import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import { buildUpdateFromEvents, type LiveEventDelta } from "./live.ts";
import { dailyTokenWindow } from "./rate.ts";
import { ingestOnce } from "./scanner.ts";
import { TokenStore } from "./store.ts";
import {
  issueTokenSnapshotHeartbeatTarget,
  scheduleTokenSnapshotHeartbeat,
} from "./token-snapshot-heartbeat.ts";
import {
  USAGE_INGEST_RECENT_MAX_EVENTS,
  USAGE_INGEST_REQUEST_MAX_BYTES,
  USAGE_INGEST_RESPONSE_MAX_BYTES,
  type UsageIngestWorkerRequest,
  type UsageIngestWorkerResponse,
} from "./usage-ingest-worker-protocol.ts";
import { workerInvocation } from "./worker-launch.ts";

const SNAPSHOT_HEARTBEAT_CHILD_ARG = "--snapshot-heartbeat-child";
const SNAPSHOT_HEARTBEAT_CHILD_READY = "ready\n";
const SNAPSHOT_HEARTBEAT_CHILD_GRACEFUL_STOP_TIMEOUT_MS = 100;
const SNAPSHOT_HEARTBEAT_CHILD_TERM_STOP_TIMEOUT_MS = 75;

type SnapshotHeartbeatTarget = NonNullable<ReturnType<typeof issueTokenSnapshotHeartbeatTarget>>;

interface IndependentSnapshotHeartbeat {
  stop(): Promise<void>;
}

async function waitForChildExit(
  child: ReturnType<typeof Bun.spawn>,
  timeoutMs: number,
): Promise<number | null> {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const deadline = new Promise<null>((resolve) => {
    timer = setTimeout(() => resolve(null), timeoutMs);
    timer.unref?.();
  });
  try {
    return await Promise.race([child.exited, deadline]);
  } finally {
    if (timer !== null) clearTimeout(timer);
  }
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function readBoundedInput(): Promise<Uint8Array> {
  const reader = Bun.stdin.stream().getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > USAGE_INGEST_REQUEST_MAX_BYTES) {
        throw new Error(`ingest request exceeds ${USAGE_INGEST_REQUEST_MAX_BYTES} bytes`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function isHeartbeatTarget(value: unknown): boolean {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const target = value as Record<string, unknown>;
  return (
    Number.isSafeInteger(target.revision) &&
    (target.revision as number) >= 0 &&
    typeof target.windowKey === "string" &&
    target.windowKey.length > 0 &&
    target.windowKey.length <= 64 &&
    Number.isSafeInteger(target.updatedAt) &&
    (target.updatedAt as number) >= 0
  );
}

function parseRequest(bytes: Uint8Array): UsageIngestWorkerRequest {
  const input = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!input.endsWith("\n") || input.indexOf("\n") !== input.length - 1) {
    throw new Error("ingest worker requires exactly one NDJSON request");
  }
  const parsed: unknown = JSON.parse(input.slice(0, -1));
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("type" in parsed) ||
    parsed.type !== "ingest" ||
    !("id" in parsed) ||
    !Number.isSafeInteger(parsed.id) ||
    !("databasePath" in parsed) ||
    typeof parsed.databasePath !== "string" ||
    !("claudeProjectsDir" in parsed) ||
    typeof parsed.claudeProjectsDir !== "string" ||
    !("codexSessionsDir" in parsed) ||
    typeof parsed.codexSessionsDir !== "string" ||
    ("grokLogsDir" in parsed && typeof parsed.grokLogsDir !== "string") ||
    ("grokSessionsDir" in parsed && typeof parsed.grokSessionsDir !== "string") ||
    ("heartbeatTarget" in parsed && !isHeartbeatTarget(parsed.heartbeatTarget))
  ) {
    throw new Error("ingest worker received an invalid request");
  }
  const request = parsed as UsageIngestWorkerRequest;
  for (const candidate of [
    request.databasePath,
    request.claudeProjectsDir,
    request.codexSessionsDir,
    request.grokLogsDir,
    request.grokSessionsDir,
  ]) {
    if (candidate === undefined) continue;
    if (!isAbsolute(candidate)) throw new Error("ingest worker received a non-absolute path");
  }
  return request;
}

function encodeResponse(response: UsageIngestWorkerResponse): string {
  const encoded = `${JSON.stringify(response)}\n`;
  if (new TextEncoder().encode(encoded).byteLength <= USAGE_INGEST_RESPONSE_MAX_BYTES) {
    return encoded;
  }
  return `${JSON.stringify({
    type: "error",
    id: response.id,
    error: `ingest response exceeds ${USAGE_INGEST_RESPONSE_MAX_BYTES} bytes`,
  } satisfies UsageIngestWorkerResponse)}\n`;
}

function publishCurrentTokenSnapshot(
  store: TokenStore,
  revision: number,
  updatedAt: number,
): boolean {
  const currentWindow = dailyTokenWindow(updatedAt);
  const cachedSnapshot = store.readCachedTokenSnapshot();
  if (
    !cachedSnapshot ||
    cachedSnapshot.revision !== revision ||
    cachedSnapshot.window.windowKey !== currentWindow.windowKey
  ) {
    store.refreshAuthoritativeTokenSnapshot(updatedAt, revision);
    return true;
  }
  store.touchCachedTokenSnapshot(
    updatedAt,
    revision,
    currentWindow.windowKey,
    cachedSnapshot.updatedAt,
  );
  return false;
}

function parseSnapshotHeartbeatChildArgs(args: string[]): {
  databasePath: string;
  target: SnapshotHeartbeatTarget;
  intervalMs: number;
} {
  if (args.length !== 5 || !isAbsolute(args[0]!)) {
    throw new Error("snapshot heartbeat child received invalid arguments");
  }
  const revision = Number(args[1]);
  const windowKey = args[2]!;
  const updatedAt = Number(args[3]);
  const intervalMs = Number(args[4]);
  if (
    !Number.isSafeInteger(revision) ||
    revision < 0 ||
    windowKey.length === 0 ||
    windowKey.length > 64 ||
    !Number.isSafeInteger(updatedAt) ||
    updatedAt < 0 ||
    !Number.isSafeInteger(intervalMs) ||
    intervalMs < 5 ||
    intervalMs > 5_000
  ) {
    throw new Error("snapshot heartbeat child received invalid arguments");
  }
  return {
    databasePath: args[0]!,
    target: { revision, windowKey, updatedAt },
    intervalMs,
  };
}

async function runSnapshotHeartbeatChild(args: string[]): Promise<void> {
  const { databasePath, target, intervalMs } = parseSnapshotHeartbeatChildArgs(args);
  const childStore = new TokenStore(databasePath);
  const heartbeat = scheduleTokenSnapshotHeartbeat(childStore, { target, intervalMs });
  let resolveSignal!: () => void;
  const signaled = new Promise<void>((resolve) => {
    resolveSignal = resolve;
  });
  const stop = () => resolveSignal();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    if (!heartbeat.active()) throw new Error("snapshot heartbeat child refused its target");
    process.stdout.write(SNAPSHOT_HEARTBEAT_CHILD_READY);
    const stdinClosed = new Response(Bun.stdin.stream()).arrayBuffer();
    await Promise.race([signaled, stdinClosed]);
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    heartbeat.clear();
    childStore.close();
  }
}

async function startIndependentSnapshotHeartbeat(
  databasePath: string,
  target: SnapshotHeartbeatTarget | null,
  intervalMs: number,
): Promise<IndependentSnapshotHeartbeat | null> {
  if (target === null) return null;
  const child = Bun.spawn(
    [
      ...workerInvocation("ingest", fileURLToPath(import.meta.url)),
      SNAPSHOT_HEARTBEAT_CHILD_ARG,
      databasePath,
      String(target.revision),
      target.windowKey,
      String(target.updatedAt),
      String(intervalMs),
    ],
    { stdin: "pipe", stdout: "pipe", stderr: "pipe" },
  );
  const reader = child.stdout.getReader();
  const ready = await reader.read();
  reader.releaseLock();
  if (ready.done || new TextDecoder().decode(ready.value) !== SNAPSHOT_HEARTBEAT_CHILD_READY) {
    const [exitCode, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
    throw new Error(
      `snapshot heartbeat child failed before readiness (${exitCode})${stderr ? `: ${stderr}` : ""}`,
    );
  }

  let stopped = false;
  return {
    async stop(): Promise<void> {
      if (stopped) return;
      stopped = true;
      const exitedBeforeStop = child.exitCode !== null;
      let forcedCleanup: "SIGTERM" | "SIGKILL" | null = null;
      try {
        await child.stdin.end();
      } catch {
        // A spontaneous child exit is classified below after its exact status is joined.
      }
      let exitCode = await waitForChildExit(
        child,
        SNAPSHOT_HEARTBEAT_CHILD_GRACEFUL_STOP_TIMEOUT_MS,
      );
      if (exitCode === null) {
        forcedCleanup = "SIGTERM";
        child.kill("SIGTERM");
        exitCode = await waitForChildExit(child, SNAPSHOT_HEARTBEAT_CHILD_TERM_STOP_TIMEOUT_MS);
      }
      if (exitCode === null) {
        forcedCleanup = "SIGKILL";
        child.kill("SIGKILL");
        exitCode = await child.exited;
      }
      const stderr = await new Response(child.stderr).text();
      if (exitedBeforeStop) {
        throw new Error(
          `snapshot heartbeat child exited before cleanup (${exitCode})${stderr ? `: ${stderr}` : ""}`,
        );
      }
      // A SIGTERM escalation with a clean exit(0) is the child's *designed*
      // shutdown path (it registers `process.once("SIGTERM", stop)` and exits
      // 0 — see runSnapshotHeartbeatChild above), not a hang: the graceful
      // stdin-close race (SNAPSHOT_HEARTBEAT_CHILD_GRACEFUL_STOP_TIMEOUT_MS =
      // 100ms) can lose to real scheduling/CPU-quota latency on this cgroup
      // even when the child is behaving correctly. Only a forced SIGKILL
      // (the child ignored SIGTERM) or a non-zero exit is a genuine defect
      // worth failing the ingest tick over; log the benign SIGTERM path so it
      // stays observable without flapping /healthz.
      if (forcedCleanup === "SIGKILL") {
        throw new Error(
          `snapshot heartbeat child required SIGKILL cleanup (${exitCode})${stderr ? `: ${stderr}` : ""}`,
        );
      }
      if (exitCode !== 0) {
        throw new Error(
          `snapshot heartbeat child required ${forcedCleanup ?? "no"} cleanup and exited ${exitCode}${stderr ? `: ${stderr}` : ""}`,
        );
      }
      if (forcedCleanup === "SIGTERM") {
        console.error(
          `snapshot heartbeat child needed SIGTERM to stop but exited cleanly (0)${stderr ? `: ${stderr}` : ""}`,
        );
      }
    },
  };
}

if (process.argv[2] === SNAPSHOT_HEARTBEAT_CHILD_ARG) {
  await runSnapshotHeartbeatChild(process.argv.slice(3));
  process.exit(0);
}

const request = parseRequest(await readBoundedInput());

// These branches are reachable only from the underscored test launcher seam.
if (request.testAction === "crash") throw new Error("injected ingest worker crash");

process.env.TOKENS_DB_PATH = request.databasePath;
process.env.CLAUDE_PROJECTS_DIR = request.claudeProjectsDir;
process.env.CODEX_SESSIONS_DIR = request.codexSessionsDir;
if (request.grokLogsDir) process.env.GROK_LOGS_DIR = request.grokLogsDir;
if (request.grokSessionsDir) process.env.GROK_SESSIONS_DIR = request.grokSessionsDir;

let response: UsageIngestWorkerResponse;
let store: TokenStore | null = null;
let heartbeat: ReturnType<typeof scheduleTokenSnapshotHeartbeat> | null = null;
let reducerHeartbeat: IndependentSnapshotHeartbeat | null = null;
const stopReducerHeartbeat = async (): Promise<void> => {
  const active = reducerHeartbeat;
  reducerHeartbeat = null;
  await active?.stop();
};
try {
  store = new TokenStore(request.databasePath);
  const injectedAction =
    request.testAction && typeof request.testAction === "object" ? request.testAction : null;
  const heartbeatIntervalMs =
    injectedAction !== null
      ? Math.max(5, Math.min(5_000, Math.trunc(injectedAction.heartbeatIntervalMs ?? 5_000)))
      : 5_000;
  const injectedDelayMs =
    injectedAction === null ? 0 : Math.max(0, Math.min(5_000, Math.trunc(injectedAction.delayMs)));
  const pauseAfterPublication = injectedAction !== null && "failAfterDelay" in injectedAction;
  heartbeat = scheduleTokenSnapshotHeartbeat(store, {
    target: request.heartbeatTarget ?? null,
    intervalMs: heartbeatIntervalMs,
  });
  if (injectedAction !== null && !pauseAfterPublication) await Bun.sleep(injectedDelayMs);
  const summary = await ingestOnce(store);
  // A successful scan has already reconciled provider discovery and may have
  // advanced the authority revision. Stop heartbeating the prior revision,
  // publish the new daily authority immediately, then keep that exact cache
  // alive while the more expensive reducers run.
  heartbeat.clear();
  heartbeat = null;
  const publishedAt = Date.now();
  let snapshotRefreshed = publishCurrentTokenSnapshot(store, summary.revision, publishedAt);
  reducerHeartbeat = await startIndependentSnapshotHeartbeat(
    request.databasePath,
    issueTokenSnapshotHeartbeatTarget(store, publishedAt),
    heartbeatIntervalMs,
  );
  // COUNT(*) is intentionally charged to this bounded transient sibling. Its
  // deferred read snapshot does not reserve SQLite's writer lock; publication
  // is a separate short revision CAS. The explicit test seam stalls between
  // those real COUNTs so only an independent heartbeat can advance through it.
  store.refreshExactStatsCache(
    summary.scannedAt,
    summary.revision,
    pauseAfterPublication ? injectedDelayMs : 0,
  );
  if (injectedAction?.failAfterDelay) throw new Error("injected delayed reducer failure");
  // The hour-wide read is deliberately charged to this bounded transient
  // sibling. The parent receives and reads only the validated metadata cache.
  const rate = store.refreshRateSnapshotCache(summary.scannedAt, summary.revision);
  // Stop the reducer heartbeat before the final pinned publication. A window
  // rollover, cache loss, or revision drift is revalidated here rather than
  // allowing an older worker to overwrite newer authority.
  await stopReducerHeartbeat();
  const completedAt = Date.now();
  snapshotRefreshed =
    publishCurrentTokenSnapshot(store, summary.revision, completedAt) || snapshotRefreshed;
  const fullUpdate = buildUpdateFromEvents(summary.events);
  const update: LiveEventDelta = {
    type: "events",
    newEvents: summary.newEvents,
    totalsDelta: fullUpdate.totalsDelta,
    recent: fullUpdate.recent.slice(-USAGE_INGEST_RECENT_MAX_EVENTS),
    scannedAt: summary.scannedAt,
  };
  // This is the worker's final durable operation. The rate window remains
  // anchored at summary.scannedAt, while cache freshness starts only after all
  // later authority/update work has completed successfully.
  store.touchCachedRateSnapshotPublication(summary.revision, rate.now);
  response = {
    type: "result",
    id: request.id,
    result: {
      files: summary.files,
      mutated: summary.mutated,
      revision: summary.revision,
      snapshotRefreshed,
      rate,
      update,
    },
  };
} catch (error) {
  response = { type: "error", id: request.id, error: errorText(error) };
} finally {
  heartbeat?.clear();
  try {
    await stopReducerHeartbeat();
  } finally {
    store?.close();
  }
}

process.stdout.write(encodeResponse(response));

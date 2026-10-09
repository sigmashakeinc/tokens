import { dirname, isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import { isRateSnapshot } from "./rate.ts";
import {
  type TokenSnapshotHeartbeatTarget,
  USAGE_INGEST_RATE_MAX_BYTES,
  USAGE_INGEST_RECENT_MAX_EVENTS,
  USAGE_INGEST_RECENT_TEXT_MAX_BYTES,
  USAGE_INGEST_REQUEST_MAX_BYTES,
  USAGE_INGEST_RESPONSE_MAX_BYTES,
  type UsageIngestWorkerRequest,
  type UsageIngestWorkerResponse,
  type UsageIngestWorkerResult,
} from "./usage-ingest-worker-protocol.ts";
import { directWorkerEnv, useSystemdLauncher, workerInvocation } from "./worker-launch.ts";

export interface UsageIngestTestSubprocess {
  stdin: {
    write(data: string | Uint8Array): number;
    end(): void;
  };
  stdout: ReadableStream<Uint8Array>;
  stderr: ReadableStream<Uint8Array>;
  exited: Promise<number>;
  kill(signal?: number | string): void;
}

export type UsageIngestTestControlSubprocess = Pick<
  UsageIngestTestSubprocess,
  "stderr" | "exited" | "kill"
>;

type IngestSubprocess = UsageIngestTestSubprocess;

interface IngestLaunch {
  unitName: string | null;
  subprocess: IngestSubprocess;
}

interface StopResult {
  exitCode: number;
  stderr: string;
}

interface ActiveIngest {
  id: number;
  launch: IngestLaunch;
  timer: ReturnType<typeof setTimeout> | null;
  settled: boolean;
  resolve(result: UsageIngestWorkerResult): void;
  reject(error: Error): void;
}

export interface UsageIngestWorkerPaths {
  databasePath: string;
  claudeProjectsDir: string;
  codexSessionsDir: string;
  grokLogsDir?: string;
  grokSessionsDir?: string;
}

export interface UsageIngestWorkerStats {
  spawned: number;
  completed: number;
  failed: number;
  active: boolean;
}

export class UsageIngestWorkerShutdownError extends Error {
  constructor() {
    super("ingest worker shut down");
    this.name = "UsageIngestWorkerShutdownError";
  }
}

type TestLauncher = "direct" | null;
export type UsageIngestTestSpawn = (command: string[]) => IngestSubprocess;
export type UsageIngestTestControlSpawn = (command: string[]) => UsageIngestTestControlSubprocess;

const WORKER_ENTRY = fileURLToPath(new URL("./usage-ingest.worker.ts", import.meta.url));
const SYSTEMD_RUN_BIN = "/usr/bin/systemd-run";
const SYSTEMCTL_BIN = "/usr/bin/systemctl";
const ENV_BIN = "/usr/bin/env";
const STDERR_MAX_BYTES = 64 * 1024;
const DEFAULT_WORKER_TIMEOUT_MS = 210_000;
const MIN_WORKER_TIMEOUT_MS = 1_000;
const MAX_WORKER_TIMEOUT_MS = 900_000;
const DEFAULT_STOP_TIMEOUT_MS = 8_000;
const MIN_STOP_TIMEOUT_MS = 100;
const MAX_STOP_TIMEOUT_MS = 30_000;
const SYSTEMD_RUNTIME_MARGIN_MS = 1_000;
const unitInstance = `${process.pid}-${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

let nextJobId = 1;
let activeIngest: ActiveIngest | null = null;
let cleanupBarrier: Promise<void> | null = null;
let cleanupFailure: Error | null = null;
let nextTestAction: UsageIngestWorkerRequest["testAction"];
let testLauncher: TestLauncher = null;
let testSpawn: UsageIngestTestSpawn | null = null;
let testControlSpawn: UsageIngestTestControlSpawn | null = null;
let spawned = 0;
let completed = 0;
let failed = 0;

function workerTimeoutMs(): number {
  const raw = process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_WORKER_TIMEOUT_MS;
  const value = Number(raw);
  return Number.isFinite(value) && value >= MIN_WORKER_TIMEOUT_MS && value <= MAX_WORKER_TIMEOUT_MS
    ? Math.trunc(value)
    : DEFAULT_WORKER_TIMEOUT_MS;
}

function workerStopTimeoutMs(): number {
  const raw = process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_STOP_TIMEOUT_MS;
  const value = Number(raw);
  return Number.isFinite(value) && value >= MIN_STOP_TIMEOUT_MS && value <= MAX_STOP_TIMEOUT_MS
    ? Math.trunc(value)
    : DEFAULT_STOP_TIMEOUT_MS;
}

function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    return String((error as { message: unknown }).message);
  }
  return String(error);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalidResult(detail: string): never {
  throw new Error(`ingest worker result is invalid: ${detail}`);
}

function assertNonNegativeSafeInteger(value: unknown, label: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    invalidResult(`${label} must be a non-negative safe integer`);
  }
}

function assertNonNegativeFiniteNumber(value: unknown, label: string): asserts value is number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > Number.MAX_SAFE_INTEGER
  ) {
    invalidResult(`${label} must be a finite non-negative number`);
  }
}

function assertBoundedText(value: unknown, label: string): asserts value is string {
  if (
    typeof value !== "string" ||
    new TextEncoder().encode(value).byteLength > USAGE_INGEST_RECENT_TEXT_MAX_BYTES
  ) {
    invalidResult(`${label} must be bounded UTF-8 text`);
  }
}

function validateResult(value: unknown): UsageIngestWorkerResult {
  if (!isRecord(value)) invalidResult("compact delta must be an object");

  assertNonNegativeSafeInteger(value.files, "files");
  if (typeof value.mutated !== "boolean") invalidResult("mutated must be a boolean");
  assertNonNegativeSafeInteger(value.revision, "revision");
  if (typeof value.snapshotRefreshed !== "boolean") {
    invalidResult("snapshotRefreshed must be a boolean");
  }
  if (!isRateSnapshot(value.rate)) invalidResult("rate must be a bounded rate snapshot");
  if (
    new TextEncoder().encode(JSON.stringify(value.rate)).byteLength > USAGE_INGEST_RATE_MAX_BYTES
  ) {
    invalidResult(`rate must not exceed ${USAGE_INGEST_RATE_MAX_BYTES} bytes`);
  }
  if (!isRecord(value.update)) invalidResult("update must be an object");

  const update = value.update;
  if (update.type !== "events") invalidResult("update.type must be events");
  assertNonNegativeSafeInteger(update.newEvents, "update.newEvents");
  assertNonNegativeSafeInteger(update.scannedAt, "update.scannedAt");
  if (value.rate.now !== update.scannedAt) {
    invalidResult("rate.now must match update.scannedAt");
  }
  if (!isRecord(update.totalsDelta)) invalidResult("update.totalsDelta must be an object");

  for (const field of [
    "inputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWrite5mTokens",
    "cacheWrite1hTokens",
  ] as const) {
    assertNonNegativeSafeInteger(update.totalsDelta[field], `update.totalsDelta.${field}`);
  }
  assertNonNegativeFiniteNumber(update.totalsDelta.cost, "update.totalsDelta.cost");

  if (!Array.isArray(update.recent)) invalidResult("update.recent must be an array");
  if (update.recent.length > USAGE_INGEST_RECENT_MAX_EVENTS) {
    invalidResult(`update.recent must contain at most ${USAGE_INGEST_RECENT_MAX_EVENTS} events`);
  }
  if (update.recent.length > update.newEvents) {
    invalidResult("update.recent cannot exceed update.newEvents");
  }
  for (const [index, event] of update.recent.entries()) {
    if (!isRecord(event)) invalidResult(`update.recent[${index}] must be an object`);
    assertNonNegativeSafeInteger(event.ts, `update.recent[${index}].ts`);
    assertBoundedText(event.model, `update.recent[${index}].model`);
    assertBoundedText(event.project, `update.recent[${index}].project`);
    assertBoundedText(event.sessionId, `update.recent[${index}].sessionId`);
    assertNonNegativeSafeInteger(event.totalTokens, `update.recent[${index}].totalTokens`);
    assertNonNegativeFiniteNumber(event.cost, `update.recent[${index}].cost`);
  }

  return value as unknown as UsageIngestWorkerResult;
}

async function readBounded(
  stream: ReadableStream<Uint8Array>,
  maxBytes: number,
  label: string,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        throw new Error(`${label} exceeds ${maxBytes} bytes`);
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

function parseResponse(bytes: Uint8Array, expectedId: number): UsageIngestWorkerResponse {
  const output = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!output.endsWith("\n") || output.indexOf("\n") !== output.length - 1) {
    throw new Error("ingest worker must return exactly one NDJSON response");
  }
  const parsed: unknown = JSON.parse(output.slice(0, -1));
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("id" in parsed) ||
    parsed.id !== expectedId ||
    !("type" in parsed) ||
    (parsed.type !== "result" && parsed.type !== "error")
  ) {
    throw new Error("ingest worker protocol mismatch");
  }
  if (parsed.type === "result") {
    if (!("result" in parsed)) {
      throw new Error("ingest worker result is missing its compact delta");
    }
    return { type: "result", id: expectedId, result: validateResult(parsed.result) };
  }
  if (!("error" in parsed) || typeof parsed.error !== "string") {
    throw new Error("ingest worker error is unreadable");
  }
  return { type: "error", id: expectedId, error: parsed.error };
}

/** Narrow parser seam for exhaustive protocol-boundary tests. */
export function _parseUsageIngestWorkerResponseForTest(
  bytes: Uint8Array,
  expectedId: number,
): UsageIngestWorkerResponse {
  return parseResponse(bytes, expectedId);
}

function assertAbsolutePaths(paths: UsageIngestWorkerPaths): void {
  for (const [label, value] of Object.entries(paths)) {
    if (value === undefined) continue;
    if (typeof value !== "string" || !isAbsolute(value)) {
      throw new Error(`ingest worker requires an absolute ${label}`);
    }
  }
}

function processCommand(
  unitName: string | null,
  databasePath: string,
  timeoutMs: number,
): string[] {
  const bunBin = process.execPath;
  if (!isAbsolute(bunBin) || !isAbsolute(WORKER_ENTRY)) {
    throw new Error("ingest worker requires absolute Bun and entry paths");
  }
  if (unitName === null) return workerInvocation("ingest", WORKER_ENTRY);
  const workerCommand = [ENV_BIN, "-i", ...workerInvocation("ingest", WORKER_ENTRY)];
  return [
    SYSTEMD_RUN_BIN,
    "--user",
    "--quiet",
    "--pipe",
    "--wait",
    "--collect",
    `--unit=${unitName}`,
    "--slice=sigmashake-tokens.slice",
    "--service-type=exec",
    "--property=BindsTo=sigmashake-tokens.service",
    "--property=After=sigmashake-tokens.service",
    `--property=RuntimeMaxSec=${timeoutMs + MAX_STOP_TIMEOUT_MS + SYSTEMD_RUNTIME_MARGIN_MS}ms`,
    "--property=MemoryHigh=208M",
    "--property=MemoryMax=256M",
    "--property=CPUQuota=45%",
    "--property=CPUWeight=100",
    "--property=ProtectHome=read-only",
    `--property=ReadWritePaths=${dirname(databasePath)}`,
    "--property=ProtectSystem=full",
    "--property=NoNewPrivileges=yes",
    "--property=PrivateTmp=yes",
    "--property=ProtectKernelTunables=yes",
    "--property=ProtectKernelModules=yes",
    "--property=ProtectControlGroups=yes",
    "--property=KillMode=control-group",
    "--property=TimeoutStopSec=5s",
    "--",
    ...workerCommand,
  ];
}

function launchIngest(
  payload: string,
  jobId: number,
  databasePath: string,
  timeoutMs: number,
): IngestLaunch {
  const unitName =
    testLauncher === "direct" || (testSpawn === null && !useSystemdLauncher())
      ? null
      : `sigmashake-tokens-ingest-${unitInstance}-${jobId}.service`;
  const command = processCommand(unitName, databasePath, timeoutMs);
  const subprocess =
    testSpawn?.(command) ??
    (Bun.spawn(command, {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      ...(unitName === null ? { env: directWorkerEnv() } : {}),
    }) as IngestSubprocess);
  const launch = { unitName, subprocess };
  try {
    subprocess.stdin.write(payload);
    subprocess.stdin.end();
  } catch (error) {
    trackCleanup(stopLaunch(launch));
    throw error;
  }
  return launch;
}

async function waitUntil<T>(promise: Promise<T>, deadlineAt: number, label: string): Promise<T> {
  const remainingMs = deadlineAt - performance.now();
  if (remainingMs <= 0) throw new Error(`${label} exceeded its cleanup deadline`);
  let timer: ReturnType<typeof setTimeout> | null = null;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} exceeded its cleanup deadline`)),
      remainingMs,
    );
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== null) clearTimeout(timer);
  }
}

function signalProcess(subprocess: IngestSubprocess, signal: "SIGTERM" | "SIGKILL"): void {
  try {
    subprocess.kill(signal);
  } catch {
    // The process already exited.
  }
}

async function terminateSubprocess(
  subprocess: IngestSubprocess,
  deadlineAt: number,
  label: string,
): Promise<void> {
  signalProcess(subprocess, "SIGTERM");
  const now = performance.now();
  const graceAt = Math.min(deadlineAt, now + Math.max(1, Math.floor((deadlineAt - now) / 2)));
  try {
    await waitUntil(subprocess.exited, graceAt, `${label} graceful stop`);
    return;
  } catch {
    signalProcess(subprocess, "SIGKILL");
  }
  await waitUntil(subprocess.exited, deadlineAt, `${label} forced stop`);
}

async function systemctlResult(
  args: string[],
  deadlineAt: number,
  label: string,
): Promise<StopResult> {
  const command = [SYSTEMCTL_BIN, "--user", ...args];
  const stop =
    testControlSpawn?.(command) ??
    Bun.spawn(command, {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "pipe",
    });
  try {
    const [exitCode, stderr] = await waitUntil(
      Promise.all([stop.exited, readBounded(stop.stderr, STDERR_MAX_BYTES, `${label} stderr`)]),
      deadlineAt,
      label,
    );
    return { exitCode, stderr: new TextDecoder().decode(stderr).trim() };
  } catch (error) {
    try {
      stop.kill(9);
    } catch {
      // The control command already exited.
    }
    throw error;
  }
}

function stopUnit(unitName: string, deadlineAt: number): Promise<StopResult> {
  return systemctlResult(["stop", unitName], deadlineAt, `stopping ${unitName}`);
}

function killUnit(unitName: string, deadlineAt: number): Promise<StopResult> {
  return systemctlResult(
    ["kill", "--kill-whom=all", "--signal=KILL", unitName],
    deadlineAt,
    `killing ${unitName}`,
  );
}

function unitIsAbsent(result: StopResult): boolean {
  return (
    result.exitCode !== 0 && /(?:not loaded|not found|could not be found)/i.test(result.stderr)
  );
}

async function stopOrKillExactUnit(
  unitName: string,
  deadlineAt: number,
  label: string,
): Promise<void> {
  const startedAt = performance.now();
  const remainingMs = deadlineAt - startedAt;
  if (remainingMs <= 0) throw new Error(`${label} exceeded its cleanup deadline`);
  const stopDeadline = Math.min(deadlineAt, startedAt + Math.max(1, Math.floor(remainingMs / 2)));
  let stopError: Error | null = null;
  try {
    const stopped = await stopUnit(unitName, stopDeadline);
    if (stopped.exitCode === 0 || unitIsAbsent(stopped)) return;
    stopError = new Error(
      `could not stop ${unitName}${stopped.stderr ? `: ${stopped.stderr}` : ""}`,
    );
  } catch (error) {
    stopError = new Error(errorText(error));
  }

  try {
    const killed = await killUnit(unitName, deadlineAt);
    if (killed.exitCode === 0 || unitIsAbsent(killed)) return;
    throw new Error(`could not kill ${unitName}${killed.stderr ? `: ${killed.stderr}` : ""}`);
  } catch (error) {
    throw new Error(`${stopError?.message ?? `could not stop ${unitName}`}; ${errorText(error)}`);
  }
}

async function stopLaunch(launch: IngestLaunch): Promise<void> {
  const timeoutMs = workerStopTimeoutMs();
  const startedAt = performance.now();
  const deadlineAt = startedAt + timeoutMs;
  if (launch.unitName === null) {
    await terminateSubprocess(launch.subprocess, deadlineAt, "ingest worker");
    return;
  }

  // Reserve distinct budgets for the initial stop, wrapper termination, and a
  // final exact-unit pass. The last pass is essential: systemd-run can report
  // the unit absent immediately before its in-flight registration completes.
  const initialUnitDeadline = Math.min(
    deadlineAt,
    startedAt + Math.max(1, Math.floor(timeoutMs / 3)),
  );
  const wrapperDeadline = Math.min(
    deadlineAt,
    startedAt + Math.max(2, Math.floor((timeoutMs * 2) / 3)),
  );
  let initialUnitError: Error | null = null;
  try {
    await stopOrKillExactUnit(launch.unitName, initialUnitDeadline, `stopping ${launch.unitName}`);
  } catch (error) {
    initialUnitError = new Error(errorText(error));
  }

  let wrapperError: Error | null = null;
  try {
    await terminateSubprocess(launch.subprocess, wrapperDeadline, "systemd-run wrapper");
  } catch (error) {
    wrapperError = new Error(errorText(error));
  }

  let finalUnitError: Error | null = null;
  try {
    // The wrapper is now gone and cannot register the unit after this check.
    await stopOrKillExactUnit(launch.unitName, deadlineAt, `verifying ${launch.unitName}`);
  } catch (error) {
    finalUnitError = new Error(errorText(error));
  }
  if (wrapperError !== null || finalUnitError !== null) {
    throw new Error(
      [
        initialUnitError === null || finalUnitError === null
          ? undefined
          : `initial unit cleanup failed: ${initialUnitError.message}`,
        wrapperError?.message,
        finalUnitError?.message,
      ]
        .filter((value) => value !== undefined)
        .join("; "),
    );
  }
}

function trackCleanup(cleanup: Promise<void>): void {
  cleanupBarrier = cleanup;
  void cleanup.then(
    () => {
      if (cleanupBarrier === cleanup) cleanupBarrier = null;
    },
    (error: unknown) => {
      if (cleanupBarrier === cleanup) cleanupBarrier = null;
      cleanupFailure = new Error(errorText(error));
    },
  );
}

function finishActive(active: ActiveIngest, response: UsageIngestWorkerResponse): void {
  if (activeIngest !== active || active.settled) return;
  active.settled = true;
  if (active.timer !== null) clearTimeout(active.timer);
  activeIngest = null;
  if (response.type === "result") {
    completed++;
    active.resolve(response.result);
  } else {
    failed++;
    active.reject(new Error(`ingest worker failed: ${response.error}`));
  }
}

async function failActive(active: ActiveIngest, error: Error, stop: boolean): Promise<void> {
  if (activeIngest !== active || active.settled) return;
  active.settled = true;
  if (active.timer !== null) clearTimeout(active.timer);
  let finalError = error;
  if (stop) {
    const cleanup = stopLaunch(active.launch);
    trackCleanup(cleanup);
    try {
      await cleanup;
    } catch (stopError) {
      finalError = new Error(`${error.message}; ${errorText(stopError)}`);
    }
  }
  if (activeIngest !== active) return;
  activeIngest = null;
  failed++;
  active.reject(finalError);
}

async function observeLaunch(active: ActiveIngest): Promise<void> {
  try {
    const [exitCode, stdout, stderr] = await Promise.all([
      active.launch.subprocess.exited,
      readBounded(
        active.launch.subprocess.stdout,
        USAGE_INGEST_RESPONSE_MAX_BYTES,
        "ingest worker response",
      ),
      readBounded(active.launch.subprocess.stderr, STDERR_MAX_BYTES, "ingest worker stderr"),
    ]);
    if (activeIngest !== active || active.settled) return;
    if (exitCode !== 0) {
      const detail = new TextDecoder().decode(stderr).trim();
      throw new Error(`ingest worker crashed with code ${exitCode}${detail ? `: ${detail}` : ""}`);
    }
    finishActive(active, parseResponse(stdout, active.id));
  } catch (error) {
    await failActive(active, new Error(errorText(error)), true);
  }
}

/** Runs one full source reconciliation in a bounded transient sibling process. */
export function ingestUsageOffThread(
  paths: UsageIngestWorkerPaths,
  heartbeatTarget?: TokenSnapshotHeartbeatTarget,
): Promise<UsageIngestWorkerResult> {
  if (cleanupFailure !== null) {
    return Promise.reject(
      new Error(`ingest worker cleanup failed; relaunch blocked: ${cleanupFailure.message}`),
    );
  }
  if (cleanupBarrier !== null) {
    return Promise.reject(
      new Error("ingest worker cleanup is still in progress; relaunch blocked"),
    );
  }
  if (activeIngest !== null) {
    return Promise.reject(new Error("ingest worker already has an active job"));
  }
  try {
    assertAbsolutePaths(paths);
  } catch (error) {
    return Promise.reject(new Error(errorText(error)));
  }

  const id = nextJobId++;
  const timeoutMs = workerTimeoutMs();
  const testAction = nextTestAction;
  nextTestAction = undefined;
  const request: UsageIngestWorkerRequest = {
    type: "ingest",
    id,
    ...paths,
    ...(heartbeatTarget === undefined ? {} : { heartbeatTarget }),
    ...(testAction === undefined ? {} : { testAction }),
  };
  let payload: string;
  try {
    payload = `${JSON.stringify(request)}\n`;
    if (new TextEncoder().encode(payload).byteLength > USAGE_INGEST_REQUEST_MAX_BYTES) {
      throw new Error(`ingest request exceeds ${USAGE_INGEST_REQUEST_MAX_BYTES} bytes`);
    }
  } catch (error) {
    return Promise.reject(new Error(errorText(error)));
  }

  return new Promise((resolve, reject) => {
    try {
      const launch = launchIngest(payload, id, paths.databasePath, timeoutMs);
      spawned++;
      const active: ActiveIngest = {
        id,
        launch,
        timer: null,
        settled: false,
        resolve,
        reject,
      };
      active.timer = setTimeout(() => {
        void failActive(active, new Error("ingest worker timed out"), true);
      }, timeoutMs);
      active.timer.unref?.();
      activeIngest = active;
      void observeLaunch(active);
    } catch (error) {
      failed++;
      reject(new Error(`ingest worker could not start: ${errorText(error)}`));
    }
  });
}

/** Stops the exact transient ingest unit and waits until it cannot outlive the parent. */
export function shutdownUsageIngestWorker(): Promise<void> {
  const active = activeIngest;
  if (active?.settled) {
    if (cleanupBarrier !== null) return cleanupBarrier;
    return cleanupFailure === null ? Promise.resolve() : Promise.reject(cleanupFailure);
  }
  if (!active) {
    if (cleanupBarrier !== null) return cleanupBarrier;
    return cleanupFailure === null ? Promise.resolve() : Promise.reject(cleanupFailure);
  }
  active.settled = true;
  if (active.timer !== null) clearTimeout(active.timer);
  activeIngest = null;
  active.reject(new UsageIngestWorkerShutdownError());
  const cleanup = stopLaunch(active.launch);
  trackCleanup(cleanup);
  return cleanup;
}

export function _usageIngestWorkerStatsForTest(): UsageIngestWorkerStats {
  return { spawned, completed, failed, active: activeIngest !== null };
}

export function _usageIngestWorkerTimeoutMsForTest(): number {
  return workerTimeoutMs();
}

export function _usageIngestWorkerCommandForTest(databasePath: string): string[] {
  return processCommand("sigmashake-tokens-ingest-test.service", databasePath, workerTimeoutMs());
}

export function _setNextUsageIngestWorkerActionForTest(
  action: UsageIngestWorkerRequest["testAction"],
): void {
  nextTestAction = action;
}

export function _setUsageIngestLauncherForTest(launcher: TestLauncher): void {
  if (activeIngest || cleanupBarrier || cleanupFailure) {
    throw new Error("ingest launcher cannot change while work is active");
  }
  testLauncher = launcher;
}

export function _setUsageIngestSpawnForTest(spawn: UsageIngestTestSpawn | null): void {
  if (activeIngest || cleanupBarrier || cleanupFailure) {
    throw new Error("ingest spawn cannot change while work is active");
  }
  testSpawn = spawn;
}

export function _setUsageIngestControlSpawnForTest(
  spawn: UsageIngestTestControlSpawn | null,
): void {
  if (activeIngest || cleanupBarrier || cleanupFailure) {
    throw new Error("ingest control spawn cannot change while work is active");
  }
  testControlSpawn = spawn;
}

export async function _resetUsageIngestWorkerForTest(): Promise<void> {
  try {
    await shutdownUsageIngestWorker();
  } catch {
    // The test reset is the sole escape hatch from an intentionally poisoned
    // cleanup state after its assertion has observed the propagated failure.
  }
  nextJobId = 1;
  nextTestAction = undefined;
  testLauncher = null;
  testSpawn = null;
  testControlSpawn = null;
  cleanupBarrier = null;
  cleanupFailure = null;
  spawned = 0;
  completed = 0;
  failed = 0;
}

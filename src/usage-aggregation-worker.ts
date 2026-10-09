import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import type { UsageAggregateOptions, UsageAggregateSnapshot } from "./store.ts";
import {
  USAGE_AGGREGATION_REQUEST_MAX_BYTES,
  USAGE_AGGREGATION_RESPONSE_MAX_BYTES,
  type UsageAggregationWorkerRequest,
  type UsageAggregationWorkerResponse,
} from "./usage-aggregation-worker-protocol.ts";
import { directWorkerEnv, useSystemdLauncher, workerInvocation } from "./worker-launch.ts";

interface AggregationJob {
  id: number;
  databasePath: string;
  options: UsageAggregateOptions;
  dispatchedAt: number | null;
  resolve(result: UsageAggregationWorkerResult): void;
  reject(error: Error): void;
}

export interface UsageAggregationTestSubprocess {
  stdin: {
    write(data: string | Uint8Array): number;
    end(): void;
  };
  stdout: ReadableStream<Uint8Array>;
  stderr: ReadableStream<Uint8Array>;
  exited: Promise<number>;
  kill(signal?: number | string): void;
}

type AggregationSubprocess = UsageAggregationTestSubprocess;

export type UsageAggregationTestControlSubprocess = Pick<
  UsageAggregationTestSubprocess,
  "stderr" | "exited" | "kill"
>;

export type UsageAggregationTestSpawn = (command: string[]) => AggregationSubprocess;
export type UsageAggregationTestControlSpawn = (
  command: string[],
) => UsageAggregationTestControlSubprocess;

interface AggregationLaunch {
  unitName: string | null;
  subprocess: AggregationSubprocess;
}

interface StopResult {
  exitCode: number;
  stderr: string;
}

interface ActiveAggregation {
  job: AggregationJob;
  launch: AggregationLaunch;
  timer: ReturnType<typeof setTimeout> | null;
  settled: boolean;
}

export interface UsageAggregationWorkerResult {
  snapshot: UsageAggregateSnapshot;
  /** Main-clock timestamp taken when the serialized job is actually sent to
   * the process, after any earlier accepted query has drained. */
  scannedAt: number;
}

export interface UsageAggregationWorkerStats {
  spawned: number;
  dispatched: number;
  completed: number;
  failed: number;
  queued: number;
  active: boolean;
}

type TestLauncher = "direct" | null;

const MAX_ACCEPTED_JOBS = 16;
const WORKER_ENTRY = fileURLToPath(new URL("./usage-aggregation.worker.ts", import.meta.url));
const SYSTEMD_RUN_BIN = "/usr/bin/systemd-run";
const SYSTEMCTL_BIN = "/usr/bin/systemctl";
const ENV_BIN = "/usr/bin/env";
const STDERR_MAX_BYTES = 64 * 1024;
const DEFAULT_STOP_TIMEOUT_MS = 8_000;
const unitInstance = `${process.pid}-${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

let nextJobId = 1;
let activeAggregation: ActiveAggregation | null = null;
let cleanupBarrier: Promise<void> | null = null;
let cleanupFailure: Error | null = null;
const queue: AggregationJob[] = [];
let nextTestAction: UsageAggregationWorkerRequest["testAction"];
let testLauncher: TestLauncher = null;
let testSpawn: UsageAggregationTestSpawn | null = null;
let testControlSpawn: UsageAggregationTestControlSpawn | null = null;
let spawned = 0;
let dispatched = 0;
let completed = 0;
let failed = 0;

function workerTimeoutMs(): number {
  const raw = process.env.TOKENS_USAGE_WORKER_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return 120_000;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 1_000 && value <= 600_000 ? Math.trunc(value) : 120_000;
}

function workerStopTimeoutMs(): number {
  const raw = process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_STOP_TIMEOUT_MS;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 100 && value <= 30_000
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

function schedulePump(): void {
  queueMicrotask(pump);
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

function parseResponse(bytes: Uint8Array, expectedId: number): UsageAggregationWorkerResponse {
  const output = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!output.endsWith("\n") || output.indexOf("\n") !== output.length - 1) {
    throw new Error("aggregation worker must return exactly one NDJSON response");
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
    throw new Error("aggregation worker protocol mismatch");
  }
  if (parsed.type === "result" && !("snapshot" in parsed)) {
    throw new Error("aggregation worker result is missing its snapshot");
  }
  if (parsed.type === "error" && (!("error" in parsed) || typeof parsed.error !== "string")) {
    throw new Error("aggregation worker error is unreadable");
  }
  return parsed as UsageAggregationWorkerResponse;
}

function processCommand(unitName: string | null): string[] {
  const bunBin = process.execPath;
  if (!isAbsolute(bunBin) || !isAbsolute(WORKER_ENTRY)) {
    throw new Error("aggregation worker requires absolute Bun and entry paths");
  }
  if (unitName === null) return workerInvocation("aggregation", WORKER_ENTRY);
  const workerCommand = [ENV_BIN, "-i", ...workerInvocation("aggregation", WORKER_ENTRY)];
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
    "--property=RuntimeMaxSec=11min",
    "--property=MemoryHigh=96M",
    "--property=MemoryMax=128M",
    "--property=CPUQuota=35%",
    "--property=CPUWeight=100",
    "--property=ProtectHome=read-only",
    "--property=ProtectSystem=full",
    "--property=NoNewPrivileges=yes",
    "--property=PrivateTmp=yes",
    "--property=ProtectControlGroups=yes",
    "--property=KillMode=control-group",
    "--property=TimeoutStopSec=5s",
    "--",
    ...workerCommand,
  ];
}

function launchAggregation(payload: string, jobId: number): AggregationLaunch {
  const unitName =
    testLauncher === "direct" || (testSpawn === null && !useSystemdLauncher())
      ? null
      : `sigmashake-tokens-usage-${unitInstance}-${jobId}.service`;
  const command = processCommand(unitName);
  const subprocess =
    testSpawn?.(command) ??
    (Bun.spawn(command, {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      ...(unitName === null ? { env: directWorkerEnv() } : {}),
    }) as AggregationSubprocess);
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

function signalProcess(subprocess: AggregationSubprocess, signal: "SIGTERM" | "SIGKILL"): void {
  try {
    subprocess.kill(signal);
  } catch {
    // The exact process handle already exited.
  }
}

async function terminateSubprocess(
  subprocess: AggregationSubprocess,
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
      stop.kill("SIGKILL");
    } catch {
      // The exact control-process handle already exited.
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

async function stopLaunch(launch: AggregationLaunch): Promise<void> {
  const timeoutMs = workerStopTimeoutMs();
  const startedAt = performance.now();
  const deadlineAt = startedAt + timeoutMs;
  if (launch.unitName === null) {
    await terminateSubprocess(launch.subprocess, deadlineAt, "aggregation worker");
    return;
  }

  // Split the fixed cleanup budget across the first unit stop, the exact
  // systemd-run wrapper handle, and a final same-name unit verification after
  // that wrapper can no longer register a late transient unit.
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

function rejectQueuedForCleanupFailure(error: Error): void {
  for (const job of queue.splice(0)) {
    failed++;
    job.reject(new Error(`aggregation worker cleanup failed; relaunch blocked: ${error.message}`));
  }
}

function trackCleanup(cleanup: Promise<void>): void {
  cleanupBarrier = cleanup;
  void cleanup.then(
    () => {
      if (cleanupBarrier === cleanup) cleanupBarrier = null;
      schedulePump();
    },
    (error: unknown) => {
      if (cleanupBarrier === cleanup) cleanupBarrier = null;
      cleanupFailure = new Error(errorText(error));
      rejectQueuedForCleanupFailure(cleanupFailure);
    },
  );
}

function finishActive(active: ActiveAggregation, response: UsageAggregationWorkerResponse): void {
  if (activeAggregation !== active || active.settled) return;
  active.settled = true;
  if (active.timer !== null) clearTimeout(active.timer);
  activeAggregation = null;
  if (response.type === "result") {
    completed++;
    active.job.resolve({
      snapshot: response.snapshot,
      scannedAt: active.job.dispatchedAt ?? Date.now(),
    });
  } else {
    failed++;
    active.job.reject(new Error(`aggregation worker failed: ${response.error}`));
  }
  schedulePump();
}

async function failActive(active: ActiveAggregation, error: Error, stop: boolean): Promise<void> {
  if (activeAggregation !== active || active.settled) return;
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
  if (activeAggregation !== active) return;
  activeAggregation = null;
  failed++;
  active.job.reject(finalError);
  schedulePump();
}

async function observeLaunch(active: ActiveAggregation): Promise<void> {
  try {
    const [exitCode, stdout, stderr] = await Promise.all([
      active.launch.subprocess.exited,
      readBounded(
        active.launch.subprocess.stdout,
        USAGE_AGGREGATION_RESPONSE_MAX_BYTES,
        "aggregation worker response",
      ),
      readBounded(active.launch.subprocess.stderr, STDERR_MAX_BYTES, "aggregation worker stderr"),
    ]);
    if (activeAggregation !== active || active.settled) return;
    if (exitCode !== 0) {
      const detail = new TextDecoder().decode(stderr).trim();
      throw new Error(
        `aggregation worker crashed with code ${exitCode}${detail ? `: ${detail}` : ""}`,
      );
    }
    finishActive(active, parseResponse(stdout, active.job.id));
  } catch (error) {
    await failActive(active, new Error(errorText(error)), true);
  }
}

function pump(): void {
  if (activeAggregation || cleanupBarrier || cleanupFailure || queue.length === 0) return;
  const job = queue.shift();
  if (!job) return;
  const testAction = nextTestAction;
  nextTestAction = undefined;
  const request: UsageAggregationWorkerRequest = {
    type: "aggregate",
    id: job.id,
    databasePath: job.databasePath,
    options: job.options,
    ...(testAction === undefined ? {} : { testAction }),
  };

  let payload: string;
  try {
    payload = `${JSON.stringify(request)}\n`;
    if (new TextEncoder().encode(payload).byteLength > USAGE_AGGREGATION_REQUEST_MAX_BYTES) {
      throw new Error(`aggregation request exceeds ${USAGE_AGGREGATION_REQUEST_MAX_BYTES} bytes`);
    }
    job.dispatchedAt = Date.now();
    const launch = launchAggregation(payload, job.id);
    spawned++;
    dispatched++;
    const active: ActiveAggregation = {
      job,
      launch,
      timer: null,
      settled: false,
    };
    active.timer = setTimeout(() => {
      void failActive(active, new Error("aggregation worker timed out"), true);
    }, workerTimeoutMs());
    active.timer.unref?.();
    activeAggregation = active;
    void observeLaunch(active);
  } catch (error) {
    failed++;
    job.reject(new Error(`aggregation worker could not start: ${errorText(error)}`));
    schedulePump();
  }
}

/** Executes a bounded, serialized read in a dedicated transient process. */
export function aggregateUsageOffThread(
  databasePath: string,
  options: UsageAggregateOptions,
): Promise<UsageAggregationWorkerResult> {
  if (cleanupFailure !== null) {
    return Promise.reject(
      new Error(`aggregation worker cleanup failed; relaunch blocked: ${cleanupFailure.message}`),
    );
  }
  if (cleanupBarrier !== null) {
    return Promise.reject(
      new Error("aggregation worker cleanup is still in progress; relaunch blocked"),
    );
  }
  if (queue.length + (activeAggregation ? 1 : 0) >= MAX_ACCEPTED_JOBS) {
    return Promise.reject(
      new Error(`aggregation worker queue limit reached (${MAX_ACCEPTED_JOBS} accepted jobs)`),
    );
  }
  return new Promise((resolve, reject) => {
    queue.push({
      id: nextJobId++,
      databasePath,
      options,
      dispatchedAt: null,
      resolve,
      reject,
    });
    pump();
  });
}

/** Stops the exact transient unit and rejects all accepted work. */
export function shutdownUsageAggregationWorker(): Promise<void> {
  const error = new Error("aggregation worker shut down");
  const active = activeAggregation;
  // failActive owns cleanup once it marks the job settled. Reuse that exact
  // barrier so shutdown cannot start a second stop sequence, overwrite the
  // first barrier, or report success while the owned cleanup failed.
  if (active?.settled) {
    for (const job of queue.splice(0)) {
      failed++;
      job.reject(error);
    }
    if (cleanupBarrier !== null) return cleanupBarrier;
    return cleanupFailure === null ? Promise.resolve() : Promise.reject(cleanupFailure);
  }
  if (active) {
    active.settled = true;
    if (active.timer !== null) clearTimeout(active.timer);
    activeAggregation = null;
    failed++;
    active.job.reject(error);
    const cleanup = stopLaunch(active.launch);
    trackCleanup(cleanup);
  }
  for (const job of queue.splice(0)) {
    failed++;
    job.reject(error);
  }
  if (active) return cleanupBarrier!;
  if (cleanupBarrier !== null) return cleanupBarrier;
  return cleanupFailure === null ? Promise.resolve() : Promise.reject(cleanupFailure);
}

export function _usageAggregationWorkerStatsForTest(): UsageAggregationWorkerStats {
  return {
    spawned,
    dispatched,
    completed,
    failed,
    queued: queue.length,
    active: activeAggregation !== null,
  };
}

export function _setNextUsageAggregationWorkerActionForTest(
  action: UsageAggregationWorkerRequest["testAction"],
): void {
  nextTestAction = action;
}

export function _setUsageAggregationLauncherForTest(launcher: TestLauncher): void {
  if (activeAggregation || queue.length > 0 || cleanupBarrier || cleanupFailure) {
    throw new Error("aggregation launcher cannot change while work is active");
  }
  testLauncher = launcher;
}

export function _setUsageAggregationSpawnForTest(spawn: UsageAggregationTestSpawn | null): void {
  if (activeAggregation || queue.length > 0 || cleanupBarrier || cleanupFailure) {
    throw new Error("aggregation spawn cannot change while work is active");
  }
  testSpawn = spawn;
}

export function _setUsageAggregationControlSpawnForTest(
  spawn: UsageAggregationTestControlSpawn | null,
): void {
  if (activeAggregation || queue.length > 0 || cleanupBarrier || cleanupFailure) {
    throw new Error("aggregation control spawn cannot change while work is active");
  }
  testControlSpawn = spawn;
}

export async function _resetUsageAggregationWorkerForTest(): Promise<void> {
  try {
    await shutdownUsageAggregationWorker();
  } catch {
    // Tests may deliberately poison cleanup; reset is their sole recovery seam.
  }
  nextJobId = 1;
  nextTestAction = undefined;
  testLauncher = null;
  testSpawn = null;
  testControlSpawn = null;
  cleanupBarrier = null;
  cleanupFailure = null;
  spawned = 0;
  dispatched = 0;
  completed = 0;
  failed = 0;
}

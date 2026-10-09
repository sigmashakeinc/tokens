// Off-thread tokenomics spawner. Mirrors the transient systemd-run pattern in
// usage-aggregation-worker.ts (spawn a sibling process per computation, one
// active job at a time, bounded NDJSON stdin/stdout protocol, timeout + kill
// cleanup) — reused here instead of inventing a new mechanism, applied to the
// CPU-bound tokenomics scan (analyzeEfficiency) that used to block the HTTP
// server's single-threaded event loop under the sigmashake-tokens.slice
// CPUQuota=50% cap.

import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import type { TokenomicsResult } from "./tokenomics.ts";
import {
  TOKENOMICS_WORKER_REQUEST_MAX_BYTES,
  TOKENOMICS_WORKER_RESPONSE_MAX_BYTES,
  type TokenomicsWorkerRequest,
  type TokenomicsWorkerResponse,
} from "./tokenomics-worker-protocol.ts";
import { directWorkerEnv, useSystemdLauncher, workerInvocation } from "./worker-launch.ts";

export interface TokenomicsTestSubprocess {
  stdin: {
    write(data: string | Uint8Array): number;
    end(): void;
  };
  stdout: ReadableStream<Uint8Array>;
  stderr: ReadableStream<Uint8Array>;
  exited: Promise<number>;
  kill(signal?: number | string): void;
}

type TokenomicsSubprocess = TokenomicsTestSubprocess;

export type TokenomicsTestControlSubprocess = Pick<
  TokenomicsTestSubprocess,
  "stderr" | "exited" | "kill"
>;

export type TokenomicsTestSpawn = (command: string[]) => TokenomicsSubprocess;
export type TokenomicsTestControlSpawn = (command: string[]) => TokenomicsTestControlSubprocess;

interface TokenomicsLaunch {
  unitName: string | null;
  subprocess: TokenomicsSubprocess;
}

interface StopResult {
  exitCode: number;
  stderr: string;
}

interface ActiveTokenomicsJob {
  id: number;
  launch: TokenomicsLaunch;
  timer: ReturnType<typeof setTimeout> | null;
  settled: boolean;
  resolve(result: TokenomicsResult): void;
  reject(error: Error): void;
}

export interface TokenomicsWorkerStats {
  spawned: number;
  completed: number;
  failed: number;
  active: boolean;
}

type TestLauncher = "direct" | null;

const WORKER_ENTRY = fileURLToPath(new URL("./tokenomics.worker.ts", import.meta.url));
const SYSTEMD_RUN_BIN = "/usr/bin/systemd-run";
const SYSTEMCTL_BIN = "/usr/bin/systemctl";
const STDERR_MAX_BYTES = 64 * 1024;
const DEFAULT_WORKER_TIMEOUT_MS = 180_000;
const DEFAULT_STOP_TIMEOUT_MS = 8_000;
const unitInstance = `${process.pid}-${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

let nextJobId = 1;
let activeJob: ActiveTokenomicsJob | null = null;
let cleanupBarrier: Promise<void> | null = null;
let cleanupFailure: Error | null = null;
let nextTestAction: TokenomicsWorkerRequest["testAction"];
let testLauncher: TestLauncher = null;
let testSpawn: TokenomicsTestSpawn | null = null;
let testControlSpawn: TokenomicsTestControlSpawn | null = null;
let spawned = 0;
let completed = 0;
let failed = 0;

function workerTimeoutMs(): number {
  const raw = process.env.TOKENS_TOKENOMICS_WORKER_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_WORKER_TIMEOUT_MS;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 1_000 && value <= 600_000
    ? Math.trunc(value)
    : DEFAULT_WORKER_TIMEOUT_MS;
}

function workerStopTimeoutMs(): number {
  const raw = process.env.TOKENS_TOKENOMICS_WORKER_STOP_TIMEOUT_MS;
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

function parseResponse(bytes: Uint8Array, expectedId: number): TokenomicsWorkerResponse {
  const output = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!output.endsWith("\n") || output.indexOf("\n") !== output.length - 1) {
    throw new Error("tokenomics worker must return exactly one NDJSON response");
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
    throw new Error("tokenomics worker protocol mismatch");
  }
  if (parsed.type === "result" && !("result" in parsed)) {
    throw new Error("tokenomics worker result is missing its payload");
  }
  if (parsed.type === "error" && (!("error" in parsed) || typeof parsed.error !== "string")) {
    throw new Error("tokenomics worker error is unreadable");
  }
  return parsed as TokenomicsWorkerResponse;
}

function processCommand(unitName: string | null): string[] {
  const bunBin = process.execPath;
  if (!isAbsolute(bunBin) || !isAbsolute(WORKER_ENTRY)) {
    throw new Error("tokenomics worker requires absolute Bun and entry paths");
  }
  const workerCommand = workerInvocation("tokenomics", WORKER_ENTRY);
  if (unitName === null) return workerCommand;
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
    "--property=NoNewPrivileges=yes",
    "--property=PrivateTmp=yes",
    "--property=ProtectControlGroups=yes",
    "--property=KillMode=control-group",
    "--property=TimeoutStopSec=5s",
    "--",
    ...workerCommand,
  ];
}

function launchTokenomics(payload: string, jobId: number): TokenomicsLaunch {
  const unitName =
    testLauncher === "direct" || (testSpawn === null && !useSystemdLauncher())
      ? null
      : `sigmashake-tokens-tokenomics-${unitInstance}-${jobId}.service`;
  const command = processCommand(unitName);
  const subprocess =
    testSpawn?.(command) ??
    (Bun.spawn(command, {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      ...(unitName === null ? { env: directWorkerEnv() } : {}),
    }) as TokenomicsSubprocess);
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

function signalProcess(subprocess: TokenomicsSubprocess, signal: "SIGTERM" | "SIGKILL"): void {
  try {
    subprocess.kill(signal);
  } catch {
    // The exact process handle already exited.
  }
}

async function terminateSubprocess(
  subprocess: TokenomicsSubprocess,
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

async function stopLaunch(launch: TokenomicsLaunch): Promise<void> {
  const timeoutMs = workerStopTimeoutMs();
  const startedAt = performance.now();
  const deadlineAt = startedAt + timeoutMs;
  if (launch.unitName === null) {
    await terminateSubprocess(launch.subprocess, deadlineAt, "tokenomics worker");
    return;
  }
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

async function failActive(job: ActiveTokenomicsJob, error: Error, stop: boolean): Promise<void> {
  if (activeJob !== job || job.settled) return;
  job.settled = true;
  if (job.timer !== null) clearTimeout(job.timer);
  let finalError = error;
  if (stop) {
    const cleanup = stopLaunch(job.launch);
    trackCleanup(cleanup);
    try {
      await cleanup;
    } catch (stopError) {
      finalError = new Error(`${error.message}; ${errorText(stopError)}`);
    }
  }
  if (activeJob !== job) return;
  activeJob = null;
  failed++;
  job.reject(finalError);
}

function finishActive(job: ActiveTokenomicsJob, response: TokenomicsWorkerResponse): void {
  if (activeJob !== job || job.settled) return;
  job.settled = true;
  if (job.timer !== null) clearTimeout(job.timer);
  activeJob = null;
  if (response.type === "result") {
    completed++;
    job.resolve(response.result);
  } else {
    failed++;
    job.reject(new Error(`tokenomics worker failed: ${response.error}`));
  }
}

async function observeLaunch(job: ActiveTokenomicsJob): Promise<void> {
  try {
    const [exitCode, stdout, stderr] = await Promise.all([
      job.launch.subprocess.exited,
      readBounded(
        job.launch.subprocess.stdout,
        TOKENOMICS_WORKER_RESPONSE_MAX_BYTES,
        "tokenomics worker response",
      ),
      readBounded(job.launch.subprocess.stderr, STDERR_MAX_BYTES, "tokenomics worker stderr"),
    ]);
    if (activeJob !== job || job.settled) return;
    if (exitCode !== 0) {
      const detail = new TextDecoder().decode(stderr).trim();
      throw new Error(
        `tokenomics worker crashed with code ${exitCode}${detail ? `: ${detail}` : ""}`,
      );
    }
    finishActive(job, parseResponse(stdout, job.id));
  } catch (error) {
    await failActive(job, new Error(errorText(error)), true);
  }
}

/** Executes a fresh tokenomics computation in a dedicated transient process.
 * At most one job runs at a time — a concurrent call while one is already in
 * flight returns a promise for the SAME job's result rather than launching a
 * second process (callers should generally single-flight above this via a
 * cache anyway, but this guard makes the invariant unconditional here). */
export function computeTokenomicsOffThread(databasePath: string): Promise<TokenomicsResult> {
  if (cleanupFailure !== null) {
    return Promise.reject(
      new Error(`tokenomics worker cleanup failed; relaunch blocked: ${cleanupFailure.message}`),
    );
  }
  if (cleanupBarrier !== null) {
    return Promise.reject(
      new Error("tokenomics worker cleanup is still in progress; relaunch blocked"),
    );
  }
  if (activeJob !== null) {
    return new Promise<TokenomicsResult>((resolve, reject) => {
      const job = activeJob as ActiveTokenomicsJob;
      const priorResolve = job.resolve;
      const priorReject = job.reject;
      job.resolve = (result) => {
        priorResolve(result);
        resolve(result);
      };
      job.reject = (error) => {
        priorReject(error);
        reject(error);
      };
    });
  }

  const id = nextJobId++;
  const testAction = nextTestAction;
  nextTestAction = undefined;
  const request: TokenomicsWorkerRequest = {
    type: "tokenomics",
    id,
    databasePath,
    ...(testAction === undefined ? {} : { testAction }),
  };

  return new Promise<TokenomicsResult>((resolve, reject) => {
    let payload: string;
    try {
      payload = `${JSON.stringify(request)}\n`;
      if (new TextEncoder().encode(payload).byteLength > TOKENOMICS_WORKER_REQUEST_MAX_BYTES) {
        throw new Error(`tokenomics request exceeds ${TOKENOMICS_WORKER_REQUEST_MAX_BYTES} bytes`);
      }
      const launch = launchTokenomics(payload, id);
      spawned++;
      const job: ActiveTokenomicsJob = { id, launch, timer: null, settled: false, resolve, reject };
      job.timer = setTimeout(() => {
        void failActive(job, new Error("tokenomics worker timed out"), true);
      }, workerTimeoutMs());
      job.timer.unref?.();
      activeJob = job;
      void observeLaunch(job);
    } catch (error) {
      failed++;
      reject(new Error(`tokenomics worker could not start: ${errorText(error)}`));
    }
  });
}

/** Stops the exact transient unit and rejects any in-flight work. */
export function shutdownTokenomicsWorker(): Promise<void> {
  const error = new Error("tokenomics worker shut down");
  const job = activeJob;
  if (job?.settled) {
    if (cleanupBarrier !== null) return cleanupBarrier;
    return cleanupFailure === null ? Promise.resolve() : Promise.reject(cleanupFailure);
  }
  if (job) {
    job.settled = true;
    if (job.timer !== null) clearTimeout(job.timer);
    activeJob = null;
    failed++;
    job.reject(error);
    const cleanup = stopLaunch(job.launch);
    trackCleanup(cleanup);
    return cleanupBarrier!;
  }
  if (cleanupBarrier !== null) return cleanupBarrier;
  return cleanupFailure === null ? Promise.resolve() : Promise.reject(cleanupFailure);
}

export function _tokenomicsWorkerStatsForTest(): TokenomicsWorkerStats {
  return { spawned, completed, failed, active: activeJob !== null };
}

export function _setNextTokenomicsWorkerActionForTest(
  action: TokenomicsWorkerRequest["testAction"],
): void {
  nextTestAction = action;
}

export function _setTokenomicsLauncherForTest(launcher: TestLauncher): void {
  if (activeJob || cleanupBarrier || cleanupFailure) {
    throw new Error("tokenomics launcher cannot change while work is active");
  }
  testLauncher = launcher;
}

export function _setTokenomicsSpawnForTest(spawn: TokenomicsTestSpawn | null): void {
  if (activeJob || cleanupBarrier || cleanupFailure) {
    throw new Error("tokenomics spawn cannot change while work is active");
  }
  testSpawn = spawn;
}

export function _setTokenomicsControlSpawnForTest(spawn: TokenomicsTestControlSpawn | null): void {
  if (activeJob || cleanupBarrier || cleanupFailure) {
    throw new Error("tokenomics control spawn cannot change while work is active");
  }
  testControlSpawn = spawn;
}

export async function _resetTokenomicsWorkerForTest(): Promise<void> {
  try {
    await shutdownTokenomicsWorker();
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
  completed = 0;
  failed = 0;
}

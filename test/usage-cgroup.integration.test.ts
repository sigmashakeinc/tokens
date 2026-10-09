import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";

const ENABLED = process.env.TOKENS_USAGE_CGROUP_INTEGRATION === "1";
const integrationTest = ENABLED ? test : test.skip;
const BASE_URL = process.env.TOKENS_USAGE_CGROUP_BASE_URL ?? "http://127.0.0.1:7913";
const SERVICE = "sigmashake-tokens.service";
const SLICE = "sigmashake-tokens.slice";
const AGGREGATION_WORKER_GLOB = "sigmashake-tokens-usage-*.service";
const INGEST_WORKER_GLOB = "sigmashake-tokens-ingest-*.service";
const SYSTEMCTL = "/usr/bin/systemctl";
const CGROUP_ROOT = "/sys/fs/cgroup";
const MIB = 1024 * 1024;

interface CommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

interface ServiceIdentity {
  mainPid: number;
  invocationId: string;
}

interface UsageBody extends Record<string, unknown> {
  scannedAt: number;
  totals: { messages: number };
  byProject: Array<{ sessions: number }>;
  range: { from: string | null; to: string | null };
}

interface CgroupSnapshot {
  controlGroup: string;
  memoryCurrent: bigint;
  memoryLow: string;
  memoryHigh: string;
  memoryMax: string;
  memoryEvents: Record<string, bigint>;
  cpuMax: string;
  cpuWeight: string;
  cpuStat: Record<string, bigint>;
}

async function command(args: string[], allowedExitCodes = [0]): Promise<CommandResult> {
  const proc = Bun.spawn(args, { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  const [exitCode, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  if (!allowedExitCodes.includes(exitCode)) {
    throw new Error(`${args.join(" ")} exited ${exitCode}: ${stderr.trim()}`);
  }
  return { exitCode, stdout, stderr };
}

function parseProperties(output: string): Record<string, string> {
  const properties: Record<string, string> = {};
  for (const line of output.trim().split("\n")) {
    const separator = line.indexOf("=");
    if (separator < 1) continue;
    properties[line.slice(0, separator)] = line.slice(separator + 1);
  }
  return properties;
}

async function unitProperties(unit: string, names: string[]): Promise<Record<string, string>> {
  const result = await command([
    SYSTEMCTL,
    "--user",
    "show",
    unit,
    ...names.map((name) => `--property=${name}`),
  ]);
  return parseProperties(result.stdout);
}

async function serviceIdentity(): Promise<ServiceIdentity> {
  const properties = await unitProperties(SERVICE, ["MainPID", "InvocationID"]);
  const mainPid = Number(properties.MainPID);
  const invocationId = properties.InvocationID ?? "";
  expect(Number.isSafeInteger(mainPid) && mainPid > 1).toBe(true);
  expect(invocationId.length).toBeGreaterThan(0);
  return { mainPid, invocationId };
}

function parseCounterFile(value: string): Record<string, bigint> {
  const counters: Record<string, bigint> = {};
  for (const line of value.trim().split("\n")) {
    const [name, count] = line.trim().split(/\s+/, 2);
    if (name && count) counters[name] = BigInt(count);
  }
  return counters;
}

async function cgroupSnapshot(unit: string): Promise<CgroupSnapshot> {
  const properties = await unitProperties(unit, ["ControlGroup"]);
  const controlGroup = properties.ControlGroup ?? "";
  expect(controlGroup.startsWith("/")).toBe(true);
  const root = `${CGROUP_ROOT}${controlGroup}`;
  const [
    memoryCurrent,
    memoryLow,
    memoryHigh,
    memoryMax,
    memoryEvents,
    cpuMax,
    cpuWeight,
    cpuStat,
  ] = await Promise.all([
    readFile(`${root}/memory.current`, "utf-8"),
    readFile(`${root}/memory.low`, "utf-8"),
    readFile(`${root}/memory.high`, "utf-8"),
    readFile(`${root}/memory.max`, "utf-8"),
    readFile(`${root}/memory.events`, "utf-8"),
    readFile(`${root}/cpu.max`, "utf-8"),
    readFile(`${root}/cpu.weight`, "utf-8"),
    readFile(`${root}/cpu.stat`, "utf-8"),
  ]);
  return {
    controlGroup,
    memoryCurrent: BigInt(memoryCurrent.trim()),
    memoryLow: memoryLow.trim(),
    memoryHigh: memoryHigh.trim(),
    memoryMax: memoryMax.trim(),
    memoryEvents: parseCounterFile(memoryEvents),
    cpuMax: cpuMax.trim(),
    cpuWeight: cpuWeight.trim(),
    cpuStat: parseCounterFile(cpuStat),
  };
}

function expectQuota(snapshot: CgroupSnapshot, percent: number): void {
  const [quota, period] = snapshot.cpuMax.split(/\s+/, 2);
  expect(quota).not.toBe("max");
  expect((Number(quota) / Number(period)) * 100).toBe(percent);
}

function expectCounters(snapshot: CgroupSnapshot): void {
  expect(snapshot.memoryCurrent).toBeGreaterThanOrEqual(0n);
  expect(snapshot.memoryEvents.max).toBeGreaterThanOrEqual(0n);
  expect(snapshot.memoryEvents.oom).toBeGreaterThanOrEqual(0n);
  expect(snapshot.memoryEvents.oom_kill).toBeGreaterThanOrEqual(0n);
  expect(snapshot.cpuStat.usage_usec).toBeGreaterThanOrEqual(0n);
}

type WorkerKind = "aggregation" | "ingest";

function workerGlob(kind: WorkerKind): string {
  return kind === "aggregation" ? AGGREGATION_WORKER_GLOB : INGEST_WORKER_GLOB;
}

function workerPrefix(kind: WorkerKind): string {
  return kind === "aggregation" ? "sigmashake-tokens-usage-" : "sigmashake-tokens-ingest-";
}

async function workerUnits(kind: WorkerKind = "aggregation"): Promise<string[]> {
  const result = await command([
    SYSTEMCTL,
    "--user",
    "list-units",
    "--all",
    "--plain",
    "--no-legend",
    workerGlob(kind),
  ]);
  return result.stdout
    .split("\n")
    .map((line) => line.trim().split(/\s+/, 1)[0] ?? "")
    .filter((unit) => unit.startsWith(workerPrefix(kind)) && unit.endsWith(".service"));
}

async function waitForWorkerUnit(
  kind: WorkerKind = "aggregation",
  timeoutMs = 10_000,
): Promise<string> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    const units = await workerUnits(kind);
    if (units.length > 0) {
      expect(units).toHaveLength(1);
      return units[0]!;
    }
    await Bun.sleep(20);
  }
  throw new Error(`transient ${kind} unit did not become observable`);
}

async function waitForWorkerMainPid(unit: string, timeoutMs = 10_000): Promise<number> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    const properties = await unitProperties(unit, ["MainPID"]);
    const pid = Number(properties.MainPID);
    if (Number.isSafeInteger(pid) && pid > 1) return pid;
    await Bun.sleep(20);
  }
  throw new Error(`${unit} never published a main PID`);
}

async function exactWorkerIsGone(kind: WorkerKind, unit: string, pid: number): Promise<boolean> {
  const entry = kind === "aggregation" ? "usage-aggregation" : "usage-ingest";
  const [units, processResult] = await Promise.all([
    workerUnits(kind),
    command(["/usr/bin/ps", "-p", String(pid), "-o", "args="], [0, 1]),
  ]);
  return (
    !units.includes(unit) &&
    (processResult.exitCode === 1 || !processResult.stdout.includes(`${entry}.worker.ts`))
  );
}

async function waitForExactWorkerGone(
  kind: WorkerKind,
  unit: string,
  pid: number,
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await exactWorkerIsGone(kind, unit, pid)) return;
    await Bun.sleep(25);
  }
  expect(await exactWorkerIsGone(kind, unit, pid)).toBe(true);
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${BASE_URL}${path}`, {
    signal: AbortSignal.timeout(120_000),
  });
  const text = await response.text();
  expect(response.status, text).toBe(200);
  return JSON.parse(text) as T;
}

async function waitForHealthy(timeoutMs = 30_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    try {
      const response = await fetch(`${BASE_URL}/healthz`, { signal: AbortSignal.timeout(1_000) });
      if (response.status === 200 && ((await response.json()) as { ok?: boolean }).ok === true) {
        return;
      }
    } catch {
      // The restarted listener is not ready yet.
    }
    await Bun.sleep(100);
  }
  throw new Error("tokens service did not become healthy after restart");
}

async function timedHealthProbe(): Promise<number> {
  const startedAt = performance.now();
  const response = await fetch(`${BASE_URL}/healthz`, { signal: AbortSignal.timeout(2_000) });
  const body = (await response.json()) as { ok?: boolean };
  const elapsedMs = performance.now() - startedAt;
  expect(response.status).toBe(200);
  expect(body.ok).toBe(true);
  return elapsedMs;
}

function canonicalUsage(body: UsageBody): Record<string, unknown> {
  const copy = structuredClone(body) as Record<string, unknown>;
  delete copy.scannedAt;
  return copy;
}

async function cleanupTransientUnits(): Promise<void> {
  const units = (
    await Promise.all([
      workerUnits("aggregation").catch(() => []),
      workerUnits("ingest").catch(() => []),
    ])
  ).flat();
  for (const unit of units) {
    await command([SYSTEMCTL, "--user", "stop", unit], [0, 5]).catch(() => undefined);
  }
}

integrationTest(
  "cold production aggregation preserves HTTP health and contains worker failure",
  async () => {
    const suiteStartedAt = performance.now();
    expect(process.platform).toBe("linux");
    expect(await Bun.file(SYSTEMCTL).exists()).toBe(true);
    expect(await Bun.file("/usr/bin/systemd-run").exists()).toBe(true);
    expect(await Bun.file(`${CGROUP_ROOT}/cgroup.controllers`).exists()).toBe(true);

    await cleanupTransientUnits();
    try {
      await command([SYSTEMCTL, "--user", "restart", SERVICE]);
      const initialIngestUnit = await waitForWorkerUnit("ingest");
      const initialIngestPid = await waitForWorkerMainPid(initialIngestUnit);
      const initialIngest = await cgroupSnapshot(initialIngestUnit);
      expect(initialIngest.controlGroup).toContain("sigmashake-tokens.slice");
      expect(initialIngest.memoryHigh).toBe(String(256 * MIB));
      expect(initialIngest.memoryMax).toBe(String(320 * MIB));
      expect(initialIngest.cpuWeight).toBe("100");
      expectQuota(initialIngest, 45);
      expect(await timedHealthProbe()).toBeLessThanOrEqual(500);

      // Crash the parent while its first scan is active. BindsTo must reap the
      // ingest sibling, and Restart=always must restore an immediately ready
      // listener without leaving either worker family behind.
      const firstIdentity = await serviceIdentity();
      await command([SYSTEMCTL, "--user", "kill", "--kill-who=main", "--signal=KILL", SERVICE]);
      await waitForHealthy();
      await waitForExactWorkerGone("ingest", initialIngestUnit, initialIngestPid);
      const postIngestCrashIdentity = await serviceIdentity();
      expect(postIngestCrashIdentity.invocationId).not.toBe(firstIdentity.invocationId);

      let identity = postIngestCrashIdentity;
      const sliceBefore = await cgroupSnapshot(SLICE);
      const serverBefore = await cgroupSnapshot(SERVICE);
      expect(sliceBefore.memoryMax).toBe(String(512 * MIB));
      expectQuota(sliceBefore, 50);
      expect(serverBefore.memoryLow).toBe(String(192 * MIB));
      expect(serverBefore.memoryMax).toBe(String(320 * MIB));
      expect(serverBefore.cpuWeight).toBe("1000");
      expect(serverBefore.cpuMax.startsWith("max ")).toBe(true);
      expectCounters(sliceBefore);
      expectCounters(serverBefore);

      const stats = await fetchJson<{ events: number }>("/api/stats");
      expect(stats.events).toBeGreaterThanOrEqual(1_282_000);

      const coldStartedAt = performance.now();
      const coldRequest = fetchJson<UsageBody>("/api/usage?from=1970-01-01");
      const coldWorkerUnit = await waitForWorkerUnit("aggregation");
      const coldWorkerPid = await waitForWorkerMainPid(coldWorkerUnit);
      const coldWorker = await cgroupSnapshot(coldWorkerUnit);
      expect(coldWorker.controlGroup).toContain("sigmashake-tokens.slice");
      expect(coldWorker.memoryHigh).toBe(String(160 * MIB));
      expect(coldWorker.memoryMax).toBe(String(192 * MIB));
      expect(coldWorker.cpuWeight).toBe("100");
      expectQuota(coldWorker, 35);
      expectCounters(coldWorker);
      expect(coldWorker.memoryCurrent).toBeLessThanOrEqual(BigInt(192 * MIB + 4_096));

      const probeLatencies = await Promise.all(Array.from({ length: 12 }, timedHealthProbe));
      for (const latency of probeLatencies) expect(latency).toBeLessThanOrEqual(500);
      const baseline = await coldRequest;
      expect(performance.now() - coldStartedAt).toBeLessThan(120_000);
      expect(baseline.totals.messages).toBeGreaterThanOrEqual(1_282_000);
      expect(
        baseline.byProject.reduce((sum, project) => sum + project.sessions, 0),
      ).toBeGreaterThanOrEqual(124_000);
      expect(baseline.range.from === null || baseline.range.from > "1970-01-03").toBe(true);
      await waitForExactWorkerGone("aggregation", coldWorkerUnit, coldWorkerPid);
      expect(await serviceIdentity()).toEqual(identity);

      const failedRequest = fetch(`${BASE_URL}/api/usage?from=1970-01-02`, {
        signal: AbortSignal.timeout(30_000),
      });
      const doomedUnit = await waitForWorkerUnit("aggregation");
      const doomedPid = await waitForWorkerMainPid(doomedUnit);
      const doomedWorker = await cgroupSnapshot(doomedUnit);
      expect(doomedWorker.memoryHigh).toBe(String(160 * MIB));
      await command([SYSTEMCTL, "--user", "kill", "--kill-who=main", "--signal=KILL", doomedUnit]);
      const failedResponse = await failedRequest;
      const failedBody = await failedResponse.text();
      expect(failedResponse.status, failedBody).toBe(500);
      expect(failedBody).toContain("aggregation worker");
      await waitForExactWorkerGone("aggregation", doomedUnit, doomedPid);
      expect(await serviceIdentity()).toEqual(identity);
      expect(await timedHealthProbe()).toBeLessThanOrEqual(500);

      const parentCrashRequest = fetch(`${BASE_URL}/api/usage?from=1970-01-04`, {
        signal: AbortSignal.timeout(30_000),
      }).then(
        (response) => ({ kind: "response" as const, status: response.status }),
        (error: unknown) => ({ kind: "error" as const, error: String(error) }),
      );
      const parentBoundUnit = await waitForWorkerUnit("aggregation");
      const parentBoundPid = await waitForWorkerMainPid(parentBoundUnit);
      const lifecycle = await unitProperties(parentBoundUnit, [
        "ActiveState",
        "After",
        "BindsTo",
        "RuntimeMaxUSec",
      ]);
      expect(["active", "activating"]).toContain(lifecycle.ActiveState);
      expect((lifecycle.BindsTo ?? "").split(" ")).toContain(SERVICE);
      expect((lifecycle.After ?? "").split(" ")).toContain(SERVICE);
      expect(lifecycle.RuntimeMaxUSec).toBe("11min");

      await command([SYSTEMCTL, "--user", "kill", "--kill-who=main", "--signal=KILL", SERVICE]);
      const crashOutcome = await Promise.race([
        parentCrashRequest,
        Bun.sleep(30_000).then(() => null),
      ]);
      expect(crashOutcome).not.toBeNull();
      await waitForHealthy();
      await waitForExactWorkerGone("aggregation", parentBoundUnit, parentBoundPid);
      const restartedIdentity = await serviceIdentity();
      expect(restartedIdentity.invocationId).not.toBe(identity.invocationId);
      identity = restartedIdentity;
      expect(await timedHealthProbe()).toBeLessThanOrEqual(500);

      const recoveryStartedAt = performance.now();
      const recoveryRequests = Array.from({ length: 8 }, () =>
        fetchJson<UsageBody>("/api/usage?from=1970-01-03"),
      );
      const recoveryUnit = await waitForWorkerUnit("aggregation");
      const recoveryPid = await waitForWorkerMainPid(recoveryUnit);
      expect(await workerUnits("aggregation")).toEqual([recoveryUnit]);
      const recovered = await Promise.all(recoveryRequests);
      const recoveryReference = recovered[0]!;
      for (const body of recovered.slice(1)) {
        expect(canonicalUsage(body)).toEqual(canonicalUsage(recoveryReference));
      }
      // The source database is live throughout this proof. New committed
      // events may arrive after the cold baseline, so recovery must be
      // coalesced and monotonic rather than byte-equal to an older snapshot.
      expect(recoveryReference.totals.messages).toBeGreaterThanOrEqual(baseline.totals.messages);
      expect(
        recoveryReference.byProject.reduce((sum, project) => sum + project.sessions, 0),
      ).toBeGreaterThanOrEqual(
        baseline.byProject.reduce((sum, project) => sum + project.sessions, 0),
      );
      expect(
        recoveryReference.range.from !== null &&
          baseline.range.from !== null &&
          recoveryReference.range.from <= baseline.range.from,
      ).toBe(true);
      expect(
        recoveryReference.range.to !== null &&
          baseline.range.to !== null &&
          recoveryReference.range.to >= baseline.range.to,
      ).toBe(true);
      expect(performance.now() - recoveryStartedAt).toBeLessThan(120_000);
      await waitForExactWorkerGone("aggregation", recoveryUnit, recoveryPid);
      expect(await serviceIdentity()).toEqual(identity);

      const sliceAfter = await cgroupSnapshot(SLICE);
      const serverAfter = await cgroupSnapshot(SERVICE);
      expectCounters(sliceAfter);
      expectCounters(serverAfter);
      expect(serverAfter.memoryEvents.oom).toBe(serverBefore.memoryEvents.oom);
      expect(serverAfter.memoryEvents.oom_kill).toBe(serverBefore.memoryEvents.oom_kill);
      expect(serverAfter.memoryCurrent).toBeLessThanOrEqual(BigInt(320 * MIB + 4_096));
      expect(performance.now() - suiteStartedAt).toBeLessThan(180_000);
    } finally {
      // A browser may legitimately start a new worker after the exact workers
      // above are gone. Restarting the parent reaps all bound siblings and
      // restores a clean, health-ready listener without racing external polls.
      await command([SYSTEMCTL, "--user", "restart", SERVICE]).catch(() => undefined);
      await waitForHealthy().catch(() => undefined);
    }
  },
  180_000,
);

import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, setSystemTime, spyOn, test } from "bun:test";
import { appendFile, chmod, mkdir, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { selectRateSnapshot } from "../src/rate.ts";
import { readSavings } from "../src/savings.ts";
import {
  _aggregationCacheSizeForTest,
  _aggregationInFlightSizeForTest,
  aggregateEvents,
  ingestOnce,
  invalidateAggregationCache,
  loadAggregations,
} from "../src/scanner.ts";
import { _resetStoreForTests, getStore, TokenStore, type StoredEvent } from "../src/store.ts";
import {
  _captureJsonlSnapshotForTest,
  _readJsonlSnapshotForTest,
  _resetTokenomicsCache,
  analyzeTokenomics,
  getConsumption,
} from "../src/tokenomics.ts";
import {
  _resetUsageAggregationWorkerForTest,
  _setNextUsageAggregationWorkerActionForTest,
  _setUsageAggregationLauncherForTest,
  _usageAggregationWorkerStatsForTest,
  aggregateUsageOffThread,
  shutdownUsageAggregationWorker,
} from "../src/usage-aggregation-worker.ts";

// ─── Fixture helpers ──────────────────────────────────────────────────────────

/** Creates a temp dir, writes token-savings.jsonl, sets env, runs fn, cleans up. */
async function withLedger(lines: string[], fn: () => Promise<void>): Promise<void> {
  const dir = join(tmpdir(), `tok-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  const prev = process.env.SIGMASHAKE_HOME;
  process.env.SIGMASHAKE_HOME = dir;
  try {
    await writeFile(join(dir, "token-savings.jsonl"), lines.join("\n") + "\n");
    await fn();
  } finally {
    if (prev === undefined) delete process.env.SIGMASHAKE_HOME;
    else process.env.SIGMASHAKE_HOME = prev;
    await rm(dir, { recursive: true, force: true });
  }
}

/** Builds a minimal assistant line containing an array of tool_use blocks. */
function assistantLine(
  ts: unknown,
  toolUses: Array<{ id: string; name: string; input: Record<string, unknown> }>,
): string {
  return JSON.stringify({
    type: "assistant",
    timestamp: ts,
    message: {
      role: "assistant",
      model: "claude-sonnet-4-6",
      content: toolUses.map((tu) => ({
        type: "tool_use",
        id: tu.id,
        name: tu.name,
        input: tu.input,
      })),
      usage: { input_tokens: 100, output_tokens: 50 },
    },
  });
}

/** Builds a minimal user line containing tool_result blocks. */
function userResultLine(
  ts: unknown,
  results: Array<{ tool_use_id: string; content: string }>,
): string {
  return JSON.stringify({
    type: "user",
    timestamp: ts,
    message: {
      role: "user",
      content: results.map((r) => ({
        type: "tool_result",
        tool_use_id: r.tool_use_id,
        content: r.content,
      })),
    },
  });
}

function storedEvent(ts: number, overrides: Partial<StoredEvent> = {}): StoredEvent {
  return {
    ts,
    date: new Date(ts).toISOString().slice(0, 10),
    model: "claude-sonnet-4-6",
    project: "sigmashake-tokens",
    projectSlug: "-home-user-sigmashake-tokens",
    sessionId: `session-${ts}`,
    inputTokens: 100,
    outputTokens: 20,
    cacheReadTokens: 30,
    cacheWrite5mTokens: 40,
    cacheWrite1hTokens: 50,
    cost: 0.5,
    filePath: `/fixture/${ts}.jsonl`,
    lineOffset: ts,
    source: "claude",
    ...overrides,
  };
}

async function jsonlSnapshot(path: string) {
  return _captureJsonlSnapshotForTest(path);
}

// ─── 1. Generalised savings: byTool is dynamic ────────────────────────────────

describe("readSavings — generalised byTool", () => {
  test("unknown tool appears in byTool as its own key + in combined", async () => {
    const lines = [
      JSON.stringify({ tool: "graphify", ts: 1000, orig_bytes: 800, comp_bytes: 300 }),
      JSON.stringify({ tool: "graphify", ts: 1001, orig_bytes: 400, comp_bytes: 100 }),
      JSON.stringify({ tool: "stk", ts: 1002, orig_bytes: 1000, comp_bytes: 600 }),
    ];
    await withLedger(lines, async () => {
      const result = await readSavings();

      // graphify has its own bucket
      expect(result.byTool.graphify).toBeDefined();
      expect(result.byTool.graphify!.count).toBe(2);
      expect(result.byTool.graphify!.origBytes).toBe(1200);
      expect(result.byTool.graphify!.compBytes).toBe(400);
      expect(result.byTool.graphify!.savedBytes).toBe(800);
      expect(result.byTool.graphify!.savedPct).toBeCloseTo((800 / 1200) * 100);
      expect(result.byTool.graphify!.savedTokensEst).toBe(Math.round(800 / 4));

      // stk is still in its own bucket
      expect(result.byTool.stk.count).toBe(1);
      expect(result.byTool.stk.origBytes).toBe(1000);

      // headroom is guaranteed present even with zero data
      expect(result.byTool.headroom.count).toBe(0);
      expect(result.byTool.headroom.origBytes).toBe(0);

      // combined sees all 3
      expect(result.combined.count).toBe(3);
      expect(result.combined.origBytes).toBe(2200);

      expect(result.present).toBe(true);
    });
  });

  test("stk and headroom always present even with only foreign tools", async () => {
    const lines = [JSON.stringify({ tool: "oracle", ts: 1000, orig_bytes: 500, comp_bytes: 200 })];
    await withLedger(lines, async () => {
      const result = await readSavings();
      expect(result.byTool.stk.count).toBe(0);
      expect(result.byTool.headroom.count).toBe(0);
      expect(result.byTool.oracle).toBeDefined();
      expect(result.byTool.oracle!.count).toBe(1);
      expect(result.combined.count).toBe(1);
    });
  });

  test("stk and headroom always present when ledger is missing", async () => {
    const dir = join(tmpdir(), `tok-test-empty-${Date.now()}`);
    await mkdir(dir, { recursive: true });
    const prev = process.env.SIGMASHAKE_HOME;
    process.env.SIGMASHAKE_HOME = dir;
    try {
      const result = await readSavings();
      expect(result.present).toBe(false);
      expect(result.byTool.stk.count).toBe(0);
      expect(result.byTool.headroom.count).toBe(0);
    } finally {
      if (prev === undefined) delete process.env.SIGMASHAKE_HOME;
      else process.env.SIGMASHAKE_HOME = prev;
      await rm(dir, { recursive: true, force: true });
    }
  });
});

// ─── 2. Tokenomics analyzer findings ─────────────────────────────────────────

describe("analyzeTokenomics — findings", () => {
  let tmpRoot: string;
  let prevProjectsDir: string | undefined;
  let prevCodexSessionsDir: string | undefined;
  let prevSigmashakeHome: string | undefined;
  let prevTokensDbPath: string | undefined;
  let prevWindowH: string | undefined;
  let prevCacheTtl: string | undefined;
  let prevUsageCacheTtl: string | undefined;
  let prevUsageWorkerTimeout: string | undefined;

  beforeEach(async () => {
    tmpRoot = join(tmpdir(), `tok-eff-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    await mkdir(join(tmpRoot, "projects"), { recursive: true });
    await mkdir(join(tmpRoot, "codex-sessions"), { recursive: true });
    await mkdir(join(tmpRoot, "home"), { recursive: true });

    // Stash and override env
    prevProjectsDir = process.env.CLAUDE_PROJECTS_DIR;
    prevCodexSessionsDir = process.env.CODEX_SESSIONS_DIR;
    prevSigmashakeHome = process.env.SIGMASHAKE_HOME;
    prevTokensDbPath = process.env.TOKENS_DB_PATH;
    prevWindowH = process.env.TOKENOMICS_WINDOW_H;
    prevCacheTtl = process.env.TOKENOMICS_CACHE_TTL_MS;
    prevUsageCacheTtl = process.env.TOKENS_USAGE_CACHE_TTL_MS;
    prevUsageWorkerTimeout = process.env.TOKENS_USAGE_WORKER_TIMEOUT_MS;

    process.env.CLAUDE_PROJECTS_DIR = join(tmpRoot, "projects");
    process.env.CODEX_SESSIONS_DIR = join(tmpRoot, "codex-sessions");
    process.env.SIGMASHAKE_HOME = join(tmpRoot, "home");
    process.env.TOKENS_DB_PATH = join(tmpRoot, "tokens.sqlite");
    process.env.TOKENOMICS_WINDOW_H = "9999"; // include all files
    process.env.TOKENOMICS_CACHE_TTL_MS = "0"; // disable cache for tests

    _resetStoreForTests();
    _resetTokenomicsCache();
    await _resetUsageAggregationWorkerForTest();
    _setUsageAggregationLauncherForTest("direct");
    invalidateAggregationCache();
  });

  afterEach(async () => {
    setSystemTime();
    await _resetUsageAggregationWorkerForTest();
    _resetStoreForTests();
    _resetTokenomicsCache();
    invalidateAggregationCache();

    // Restore env
    if (prevProjectsDir === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
    else process.env.CLAUDE_PROJECTS_DIR = prevProjectsDir;
    if (prevCodexSessionsDir === undefined) delete process.env.CODEX_SESSIONS_DIR;
    else process.env.CODEX_SESSIONS_DIR = prevCodexSessionsDir;
    if (prevSigmashakeHome === undefined) delete process.env.SIGMASHAKE_HOME;
    else process.env.SIGMASHAKE_HOME = prevSigmashakeHome;
    if (prevTokensDbPath === undefined) delete process.env.TOKENS_DB_PATH;
    else process.env.TOKENS_DB_PATH = prevTokensDbPath;
    if (prevWindowH === undefined) delete process.env.TOKENOMICS_WINDOW_H;
    else process.env.TOKENOMICS_WINDOW_H = prevWindowH;
    if (prevCacheTtl === undefined) delete process.env.TOKENOMICS_CACHE_TTL_MS;
    else process.env.TOKENOMICS_CACHE_TTL_MS = prevCacheTtl;
    if (prevUsageCacheTtl === undefined) delete process.env.TOKENS_USAGE_CACHE_TTL_MS;
    else process.env.TOKENS_USAGE_CACHE_TTL_MS = prevUsageCacheTtl;
    if (prevUsageWorkerTimeout === undefined) delete process.env.TOKENS_USAGE_WORKER_TIMEOUT_MS;
    else process.env.TOKENS_USAGE_WORKER_TIMEOUT_MS = prevUsageWorkerTimeout;

    await rm(tmpRoot, { recursive: true, force: true });
  });

  test("re-read-loop: same file read 3+ times in a session emits finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-a");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:00:00.000Z";
    const lines = [
      assistantLine(ts, [{ id: "t1", name: "Read", input: { file_path: "/src/foo.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: "content" }]),
      assistantLine(ts, [{ id: "t2", name: "Read", input: { file_path: "/src/foo.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t2", content: "content" }]),
      assistantLine(ts, [{ id: "t3", name: "Read", input: { file_path: "/src/foo.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t3", content: "content" }]),
    ];
    await writeFile(join(projDir, "session-aaa.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const rereadFindings = result.efficiency.findings.filter((f) => f.kind === "re-read-loop");
    expect(rereadFindings.length).toBeGreaterThanOrEqual(1);
    expect(rereadFindings[0]!.count).toBeGreaterThanOrEqual(3);
    expect(rereadFindings[0]!.detail).toContain("/src/foo.ts");
    expect(rereadFindings[0]!.severity).toBe("info");
  });

  test("re-read-loop: file read only 2 times does NOT emit finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-b");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:00:00.000Z";
    const lines = [
      assistantLine(ts, [{ id: "t1", name: "Read", input: { file_path: "/src/bar.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: "content" }]),
      assistantLine(ts, [{ id: "t2", name: "Read", input: { file_path: "/src/bar.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t2", content: "content" }]),
    ];
    await writeFile(join(projDir, "session-bbb.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const rereadFindings = result.efficiency.findings.filter(
      (f) => f.kind === "re-read-loop" && f.detail.includes("/src/bar.ts"),
    );
    expect(rereadFindings.length).toBe(0);
  });

  test("oversized-output: tool_result > 32KB emits finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-c");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:01:00.000Z";
    const bigContent = "x".repeat(33 * 1024); // 33KB > 32KB threshold
    const lines = [
      assistantLine(ts, [{ id: "t1", name: "Read", input: { file_path: "/big.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: bigContent }]),
    ];
    await writeFile(join(projDir, "session-ccc.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const oversized = result.efficiency.findings.filter((f) => f.kind === "oversized-output");
    expect(oversized.length).toBeGreaterThanOrEqual(1);
    expect(oversized[0]!.severity).toBe("warn");
    expect(oversized[0]!.detail).toContain("KB");
  });

  test("oversized-output: stk result NOT flagged even if large", async () => {
    const projDir = join(tmpRoot, "projects", "proj-stk");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:02:00.000Z";
    const bigContent = "y".repeat(33 * 1024);
    const lines = [
      assistantLine(ts, [{ id: "t1", name: "Bash", input: { command: "stk cat /some/file" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: bigContent }]),
    ];
    await writeFile(join(projDir, "session-stk.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    // stk results should not be flagged as oversized
    const oversized = result.efficiency.findings.filter(
      (f) => f.kind === "oversized-output" && f.sessionId === "session-stk",
    );
    expect(oversized.length).toBe(0);
  });

  test("reducer-bypass: verbose Bash with large result emits finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-d");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:03:00.000Z";
    // > 8KB bypass threshold, verbose cmd (cat), no stk/headroom wrapper
    const largeOutput = "z".repeat(9 * 1024);
    const lines = [
      assistantLine(ts, [
        { id: "t1", name: "Bash", input: { command: "cat /some/large/file.log" } },
      ]),
      userResultLine(ts, [{ tool_use_id: "t1", content: largeOutput }]),
    ];
    await writeFile(join(projDir, "session-ddd.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const bypass = result.efficiency.findings.filter((f) => f.kind === "reducer-bypass");
    expect(bypass.length).toBeGreaterThanOrEqual(1);
    expect(bypass[0]!.severity).toBe("warn");
    expect(bypass[0]!.detail).toContain("KB");
  });

  test("reducer-bypass: verbose Bash piped through stk NOT flagged", async () => {
    const projDir = join(tmpRoot, "projects", "proj-e");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:04:00.000Z";
    const largeOutput = "w".repeat(9 * 1024);
    const lines = [
      assistantLine(ts, [
        { id: "t1", name: "Bash", input: { command: "cat /some/large/file.log | stk" } },
      ]),
      userResultLine(ts, [{ tool_use_id: "t1", content: largeOutput }]),
    ];
    await writeFile(join(projDir, "session-eee.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const bypass = result.efficiency.findings.filter(
      (f) => f.kind === "reducer-bypass" && f.sessionId === "session-eee",
    );
    expect(bypass.length).toBe(0);
  });

  test("observedByTool tallies Read, Edit/Write, verbose-Bash, graphify categories", async () => {
    const projDir = join(tmpRoot, "projects", "proj-f");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:05:00.000Z";
    const lines = [
      assistantLine(ts, [
        { id: "t1", name: "Read", input: { file_path: "/a.ts" } },
        { id: "t2", name: "Edit", input: { file_path: "/b.ts", old_string: "x", new_string: "y" } },
        { id: "t3", name: "Bash", input: { command: "graphify query test" } },
        { id: "t4", name: "Bash", input: { command: "cat /some/file" } },
      ]),
      userResultLine(ts, [
        { tool_use_id: "t1", content: "file" },
        { tool_use_id: "t2", content: "ok" },
        { tool_use_id: "t3", content: "result" },
        { tool_use_id: "t4", content: "output" },
      ]),
    ];
    await writeFile(join(projDir, "session-fff.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const obs = result.efficiency.observedByTool;

    expect(obs["Read"]).toBeDefined();
    expect(obs["Read"]!.count).toBeGreaterThanOrEqual(1);

    expect(obs["Edit/Write"]).toBeDefined();
    expect(obs["Edit/Write"]!.count).toBeGreaterThanOrEqual(1);

    expect(obs["graphify"]).toBeDefined();
    expect(obs["graphify"]!.count).toBeGreaterThanOrEqual(1);

    expect(obs["verbose-Bash"]).toBeDefined();
    expect(obs["verbose-Bash"]!.count).toBeGreaterThanOrEqual(1);
  });

  test("consumption has separate allTime and window keys (SPENT / AVOIDED separation)", async () => {
    const result = await analyzeTokenomics();
    // Structural contract: consumption (SPENT) and savings (AVOIDED) are separate
    expect(result).toHaveProperty("consumption");
    expect(result).toHaveProperty("savings");
    expect(result.consumption).toHaveProperty("allTime");
    expect(result.consumption).toHaveProperty("window");
    expect(result.consumption).toHaveProperty("windowHours");
    // There must be no merged/net combined key at the top level
    expect((result as Record<string, unknown>).combined).toBeUndefined();
    expect((result as Record<string, unknown>).net).toBeUndefined();
  });

  test("consumption uses constant-memory SQL totals without selecting all event rows", async () => {
    const now = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(now));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const store = getStore();
    store.insertEvents([
      storedEvent(now - 2 * 3_600_000, { inputTokens: 10, lineOffset: 1 }),
      storedEvent(now - 30 * 60_000, { inputTokens: 20, lineOffset: 2 }),
    ]);
    const selectEvents = spyOn(store, "selectEvents").mockImplementation(() => {
      throw new Error("tokenomics must not materialize event rows");
    });
    try {
      const result = await analyzeTokenomics();
      expect(result.consumption.allTime.messages).toBe(2);
      expect(result.consumption.allTime.inputTokens).toBe(30);
      expect(result.consumption.window.messages).toBe(1);
      expect(result.consumption.window.inputTokens).toBe(20);
      expect(selectEvents).not.toHaveBeenCalled();
    } finally {
      selectEvents.mockRestore();
    }
  });

  test("request-path usage aggregation never materializes matching event rows", async () => {
    const now = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(now));
    const store = getStore();
    store.insertEvents([
      storedEvent(now - 1_000, { inputTokens: 10, lineOffset: 1 }),
      storedEvent(now, { inputTokens: 20, lineOffset: 2 }),
    ]);
    const selectEvents = spyOn(store, "selectEvents").mockImplementation(() => {
      throw new Error("usage aggregation must not materialize event rows");
    });
    try {
      const result = await loadAggregations({ rescan: false, cache: false });
      expect(result.totals.messages).toBe(2);
      expect(result.totals.inputTokens).toBe(30);
      expect(result.topSessions).toHaveLength(2);
      expect(selectEvents).not.toHaveBeenCalled();
    } finally {
      selectEvents.mockRestore();
    }
  });

  test("SQL usage snapshots preserve the complete in-memory aggregation contract", async () => {
    const now = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(now));
    const store = getStore();
    store.insertEvents([
      storedEvent(now - 86_400_000, {
        model: "claude-sonnet-4-6",
        project: "project-a",
        projectSlug: "-workspace-project-a",
        sessionId: "session-a",
        inputTokens: 100,
        outputTokens: 10,
        cacheReadTokens: 1,
        cacheWrite5mTokens: 2,
        cacheWrite1hTokens: 3,
        cost: 4,
        lineOffset: 1,
      }),
      storedEvent(now - 43_200_000, {
        model: "gpt-5.5-codex",
        project: "project-b",
        projectSlug: "-workspace-project-b",
        sessionId: "session-b",
        source: "codex",
        inputTokens: 200,
        outputTokens: 20,
        cacheReadTokens: 2,
        cacheWrite5mTokens: 4,
        cacheWrite1hTokens: 6,
        cost: 3,
        lineOffset: 2,
      }),
      storedEvent(now - 21_600_000, {
        model: "claude-opus-4-1",
        project: "project-a",
        projectSlug: "-workspace-project-a",
        sessionId: "session-a",
        inputTokens: 300,
        outputTokens: 30,
        cacheReadTokens: 3,
        cacheWrite5mTokens: 6,
        cacheWrite1hTokens: 9,
        cost: 2,
        lineOffset: 3,
      }),
      storedEvent(now, {
        model: "claude-haiku-4-5",
        project: "project-a",
        projectSlug: "-workspace-project-a",
        sessionId: "session-c",
        inputTokens: 400,
        outputTokens: 40,
        cacheReadTokens: 4,
        cacheWrite5mTokens: 8,
        cacheWrite1hTokens: 12,
        cost: 1,
        lineOffset: 4,
      }),
    ]);

    const expectedAll = aggregateEvents(store.selectEvents(), { files: 0, scannedAt: now });
    const actualAll = await loadAggregations({ rescan: false, cache: false });
    expect(actualAll).toEqual(expectedAll);

    const from = "2026-07-12";
    const models = ["claude-sonnet-4-6", "claude-opus-4-1"];
    const projects = ["project-a"];
    const fromTs = Date.parse(`${from}T00:00:00Z`);
    const expectedFiltered = aggregateEvents(
      store
        .selectEvents({ fromTs })
        .filter((event) => models.includes(event.model) && projects.includes(event.project)),
      { files: 0, scannedAt: now },
    );
    const actualFiltered = await loadAggregations({
      from,
      models,
      projects,
      rescan: false,
      cache: false,
    });
    expect(actualFiltered).toEqual(expectedFiltered);

    store.insertEvents(
      Array.from({ length: 30 }, (_, index) =>
        storedEvent(now + index + 1, {
          model: "rank-model",
          project: "rank-project",
          projectSlug: "-workspace-rank-project",
          sessionId: `rank-session-${index}`,
          inputTokens: index + 1,
          outputTokens: 0,
          cacheReadTokens: 0,
          cacheWrite5mTokens: 0,
          cacheWrite1hTokens: 0,
          cost: 100 + index,
          lineOffset: 100 + index,
        }),
      ),
    );
    const expectedRanked = aggregateEvents(store.selectEvents(), { files: 0, scannedAt: now });
    const actualRanked = await loadAggregations({ rescan: false, cache: false });
    expect(actualRanked.topSessions).toEqual(expectedRanked.topSessions);
    expect(actualRanked.topSessions).toHaveLength(25);
    expect(
      actualRanked.byProject.find((project) => project.project === "rank-project")?.sessions,
    ).toBe(30);
  });

  test("date-filtered /api/usage byHarness rows carry a non-null harness label that reconciles with byModel and totals", async () => {
    const now = Date.parse("2026-07-20T12:00:00.000Z");
    setSystemTime(new Date(now));
    const store = getStore();
    store.insertEvents([
      storedEvent(now - 3_600_000, {
        model: "claude-sonnet-4-6",
        source: "claude",
        project: "project-a",
        projectSlug: "-workspace-project-a",
        sessionId: "session-claude-a",
        inputTokens: 100,
        outputTokens: 10,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        cost: 1,
        lineOffset: 1,
      }),
      storedEvent(now - 1_800_000, {
        model: "claude-opus-4-1",
        source: "claude",
        project: "project-a",
        projectSlug: "-workspace-project-a",
        sessionId: "session-claude-b",
        inputTokens: 200,
        outputTokens: 20,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        cost: 2,
        lineOffset: 2,
      }),
      storedEvent(now - 900_000, {
        model: "gpt-5-codex",
        source: "codex",
        project: "project-b",
        projectSlug: "-workspace-project-b",
        sessionId: "session-codex-a",
        inputTokens: 300,
        outputTokens: 30,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        cost: 3,
        lineOffset: 3,
      }),
      // Outside the filter window entirely — must not leak into the
      // date-filtered byHarness totals.
      storedEvent(now - 30 * 86_400_000, {
        model: "claude-haiku-4-5",
        source: "claude",
        project: "project-a",
        projectSlug: "-workspace-project-a",
        sessionId: "session-claude-old",
        inputTokens: 9999,
        outputTokens: 999,
        cost: 99,
        lineOffset: 4,
      }),
    ]);

    const from = "2026-07-20";
    const to = "2026-07-20";
    const result = await loadAggregations({ from, to, rescan: false, cache: false });

    expect(result.byHarness.length).toBe(2);
    for (const bucket of result.byHarness) {
      // The vacuous version of this assertion only checks length/totals —
      // the historical defect (source vs harness key drift) reproduces with
      // both of those passing while the label itself is missing.
      expect(bucket.harness).not.toBeNull();
      expect(["claude", "codex"]).toContain(bucket.harness);
    }
    const claudeHarness = result.byHarness.find((h) => h.harness === "claude");
    const codexHarness = result.byHarness.find((h) => h.harness === "codex");
    expect(claudeHarness?.totalTokens).toBe(330); // 110 + 220, excludes the 30-day-old event
    expect(codexHarness?.totalTokens).toBe(330);

    // Reconciliation: sum(byHarness) === totals, and byModel grouped by its
    // own `harness` field reconciles with the corresponding byHarness row —
    // both must be derived from the same normalized identity, never a
    // second, divergent derivation.
    const harnessTotalSum = result.byHarness.reduce((sum, h) => sum + h.totalTokens, 0);
    expect(harnessTotalSum).toBe(result.totals.totalTokens);

    const byModelGroupedByHarness = new Map<string, number>();
    for (const model of result.byModel) {
      byModelGroupedByHarness.set(
        model.harness,
        (byModelGroupedByHarness.get(model.harness) ?? 0) + model.totalTokens,
      );
    }
    for (const bucket of result.byHarness) {
      expect(byModelGroupedByHarness.get(bucket.harness)).toBe(bucket.totalTokens);
    }
  });

  test("usage cursor preserves first-seen order, stable ties, floats, and filtered identity", async () => {
    const now = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(now));
    const store = getStore();
    const events: StoredEvent[] = [
      storedEvent(now + 5_000, {
        model: "model-z",
        project: "order-project",
        projectSlug: "-workspace-order-project",
        sessionId: "order-session",
        cost: 0,
        lineOffset: 1,
      }),
      storedEvent(now + 5_000, {
        model: "model-a",
        project: "order-project",
        projectSlug: "-workspace-order-project",
        sessionId: "order-session",
        cost: 0,
        lineOffset: 2,
      }),
      storedEvent(now + 5_000, {
        model: "model-z",
        project: "order-project",
        projectSlug: "-workspace-order-project",
        sessionId: "order-session",
        cost: 0,
        lineOffset: 3,
      }),
      // Inserted last but timestamped first: ordering is `(ts, id)`, while
      // same-timestamp ties above retain insertion/row-id order.
      storedEvent(now + 4_999, {
        model: "model-b",
        project: "order-project",
        projectSlug: "-workspace-order-project",
        sessionId: "order-session",
        cost: 0,
        lineOffset: 4,
      }),
      // Global JS addition is 1 + 1e16 - 1e16 === 0. Regrouping the two
      // dates and adding their subtotals would incorrectly produce 1.
      storedEvent(now + 1_000, {
        date: "2026-07-14",
        project: "float-project",
        projectSlug: "-workspace-float-project",
        sessionId: "float-session",
        cost: 1,
        lineOffset: 5,
      }),
      storedEvent(now + 2_000, {
        date: "2026-07-13",
        project: "float-project",
        projectSlug: "-workspace-float-project",
        sessionId: "float-session",
        cost: 1e16,
        lineOffset: 6,
      }),
      storedEvent(now + 3_000, {
        date: "2026-07-13",
        project: "float-project",
        projectSlug: "-workspace-float-project",
        sessionId: "float-session",
        cost: -1e16,
        lineOffset: 7,
      }),
      storedEvent(now + 6_000, {
        model: "excluded-model",
        project: "identity-project",
        projectSlug: "-workspace-excluded-identity",
        sessionId: "identity-session",
        cost: 0,
        lineOffset: 8,
      }),
      storedEvent(now + 6_001, {
        model: "included-model",
        project: "identity-project",
        projectSlug: "-workspace-filtered-identity",
        sessionId: "identity-session",
        cost: 0,
        lineOffset: 9,
      }),
      ...Array.from({ length: 27 }, (_, index) =>
        storedEvent(now + 7_000, {
          model: "tie-model",
          project: "tie-project",
          projectSlug: "-workspace-tie-project",
          sessionId: `tie-session-${String(26 - index).padStart(2, "0")}`,
          inputTokens: 1,
          outputTokens: 0,
          cacheReadTokens: 0,
          cacheWrite5mTokens: 0,
          cacheWrite1hTokens: 0,
          cost: 10,
          lineOffset: 100 + index,
        }),
      ),
    ];
    store.insertEvents(events);

    const expectedFor = (predicate: (event: StoredEvent) => boolean) =>
      aggregateEvents(store.selectEvents().filter(predicate), { files: 0, scannedAt: now });

    const orderActual = await loadAggregations({
      projects: ["order-project"],
      rescan: false,
      cache: false,
    });
    expect(orderActual).toEqual(expectedFor((event) => event.project === "order-project"));
    expect(orderActual.topSessions[0]?.models).toEqual(["model-b", "model-z", "model-a"]);

    const floatActual = await loadAggregations({
      projects: ["float-project"],
      rescan: false,
      cache: false,
    });
    expect(floatActual).toEqual(expectedFor((event) => event.project === "float-project"));
    expect(floatActual.totals.cost).toBe(0);
    expect(floatActual.daily.map((day) => [day.date, day.cost])).toEqual([
      ["2026-07-13", 0],
      ["2026-07-14", 1],
    ]);

    const identityActual = await loadAggregations({
      models: ["included-model"],
      projects: ["identity-project"],
      rescan: false,
      cache: false,
    });
    expect(identityActual).toEqual(
      expectedFor(
        (event) => event.model === "included-model" && event.project === "identity-project",
      ),
    );
    expect(identityActual.byProject[0]?.projectSlug).toBe("-workspace-filtered-identity");
    expect(identityActual.topSessions[0]?.projectSlug).toBe("-workspace-filtered-identity");

    const tieActual = await loadAggregations({
      projects: ["tie-project"],
      rescan: false,
      cache: false,
    });
    expect(tieActual).toEqual(expectedFor((event) => event.project === "tie-project"));
    expect(tieActual.topSessions.map((session) => session.sessionId)).toEqual(
      events
        .filter((event) => event.project === "tie-project")
        .slice(0, 25)
        .map((event) => event.sessionId),
    );
  });

  test("one-million-row usage snapshot has bounded JS heap and response cardinality", async () => {
    const now = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(now));
    const store = getStore();
    const raw = new Database(process.env.TOKENS_DB_PATH!);
    try {
      raw.exec(`
          WITH digits(d) AS (
            VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)
          ), million(n) AS (
            SELECT a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d + 100000*f.d
            FROM digits a, digits b, digits c, digits d, digits e, digits f
          )
          INSERT INTO events (
            ts, date, model, project, project_slug, session_id,
            input_tokens, output_tokens, cache_read_tokens,
            cache_write_5m_tokens, cache_write_1h_tokens, cost,
            file_path, line_offset, source
          )
          SELECT
            ${now} + n, '2026-07-13', 'claude-sonnet-4-6', 'scale-project',
            '-workspace-scale-project', 'scale-session',
            1, 2, 3, 4, 5, 0.25, '/fixture/scale.jsonl', n, 'claude'
          FROM million
        `);
    } finally {
      raw.close();
    }
    invalidateAggregationCache();
    const selectEvents = spyOn(store, "selectEvents").mockImplementation(() => {
      throw new Error("million-row usage must stay inside SQL roll-ups");
    });
    const heapBefore = process.memoryUsage().heapUsed;
    const rssBefore = process.memoryUsage().rss;
    let peakRss = rssBefore;
    let eventLoopTicks = 0;
    const rssSampler = setInterval(() => {
      eventLoopTicks++;
      peakRss = Math.max(peakRss, process.memoryUsage().rss);
    }, 10);
    try {
      const aggregationStarted = performance.now();
      const result = await loadAggregations({ rescan: false, cache: false });
      const aggregationMs = performance.now() - aggregationStarted;
      const heapGrowth = process.memoryUsage().heapUsed - heapBefore;
      expect(result.totals).toMatchObject({
        messages: 1_000_000,
        inputTokens: 1_000_000,
        outputTokens: 2_000_000,
        totalTokens: 15_000_000,
        cost: 250_000,
      });
      expect(result.daily).toHaveLength(1);
      expect(result.byModel).toHaveLength(1);
      expect(result.byProject).toHaveLength(1);
      expect(result.topSessions).toHaveLength(1);
      expect(result.byHarness).toHaveLength(1);
      expect(JSON.stringify(result).length).toBeLessThan(16_384);
      expect(heapGrowth).toBeLessThan(64 * 1024 * 1024);
      expect(eventLoopTicks).toBeGreaterThanOrEqual(10);
      expect(peakRss).toBeLessThan(512 * 1024 * 1024);
      expect(peakRss - rssBefore).toBeLessThan(384 * 1024 * 1024);
      expect(aggregationMs).toBeLessThan(20_000);
      expect(selectEvents).not.toHaveBeenCalled();
    } finally {
      clearInterval(rssSampler);
      selectEvents.mockRestore();
    }
  }, 60_000);

  test("authoritative store failures reject instead of returning stale or all-zero tokenomics", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "0";
    const store = getStore();
    const successful = await analyzeTokenomics();
    expect(successful.consumption.allTime.messages).toBe(0);

    const aggregateEventTotals = spyOn(store, "aggregateEventTotals").mockImplementation(() => {
      throw new Error("injected authoritative store failure");
    });
    try {
      await expect(analyzeTokenomics()).rejects.toThrow("injected authoritative store failure");
    } finally {
      aggregateEventTotals.mockRestore();
    }
  });

  test("read snapshot isolates consumption from a writer commit between aggregate windows", async () => {
    const dbPath = join(tmpRoot, "consumption-race.sqlite");
    const writer = new TokenStore(dbPath);
    const reader = new TokenStore(dbPath, { readonly: true });
    try {
      const now = Date.parse("2026-07-13T04:00:00.000Z");
      setSystemTime(new Date(now));
      writer.insertEvents([
        storedEvent(now - 2 * 3_600_000, { inputTokens: 10, lineOffset: 1 }),
        storedEvent(now - 30 * 60_000, { inputTokens: 20, lineOffset: 2 }),
      ]);
      const realAgg = reader.aggregateEventTotals.bind(reader);
      let bumpLine = 3;
      const installBumpSpy = () => {
        let aggCalls = 0;
        return spyOn(reader, "aggregateEventTotals").mockImplementation((opts) => {
          const result = realAgg(opts);
          if (++aggCalls === 1) {
            writer.insertEvents([
              storedEvent(now - 10 * 60_000, {
                inputTokens: 99,
                lineOffset: bumpLine++,
              }),
            ]);
          }
          return result;
        });
      };

      // Repro: without withReadSnapshot a mid-read writer commit rejects.
      const revisionBefore = reader.getRevision();
      const raceSpy = installBumpSpy();
      try {
        const withoutSnapshot = {
          getRevision: () => reader.getRevision(),
          aggregateEventTotals: reader.aggregateEventTotals.bind(reader),
        };
        await expect(getConsumption(withoutSnapshot, 1, revisionBefore)).rejects.toThrow(
          "tokenomics authority revision changed during consumption read",
        );
      } finally {
        raceSpy.mockRestore();
      }

      // Fix: the same bump during a snapshotted read stays consistent.
      const revision = reader.getRevision();
      const baseline = reader.aggregateEventTotals();
      const snapSpy = installBumpSpy();
      try {
        const consumption = await getConsumption(reader, 1, revision);
        expect(consumption.allTime.messages).toBe(baseline.messages);
        expect(consumption.allTime.inputTokens).toBe(baseline.inputTokens);
        expect(reader.getRevision()).toBeGreaterThan(revision);
      } finally {
        snapSpy.mockRestore();
      }
    } finally {
      reader.close();
      writer.close();
      setSystemTime();
    }
  });

  test("stale expected revision and real aggregate failures still fail closed", async () => {
    const store = getStore();
    await expect(getConsumption(store, 72, store.getRevision() - 1)).rejects.toThrow(
      "tokenomics authority revision changed before consumption read",
    );
    const aggregateEventTotals = spyOn(store, "aggregateEventTotals").mockImplementation(() => {
      throw new Error("injected aggregate failure");
    });
    try {
      await expect(getConsumption(store, 72, store.getRevision())).rejects.toThrow(
        "injected aggregate failure",
      );
    } finally {
      aggregateEventTotals.mockRestore();
    }
  });

  // chmod 000 does not block reads on Windows, so the unreadable-file case cannot be built there.
  test.skipIf(process.platform === "win32")("an unreadable JSONL source is skipped and reported as incomplete, not a total outage", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "0";
    const projectDir = join(tmpRoot, "projects", "unreadable-source");
    const okDir = join(tmpRoot, "projects", "readable-source");
    const path = join(projectDir, "session.jsonl");
    await mkdir(projectDir, { recursive: true });
    await mkdir(okDir, { recursive: true });
    const ts = new Date().toISOString();
    await writeFile(path, `${assistantLine(ts, [])}\n`);
    await writeFile(
      join(okDir, "session-ok.jsonl"),
      `${assistantLine(ts, [{ id: "ok", name: "Read", input: { file_path: "/ok.ts" } }])}\n`,
    );
    await chmod(path, 0o000);

    try {
      const result = await analyzeTokenomics();
      expect(result.efficiency.incomplete).toBe(true);
      expect(result.efficiency.skippedFiles).toBeGreaterThanOrEqual(1);
      // The readable file's data still counts — one bad file does not blank
      // out everything else.
      expect(result.efficiency.filesScanned).toBeGreaterThanOrEqual(1);
      expect(result.efficiency.observedByTool["Read"]?.count).toBeGreaterThanOrEqual(1);
    } finally {
      await chmod(path, 0o600);
    }
  });

  test("an unreadable savings ledger rejects instead of reporting present=false zeros", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "0";
    await mkdir(join(tmpRoot, "home", "token-savings.jsonl"));

    await expect(analyzeTokenomics()).rejects.toThrow();
  });

  test("an over-limit JSONL record is skipped and reported honestly, never silently or fatally", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "0";
    const projectDir = join(tmpRoot, "projects", "over-limit-source");
    await mkdir(projectDir, { recursive: true });
    const ts = "2026-06-27T10:00:00.000Z";
    const oversizedLine = "x".repeat(1024 * 1024 + 1);
    const validLine = assistantLine(ts, [
      { id: "t1", name: "Read", input: { file_path: "/valid-after-oversized.ts" } },
    ]);
    // Oversized record straddles a valid record on either side so both the
    // "skip mid-stream" and "resume after skip" paths are exercised.
    await writeFile(
      join(projectDir, "session.jsonl"),
      `${validLine}\n${oversizedLine}\n${oversizedLine}\n${validLine}\n`,
    );

    // Must SUCCEED — no throw, no total outage over one bad record.
    const result = await analyzeTokenomics();

    // Incompleteness must be explicit, not silently absorbed into a
    // confident-looking complete number.
    expect(result.efficiency.incomplete).toBe(true);
    expect(result.efficiency.skippedRecords).toBe(2);
    expect(result.efficiency.skippedRecordBytes).toBeGreaterThanOrEqual(2 * 1024 * 1024);
    expect(result.efficiency.capped).toBe(false);

    // The two valid, non-oversized records around the skipped lines must
    // still be fully counted — the skip must not corrupt or drop them.
    expect(result.efficiency.observedByTool["Read"]?.count).toBe(2);
    expect(result.efficiency.filesScanned).toBe(1);
  });

  test("request-path aggregation cache is revision-aware and truthfully timestamped", async () => {
    const frozen = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(frozen));
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const store = getStore();
    store.insertEvents([storedEvent(frozen - 1_000, { inputTokens: 100, lineOffset: 1 })]);

    const first = await loadAggregations({ rescan: false });
    expect(first.scannedAt).toBe(frozen);
    expect(first.totals.inputTokens).toBe(100);

    store.insertEvents([storedEvent(frozen, { inputTokens: 200, lineOffset: 2 })]);
    setSystemTime(new Date(frozen + 59_999));
    const refreshed = await loadAggregations({ rescan: false });
    expect(refreshed).not.toBe(first);
    expect(refreshed.scannedAt).toBe(frozen + 59_999);
    expect(refreshed.totals.inputTokens).toBe(300);

    setSystemTime(new Date(frozen + 60_000));
    const cached = await loadAggregations({ rescan: false });
    expect(cached).toBe(refreshed);
    expect(cached.scannedAt).toBe(frozen + 59_999);
  });

  test("identical aggregation misses coalesce and cached query cardinality stays bounded", async () => {
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);

    const [first, second, third] = await Promise.all([
      loadAggregations({ rescan: false }),
      loadAggregations({ rescan: false }),
      loadAggregations({ rescan: false }),
    ]);
    expect(second).toBe(first);
    expect(third).toBe(first);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({
      spawned: 1,
      dispatched: 1,
      completed: 1,
      active: false,
    });

    for (let i = 0; i < 20; i++) {
      await loadAggregations({ rescan: false, models: [`missing-model-${i}`] });
    }
    expect(_aggregationCacheSizeForTest()).toBe(16);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({
      spawned: 21,
      dispatched: 21,
      completed: 21,
      active: false,
    });
  });

  test("oversized worker requests fail before launch and do not poison recovery", async () => {
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);
    const oversizedModels = Array.from({ length: 4_096 }, (_, i) => `model-${i}-${"x".repeat(20)}`);

    await expect(
      aggregateUsageOffThread(store.databasePath, {
        fromTs: 0,
        toTs: Number.MAX_SAFE_INTEGER,
        models: oversizedModels,
      }),
    ).rejects.toThrow("aggregation request exceeds 65536 bytes");
    const recovered = await aggregateUsageOffThread(store.databasePath, {
      fromTs: 0,
      toTs: Number.MAX_SAFE_INTEGER,
    });

    expect(recovered.snapshot.totals.messages).toBe(1);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({
      spawned: 1,
      dispatched: 1,
      completed: 1,
      failed: 1,
      active: false,
    });
  });

  test("a slow worker aggregation leaves the main event loop responsive", async () => {
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);
    _setNextUsageAggregationWorkerActionForTest({ delayMs: 200 });

    let timerTicks = 0;
    const timer = setInterval(() => timerTicks++, 10);
    try {
      const result = await loadAggregations({ rescan: false });
      expect(result.totals.messages).toBe(1);
      expect(timerTicks).toBeGreaterThanOrEqual(5);
    } finally {
      clearInterval(timer);
    }
  });

  test("queued aggregations are stamped when dispatched, not when admitted", async () => {
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);
    _setNextUsageAggregationWorkerActionForTest({ delayMs: 200 });

    const first = loadAggregations({ rescan: false, models: ["claude-sonnet-4-6"] });
    const second = loadAggregations({ rescan: false, projects: ["sigmashake-tokens"] });
    for (let attempts = 0; attempts < 100; attempts++) {
      const stats = _usageAggregationWorkerStatsForTest();
      if (stats.dispatched === 1 && stats.queued === 1) break;
      await Bun.sleep(2);
    }
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({ dispatched: 1, queued: 1 });

    const secondDispatchTime = Date.now() + 60_000;
    setSystemTime(new Date(secondDispatchTime));
    await first;
    expect((await second).scannedAt).toBe(secondDispatchTime);
  });

  test("default rescan mode bypasses and invalidates request-path aggregation snapshots", async () => {
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const cached = await loadAggregations({ rescan: false });
    expect(_aggregationCacheSizeForTest()).toBe(1);

    const rescanned = await loadAggregations();

    expect(rescanned).not.toBe(cached);
    expect(_aggregationCacheSizeForTest()).toBe(0);
  });

  test("invalidation bypasses stale data and a failed old generation cannot poison recovery", async () => {
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { inputTokens: 100, lineOffset: 1 })]);
    _setNextUsageAggregationWorkerActionForTest("crash");

    const failedOldGeneration = loadAggregations({ rescan: false });
    invalidateAggregationCache();
    store.insertEvents([storedEvent(Date.now() + 1, { inputTokens: 200, lineOffset: 2 })]);
    const recovered = loadAggregations({ rescan: false });
    const [failedOutcome, recoveredOutcome] = await Promise.allSettled([
      failedOldGeneration,
      recovered,
    ]);

    expect(failedOutcome.status).toBe("rejected");
    expect(String(failedOutcome.status === "rejected" && failedOutcome.reason)).toContain(
      "aggregation worker crashed",
    );
    expect(recoveredOutcome.status).toBe("fulfilled");
    if (recoveredOutcome.status !== "fulfilled") throw recoveredOutcome.reason;
    expect(recoveredOutcome.value.totals.inputTokens).toBe(300);
    expect(await loadAggregations({ rescan: false })).toBe(recoveredOutcome.value);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({
      spawned: 2,
      completed: 1,
      active: false,
    });

    store.insertEvents([storedEvent(Date.now() + 2, { inputTokens: 400, lineOffset: 3 })]);
    const bypassed = await loadAggregations({ rescan: false, cache: false });
    expect(bypassed.totals.inputTokens).toBe(700);
    expect(bypassed).not.toBe(recoveredOutcome.value);
  });

  test("worker timeout retires the stuck isolate and the next request recovers", async () => {
    process.env.TOKENS_USAGE_WORKER_TIMEOUT_MS = "1000";
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);
    _setNextUsageAggregationWorkerActionForTest({ delayMs: 1_500 });

    await expect(
      aggregateUsageOffThread(store.databasePath, { fromTs: 0, toTs: Number.MAX_SAFE_INTEGER }),
    ).rejects.toThrow("aggregation worker timed out");
    const recovered = await aggregateUsageOffThread(store.databasePath, {
      fromTs: 0,
      toTs: Number.MAX_SAFE_INTEGER,
    });

    expect(recovered.snapshot.totals.messages).toBe(1);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({ spawned: 2, completed: 1 });
  }, 3_000);

  test("read-only schema errors neither migrate nor poison the worker's next database switch", async () => {
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);
    const options = { fromTs: 0, toTs: Number.MAX_SAFE_INTEGER };
    expect(
      (await aggregateUsageOffThread(store.databasePath, options)).snapshot.totals.messages,
    ).toBe(1);

    const incompatiblePath = join(tmpRoot, "incompatible-read-only.sqlite");
    const incompatible = new Database(incompatiblePath);
    incompatible.exec("PRAGMA user_version = 4");
    incompatible.close();
    await expect(aggregateUsageOffThread(incompatiblePath, options)).rejects.toThrow(
      "aggregation worker failed",
    );
    const inspected = new Database(incompatiblePath, { readonly: true });
    try {
      expect(
        (inspected.query("PRAGMA user_version").get() as { user_version: number }).user_version,
      ).toBe(4);
      expect(
        inspected.query("SELECT name FROM sqlite_master WHERE type = 'table'").all(),
      ).toHaveLength(0);
    } finally {
      inspected.close();
    }

    const recovered = await aggregateUsageOffThread(store.databasePath, options);
    expect(recovered.snapshot.totals.messages).toBe(1);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({ spawned: 3, completed: 2 });
  });

  test("shutdown rejects active and queued work without preventing a clean restart", async () => {
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);
    const options = { fromTs: 0, toTs: Number.MAX_SAFE_INTEGER };
    _setNextUsageAggregationWorkerActionForTest({ delayMs: 200 });
    const active = aggregateUsageOffThread(store.databasePath, options);
    const queued = aggregateUsageOffThread(store.databasePath, {
      ...options,
      models: ["claude-sonnet-4-6"],
    });
    const settled = Promise.allSettled([active, queued]);
    expect(_usageAggregationWorkerStatsForTest()).toMatchObject({ active: true, queued: 1 });

    await shutdownUsageAggregationWorker();
    const outcomes = await settled;
    expect(outcomes).toHaveLength(2);
    expect(outcomes.every((outcome) => outcome.status === "rejected")).toBe(true);
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") expect(String(outcome.reason)).toContain("shut down");
    }

    const recovered = await aggregateUsageOffThread(store.databasePath, options);
    expect(recovered.snapshot.totals.messages).toBe(1);
  });

  test("concurrent aggregation admission stays bounded and repeated active keys still coalesce", async () => {
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const store = getStore();
    store.insertEvents([storedEvent(Date.now(), { lineOffset: 1 })]);

    const distinct = Array.from({ length: 17 }, (_, i) =>
      loadAggregations({ rescan: false, models: [`admission-model-${i}`] }),
    );
    const repeatedFirst = loadAggregations({
      rescan: false,
      models: ["admission-model-0"],
    });

    expect(_aggregationInFlightSizeForTest()).toBe(16);
    const outcomes = await Promise.allSettled(distinct);
    expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(16);
    const rejected = outcomes.filter(
      (outcome): outcome is PromiseRejectedResult => outcome.status === "rejected",
    );
    expect(rejected).toHaveLength(1);
    expect(String(rejected[0]!.reason)).toContain("aggregation admission limit reached");

    const firstOutcome = outcomes[0];
    if (firstOutcome?.status !== "fulfilled") throw firstOutcome?.reason;
    expect(await repeatedFirst).toBe(firstOutcome.value);
    expect(_usageAggregationWorkerStatsForTest().dispatched).toBe(16);
    expect(_aggregationInFlightSizeForTest()).toBe(0);
  });

  test("JSONL snapshot excludes appended bytes and releases the file handle", async () => {
    const path = join(tmpRoot, "growing.jsonl");
    await writeFile(path, '{"record":"captured"}\n');
    const snapshot = await jsonlSnapshot(path);
    await appendFile(path, '{"record":"appended-after-stat"}\n');

    const result = await _readJsonlSnapshotForTest(snapshot);

    expect(result.status).toBe("scanned");
    expect(result.bytesRead).toBe(snapshot.size);
    expect(result.lines).toEqual(['{"record":"captured"}']);
    const moved = `${path}.moved`;
    await rename(path, moved);
    await rename(moved, path);
  });

  test("JSONL snapshot rejects a same-size replacement and cleans up its descriptor", async () => {
    const path = join(tmpRoot, "replaced.jsonl");
    const replacement = join(tmpRoot, "replacement.jsonl");
    const displaced = join(tmpRoot, "displaced.jsonl");
    await writeFile(path, '{"record":"original"}\n');
    const snapshot = await jsonlSnapshot(path);
    await writeFile(replacement, '{"record":"replaced"}\n');
    await rename(path, displaced);
    await rename(replacement, path);

    const result = await _readJsonlSnapshotForTest(snapshot);

    expect(result).toMatchObject({ status: "changed", bytesRead: 0, lines: [] });
    const movedAgain = `${path}.again`;
    await rename(path, movedAgain);
    await rename(movedAgain, path);
  });

  test("JSONL snapshot rejects a same-inode same-length rewrite before reading", async () => {
    const path = join(tmpRoot, "rewritten-in-place.jsonl");
    const original = '{"record":"original"}\n';
    const rewritten = '{"record":"mutated!"}\n';
    expect(rewritten.length).toBe(original.length);
    await writeFile(path, original);
    const snapshot = await jsonlSnapshot(path);

    await writeFile(path, rewritten);
    const rewrittenStat = await stat(path);
    expect(rewrittenStat.ino).toBe(snapshot.ino);
    await utimes(path, new Date(snapshot.mtime + 5_000), new Date(snapshot.mtime + 5_000));

    const result = await _readJsonlSnapshotForTest(snapshot);

    expect(result).toMatchObject({ status: "changed", bytesRead: 0, lines: [] });
    const moved = `${path}.moved`;
    await rename(path, moved);
    await rename(moved, path);
  });

  test("JSONL snapshot rejects a captured-prefix rewrite hidden behind an append", async () => {
    const path = join(tmpRoot, "rewritten-and-appended.jsonl");
    const original = '{"record":"original"}\n';
    const rewritten = '{"record":"mutated!"}\n';
    expect(rewritten.length).toBe(original.length);
    await writeFile(path, original);
    const snapshot = await jsonlSnapshot(path);

    await writeFile(path, rewritten);
    await appendFile(path, '{"record":"appended"}\n');
    const rewrittenStat = await stat(path);
    expect(rewrittenStat.ino).toBe(snapshot.ino);

    const result = await _readJsonlSnapshotForTest(snapshot);

    expect(result).toMatchObject({
      status: "changed",
      bytesRead: snapshot.size,
      lines: [],
    });
    const moved = `${path}.moved`;
    await rename(path, moved);
    await rename(moved, path);
  });

  test("JSONL snapshot drops an over-limit line without hiding later bounded records", async () => {
    const path = join(tmpRoot, "long-line.jsonl");
    const valid = '{"record":"after-long-line"}';
    const oversized = "x".repeat(1024 * 1024 + 1);
    await writeFile(path, `${oversized}\n${valid}\n`);
    const snapshot = await jsonlSnapshot(path);

    const result = await _readJsonlSnapshotForTest(snapshot);

    expect(result.status).toBe("scanned");
    expect(result.bytesRead).toBe(snapshot.size);
    expect(result.lineCapped).toBe(true);
    expect(result.lines).toEqual([valid]);
    expect(result.skippedLines).toBe(1);
    expect(result.skippedBytes).toBeGreaterThanOrEqual(oversized.length);
  });

  test("JSONL snapshot counts an over-limit final line with no trailing newline as skipped", async () => {
    const path = join(tmpRoot, "long-line-no-trailing-newline.jsonl");
    const valid = '{"record":"before-long-line"}';
    const oversized = "y".repeat(1024 * 1024 + 1);
    // No trailing newline after the oversized line — it must still be
    // counted as skipped, not silently dropped uncounted at EOF.
    await writeFile(path, `${valid}\n${oversized}`);
    const snapshot = await jsonlSnapshot(path);

    const result = await _readJsonlSnapshotForTest(snapshot);

    expect(result.status).toBe("scanned");
    expect(result.lineCapped).toBe(true);
    expect(result.lines).toEqual([valid]);
    expect(result.skippedLines).toBe(1);
    expect(result.skippedBytes).toBeGreaterThanOrEqual(oversized.length);
  });

  test("malformed JSONL lines are skipped, rest of session still processed", async () => {
    const projDir = join(tmpRoot, "projects", "proj-g");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:06:00.000Z";
    const lines = [
      "not json at all",
      "",
      assistantLine(ts, [{ id: "t1", name: "Read", input: { file_path: "/ok.ts" } }]),
      "{ broken",
      userResultLine(ts, [{ tool_use_id: "t1", content: "fine" }]),
    ];
    await writeFile(join(projDir, "session-ggg.jsonl"), lines.join("\n") + "\n");

    // Should not throw, should still count the valid Read call
    const result = await analyzeTokenomics();
    expect(result.efficiency.filesScanned).toBeGreaterThanOrEqual(1);
    const obs = result.efficiency.observedByTool;
    expect(obs["Read"]?.count).toBeGreaterThanOrEqual(1);
  });

  test("recent file excludes records older than the efficiency window", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-old-record");
    await mkdir(projDir, { recursive: true });
    const oldTs = "2026-07-13T02:59:59.999Z";
    await writeFile(
      join(projDir, "session-old-record.jsonl"),
      [
        assistantLine(oldTs, [{ id: "old", name: "Read", input: { file_path: "/old.ts" } }]),
        userResultLine(oldTs, [{ tool_use_id: "old", content: "x".repeat(33 * 1024) }]),
      ].join("\n") + "\n",
    );

    const result = await analyzeTokenomics();
    expect(result.efficiency.filesScanned).toBe(1);
    expect(result.efficiency.observedByTool["Read"]).toBeUndefined();
    expect(result.efficiency.findings).toHaveLength(0);
  });

  test("file mtime remains the coarse prefilter even when records are fresh", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-old-file");
    await mkdir(projDir, { recursive: true });
    const path = join(projDir, "session-old-file.jsonl");
    const freshTs = "2026-07-13T03:30:00.000Z";
    await writeFile(
      path,
      [
        assistantLine(freshTs, [{ id: "fresh", name: "Read", input: { file_path: "/fresh.ts" } }]),
        userResultLine(freshTs, [{ tool_use_id: "fresh", content: "x".repeat(33 * 1024) }]),
      ].join("\n") + "\n",
    );
    const oldMtime = new Date("2026-07-13T02:59:59.999Z");
    await utimes(path, oldMtime, oldMtime);

    const result = await analyzeTokenomics();
    expect(result.efficiency.filesScanned).toBe(0);
    expect(result.efficiency.findings).toHaveLength(0);
  });

  test("record exactly at the cutoff is eligible", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-boundary");
    await mkdir(projDir, { recursive: true });
    const cutoffTs = "2026-07-13T03:00:00.000Z";
    await writeFile(
      join(projDir, "session-boundary.jsonl"),
      [
        assistantLine(cutoffTs, [
          { id: "boundary", name: "Read", input: { file_path: "/boundary.ts" } },
        ]),
        userResultLine(cutoffTs, [{ tool_use_id: "boundary", content: "x".repeat(33 * 1024) }]),
      ].join("\n") + "\n",
    );

    const result = await analyzeTokenomics();
    expect(result.efficiency.observedByTool["Read"]?.count).toBe(1);
    expect(result.efficiency.findings.filter((f) => f.kind === "oversized-output")).toHaveLength(1);
  });

  test("missing, non-string, invalid, non-ISO, and impossible timestamps fall back to mtime", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-fallback");
    await mkdir(projDir, { recursive: true });
    const timestamps: unknown[] = [
      undefined,
      1783915200000,
      "not-a-date",
      "2026-07-13 03:30:00Z",
      "2026-02-30T03:30:00.000Z",
    ];
    const lines: string[] = [];
    timestamps.forEach((ts, i) => {
      const id = `fallback-${i}`;
      const assistant = JSON.parse(
        assistantLine(ts, [{ id, name: "Read", input: { file_path: `/fallback-${i}.ts` } }]),
      ) as Record<string, unknown>;
      if (i === 0) {
        (assistant.message as Record<string, unknown>).timestamp = "2026-07-13T02:00:00.000Z";
      }
      lines.push(JSON.stringify(assistant));
      lines.push(userResultLine(ts, [{ tool_use_id: id, content: "x".repeat(33 * 1024) }]));
    });
    await writeFile(join(projDir, "session-fallback.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    expect(result.efficiency.observedByTool["Read"]?.count).toBe(5);
    expect(result.efficiency.findings.filter((f) => f.kind === "oversized-output")).toHaveLength(5);
  });

  test("tool correlation is exact-ID, latest-preceding, and never looks ahead", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-correlation");
    await mkdir(projDir, { recursive: true });
    const freshTs = "2026-07-13T03:30:00.000Z";
    const big = "x".repeat(33 * 1024);
    const lines = [
      userResultLine(freshTs, [{ tool_use_id: "future", content: big }]),
      assistantLine(freshTs, [
        { id: "future", name: "Bash", input: { command: "stk cat /future" } },
        { id: "stk", name: "Bash", input: { command: "stk cat /large" } },
        { id: "read", name: "Read", input: { file_path: "/large.ts" } },
      ]),
      userResultLine(freshTs, [
        { tool_use_id: "read", content: big },
        { tool_use_id: "stk", content: big },
      ]),
      assistantLine(freshTs, [
        { id: "duplicate", name: "Bash", input: { command: "stk cat /old" } },
      ]),
      assistantLine(freshTs, [{ id: "duplicate", name: "Read", input: { file_path: "/new.ts" } }]),
      userResultLine(freshTs, [{ tool_use_id: "duplicate", content: big }]),
    ];
    await writeFile(join(projDir, "session-correlation.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const oversized = result.efficiency.findings.filter((f) => f.kind === "oversized-output");
    expect(oversized.map((f) => f.detail)).toEqual([
      "unknown result 33KB exceeds 32KB threshold",
      "Read result 33KB exceeds 32KB threshold",
      "Read result 33KB exceeds 32KB threshold",
    ]);
  });

  test("old tools classify fresh results, including reducer exemptions", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-old-tool");
    await mkdir(projDir, { recursive: true });
    const oldTs = "2026-07-13T02:00:00.000Z";
    const freshTs = "2026-07-13T03:30:00.000Z";
    const big = "x".repeat(33 * 1024);
    await writeFile(
      join(projDir, "session-old-tool.jsonl"),
      [
        assistantLine(oldTs, [
          { id: "old-read", name: "Read", input: { file_path: "/old.ts" } },
          { id: "old-stk", name: "Bash", input: { command: "stk cat /old" } },
        ]),
        userResultLine(freshTs, [
          { tool_use_id: "old-read", content: big },
          { tool_use_id: "old-stk", content: big },
        ]),
      ].join("\n") + "\n",
    );

    const result = await analyzeTokenomics();
    expect(result.efficiency.observedByTool["Read"]).toBeUndefined();
    expect(result.efficiency.observedByTool["stk"]).toBeUndefined();
    const oversized = result.efficiency.findings.filter((f) => f.kind === "oversized-output");
    expect(oversized.map((f) => f.detail)).toEqual(["Read result 33KB exceeds 32KB threshold"]);
  });

  test("fresh tool calls tally but old results cannot emit warnings", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-old-result");
    await mkdir(projDir, { recursive: true });
    const freshTs = "2026-07-13T03:30:00.000Z";
    const oldTs = "2026-07-13T02:00:00.000Z";
    await writeFile(
      join(projDir, "session-old-result.jsonl"),
      [
        assistantLine(freshTs, [
          { id: "fresh-read", name: "Read", input: { file_path: "/fresh.ts" } },
        ]),
        userResultLine(oldTs, [{ tool_use_id: "fresh-read", content: "x".repeat(33 * 1024) }]),
      ].join("\n") + "\n",
    );

    const result = await analyzeTokenomics();
    expect(result.efficiency.observedByTool["Read"]?.count).toBe(1);
    expect(result.efficiency.findings.filter((f) => f.kind === "oversized-output")).toHaveLength(0);
  });

  test("old Read suppresses fresh Write without inflating fresh re-read counts", async () => {
    setSystemTime(new Date("2026-07-13T04:00:00.000Z"));
    process.env.TOKENOMICS_WINDOW_H = "1";
    const projDir = join(tmpRoot, "projects", "window-read-history");
    await mkdir(projDir, { recursive: true });
    const oldTs = "2026-07-13T02:00:00.000Z";
    const freshTs = "2026-07-13T03:30:00.000Z";
    const path = "/src/existing.ts";
    await writeFile(
      join(projDir, "session-read-history.jsonl"),
      [
        assistantLine(oldTs, [{ id: "old-read", name: "Read", input: { file_path: path } }]),
        assistantLine(freshTs, [
          { id: "write", name: "Write", input: { file_path: path, content: "new" } },
        ]),
        assistantLine(freshTs, [{ id: "fresh-read-1", name: "Read", input: { file_path: path } }]),
        assistantLine(freshTs, [{ id: "fresh-read-2", name: "Read", input: { file_path: path } }]),
      ].join("\n") + "\n",
    );

    const result = await analyzeTokenomics();
    expect(result.efficiency.observedByTool["Read"]?.count).toBe(2);
    expect(result.efficiency.observedByTool["Edit/Write"]?.count).toBe(1);
    expect(result.efficiency.ponytail.newFiles).toBe(0);
    expect(result.efficiency.findings.some((f) => f.kind === "ponytail-new-file")).toBe(false);
    expect(result.efficiency.findings.some((f) => f.kind === "re-read-loop")).toBe(false);
  });

  test("cache remains authoritative until its TTL expires", async () => {
    const frozen = Date.parse("2026-07-13T04:00:00.000Z");
    setSystemTime(new Date(frozen));
    process.env.TOKENOMICS_WINDOW_H = "1";
    process.env.TOKENOMICS_CACHE_TTL_MS = "60000";
    const projDir = join(tmpRoot, "projects", "window-cache");
    await mkdir(projDir, { recursive: true });
    const path = join(projDir, "session-cache.jsonl");
    const ts = "2026-07-13T03:30:00.000Z";
    const smallLines = [
      assistantLine(ts, [{ id: "cached", name: "Read", input: { file_path: "/cached.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "cached", content: "small" }]),
    ];
    await writeFile(path, smallLines.join("\n") + "\n");

    const first = await analyzeTokenomics();
    expect(first.efficiency.findings).toHaveLength(0);
    await writeFile(
      path,
      [
        ...smallLines,
        assistantLine(ts, [{ id: "new", name: "Read", input: { file_path: "/new.ts" } }]),
        userResultLine(ts, [{ tool_use_id: "new", content: "x".repeat(33 * 1024) }]),
      ].join("\n") + "\n",
    );

    setSystemTime(new Date(frozen + 59_999));
    const cached = await analyzeTokenomics();
    expect(cached).toBe(first);
    expect(cached.efficiency.findings).toHaveLength(0);

    setSystemTime(new Date(frozen + 60_000));
    const refreshed = await analyzeTokenomics();
    expect(refreshed).not.toBe(first);
    expect(refreshed.efficiency.findings.filter((f) => f.kind === "oversized-output")).toHaveLength(
      1,
    );
  });

  test("concurrent cache misses share one in-flight tokenomics computation", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "0";

    const [first, second, third] = await Promise.all([
      analyzeTokenomics(),
      analyzeTokenomics(),
      analyzeTokenomics(),
    ]);

    expect(second).toBe(first);
    expect(third).toBe(first);
  });

  test("store revision changes invalidate usage and tokenomics caches immediately", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "60000";
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const firstTokenomics = await analyzeTokenomics();
    const firstUsage = await loadAggregations({ rescan: false });
    expect(firstTokenomics.consumption.allTime.totalTokens).toBe(0);
    expect(firstUsage.totals.totalTokens).toBe(0);

    const now = Date.parse("2026-07-15T20:00:00.000Z");
    getStore().insertEvents([
      storedEvent(now, {
        inputTokens: 80,
        cacheReadTokens: 20,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        outputTokens: 10,
        lineOffset: 99,
      }),
    ]);

    const refreshedTokenomics = await analyzeTokenomics();
    const refreshedUsage = await loadAggregations({ rescan: false });
    expect(refreshedTokenomics).not.toBe(firstTokenomics);
    expect(refreshedTokenomics.consumption.allTime.totalTokens).toBe(110);
    expect(refreshedUsage.totals.totalTokens).toBe(110);
  });

  test("inflated Codex v2 fork repair invalidates every downward accounting surface", async () => {
    process.env.TOKENOMICS_CACHE_TTL_MS = "60000";
    process.env.TOKENS_USAGE_CACHE_TTL_MS = "60000";
    const now = Date.parse("2026-07-15T11:00:05.000Z");
    setSystemTime(new Date(now));
    const sessionDir = join(tmpRoot, "codex-sessions", "2026", "07", "15");
    await mkdir(sessionDir, { recursive: true });
    const parentId = "019f6520-0000-7000-8000-000000000020";
    const childId = "019f6521-0000-7000-8000-000000000021";
    const parentPath = join(sessionDir, "a-parent.jsonl");
    const forkPath = join(sessionDir, "b-fork.jsonl");
    const token = (
      timestamp: string,
      total: {
        input_tokens: number;
        cached_input_tokens: number;
        output_tokens: number;
        reasoning_output_tokens: number;
      },
      last = total,
    ) =>
      JSON.stringify({
        type: "event_msg",
        timestamp,
        payload: {
          type: "token_count",
          info: { last_token_usage: last, total_token_usage: total },
        },
      });
    const parentMeta = JSON.stringify({
      type: "session_meta",
      timestamp: "2026-07-15T10:00:00.000Z",
      payload: {
        id: parentId,
        session_id: parentId,
        thread_source: "user",
        cwd: "/srv/example/sigmashake-tokens",
        model: "gpt-5.5-codex",
      },
    });
    const parentRows = [
      token("2026-07-15T10:00:01.000Z", {
        input_tokens: 100,
        cached_input_tokens: 20,
        output_tokens: 10,
        reasoning_output_tokens: 4,
      }),
      token(
        "2026-07-15T10:00:02.000Z",
        {
          input_tokens: 200,
          cached_input_tokens: 40,
          output_tokens: 30,
          reasoning_output_tokens: 12,
        },
        {
          input_tokens: 100,
          cached_input_tokens: 20,
          output_tokens: 20,
          reasoning_output_tokens: 8,
        },
      ),
    ];
    const forkRows = [
      JSON.stringify({
        type: "session_meta",
        timestamp: "2026-07-15T11:00:00.000Z",
        payload: {
          id: childId,
          session_id: parentId,
          forked_from_id: parentId,
          thread_source: "subagent",
          parent_thread_id: parentId,
          source: {
            subagent: {
              thread_spawn: { parent_thread_id: parentId, depth: 1 },
            },
          },
          cwd: "/srv/example/sigmashake-tokens",
          model: "gpt-5.5-codex",
        },
      }),
      parentMeta,
      token("2026-07-15T11:00:01.000Z", {
        input_tokens: 100,
        cached_input_tokens: 20,
        output_tokens: 10,
        reasoning_output_tokens: 4,
      }),
      token(
        "2026-07-15T11:00:02.000Z",
        {
          input_tokens: 200,
          cached_input_tokens: 40,
          output_tokens: 30,
          reasoning_output_tokens: 12,
        },
        {
          input_tokens: 100,
          cached_input_tokens: 20,
          output_tokens: 20,
          reasoning_output_tokens: 8,
        },
      ),
      JSON.stringify({
        type: "inter_agent_communication_metadata",
        timestamp: "2026-07-15T11:00:03.000Z",
        payload: { trigger_turn: true },
      }),
      token(
        "2026-07-15T11:00:04.000Z",
        {
          input_tokens: 230,
          cached_input_tokens: 45,
          output_tokens: 35,
          reasoning_output_tokens: 14,
        },
        {
          input_tokens: 30,
          cached_input_tokens: 5,
          output_tokens: 5,
          reasoning_output_tokens: 2,
        },
      ),
    ];
    await writeFile(parentPath, `${[parentMeta, ...parentRows].join("\n")}\n`);
    await writeFile(forkPath, `${forkRows.join("\n")}\n`);

    const store = getStore();
    const initial = await ingestOnce(store);
    expect(initial.newEvents).toBe(3);
    const forkStat = await stat(forkPath);
    const forkState = store.getLatestSourceFile(
      "codex",
      String(forkStat.dev),
      String(forkStat.ino),
    )!;
    const lineBounds = forkRows.map((row, lineNumber, rows) => {
      const byteStart = rows
        .slice(0, lineNumber)
        .reduce((sum, previous) => sum + Buffer.byteLength(previous, "utf8") + 1, 0);
      return {
        lineNumber,
        byteStart,
        byteEnd: byteStart + Buffer.byteLength(row, "utf8") + 1,
      };
    });
    const inflatedIds: number[] = [];
    const db = new Database(process.env.TOKENS_DB_PATH!);
    try {
      const checkpoint = JSON.parse(forkState.codexStateJson!) as Record<string, unknown>;
      checkpoint.usageParserVersion = 2;
      db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
        JSON.stringify(checkpoint),
        forkState.generationKey,
      );
      const insertInflated = db.prepare(
        "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, source, canonical_key, canonical_ordinal, raw_record_sha256, class_input_tokens, class_cached_read_tokens, class_cache_write_tokens, class_visible_output_tokens, class_reasoning_output_tokens, classes_complete) VALUES (?, '2026-07-15', 'gpt-5.5-codex', 'sigmashake-tokens', '-home-user-ssd-sigmashake-tokens', ?, ?, ?, ?, 0, 0, 0, ?, ?, 'codex', ?, 0, ?, ?, ?, NULL, ?, ?, 0)",
      );
      const insertOccurrence = db.prepare(
        "INSERT INTO event_occurrences (event_id, canonical_key, generation_key, observed_path, byte_start, byte_end, line_number, raw_record_sha256, logical_message_key, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
      );
      for (const inflated of [
        {
          lineNumber: 2,
          ts: Date.parse("2026-07-15T11:00:01.000Z"),
          input: 80,
          cached: 20,
          output: 10,
          visible: 6,
          reasoning: 4,
        },
        {
          lineNumber: 3,
          ts: Date.parse("2026-07-15T11:00:02.000Z"),
          input: 80,
          cached: 20,
          output: 20,
          visible: 12,
          reasoning: 8,
        },
      ]) {
        const canonicalKey = `legacy-inflated-${inflated.lineNumber}`;
        const rawRecordSha256 = `legacy-raw-${inflated.lineNumber}`;
        const inserted = insertInflated.run(
          inflated.ts,
          childId,
          inflated.input,
          inflated.output,
          inflated.cached,
          forkPath,
          inflated.lineNumber,
          canonicalKey,
          rawRecordSha256,
          inflated.input,
          inflated.cached,
          inflated.visible,
          inflated.reasoning,
        );
        const eventId = Number(inserted.lastInsertRowid);
        inflatedIds.push(eventId);
        const bounds = lineBounds[inflated.lineNumber]!;
        insertOccurrence.run(
          eventId,
          canonicalKey,
          forkState.generationKey,
          forkPath,
          bounds.byteStart,
          bounds.byteEnd,
          bounds.lineNumber,
          rawRecordSha256,
          now,
        );
      }
    } finally {
      db.close();
    }

    const revisionBefore = store.getRevision();
    const usageBefore = await loadAggregations({ rescan: false });
    const tokenomicsBefore = await analyzeTokenomics();
    const rateBefore = selectRateSnapshot(store, now);
    const classesBefore = store.buildAuthoritativeTokenSnapshot(now);
    expect(usageBefore.totals.totalTokens).toBe(495);
    expect(tokenomicsBefore.consumption.allTime.totalTokens).toBe(495);
    expect(rateBefore.buckets[0]?.tokens).toBe(265);
    expect(classesBefore.providers.codex.discoveryComplete).toBe(false);
    expect(classesBefore.providers.codex.classes.input).toEqual({ tokens: null, complete: false });
    expect(classesBefore.providers.codex.classes.visibleOutput).toEqual({
      tokens: null,
      complete: false,
    });

    const repaired = await ingestOnce(store);
    expect(repaired.revision).toBe(revisionBefore + 1);
    expect(repaired.newEvents).toBe(0);
    const usageAfter = await loadAggregations({ rescan: false });
    const tokenomicsAfter = await analyzeTokenomics();
    const rateAfter = selectRateSnapshot(store, now);
    const classesAfter = store.buildAuthoritativeTokenSnapshot(now);
    expect(usageAfter).not.toBe(usageBefore);
    expect(tokenomicsAfter).not.toBe(tokenomicsBefore);
    expect(usageAfter.totals.totalTokens).toBe(265);
    expect(usageAfter.daily).toEqual([
      expect.objectContaining({ date: "2026-07-15", totalTokens: 265, messages: 3 }),
    ]);
    expect(usageAfter.byHarness.find((bucket) => bucket.harness === "codex")).toEqual(
      expect.objectContaining({ totalTokens: 265, messages: 3 }),
    );
    expect(usageAfter.totals.tokensPerSecond).toBeLessThan(usageBefore.totals.tokensPerSecond!);
    expect(tokenomicsAfter.consumption.allTime.totalTokens).toBe(265);
    expect(rateAfter.buckets[0]?.tokens).toBe(35);
    expect(rateAfter.buckets[0]?.tokensPerSecond).toBeLessThan(
      rateBefore.buckets[0]!.tokensPerSecond,
    );
    expect(classesAfter.revision).toBe(repaired.revision);
    expect(classesAfter.rawTotals.inputTokens).toBe(185);
    expect(classesAfter.rawTotals.rawInputTokens).toBe(230);
    expect(classesAfter.rawTotals.outputTokens).toBe(35);
    expect(classesAfter.providers.codex.classes.input.tokens).toBe(185);
    expect(classesAfter.providers.codex.classes.cachedRead.tokens).toBe(45);
    expect(classesAfter.providers.codex.classes.visibleOutput.tokens).toBe(21);
    expect(classesAfter.providers.codex.classes.reasoningOutput.tokens).toBe(14);
    expect(store.selectEvents().filter((event) => event.sessionId === parentId)).toHaveLength(2);
    expect(store.selectEvents().filter((event) => event.sessionId === childId)).toHaveLength(1);
    const inspected = new Database(process.env.TOKENS_DB_PATH!, { readonly: true });
    try {
      const remaining = inspected
        .query(`SELECT id FROM events WHERE id IN (${inflatedIds.map(() => "?").join(",")})`)
        .all(...inflatedIds);
      expect(remaining).toHaveLength(0);
    } finally {
      inspected.close();
    }
  });

  test("cap respected: MAX_FINDINGS limits findings list length", async () => {
    // Create a session with many re-read paths to exhaust the cap
    const projDir = join(tmpRoot, "projects", "proj-h");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T10:07:00.000Z";
    const lines: string[] = [];
    // 110 distinct file paths each read 3 times — would produce 110 findings
    for (let i = 0; i < 110; i++) {
      for (let j = 1; j <= 3; j++) {
        const id = `t${i * 3 + j}`;
        lines.push(assistantLine(ts, [{ id, name: "Read", input: { file_path: `/f${i}.ts` } }]));
        lines.push(userResultLine(ts, [{ tool_use_id: id, content: "x" }]));
      }
    }
    await writeFile(join(projDir, "session-hhh.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    expect(result.efficiency.findings.length).toBeLessThanOrEqual(100);
  });

  // ─── Ponytail dimension ───────────────────────────────────────────────────

  test("ponytail-new-file: Write to fresh .ts path (no prior Read) emits finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-pt1");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T11:00:00.000Z";
    const lines = [
      // Write to /src/new-module.ts — never Read first
      assistantLine(ts, [
        {
          id: "t1",
          name: "Write",
          input: { file_path: "/src/new-module.ts", content: "export {}" },
        },
      ]),
      userResultLine(ts, [{ tool_use_id: "t1", content: "ok" }]),
    ];
    await writeFile(join(projDir, "session-pt1.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const found = result.efficiency.findings.filter((f) => f.kind === "ponytail-new-file");
    expect(found.length).toBeGreaterThanOrEqual(1);
    expect(found[0]!.detail).toBe("/src/new-module.ts");
    expect(found[0]!.severity).toBe("info");
    expect(result.efficiency.ponytail.newFiles).toBeGreaterThanOrEqual(1);
  });

  test("ponytail-new-file: Write to path that WAS Read first does NOT emit finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-pt2");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T11:01:00.000Z";
    const lines = [
      // Read the file first, then Write — this is a normal edit, not a new file
      assistantLine(ts, [{ id: "t1", name: "Read", input: { file_path: "/src/existing.ts" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: "existing contents" }]),
      assistantLine(ts, [
        { id: "t2", name: "Write", input: { file_path: "/src/existing.ts", content: "updated" } },
      ]),
      userResultLine(ts, [{ tool_use_id: "t2", content: "ok" }]),
    ];
    await writeFile(join(projDir, "session-pt2.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const found = result.efficiency.findings.filter(
      (f) => f.kind === "ponytail-new-file" && f.detail === "/src/existing.ts",
    );
    expect(found.length).toBe(0);
  });

  test("ponytail-dep-install: pnpm add foo emits finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-pt3");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T11:02:00.000Z";
    const lines = [
      assistantLine(ts, [{ id: "t1", name: "Bash", input: { command: "pnpm add zod" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: "added" }]),
    ];
    await writeFile(join(projDir, "session-pt3.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const found = result.efficiency.findings.filter((f) => f.kind === "ponytail-dep-install");
    expect(found.length).toBeGreaterThanOrEqual(1);
    expect(found[0]!.detail).toBe("pnpm add dependency (arguments redacted)");
    expect(found[0]!.severity).toBe("warn");
    expect(result.efficiency.ponytail.depInstalls).toBeGreaterThanOrEqual(1);
  });

  test("ponytail-dep-install never emits authenticated URLs or shell credentials", async () => {
    const projDir = join(tmpRoot, "projects", "proj-pt-secret");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T11:02:30.000Z";
    const credential = "super-secret-password";
    // Build the fake token at run time. The literal must not appear in source.
    const token = `${"gh"}${"p_"}abcdefghijklmnopqrstuvwxyz123456`;
    const rawCommand = `npm install https://build:${credential}@registry.example.invalid/pkg.tgz?token=${token}`;
    await writeFile(
      join(projDir, "session-pt-secret.jsonl"),
      `${assistantLine(ts, [{ id: "secret-install", name: "Bash", input: { command: rawCommand } }])}\n`,
    );

    const result = await analyzeTokenomics();
    const found = result.efficiency.findings.filter(
      (finding) =>
        finding.kind === "ponytail-dep-install" && finding.sessionId === "session-pt-secret",
    );
    const serialized = JSON.stringify(result.efficiency.findings);

    expect(found).toHaveLength(1);
    expect(found[0]!.detail).toBe("npm install dependency (arguments redacted)");
    expect(serialized).not.toContain(rawCommand);
    expect(serialized).not.toContain(credential);
    expect(serialized).not.toContain(token);
    expect(serialized).not.toContain("build:");
  });

  test("ponytail-dep-install: bare bun install (no package) does NOT emit finding", async () => {
    const projDir = join(tmpRoot, "projects", "proj-pt4");
    await mkdir(projDir, { recursive: true });
    const ts = "2026-06-27T11:03:00.000Z";
    const lines = [
      // bare lockfile restore — should NOT be flagged
      assistantLine(ts, [{ id: "t1", name: "Bash", input: { command: "bun install" } }]),
      userResultLine(ts, [{ tool_use_id: "t1", content: "done" }]),
      // also test bare npm install
      assistantLine(ts, [{ id: "t2", name: "Bash", input: { command: "npm install" } }]),
      userResultLine(ts, [{ tool_use_id: "t2", content: "done" }]),
    ];
    await writeFile(join(projDir, "session-pt4.jsonl"), lines.join("\n") + "\n");

    const result = await analyzeTokenomics();
    const found = result.efficiency.findings.filter(
      (f) => f.kind === "ponytail-dep-install" && f.sessionId === "session-pt4",
    );
    expect(found.length).toBe(0);
    // rollup count should not include these
    // (other sessions in this test run may have dep installs from proj-pt3,
    //  but this session contributes 0)
  });
});

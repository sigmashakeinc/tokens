import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { appendFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { computeRateSnapshot, isRateSnapshot, RATE_MODEL_BUCKET_MAX } from "../src/rate.ts";
import { TokenStore } from "../src/store.ts";
import { issueTokenSnapshotHeartbeatTarget } from "../src/token-snapshot-heartbeat.ts";
import {
  _parseUsageIngestWorkerResponseForTest,
  _resetUsageIngestWorkerForTest,
  _setNextUsageIngestWorkerActionForTest,
  _setUsageIngestControlSpawnForTest,
  _setUsageIngestLauncherForTest,
  _setUsageIngestSpawnForTest,
  _usageIngestWorkerCommandForTest,
  _usageIngestWorkerStatsForTest,
  _usageIngestWorkerTimeoutMsForTest,
  ingestUsageOffThread,
  shutdownUsageIngestWorker,
  type UsageIngestTestControlSubprocess,
  type UsageIngestTestSubprocess,
  type UsageIngestWorkerPaths,
} from "../src/usage-ingest-worker.ts";
import {
  USAGE_INGEST_RATE_MAX_BYTES,
  USAGE_INGEST_RECENT_MAX_EVENTS,
  USAGE_INGEST_RECENT_TEXT_MAX_BYTES,
} from "../src/usage-ingest-worker-protocol.ts";

const roots = new Set<string>();
const encoder = new TextEncoder();
type CachedTokenSnapshot = NonNullable<ReturnType<TokenStore["readCachedTokenSnapshot"]>>;

function closedByteStream(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.close();
    },
  });
}

function controlledSubprocess(
  options: {
    stdinError?: Error;
    exitOnSigkill?: boolean;
    exitOnSigterm?: boolean;
    onSignal?(signal: number | string | undefined): void;
  } = {},
): {
  subprocess: UsageIngestTestSubprocess;
  signals: Array<number | string | undefined>;
  resolveExit(exitCode: number): void;
} {
  let resolveExit!: (exitCode: number) => void;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  const signals: Array<number | string | undefined> = [];
  return {
    subprocess: {
      stdin: {
        write(data) {
          if (options.stdinError) throw options.stdinError;
          return typeof data === "string" ? encoder.encode(data).byteLength : data.byteLength;
        },
        end() {},
      },
      stdout: closedByteStream(),
      stderr: closedByteStream(),
      exited,
      kill(signal) {
        signals.push(signal);
        options.onSignal?.(signal);
        if (options.exitOnSigterm && (signal === "SIGTERM" || signal === 15)) resolveExit(0);
        if (options.exitOnSigkill && (signal === "SIGKILL" || signal === 9)) resolveExit(137);
      },
    },
    signals,
    resolveExit,
  };
}

function completedControlSubprocess(
  exitCode: number,
  stderr = "",
): UsageIngestTestControlSubprocess {
  return {
    stderr: new ReadableStream<Uint8Array>({
      start(controller) {
        if (stderr) controller.enqueue(encoder.encode(stderr));
        controller.close();
      },
    }),
    exited: Promise.resolve(exitCode),
    kill() {},
  };
}

function validCompactResult() {
  const scannedAt = 1_700_000_000_001;
  return {
    files: 1,
    mutated: true,
    revision: 2,
    snapshotRefreshed: true,
    rate: computeRateSnapshot([], scannedAt),
    update: {
      type: "events",
      newEvents: 1,
      totalsDelta: {
        inputTokens: 10,
        outputTokens: 20,
        cacheReadTokens: 30,
        cacheWrite5mTokens: 40,
        cacheWrite1hTokens: 50,
        cost: 0.25,
      },
      recent: [
        {
          ts: 1_700_000_000_000,
          model: "claude-sonnet-4-6",
          project: "sigmashake-tokens",
          sessionId: "session-1",
          totalTokens: 150,
          cost: 0.25,
        },
      ],
      scannedAt,
    },
  };
}

function encodedResult(result: unknown): Uint8Array {
  return encoder.encode(`${JSON.stringify({ type: "result", id: 7, result })}\n`);
}

function codexFixturePath(paths: UsageIngestWorkerPaths): string {
  return join(paths.codexSessionsDir, "2026", "07", "18", "codex.jsonl");
}

async function waitForCachedSnapshot(
  databasePath: string,
  predicate: (snapshot: CachedTokenSnapshot, revision: number) => boolean,
  timeoutMs = 2_000,
): Promise<CachedTokenSnapshot> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const store = new TokenStore(databasePath, { readonly: true });
    try {
      const snapshot = store.readCachedTokenSnapshot();
      if (snapshot && predicate(snapshot, store.getRevision())) return snapshot;
    } finally {
      store.close();
    }
    await Bun.sleep(10);
  }
  throw new Error("timed out waiting for cached token snapshot state");
}

async function fixture(withEvent = false, withCodexEvent = false): Promise<UsageIngestWorkerPaths> {
  const root = await mkdtemp(join(tmpdir(), "tokens-ingest-worker-"));
  roots.add(root);
  const claudeProjectsDir = join(root, "claude");
  const codexSessionsDir = join(root, "codex");
  const projectDir = join(claudeProjectsDir, "-home-user-project");
  await mkdir(projectDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });
  if (withEvent) {
    await writeFile(
      join(projectDir, "00000000-1111-2222-3333-444444444444.jsonl"),
      `${JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T12:00:00.000Z",
        cwd: "/srv/example/project",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 20 },
        },
      })}\n`,
    );
  }
  if (withCodexEvent) {
    const paths = { databasePath: "", claudeProjectsDir, codexSessionsDir };
    const codexPath = codexFixturePath(paths);
    await mkdir(join(codexSessionsDir, "2026", "07", "18"), { recursive: true });
    const timestamp = new Date().toISOString();
    await writeFile(
      join(projectDir, "00000000-1111-2222-3333-555555555555.jsonl"),
      `${JSON.stringify({
        type: "assistant",
        timestamp,
        cwd: "/srv/example/project",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 7, output_tokens: 3 },
        },
      })}\n`,
    );
    const lines = [
      {
        type: "session_meta",
        timestamp,
        payload: { session_id: "worker-codex-session", cwd: "/srv/example/project" },
      },
      {
        type: "turn_context",
        timestamp,
        payload: { model: "gpt-5.6-sol", cwd: "/srv/example/project" },
      },
      {
        type: "event_msg",
        timestamp,
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 10,
              cached_input_tokens: 2,
              output_tokens: 5,
              reasoning_output_tokens: 2,
            },
          },
        },
      },
    ];
    await writeFile(codexPath, `${lines.map(JSON.stringify).join("\n")}\n`);
  }
  const databasePath = join(root, "tokens.sqlite");
  new TokenStore(databasePath).close();
  return { databasePath, claudeProjectsDir, codexSessionsDir };
}

afterEach(async () => {
  await _resetUsageIngestWorkerForTest();
  await Promise.all([...roots].map((root) => rm(root, { recursive: true, force: true })));
  roots.clear();
});

describe("usage ingest process isolation", () => {
  test("accepts a bounded ten-minute operational worker timeout override", () => {
    const previous = process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
    try {
      delete process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
      expect(_usageIngestWorkerTimeoutMsForTest()).toBe(210_000);
      process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = "600000";
      expect(_usageIngestWorkerTimeoutMsForTest()).toBe(600_000);
      process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = "900000";
      expect(_usageIngestWorkerTimeoutMsForTest()).toBe(900_000);
    } finally {
      if (previous === undefined) delete process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
      else process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = previous;
    }
  });

  test("worker timeout overrides remain fail-closed outside the bounded range", () => {
    const previous = process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
    try {
      for (const invalid of ["", "999", "900001", "Infinity", "not-a-number"]) {
        process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = invalid;
        expect(_usageIngestWorkerTimeoutMsForTest()).toBe(210_000);
      }
    } finally {
      if (previous === undefined) delete process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
      else process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = previous;
    }
  });

  test("systemd runtime is ordered after the same parsed parent timeout and cleanup bound", () => {
    const previous = process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
    const runtimeProperty = () =>
      _usageIngestWorkerCommandForTest("/tmp/tokens.sqlite").find((part) =>
        part.startsWith("--property=RuntimeMaxSec="),
      );
    try {
      delete process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
      expect(runtimeProperty()).toBe("--property=RuntimeMaxSec=241000ms");
      process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = "600000";
      expect(runtimeProperty()).toBe("--property=RuntimeMaxSec=631000ms");
      process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = "900001";
      expect(runtimeProperty()).toBe("--property=RuntimeMaxSec=241000ms");
    } finally {
      if (previous === undefined) delete process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS;
      else process.env.TOKENS_INGEST_WORKER_TIMEOUT_MS = previous;
    }
  });

  test("the entrypoint rejects malformed, multi-record, and over-limit input", async () => {
    const entry = resolve(import.meta.dir, "../src/usage-ingest.worker.ts");
    const invalidInputs: Array<string | Uint8Array> = [
      new Uint8Array([0xff, 0x0a]),
      "{}\n{}\n",
      new Uint8Array(64 * 1024 + 1).fill(0x61),
    ];
    for (const input of invalidInputs) {
      const proc = Bun.spawn([process.execPath, entry], {
        stdin: "pipe",
        stdout: "pipe",
        stderr: "pipe",
      });
      proc.stdin.write(input);
      proc.stdin.end();
      const [exitCode, stdout] = await Promise.all([
        proc.exited,
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
      ]);
      expect(exitCode).not.toBe(0);
      expect(stdout).toBe("");
    }
  });

  test("returns only the exact compact committed delta", async () => {
    const paths = await fixture(true);
    _setUsageIngestLauncherForTest("direct");

    const first = await ingestUsageOffThread(paths);
    expect(first.files).toBe(1);
    expect(first.mutated).toBe(true);
    expect(first.update.newEvents).toBe(1);
    expect(first.update.totalsDelta).toMatchObject({ inputTokens: 10, outputTokens: 20 });
    expect(first.update.recent).toHaveLength(1);
    expect(first.update.recent[0]).toMatchObject({
      model: "claude-sonnet-4-6",
      totalTokens: 30,
    });
    expect(first.update.recent.length).toBeLessThanOrEqual(10);
    expect(first.snapshotRefreshed).toBe(true);
    expect(isRateSnapshot(first.rate)).toBe(true);
    expect("events" in first).toBe(false);

    let firstAuthority: ReturnType<TokenStore["readCachedTokenSnapshot"]> = null;
    const afterCommittedIngest = new TokenStore(paths.databasePath, { readonly: true });
    try {
      expect(afterCommittedIngest.readCachedStats()).toEqual({
        revision: first.revision,
        events: 1,
        files: 1,
        scannedAt: first.update.scannedAt,
      });
      const cachedRate = afterCommittedIngest.readCachedRateSnapshot();
      expect(cachedRate).toEqual({
        revision: first.revision,
        publishedAt: expect.any(Number),
        snapshot: first.rate,
      });
      firstAuthority = afterCommittedIngest.readCachedTokenSnapshot();
      expect(firstAuthority?.updatedAt).toBeGreaterThanOrEqual(first.update.scannedAt);
      expect(cachedRate!.publishedAt).toBeGreaterThanOrEqual(firstAuthority!.updatedAt);
    } finally {
      afterCommittedIngest.close();
    }

    await Bun.sleep(10);
    const noOp = await ingestUsageOffThread(paths);
    expect(noOp.update.newEvents).toBe(0);
    expect(noOp.update.recent).toEqual([]);
    expect(noOp.snapshotRefreshed).toBe(false);
    const afterWorkerRestart = new TokenStore(paths.databasePath, { readonly: true });
    try {
      expect(afterWorkerRestart.readCachedStats()).toEqual({
        revision: noOp.revision,
        events: 1,
        files: 1,
        scannedAt: noOp.update.scannedAt,
      });
      const noOpAuthority = afterWorkerRestart.readCachedTokenSnapshot();
      expect(noOpAuthority).not.toBeNull();
      expect(noOpAuthority!.updatedAt).toBeGreaterThanOrEqual(noOp.update.scannedAt);
      expect(noOpAuthority!.updatedAt).toBeGreaterThan(firstAuthority!.updatedAt);
      expect({
        ...noOpAuthority!,
        updatedAt: firstAuthority!.updatedAt,
      }).toEqual(firstAuthority);
    } finally {
      afterWorkerRestart.close();
    }
    expect(_usageIngestWorkerStatsForTest()).toMatchObject({
      spawned: 2,
      completed: 2,
      failed: 0,
      active: false,
    });
  });

  test("rejects every malformed compact-result region at the protocol boundary", () => {
    const valid = validCompactResult();
    const incompleteTotals = { ...valid.update.totalsDelta } as Record<string, unknown>;
    delete incompleteTotals.outputTokens;
    const oversizedEvent = {
      ...valid.update.recent[0],
      model: "x".repeat(USAGE_INGEST_RECENT_TEXT_MAX_BYTES + 1),
    };
    const tooManyModels = {
      ...valid.rate,
      buckets: valid.rate.buckets.map((bucket, index) =>
        index === 0
          ? {
              ...bucket,
              byModel: Array.from({ length: RATE_MODEL_BUCKET_MAX + 1 }, (_, modelIndex) => ({
                model: `model-${modelIndex}`,
                tokens: 0,
                tokensPerSecond: 0,
                messages: 0,
              })),
            }
          : bucket,
      ),
    };
    const invalidResults: Array<[string, unknown]> = [
      ["non-object result", null],
      ["unsafe file count", { ...valid, files: Number.MAX_SAFE_INTEGER + 1 }],
      ["non-boolean mutation", { ...valid, mutated: "yes" }],
      ["negative revision", { ...valid, revision: -1 }],
      ["non-boolean snapshot refresh", { ...valid, snapshotRefreshed: 1 }],
      ["missing rate", { ...valid, rate: null }],
      ["unbounded rate cardinality", { ...valid, rate: tooManyModels }],
      ["mismatched rate time", { ...valid, rate: { ...valid.rate, now: valid.rate.now - 1 } }],
      [
        "oversized rate encoding",
        {
          ...valid,
          rate: { ...valid.rate, padding: "x".repeat(USAGE_INGEST_RATE_MAX_BYTES) },
        },
      ],
      ["missing update", { ...valid, update: null }],
      ["wrong update type", { ...valid, update: { ...valid.update, type: "snapshot" } }],
      ["fractional event count", { ...valid, update: { ...valid.update, newEvents: 1.5 } }],
      [
        "incomplete totals",
        { ...valid, update: { ...valid.update, totalsDelta: incompleteTotals } },
      ],
      [
        "non-finite cost",
        {
          ...valid,
          update: { ...valid.update, totalsDelta: { ...valid.update.totalsDelta, cost: null } },
        },
      ],
      ["non-array recent", { ...valid, update: { ...valid.update, recent: {} } }],
      [
        "too many recent events",
        {
          ...valid,
          update: {
            ...valid.update,
            newEvents: USAGE_INGEST_RECENT_MAX_EVENTS + 1,
            recent: Array.from(
              { length: USAGE_INGEST_RECENT_MAX_EVENTS + 1 },
              () => valid.update.recent[0],
            ),
          },
        },
      ],
      ["recent exceeds event count", { ...valid, update: { ...valid.update, newEvents: 0 } }],
      ["non-object recent event", { ...valid, update: { ...valid.update, recent: [null] } }],
      [
        "fractional timestamp",
        {
          ...valid,
          update: {
            ...valid.update,
            recent: [{ ...valid.update.recent[0], ts: 1.5 }],
          },
        },
      ],
      [
        "oversized recent text",
        { ...valid, update: { ...valid.update, recent: [oversizedEvent] } },
      ],
      [
        "negative token total",
        {
          ...valid,
          update: {
            ...valid.update,
            recent: [{ ...valid.update.recent[0], totalTokens: -1 }],
          },
        },
      ],
      ["unsafe scan time", { ...valid, update: { ...valid.update, scannedAt: Infinity } }],
    ];

    expect(_parseUsageIngestWorkerResponseForTest(encodedResult(valid), 7)).toEqual({
      type: "result",
      id: 7,
      result: valid,
    });
    for (const [label, result] of invalidResults) {
      expect(() => _parseUsageIngestWorkerResponseForTest(encodedResult(result), 7), label).toThrow(
        "ingest worker result is invalid",
      );
    }
  });

  test("a no-op scan republishes a missing authoritative cache", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);

    const db = new Database(paths.databasePath);
    try {
      db.query("DELETE FROM store_meta WHERE key = ?").run("authoritative-token-snapshot-v1");
    } finally {
      db.close();
    }

    const warmed = await ingestUsageOffThread(paths);
    expect(warmed.mutated).toBe(false);
    expect(warmed.update.newEvents).toBe(0);
    expect(warmed.snapshotRefreshed).toBe(true);
  });

  test("a no-op scan republishes authority when the LA daily window rolls", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);

    const store = new TokenStore(paths.databasePath);
    let oldWindowKey: string;
    try {
      const oldSnapshot = store.buildAuthoritativeTokenSnapshot(Date.now() - 3 * 86_400_000);
      oldWindowKey = oldSnapshot.window.windowKey;
      store.writeCachedTokenSnapshot(oldSnapshot);
    } finally {
      store.close();
    }

    const rolled = await ingestUsageOffThread(paths);
    expect(rolled.mutated).toBe(false);
    expect(rolled.update.newEvents).toBe(0);
    expect(rolled.snapshotRefreshed).toBe(true);
    const reopened = new TokenStore(paths.databasePath, { readonly: true });
    try {
      expect(reopened.readCachedTokenSnapshot()?.window.windowKey).not.toBe(oldWindowKey!);
    } finally {
      reopened.close();
    }
  });

  test("completion republishes an LA window rolled during the synchronous reducer phase", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);
    const initialStore = new TokenStore(paths.databasePath, { readonly: true });
    const initial = initialStore.readCachedTokenSnapshot()!;
    initialStore.close();
    _setNextUsageIngestWorkerActionForTest({
      delayMs: 800,
      heartbeatIntervalMs: 20,
      failAfterDelay: false,
    });

    const active = ingestUsageOffThread(paths);
    const early = await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > initial.updatedAt,
    );
    await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > early.updatedAt,
    );
    const rolloverStore = new TokenStore(paths.databasePath);
    let oldWindowKey: string;
    try {
      const oldSnapshot = rolloverStore.buildAuthoritativeTokenSnapshot(
        Date.now() - 3 * 86_400_000,
      );
      oldWindowKey = oldSnapshot.window.windowKey;
      rolloverStore.writeCachedTokenSnapshot(oldSnapshot);
    } finally {
      rolloverStore.close();
    }

    const rolled = await active;
    expect(rolled).toMatchObject({ snapshotRefreshed: true });
    const reopened = new TokenStore(paths.databasePath, { readonly: true });
    try {
      expect(reopened.readCachedTokenSnapshot()!.window.windowKey).not.toBe(oldWindowKey!);
    } finally {
      reopened.close();
    }
  }, 10_000);

  test("a slow initial ingest leaves the parent event loop responsive and stays single-flight", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    _setNextUsageIngestWorkerActionForTest({ delayMs: 250 });

    const active = ingestUsageOffThread(paths);
    let parentCheckpointObserved = false;
    await Promise.resolve().then(() => {
      parentCheckpointObserved = true;
    });
    expect(parentCheckpointObserved).toBe(true);
    expect(_usageIngestWorkerStatsForTest().active).toBe(true);
    await expect(ingestUsageOffThread(paths)).rejects.toThrow("already has an active job");
    await expect(active).resolves.toMatchObject({ update: { newEvents: 0 } });
  });

  test("a delayed isolated ingest heartbeats while the parent stays responsive then clears", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);
    const beforeStore = new TokenStore(paths.databasePath, { readonly: true });
    const before = beforeStore.readCachedTokenSnapshot()!;
    const target = issueTokenSnapshotHeartbeatTarget(beforeStore);
    expect(target).not.toBeNull();
    beforeStore.close();

    _setNextUsageIngestWorkerActionForTest({ delayMs: 500, heartbeatIntervalMs: 20 });
    const active = ingestUsageOffThread(paths, target ?? undefined);
    let parentCheckpointObserved = false;
    await Promise.resolve().then(() => {
      parentCheckpointObserved = true;
    });
    expect(parentCheckpointObserved).toBe(true);
    await Bun.sleep(200);
    const duringStore = new TokenStore(paths.databasePath, { readonly: true });
    const during = duringStore.readCachedTokenSnapshot()!;
    duringStore.close();
    expect(during.updatedAt).toBeGreaterThan(before.updatedAt);
    expect(_usageIngestWorkerStatsForTest().active).toBe(true);

    await active;
    const completedStore = new TokenStore(paths.databasePath, { readonly: true });
    const completedAt = completedStore.readCachedTokenSnapshot()!.updatedAt;
    completedStore.close();
    await Bun.sleep(60);
    const afterStore = new TokenStore(paths.databasePath, { readonly: true });
    expect(afterStore.readCachedTokenSnapshot()!.updatedAt).toBe(completedAt);
    afterStore.close();
  });

  test("independent heartbeat advances repeatedly during the actual exact-stats read phase", async () => {
    const paths = await fixture(false, true);
    _setUsageIngestLauncherForTest("direct");
    _setNextUsageIngestWorkerActionForTest({
      delayMs: 800,
      heartbeatIntervalMs: 20,
      failAfterDelay: false,
    });

    const active = ingestUsageOffThread(paths);
    let settled = false;
    void active.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    const first = await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot, revision) =>
        snapshot.revision === revision &&
        snapshot.providers.claude.discoveryComplete &&
        snapshot.providers.claude.models.length > 0 &&
        snapshot.providers.codex.discoveryComplete &&
        snapshot.providers.codex.models.length > 0,
    );
    expect(first.providers.claude).toMatchObject({
      available: true,
      discoveryComplete: true,
    });
    expect(first.providers.codex).toMatchObject({
      available: true,
      discoveryComplete: true,
    });
    expect(first.providers.codex.models).toEqual([
      expect.objectContaining({ model: "gpt-5.6-sol" }),
    ]);
    expect(first.providers.claude.models).toEqual([
      expect.objectContaining({ model: "claude-sonnet-4-6" }),
    ]);

    const second = await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > first.updatedAt,
    );
    const third = await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > second.updatedAt,
    );
    await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > third.updatedAt,
    );
    expect(settled).toBe(false);
    const duringRead = new TokenStore(paths.databasePath, { readonly: true });
    expect(duringRead.readCachedStats()).toBeNull();
    duringRead.close();
    await expect(active).resolves.toMatchObject({
      snapshotRefreshed: true,
      revision: first.revision,
    });
    const afterRead = new TokenStore(paths.databasePath, { readonly: true });
    expect(afterRead.readCachedStats()).toMatchObject({ revision: first.revision });
    afterRead.close();
  }, 10_000);

  test("releases the independent heartbeat child within the worker lifecycle budget", async () => {
    const paths = await fixture(false, true);
    _setUsageIngestLauncherForTest("direct");
    const active = ingestUsageOffThread(paths);
    const timedOut = Symbol("heartbeat child cleanup timeout");
    let timer: ReturnType<typeof setTimeout> | null = null;
    const deadline = new Promise<typeof timedOut>((resolve) => {
      timer = setTimeout(() => resolve(timedOut), 1_000);
      timer.unref?.();
    });
    const settled = await Promise.race([active, deadline]);
    if (timer !== null) clearTimeout(timer);
    if (settled === timedOut) {
      await shutdownUsageIngestWorker();
      throw new Error("independent heartbeat child retained the ingest worker past 1000ms");
    }
    expect(settled.revision).toBeGreaterThan(0);
    expect(_usageIngestWorkerStatsForTest()).toMatchObject({
      completed: 1,
      failed: 0,
      active: false,
    });
  });

  test("hands heartbeat from N to N+1 and keeps advancing updatedAt across N+2 revision drift", async () => {
    const paths = await fixture(false, true);
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);
    const initialStore = new TokenStore(paths.databasePath, { readonly: true });
    const initial = initialStore.readCachedTokenSnapshot()!;
    const target = issueTokenSnapshotHeartbeatTarget(initialStore);
    initialStore.close();
    expect(target).not.toBeNull();

    await appendFile(
      codexFixturePath(paths),
      `${JSON.stringify({
        type: "event_msg",
        timestamp: new Date().toISOString(),
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 20,
              cached_input_tokens: 4,
              output_tokens: 10,
              reasoning_output_tokens: 4,
            },
          },
        },
      })}\n`,
    );
    _setNextUsageIngestWorkerActionForTest({
      delayMs: 800,
      heartbeatIntervalMs: 20,
      failAfterDelay: false,
    });
    const active = ingestUsageOffThread(paths, target ?? undefined);

    const advanced = await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.revision === initial.revision + 1,
    );
    expect(advanced.revision).toBe(initial.revision + 1);
    await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > advanced.updatedAt,
    );

    const driftStore = new TokenStore(paths.databasePath);
    driftStore.reconcileDiscovery([], [{ source: "codex", available: true, complete: false }]);
    const driftRevision = driftStore.getRevision();
    const driftedAt = driftStore.readCachedTokenSnapshot()!.updatedAt;
    driftStore.close();
    expect(driftRevision).toBe(advanced.revision + 1);

    // C-2/C-3: the heartbeat no longer pins against the *live* store
    // revision (only the cached snapshot's own revision/window/updatedAt via
    // touchCachedTokenSnapshot's CAS — store.ts:2885-2891). A foreign
    // revision bump like `driftStore.reconcileDiscovery` above therefore does
    // NOT stop the heartbeat: the cached snapshot's own revision is
    // untouched, so the CAS keeps matching and `updatedAt` keeps advancing
    // right up until the ingest worker's own refresh-path pin
    // (`store.ts:2803-2804`, still intact) rejects with "revision changed".
    await waitForCachedSnapshot(paths.databasePath, (snapshot) => snapshot.updatedAt > driftedAt);
    await expect(active).rejects.toThrow("revision changed");

    const stoppedStore = new TokenStore(paths.databasePath, { readonly: true });
    const stopped = stoppedStore.readCachedTokenSnapshot()!;
    expect(stopped.updatedAt).toBeGreaterThan(driftedAt);
    // The heartbeat only ever touches `updatedAt` — the surviving cached
    // snapshot still carries the pre-drift revision/content, i.e. the
    // rejected worker's own refresh never got to publish anything new.
    expect(stopped.revision).toBe(advanced.revision);
    expect(stoppedStore.readCachedStats()?.revision).toBe(initial.revision);
    stoppedStore.close();
  }, 10_000);

  test("touchCachedTokenSnapshot advances only updatedAt, leaving revision/window/content byte-identical", async () => {
    // Store-level coverage for the bound that makes C-2/C-3 safe without the
    // old live-revision pin: the serve-layer supersession tracker
    // (server.ts:110-127) keys off `cached.revision` identity and counts
    // "how long has this exact revision been superseded" from the first time
    // it observed that revision behind live — NOT from `updatedAt`. If a
    // heartbeat touch mutated anything other than `updatedAt` (e.g. bumped
    // `revision`, or reshaped `window`/provider content), a republish would
    // look like a *new* snapshot identity and silently reset that clock,
    // letting a persistently-behind cache be served forever just because its
    // heartbeat keeps ticking. This asserts the touch is updatedAt-only.
    const root = await mkdtemp(join(tmpdir(), "tokens-heartbeat-touch-"));
    roots.add(root);
    const databasePath = join(root, "tokens.sqlite");
    const store = new TokenStore(databasePath);
    try {
      store.reconcileDiscovery(
        [],
        [
          { source: "claude", available: true, complete: true },
          { source: "codex", available: true, complete: true },
        ],
      );
      const before = store.refreshAuthoritativeTokenSnapshot(1_700_000_000_000);
      const touched = store.touchCachedTokenSnapshot(
        1_700_000_005_000,
        before.revision,
        before.window.windowKey,
        before.updatedAt,
      );
      expect(touched.updatedAt).toBe(1_700_000_005_000);
      expect(touched).toEqual({ ...before, updatedAt: 1_700_000_005_000 });
      expect(touched.revision).toBe(before.revision);
      expect(touched.window).toEqual(before.window);
      expect(touched.providers).toEqual(before.providers);
      expect(touched.rawTotals).toEqual(before.rawTotals);
      expect(touched.classes).toEqual(before.classes);
      expect(touched.hud).toEqual(before.hud);
      expect(touched.totalCost).toBe(before.totalCost);
      expect(touched.calls).toBe(before.calls);

      const persisted = store.readCachedTokenSnapshot();
      expect(persisted).toEqual(touched);
    } finally {
      store.close();
    }
  });

  test("touchCachedTokenSnapshot survives transient contention and still fails after exhausting attempts", async () => {
    // C-4 coverage (store.ts:2905-2916): drive the REAL method (not a stub)
    // against a scratch DB and prove the bounded 3-attempt retry both (a)
    // recovers from a transient contention error and (b) still ultimately
    // throws once every attempt fails.
    const root = await mkdtemp(join(tmpdir(), "tokens-heartbeat-retry-"));
    roots.add(root);
    const databasePath = join(root, "tokens.sqlite");
    const store = new TokenStore(databasePath);
    try {
      store.reconcileDiscovery(
        [],
        [
          { source: "claude", available: true, complete: true },
          { source: "codex", available: true, complete: true },
        ],
      );
      const before = store.refreshAuthoritativeTokenSnapshot(1_700_000_000_000);

      const rawDb = (store as unknown as { db: Database }).db;
      const originalTransaction = rawDb.transaction.bind(rawDb);
      let calls = 0;
      const contentionError = () => {
        const error = new Error("database is locked");
        error.name = "SQLiteError";
        (error as { code?: string }).code = "SQLITE_BUSY";
        return error;
      };

      // Recovery case: fail transiently twice, succeed on the 3rd attempt
      // (still within the bounded maxAttempts = 3 loop).
      (rawDb as unknown as { transaction: typeof rawDb.transaction }).transaction = ((
        fn: () => unknown,
      ) => {
        calls += 1;
        if (calls <= 2) {
          const failing = () => {
            throw contentionError();
          };
          return originalTransaction(failing);
        }
        return originalTransaction(fn);
      }) as typeof rawDb.transaction;
      try {
        const touched = store.touchCachedTokenSnapshot(
          1_700_000_005_000,
          before.revision,
          before.window.windowKey,
          before.updatedAt,
        );
        expect(touched.updatedAt).toBe(1_700_000_005_000);
        expect(calls).toBe(3);
      } finally {
        rawDb.transaction = originalTransaction;
      }

      // Exhaustion case: every attempt (all 3) hits transient contention —
      // the bounded retry must still surface the error rather than hang or
      // silently swallow it.
      calls = 0;
      (rawDb as unknown as { transaction: typeof rawDb.transaction }).transaction = ((
        fn: () => unknown,
      ) => {
        calls += 1;
        void fn;
        const failing = () => {
          throw contentionError();
        };
        return originalTransaction(failing);
      }) as typeof rawDb.transaction;
      try {
        // CAS-valid expectedPriorUpdatedAt: this is the CURRENT cached
        // updatedAt (set by the successful touch above), not the stale
        // `before.updatedAt`. Reusing the stale value would make the CAS
        // precondition itself fail — the test would then "pass" even if the
        // injected contention fault (and the retry loop under test) were
        // deleted entirely, for the wrong reason. Asserting the exact thrown
        // message also pins the failure to the transient-contention path
        // specifically, not the CAS staleness path ("...heartbeat target is
        // stale") or the unreachable retry-exhaustion fallback.
        expect(() =>
          store.touchCachedTokenSnapshot(
            1_700_000_010_000,
            before.revision,
            before.window.windowKey,
            1_700_000_005_000,
          ),
        ).toThrow("database is locked");
        expect(calls).toBe(3);
      } finally {
        rawDb.transaction = originalTransaction;
      }
    } finally {
      store.close();
    }
  });

  test("a delayed reducer failure clears its published heartbeat and retry recovers", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);
    const db = new Database(paths.databasePath);
    try {
      db.query("DELETE FROM store_meta WHERE key = ?").run("authoritative-token-snapshot-v1");
    } finally {
      db.close();
    }

    _setNextUsageIngestWorkerActionForTest({
      delayMs: 500,
      heartbeatIntervalMs: 20,
      failAfterDelay: true,
    });
    const active = ingestUsageOffThread(paths);
    const during = await waitForCachedSnapshot(paths.databasePath, () => true);
    await waitForCachedSnapshot(
      paths.databasePath,
      (snapshot) => snapshot.updatedAt > during.updatedAt,
    );
    await expect(active).rejects.toThrow("injected delayed reducer failure");

    const failedStore = new TokenStore(paths.databasePath, { readonly: true });
    const failedAt = failedStore.readCachedTokenSnapshot()!.updatedAt;
    failedStore.close();
    await Bun.sleep(60);
    const afterStore = new TokenStore(paths.databasePath, { readonly: true });
    expect(afterStore.readCachedTokenSnapshot()!.updatedAt).toBe(failedAt);
    afterStore.close();
    await expect(ingestUsageOffThread(paths)).resolves.toMatchObject({
      update: { newEvents: 0 },
    });
  }, 15_000);

  test("a starting or degraded request omits heartbeat authority and cannot revive the cache", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    await ingestUsageOffThread(paths);
    const beforeStore = new TokenStore(paths.databasePath, { readonly: true });
    const before = beforeStore.readCachedTokenSnapshot()!;
    beforeStore.close();

    _setNextUsageIngestWorkerActionForTest({ delayMs: 250, heartbeatIntervalMs: 20 });
    const active = ingestUsageOffThread(paths);
    await Bun.sleep(100);
    const duringStore = new TokenStore(paths.databasePath, { readonly: true });
    expect(duringStore.readCachedTokenSnapshot()!.updatedAt).toBe(before.updatedAt);
    duringStore.close();
    await active;
  });

  test("a crashed child is contained and the next ingest recovers", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    _setNextUsageIngestWorkerActionForTest("crash");

    await expect(ingestUsageOffThread(paths)).rejects.toThrow("ingest worker crashed");
    await expect(ingestUsageOffThread(paths)).resolves.toMatchObject({
      update: { newEvents: 0 },
    });
    expect(_usageIngestWorkerStatsForTest()).toMatchObject({
      spawned: 2,
      completed: 1,
      failed: 1,
      active: false,
    });
  });

  test("tracks cleanup when worker stdin fails before activation", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    const controlled = controlledSubprocess({ stdinError: new Error("injected pipe close") });
    _setUsageIngestSpawnForTest(() => controlled.subprocess);

    await expect(ingestUsageOffThread(paths)).rejects.toThrow(
      "ingest worker could not start: injected pipe close",
    );
    await expect(ingestUsageOffThread(paths)).rejects.toThrow(
      "ingest worker cleanup is still in progress; relaunch blocked",
    );
    expect(controlled.signals).toContain("SIGTERM");

    controlled.resolveExit(0);
    await expect(shutdownUsageIngestWorker()).resolves.toBeUndefined();
  });

  test("rechecks the exact transient unit after the systemd-run wrapper exits", async () => {
    const paths = await fixture();
    const cleanupOrder: string[] = [];
    const wrapper = controlledSubprocess({
      exitOnSigterm: true,
      onSignal: (signal) => cleanupOrder.push(`wrapper:${String(signal)}`),
    });
    const controlCommands: string[][] = [];
    let stopCalls = 0;
    _setUsageIngestSpawnForTest(() => wrapper.subprocess);
    _setUsageIngestControlSpawnForTest((command) => {
      controlCommands.push(command);
      cleanupOrder.push(`unit:${command[2]}`);
      if (command[2] === "stop") {
        stopCalls++;
        return stopCalls === 1
          ? completedControlSubprocess(5, "Unit not loaded")
          : completedControlSubprocess(0);
      }
      return completedControlSubprocess(0);
    });
    const active = ingestUsageOffThread(paths);
    const observedError = active.then(
      () => null,
      (error: unknown) => error,
    );

    await expect(shutdownUsageIngestWorker()).resolves.toBeUndefined();

    const stopCommands = controlCommands.filter((command) => command[2] === "stop");
    expect(stopCommands).toHaveLength(2);
    expect(stopCommands[0]?.at(-1)).toBe(stopCommands[1]?.at(-1));
    expect(controlCommands.some((command) => command[2] === "kill")).toBe(false);
    expect(wrapper.signals).toEqual(["SIGTERM"]);
    expect(cleanupOrder).toEqual(["unit:stop", "wrapper:SIGTERM", "unit:stop"]);
    const error = await observedError;
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe("ingest worker shut down");
  });

  test("shutdown escalates a TERM-resistant child to KILL within its deadline", async () => {
    const paths = await fixture();
    const previousStopTimeout = process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS;
    process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS = "100";
    try {
      _setUsageIngestLauncherForTest("direct");
      const controlled = controlledSubprocess({ exitOnSigkill: true });
      _setUsageIngestSpawnForTest(() => controlled.subprocess);
      const active = ingestUsageOffThread(paths);
      const observedError = active.then(
        () => null,
        (error: unknown) => error,
      );

      await expect(shutdownUsageIngestWorker()).resolves.toBeUndefined();
      expect(controlled.signals).toEqual(["SIGTERM", "SIGKILL"]);
      const error = await observedError;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toBe("ingest worker shut down");
    } finally {
      if (previousStopTimeout === undefined) {
        delete process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS;
      } else {
        process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS = previousStopTimeout;
      }
    }
  });

  test("shutdown propagates a child cleanup deadline failure and blocks relaunch", async () => {
    const paths = await fixture();
    const previousStopTimeout = process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS;
    process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS = "100";
    try {
      _setUsageIngestLauncherForTest("direct");
      const controlled = controlledSubprocess();
      _setUsageIngestSpawnForTest(() => controlled.subprocess);
      const active = ingestUsageOffThread(paths);
      const observedError = active.then(
        () => null,
        (error: unknown) => error,
      );

      await expect(shutdownUsageIngestWorker()).rejects.toThrow(
        "ingest worker forced stop exceeded its cleanup deadline",
      );
      const error = await observedError;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toBe("ingest worker shut down");
      await expect(ingestUsageOffThread(paths)).rejects.toThrow(
        "ingest worker cleanup failed; relaunch blocked: ingest worker forced stop exceeded its cleanup deadline",
      );
    } finally {
      if (previousStopTimeout === undefined) {
        delete process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS;
      } else {
        process.env.TOKENS_INGEST_WORKER_STOP_TIMEOUT_MS = previousStopTimeout;
      }
    }
  });

  test("shutdown awaits child termination and leaves no active ingest", async () => {
    const paths = await fixture();
    _setUsageIngestLauncherForTest("direct");
    _setNextUsageIngestWorkerActionForTest({ delayMs: 2_000 });
    const active = ingestUsageOffThread(paths);
    const observedError = active.then(
      () => null,
      (error: unknown) => error,
    );
    expect(_usageIngestWorkerStatsForTest().active).toBe(true);

    await shutdownUsageIngestWorker();
    const error = await observedError;
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe("ingest worker shut down");
    expect(_usageIngestWorkerStatsForTest()).toMatchObject({
      failed: 0,
      active: false,
    });
  });
});

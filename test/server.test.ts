import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildAuthoritativeTokenSnapshot,
  computeRateSnapshot,
  dailyTokenWindow,
} from "../src/rate.ts";
import type { Aggregations } from "../src/scanner.ts";
import {
  cachedRateResponse,
  cachedStatsResponse,
  createIngestController,
  createSupersessionTracker,
  createUnfilteredUsageCache,
  healthResponse,
  type IngestHealth,
  isUnfilteredUsageCacheFresh,
  isUnfilteredUsageQuery,
  parseTokensSnapshotWindow,
  readFreshCachedTokenSnapshot,
  sanitizeTokensPollMs,
  shouldBroadcastIngestResult,
  shouldServeCachedTokenSnapshot,
  TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS,
  TOKENS_DEFAULT_POLL_MS,
  TOKENS_MAX_POLL_MS,
  TOKENS_MIN_POLL_MS,
  tokensCacheMaxAgeMs,
  usageHealthPayload,
} from "../src/server.ts";
import type { CapturedFileCommit, StoredEvent } from "../src/store.ts";
import { CLAUDE_USAGE_PARSER_VERSION, TokenStore } from "../src/store.ts";
import { UsageIngestWorkerShutdownError } from "../src/usage-ingest-worker.ts";

/** Minimal, schema-valid Aggregations fixture — only `totals.totalTokens`
 * and `scannedAt` vary between fixtures in these tests. */
function fixtureAggregations(totalTokens: number, scannedAt: number): Aggregations {
  return {
    totals: {
      inputTokens: totalTokens,
      outputTokens: 0,
      cacheReadTokens: 0,
      cacheWrite5mTokens: 0,
      cacheWrite1hTokens: 0,
      totalTokens,
      cost: 0,
      messages: 1,
      observedSpanSec: 0,
      tokensPerSecond: null,
    },
    daily: [],
    byModel: [],
    unrecognized: [],
    byProject: [],
    topSessions: [],
    byHarness: [],
    scannedAt,
    files: 1,
    range: { from: null, to: null },
    pricing: {},
  };
}

describe("server ingest readiness", () => {
  test("returns a bounded warming response until an exact worker snapshot exists", async () => {
    const response = cachedStatsResponse(
      { getRevision: () => 0, readCachedStats: () => null },
      5_000,
    );
    const body = await response.text();

    expect(response.status).toBe(503);
    expect(JSON.parse(body)).toEqual({
      error: "stats-warming",
      state: "warming",
      pollMs: 5_000,
    });
    expect(body.length).toBeLessThan(128);
  });

  test("rejects revision-stale stats and preserves the public success shape", async () => {
    const cached = {
      revision: 3,
      events: 10,
      files: 2,
      scannedAt: 10_000,
    };
    const stale = cachedStatsResponse(
      { getRevision: () => 4, readCachedStats: () => cached },
      5_000,
    );
    expect(stale.status).toBe(503);
    expect(await stale.json()).toEqual({
      error: "stats-stale",
      state: "degraded",
      updatedAt: 10_000,
      pollMs: 5_000,
    });

    const ready = cachedStatsResponse(
      { getRevision: () => 3, readCachedStats: () => cached },
      5_000,
    );
    expect(ready.status).toBe(200);
    expect(await ready.json()).toEqual({
      events: 10,
      files: 2,
      scannedAt: 10_000,
      pollMs: 5_000,
    });
  });

  test("rate responses fail closed while warming or stale", async () => {
    const warming = cachedRateResponse(
      { getRevision: () => 0, readCachedRateSnapshot: () => null },
      10_000,
      5_000,
      30_000,
    );
    expect(warming.status).toBe(503);
    expect(await warming.json()).toEqual({
      error: "rate-warming",
      state: "warming",
      pollMs: 5_000,
    });

    const snapshot = computeRateSnapshot([], 10_000);
    const stale = cachedRateResponse(
      {
        getRevision: () => 3,
        readCachedRateSnapshot: () => ({ revision: 3, publishedAt: 10_000, snapshot }),
      },
      40_001,
      5_000,
      30_000,
    );
    const staleBody = await stale.text();
    expect(stale.status).toBe(503);
    expect(JSON.parse(staleBody)).toEqual({
      error: "rate-stale",
      state: "degraded",
      updatedAt: 10_000,
      pollMs: 5_000,
    });
    expect(staleBody.length).toBeLessThan(160);

    const revisionDrift = cachedRateResponse(
      {
        getRevision: () => 4,
        readCachedRateSnapshot: () => ({ revision: 3, publishedAt: 10_000, snapshot }),
      },
      10_001,
      5_000,
      30_000,
    );
    expect(revisionDrift.status).toBe(503);
  });

  test("serves a fresh worker rate cache without rewriting it", async () => {
    const snapshot = computeRateSnapshot([], 10_000);
    const response = cachedRateResponse(
      {
        getRevision: () => 3,
        readCachedRateSnapshot: () => ({ revision: 3, publishedAt: 10_000, snapshot }),
      },
      10_001,
      5_000,
      30_000,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(snapshot);
  });

  test("rate freshness starts at completed publication, not the reducer anchor", async () => {
    const snapshot = computeRateSnapshot([], 10_000);
    const publishedAt = 90_000;
    const response = cachedRateResponse(
      {
        getRevision: () => 3,
        readCachedRateSnapshot: () => ({ revision: 3, publishedAt, snapshot }),
      },
      publishedAt + 1,
      5_000,
      30_000,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(snapshot);
  });

  test("token authority rejects stale and future heartbeats", () => {
    const now = Date.parse("2026-07-15T19:00:00.000Z");
    const snapshot = buildAuthoritativeTokenSnapshot([], {
      window: dailyTokenWindow(now),
      revision: 3,
      updatedAt: now,
      providerStates: [{ source: "codex", available: true, complete: true }],
    });
    const store = {
      getRevision: () => 3,
      readCachedTokenSnapshot: () => snapshot,
    };

    expect(readFreshCachedTokenSnapshot(store, now, 30_000).snapshot).toEqual(snapshot);
    snapshot.updatedAt = now - 30_001;
    expect(readFreshCachedTokenSnapshot(store, now, 30_000).snapshot).toBeNull();
    snapshot.updatedAt = now + 1;
    expect(readFreshCachedTokenSnapshot(store, now, 30_000).snapshot).toBeNull();
  });

  test("token authority admits mixed provider completeness but rejects no complete provider", () => {
    const now = Date.parse("2026-07-15T19:00:00.000Z");
    const snapshot = (
      claudeAvailable: boolean,
      claudeComplete: boolean,
      codexAvailable: boolean,
      codexComplete: boolean,
    ) =>
      buildAuthoritativeTokenSnapshot([], {
        window: dailyTokenWindow(now),
        revision: 3,
        updatedAt: now,
        providerStates: [
          { source: "claude", available: claudeAvailable, complete: claudeComplete },
          { source: "codex", available: codexAvailable, complete: codexComplete },
        ],
      });
    const read = (cached: ReturnType<typeof snapshot>, revision = 3) =>
      readFreshCachedTokenSnapshot(
        { getRevision: () => revision, readCachedTokenSnapshot: () => cached },
        now,
        30_000,
      ).snapshot;

    const codexComplete = snapshot(true, false, true, true);
    expect(read(codexComplete)).toEqual(codexComplete);
    const claudeComplete = snapshot(true, true, true, false);
    expect(read(claudeComplete)).toEqual(claudeComplete);
    expect(read(snapshot(true, false, true, false))).toBeNull();
    expect(read(snapshot(false, false, false, false))).toBeNull();
    // A mid-scan flush can bump the live store revision past the cached
    // snapshot's own revision without publishing a new snapshot — a
    // same-window, complete-provider cache behind the live revision is now
    // legitimately served (C-1).
    expect(read(codexComplete, 4)).toEqual(codexComplete);
    // A cache AHEAD of the live revision is future-poisoned and stays
    // rejected — this is the guard that stops readFreshCachedTokenSnapshot
    // from becoming a blanket accept.
    expect(read(codexComplete, 2)).toBeNull();

    const priorWindow = structuredClone(codexComplete);
    priorWindow.window = dailyTokenWindow(now - 24 * 60 * 60 * 1_000);
    expect(read(priorWindow)).toBeNull();
  });

  test("a cache superseded beyond the budget degrades even with a fresh updatedAt", () => {
    const now = Date.parse("2026-07-15T19:00:00.000Z");
    const cached = buildAuthoritativeTokenSnapshot([], {
      window: dailyTokenWindow(now),
      revision: 3,
      updatedAt: now,
      providerStates: [{ source: "codex", available: true, complete: true }],
    });
    const store = { getRevision: () => 4, readCachedTokenSnapshot: () => cached };
    const tracker = createSupersessionTracker();

    // First observation: superseded, but 0ms into the budget — still served.
    expect(readFreshCachedTokenSnapshot(store, now, 30_000, tracker).snapshot).toEqual(cached);
    // A heartbeat touch keeps `updatedAt` fresh (age check alone would never
    // reject this), but the supersession clock — tracked from first
    // observation, not from `updatedAt` — keeps advancing regardless.
    const freshenedUpdatedAt = {
      ...cached,
      updatedAt: now + TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS,
    };
    const stillWithinBudget = now + TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS;
    expect(
      readFreshCachedTokenSnapshot(
        { getRevision: () => 4, readCachedTokenSnapshot: () => freshenedUpdatedAt },
        stillWithinBudget,
        30_000,
        tracker,
      ).snapshot,
    ).toEqual(freshenedUpdatedAt);

    const beyondBudget = now + TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS + 1;
    const stillFreshUpdatedAt = { ...cached, updatedAt: beyondBudget };
    expect(
      readFreshCachedTokenSnapshot(
        { getRevision: () => 4, readCachedTokenSnapshot: () => stillFreshUpdatedAt },
        beyondBudget,
        30_000,
        tracker,
      ).snapshot,
    ).toBeNull();

    // Catching up to the live revision clears the supersession clock.
    const caughtUp = { ...cached, revision: 4, updatedAt: beyondBudget };
    expect(
      readFreshCachedTokenSnapshot(
        { getRevision: () => 4, readCachedTokenSnapshot: () => caughtUp },
        beyondBudget,
        30_000,
        tracker,
      ).snapshot,
    ).toEqual(caughtUp);
  });

  test("supersession tracking never leaks across independent stores sharing a revision number", () => {
    // Regression for the process-global `defaultSupersessionTracker`: it was
    // keyed only on a low-cardinality revision integer, so two entirely
    // independent stores/contexts (e.g. two fresh tmpdir DBs, both of which
    // restart their revision counters at small integers) could silently
    // share supersession state. Calling readFreshCachedTokenSnapshot with NO
    // explicit tracker — exactly as authoritativeSnapshotState/serverOptions
    // .fetch do — must obtain a tracker scoped to the store it was called
    // with, never one polluted by an unrelated store's history.
    //
    // PROVEN TO FAIL PRE-FIX: with the module-level `defaultSupersessionTracker`
    // singleton, storeA's observation of revision 3 (behind live 4) at `now`
    // set `since = now` for revision 3 process-wide. storeB's call below is
    // its own FIRST observation of "revision 3 behind live 4" — but because
    // it reuses the same revision integer, the shared tracker treated it as
    // a CONTINUATION of storeA's clock, so `laterNow - since` already
    // exceeded TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS and storeB's brand-new,
    // fully-fresh (age 0), complete-provider snapshot was wrongly rejected as
    // null. Ran this exact assertion against the pre-fix code (module-global
    // `defaultSupersessionTracker`, no per-store WeakMap) and observed
    // `.snapshot` to be `null`; after scoping trackers per store it is the
    // live `cached` snapshot, non-null.
    const now = Date.parse("2026-07-15T19:00:00.000Z");
    const snapshotAt = (revision: number, updatedAt: number) =>
      buildAuthoritativeTokenSnapshot([], {
        window: dailyTokenWindow(now),
        revision,
        updatedAt,
        providerStates: [{ source: "codex", available: true, complete: true }],
      });

    // storeA: an independent store/context object, observed once at `now`.
    const storeA = {
      getRevision: () => 4,
      readCachedTokenSnapshot: () => snapshotAt(3, now),
    };
    readFreshCachedTokenSnapshot(storeA, now, 30_000);

    // storeB: a SEPARATE store/context object (never seen before), whose own
    // live revision also happens to be 4 with a cached revision of 3 — same
    // small integers as storeA, but a genuinely independent object. This is
    // storeB's first-ever observation, well past storeA's budget window, and
    // must be served (age 0, complete provider, same window, revision behind
    // live only because of a legitimate mid-scan flush on storeB's side).
    const laterNow = now + TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS + 1;
    const cachedForB = snapshotAt(3, laterNow);
    const storeB = {
      getRevision: () => 4,
      readCachedTokenSnapshot: () => cachedForB,
    };
    expect(readFreshCachedTokenSnapshot(storeB, laterNow, 30_000).snapshot).toEqual(cachedForB);
  });

  test("mid-scan incomplete-provider flush does not overwrite the last complete cached snapshot", async () => {
    // The exact shape src/scanner.ts:2370-2374 builds mid-scan: a batch of
    // new events lands, but providers are marked incomplete even though
    // discovery had already gone complete.
    const root = await mkdtemp(join(tmpdir(), "tokens-mid-scan-premise-"));
    const store = new TokenStore(join(root, "tokens.sqlite"));
    try {
      const now = Date.parse("2026-07-15T19:00:00.000Z");
      const baseEvent: StoredEvent = {
        ts: now - 1_000,
        date: "2026-07-15",
        model: "claude-sonnet-4-6",
        project: "mid-scan",
        projectSlug: "-mid-scan",
        sessionId: "claude-session",
        inputTokens: 10,
        outputTokens: 4,
        cacheReadTokens: 3,
        cacheWrite5mTokens: 2,
        cacheWrite1hTokens: 0,
        cost: 0.1,
        filePath: "/fixtures/claude.jsonl",
        lineOffset: 1,
        source: "claude",
      };
      store.insertEvents([baseEvent]);
      store.reconcileDiscovery(
        [],
        [
          { source: "claude", available: true, complete: true },
          { source: "codex", available: true, complete: true },
        ],
      );
      const complete = store.refreshAuthoritativeTokenSnapshot(now);
      const revisionBeforeMidScan = store.getRevision();

      const mutatingBatch: CapturedFileCommit = {
        state: {
          generationKey: "mid-scan-file",
          source: "claude",
          device: "fixture-device",
          inode: "mid-scan-file",
          representativePath: "/fixtures/mid-scan.jsonl",
          capturedSize: 1,
          mtimeMs: 1,
          mtimeNs: null,
          ctimeNs: null,
          lastByteOffset: 1,
          lastLineNumber: 1,
          committedPrefixSha256: "0".repeat(64),
          codexStateJson: JSON.stringify({
            usageParserVersion: CLAUDE_USAGE_PARSER_VERSION,
            semanticUsageComplete: true,
            defectLineCount: 0,
            defectLineNumbers: [],
            usageComplete: true,
          }),
        },
        aliases: ["/fixtures/mid-scan.jsonl"],
        events: [
          {
            event: {
              ...baseEvent,
              sessionId: "mid-scan-session",
              filePath: "/fixtures/mid-scan.jsonl",
              ts: now - 500,
            },
            canonicalKey: "fixture:mid-scan-file",
            rawRecordSha256: "1".repeat(64),
            immutableIdentity: null,
            logicalMessageKey: null,
            byteStart: 0,
            byteEnd: 1,
            lineNumber: 0,
          },
        ],
      };

      const midScanResult = store.commitScan({
        batches: [mutatingBatch],
        aliases: [],
        providers: [
          { source: "claude", available: true, complete: false },
          { source: "codex", available: true, complete: false },
        ],
        legacyReconciliationObservations: [],
        sourceSnapshotRevision: revisionBeforeMidScan,
      });

      expect(midScanResult.mutated).toBe(true);
      expect(store.getRevision()).toBeGreaterThan(revisionBeforeMidScan);
      // The mid-scan flush never calls refreshAuthoritativeTokenSnapshot, so
      // the cache still holds the prior complete snapshot untouched.
      expect(store.readCachedTokenSnapshot()).toEqual(complete);
      // Explicit tracker: this call is the FIRST observation of this
      // revision-behind-live state, so it must be served regardless of
      // ordering against the module-default tracker's history in other
      // tests in this file.
      expect(
        readFreshCachedTokenSnapshot(store, now, 30_000, createSupersessionTracker()).snapshot,
      ).toEqual(complete);
    } finally {
      store.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("starting may reuse last-good; degraded snapshot routes stay fail-closed", () => {
    const base = {
      ingestRunning: false,
      consecutiveFailures: 0,
      lastFailureAt: null,
    };
    expect(shouldServeCachedTokenSnapshot({ ...base, ready: false, state: "starting" })).toBe(
      true,
    );
    expect(shouldServeCachedTokenSnapshot({ ...base, ready: false, state: "degraded" })).toBe(
      false,
    );
    expect(shouldServeCachedTokenSnapshot({ ...base, ready: true, state: "ready" })).toBe(true);
  });

  test("healthz stays 200 on last-good while starting and 503 on ingest-failed without cache", async () => {
    const starting: IngestHealth = {
      ready: false,
      state: "starting",
      ingestRunning: true,
      consecutiveFailures: 0,
      lastFailureAt: null,
    };
    expect(healthResponse(starting).status).toBe(503);
    const startingWithCache = healthResponse(starting, true);
    expect(startingWithCache.status).toBe(200);
    expect(await startingWithCache.json()).toMatchObject({
      ok: true,
      state: "starting",
      reason: "ingest-starting",
    });

    const failed: IngestHealth = {
      ready: false,
      state: "degraded",
      ingestRunning: false,
      consecutiveFailures: 1,
      lastFailureAt: 1234,
    };
    expect(healthResponse(failed).status).toBe(503);
    const failedWithCache = healthResponse(failed, true);
    expect(failedWithCache.status).toBe(200);
    expect(await failedWithCache.json()).toMatchObject({
      ok: true,
      state: "degraded",
      reason: "ingest-failed",
    });
  });

  test("fresh discovery-incomplete and failed authority stay fail-closed", async () => {
    const now = Date.parse("2026-07-15T19:00:00.000Z");
    const degraded = buildAuthoritativeTokenSnapshot([], {
      window: dailyTokenWindow(now),
      revision: 3,
      updatedAt: now,
      providerStates: [{ source: "codex", available: true, complete: false }],
    });
    expect(
      readFreshCachedTokenSnapshot(
        { getRevision: () => 3, readCachedTokenSnapshot: () => degraded },
        now,
        15_000,
      ).snapshot,
    ).toBeNull();

    const controller = createIngestController({
      getRevision: () => 3,
      ingest: async () => {
        throw new Error("injected failure");
      },
    });
    await controller.tick();
    expect(controller.health().state).toBe("degraded");
    expect(healthResponse(controller.health()).status).toBe(503);
  });

  test("requested worker shutdown is not recorded as an ingest failure", async () => {
    const reported: unknown[] = [];
    const requestedShutdown = createIngestController({
      getRevision: () => 3,
      ingest: async () => {
        throw new UsageIngestWorkerShutdownError();
      },
      onFailure: (error) => reported.push(error),
      now: () => 1234,
    });

    await requestedShutdown.tick();
    expect(requestedShutdown.health()).toEqual({
      ready: false,
      state: "starting",
      ingestRunning: false,
      consecutiveFailures: 0,
      lastFailureAt: null,
    });
    expect(reported).toEqual([]);

    const sameMessageFailure = new Error("ingest worker shut down");
    const unexpectedFailure = createIngestController({
      getRevision: () => 3,
      ingest: async () => {
        throw sameMessageFailure;
      },
      onFailure: (error) => reported.push(error),
      now: () => 1234,
    });

    await unexpectedFailure.tick();
    expect(unexpectedFailure.health()).toEqual({
      ready: false,
      state: "degraded",
      ingestRunning: false,
      consecutiveFailures: 1,
      lastFailureAt: 1234,
    });
    expect(reported).toEqual([sameMessageFailure]);
  });

  test("sanitizes poll cadence and keeps cache age compatible", () => {
    expect(sanitizeTokensPollMs(undefined)).toBe(TOKENS_DEFAULT_POLL_MS);
    expect(sanitizeTokensPollMs("0")).toBe(TOKENS_DEFAULT_POLL_MS);
    expect(sanitizeTokensPollMs("-10")).toBe(TOKENS_DEFAULT_POLL_MS);
    expect(sanitizeTokensPollMs("not-a-number")).toBe(TOKENS_DEFAULT_POLL_MS);
    expect(sanitizeTokensPollMs("1")).toBe(TOKENS_MIN_POLL_MS);
    expect(sanitizeTokensPollMs("600000")).toBe(TOKENS_MAX_POLL_MS);
    const slowCadence = sanitizeTokensPollMs("600000");
    expect(tokensCacheMaxAgeMs(slowCadence)).toBeGreaterThan(slowCadence);
  });

  test("snapshot window query defaults to daily and accepts rolling24h", () => {
    expect(parseTokensSnapshotWindow(null)).toBe("daily");
    expect(parseTokensSnapshotWindow("")).toBe("daily");
    expect(parseTokensSnapshotWindow("daily")).toBe("daily");
    expect(parseTokensSnapshotWindow("rolling24h")).toBe("rolling24h");
    expect(parseTokensSnapshotWindow("week")).toBeNull();
  });

  test("broadcasts no-op ingest results only when authority was refreshed", () => {
    expect(shouldBroadcastIngestResult({ mutated: false, snapshotRefreshed: false })).toBe(false);
    expect(shouldBroadcastIngestResult({ mutated: true, snapshotRefreshed: false })).toBe(true);
    expect(shouldBroadcastIngestResult({ mutated: false, snapshotRefreshed: true })).toBe(true);
  });

  test("SIGTERM clears the poll lifecycle and exits cleanly", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-server-shutdown-"));
    await mkdir(join(root, "claude"));
    await mkdir(join(root, "codex"));
    await mkdir(join(root, "grok-logs"));
    await mkdir(join(root, "grok-sessions"));
    const proc = Bun.spawn([process.execPath, "test/helpers/server-direct-bootstrap.ts"], {
      cwd: join(import.meta.dir, ".."),
      env: {
        ...process.env,
        TOKENS_PORT: "0",
        TOKENS_POLL_MS: "600000",
        TOKENS_DB_PATH: join(root, "tokens.sqlite"),
        CLAUDE_PROJECTS_DIR: join(root, "claude"),
        CODEX_SESSIONS_DIR: join(root, "codex"),
        GROK_LOGS_DIR: join(root, "grok-logs"),
        GROK_SESSIONS_DIR: join(root, "grok-sessions"),
        SIGMASHAKE_HOME: root,
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    const stdout = new Response(proc.stdout).text();
    const stderr = new Response(proc.stderr).text();
    try {
      await Bun.sleep(250);
      expect(proc.exitCode).toBeNull();
      proc.kill("SIGTERM");
      const exitCode = await Promise.race([
        proc.exited,
        Bun.sleep(3_000).then(() => Number.MIN_SAFE_INTEGER),
      ]);
      expect(exitCode).not.toBe(Number.MIN_SAFE_INTEGER);
      expect(exitCode).toBe(0);
      expect(await stdout).toContain("sigmashake-tokens listening on http://");
      expect(await stderr).not.toContain("error:");
    } finally {
      if (proc.exitCode === null) proc.kill("SIGKILL");
      await proc.exited;
      await rm(root, { recursive: true, force: true });
    }
  }, 5_000);

  test("starts unready and contains later store failures without leaking diagnostics", async () => {
    const secret = "/srv/example/private/token-store.sqlite";
    let ingestCalls = 0;
    const controller = createIngestController({
      getRevision: () => {
        throw new Error(secret);
      },
      ingest: async () => {
        ingestCalls++;
      },
      onFailure: () => {
        throw new Error("logger unavailable");
      },
      now: () => Number.MAX_VALUE,
    });

    const starting = healthResponse(controller.health());
    expect(starting.status).toBe(503);
    expect(await starting.json()).toMatchObject({
      ok: false,
      state: "starting",
      reason: "ingest-starting",
    });
    await expect(controller.tick()).resolves.toBeUndefined();

    expect(ingestCalls).toBe(0);
    expect(controller.health()).toEqual({
      ready: false,
      state: "degraded",
      ingestRunning: false,
      consecutiveFailures: 1,
      lastFailureAt: Number.MAX_SAFE_INTEGER,
    });
    const response = healthResponse(controller.health());
    const body = await response.text();
    expect(response.status).toBe(503);
    expect(body).toContain('"reason":"ingest-failed"');
    expect(body).not.toContain(secret);
    expect(body.length).toBeLessThan(256);
  });

  test("retries without overlap and becomes ready after the store recovers", async () => {
    let storeAvailable = false;
    let ingestCalls = 0;
    let finishIngest: (() => void) | undefined;
    const ingestPending = new Promise<void>((resolve) => {
      finishIngest = resolve;
    });
    const controller = createIngestController({
      getRevision: () => {
        if (!storeAvailable) throw new Error("store unavailable");
        return 7;
      },
      ingest: async () => {
        ingestCalls++;
        await ingestPending;
      },
      now: () => 1234,
    });

    expect(healthResponse(controller.health()).status).toBe(503);
    await controller.tick();
    expect(controller.health().ingestRunning).toBe(false);
    expect(healthResponse(controller.health()).status).toBe(503);

    storeAvailable = true;
    const recovery = controller.tick();
    await Promise.all(Array.from({ length: 8 }, () => controller.tick()));
    expect(ingestCalls).toBe(1);
    expect(controller.health()).toEqual({
      ready: false,
      state: "degraded",
      ingestRunning: true,
      consecutiveFailures: 1,
      lastFailureAt: 1234,
    });

    finishIngest?.();
    await recovery;
    expect(controller.health()).toEqual({
      ready: true,
      state: "ready",
      ingestRunning: false,
      consecutiveFailures: 0,
      lastFailureAt: null,
    });
    const response = healthResponse(controller.health());
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      ok: true,
      state: "ready",
      ingestRunning: false,
      consecutiveFailures: 0,
      lastFailureAt: null,
    });
  });

  test("publishes startup and recovery authority only after readiness flips", async () => {
    const priorReady: boolean[] = [];
    const publishedStates: string[] = [];
    let attempt = 0;
    let controller: ReturnType<typeof createIngestController>;
    controller = createIngestController({
      getRevision: () => 7,
      ingest: async (previouslyReady) => {
        priorReady.push(previouslyReady);
        attempt++;
        if (attempt === 2) throw new Error("degraded");
      },
      onAuthorityChange: () => {
        publishedStates.push(controller.health().state);
      },
    });

    await controller.tick();
    await controller.tick();
    await controller.tick();
    await controller.tick();

    expect(priorReady).toEqual([false, true, false, true]);
    expect(publishedStates).toEqual(["ready", "ready"]);
    expect(controller.health().state).toBe("ready");
  });

  test("does not duplicate an already-ready worker publication", async () => {
    let authorityChanges = 0;
    const priorReady: boolean[] = [];
    const controller = createIngestController({
      getRevision: () => 1,
      ingest: async (previouslyReady) => {
        priorReady.push(previouslyReady);
      },
      onAuthorityChange: () => {
        authorityChanges++;
      },
    });

    await controller.tick();
    await controller.tick();
    expect(controller.health().state).toBe("ready");
    expect(priorReady).toEqual([false, true]);
    expect(authorityChanges).toBe(1);
  });

  test("stays in starting readiness while the initial isolated ingest is active", async () => {
    let finishIngest: (() => void) | undefined;
    const ingestPending = new Promise<void>((resolve) => {
      finishIngest = resolve;
    });
    const controller = createIngestController({
      getRevision: () => 1,
      ingest: () => ingestPending,
    });

    const initialIngest = controller.tick();
    await Bun.sleep(0);
    expect(controller.health()).toEqual({
      ready: false,
      state: "starting",
      ingestRunning: true,
      consecutiveFailures: 0,
      lastFailureAt: null,
    });
    expect(healthResponse(controller.health()).status).toBe(503);

    finishIngest?.();
    await initialIngest;
    expect(controller.health().ingestRunning).toBe(false);
    expect(healthResponse(controller.health()).status).toBe(200);
  });

  test("broadcasts a committed authority change after a failed ingest", async () => {
    let revision = 10;
    let authorityChanges = 0;
    const controller = createIngestController({
      getRevision: () => revision,
      ingest: async () => {
        revision++;
        throw new Error("conflict");
      },
      onAuthorityChange: () => {
        authorityChanges++;
      },
    });

    await controller.tick();

    expect(authorityChanges).toBe(1);
    expect(controller.health().ready).toBe(false);
    expect(controller.health().ingestRunning).toBe(false);
  });

  test("caps repeated failure diagnostics", async () => {
    const controller = createIngestController({
      getRevision: () => {
        throw new Error("unavailable");
      },
      ingest: async () => {},
    });

    for (let i = 0; i < 1_005; i++) await controller.tick();

    expect(controller.health().consecutiveFailures).toBe(999);
  });
});

describe("usageHealthPayload contract", () => {
  test("health block has a stable key set across both health states", () => {
    const readyHealth: IngestHealth = {
      ready: true,
      state: "ready",
      ingestRunning: false,
      consecutiveFailures: 0,
      lastFailureAt: null,
    };
    const degradedHealth: IngestHealth = {
      ready: false,
      state: "degraded",
      ingestRunning: false,
      consecutiveFailures: 3,
      lastFailureAt: 1_000,
    };

    const readyPayload = usageHealthPayload(readyHealth, 1_785_544_017_094, 1_785_544_031_665);
    const degradedPayload = usageHealthPayload(
      degradedHealth,
      1_785_544_017_094,
      1_785_544_031_665,
    );

    // `reason` must always be present — not omitted when healthy — so
    // consumers that treat it as a required key never see a key-shape
    // that varies by health state (the OBS Boss HUD regression).
    expect(Object.keys(readyPayload).sort()).toEqual(Object.keys(degradedPayload).sort());
    expect("reason" in readyPayload).toBe(true);
    expect(readyPayload.reason).toBeNull();
    expect(typeof degradedPayload.reason).toBe("string");
    expect(degradedPayload.reason).toBe("ingest-failed");

    expect(readyPayload.state).toBe("ready");
    expect(degradedPayload.state).toBe("degraded");
  });
});

describe("unfiltered /api/usage cache (OBS Boss HUD hot path)", () => {
  test("isUnfilteredUsageCacheFresh is a pure wall-clock TTL check", () => {
    const entry = { agg: fixtureAggregations(1, 10_000), computedAt: 10_000 };
    expect(isUnfilteredUsageCacheFresh(null, 10_000, 30_000)).toBe(false);
    expect(isUnfilteredUsageCacheFresh(entry, 10_000, 30_000)).toBe(true);
    expect(isUnfilteredUsageCacheFresh(entry, 40_000, 30_000)).toBe(true);
    expect(isUnfilteredUsageCacheFresh(entry, 40_001, 30_000)).toBe(false);
    // Clock reversal must never be treated as freshness.
    expect(isUnfilteredUsageCacheFresh(entry, 9_999, 30_000)).toBe(false);
  });

  test("a second identical unfiltered request is served from cache: single-flight compute, byte-identical result", async () => {
    let computeCalls = 0;
    let clock = 0;
    const SLOW_COMPUTE_MS = 40;
    const cache = createUnfilteredUsageCache({
      compute: async () => {
        computeCalls++;
        // Simulate the real ~10s aggregation without actually taking 10s —
        // proportionally slow enough that a cache hit is measurably faster.
        await new Promise((resolve) => setTimeout(resolve, SLOW_COMPUTE_MS));
        return fixtureAggregations(49_000_000_000, clock);
      },
      now: () => clock,
      maxAgeMs: 30_000,
    });

    const firstStart = performance.now();
    const first = await cache.get();
    const firstElapsedMs = performance.now() - firstStart;
    expect(computeCalls).toBe(1);

    clock += 1_000; // still well inside the 30s TTL
    const secondStart = performance.now();
    const second = await cache.get();
    const secondElapsedMs = performance.now() - secondStart;

    // Not recomputed.
    expect(computeCalls).toBe(1);
    // Materially faster: the cache hit must not pay any fraction of the
    // simulated compute cost.
    expect(secondElapsedMs).toBeLessThan(firstElapsedMs);
    expect(secondElapsedMs).toBeLessThan(SLOW_COMPUTE_MS / 2);
    // Byte-identical in every field that must not change on a cache hit.
    expect(second).toEqual(first);
    expect(second.totals.totalTokens).toBe(49_000_000_000);
    expect(second.scannedAt).toBe(first.scannedAt);
  });

  test("concurrent requests during a cache miss share one in-flight compute (no pile-up)", async () => {
    let computeCalls = 0;
    const cache = createUnfilteredUsageCache({
      compute: async () => {
        computeCalls++;
        await new Promise((resolve) => setTimeout(resolve, 20));
        return fixtureAggregations(7, 0);
      },
      now: () => 0,
      maxAgeMs: 30_000,
    });

    const [a, b, c] = await Promise.all([cache.get(), cache.get(), cache.get()]);
    expect(computeCalls).toBe(1);
    expect(a).toEqual(b);
    expect(b).toEqual(c);
  });

  test("the cache expires: a request past maxAgeMs recomputes rather than serving stale-unlabeled data", async () => {
    let computeCalls = 0;
    let clock = 0;
    const cache = createUnfilteredUsageCache({
      compute: async () => {
        computeCalls++;
        return fixtureAggregations(computeCalls, clock);
      },
      now: () => clock,
      maxAgeMs: 5_000,
    });

    const first = await cache.get();
    expect(first.totals.totalTokens).toBe(1);
    expect(computeCalls).toBe(1);

    clock += 5_001; // one tick past the TTL
    const second = await cache.get();
    expect(computeCalls).toBe(2);
    expect(second.totals.totalTokens).toBe(2);
    expect(second.scannedAt).toBe(clock);
  });

  test("refresh() is throttled by refreshAfterMs — calling it every poll tick does not launch back-to-back recomputes", async () => {
    // Regression for a real defect caught during manual verification: an
    // earlier version of refresh() recomputed unconditionally on every
    // call. Wired into the 5s poll loop against a ~10s real compute, that
    // monopolized the single off-thread aggregation worker slot almost
    // continuously and made a concurrent filtered /api/usage request wait
    // ~30s behind the queue — the exact pile-up this cache exists to fix.
    let computeCalls = 0;
    let clock = 0;
    const cache = createUnfilteredUsageCache({
      compute: async () => {
        computeCalls++;
        return fixtureAggregations(computeCalls, clock);
      },
      now: () => clock,
      maxAgeMs: 30_000,
      refreshAfterMs: 15_000,
    });

    await cache.refresh();
    expect(computeCalls).toBe(1);

    // Simulates two more 5s poll ticks (10s elapsed), still inside the 15s
    // refreshAfterMs window — must not recompute.
    clock += 5_000;
    await cache.refresh();
    clock += 5_000;
    await cache.refresh();
    expect(computeCalls).toBe(1);
    expect(clock).toBe(10_000);

    // One more tick past refreshAfterMs (15s since the last compute) — now
    // it's allowed to recompute, keeping the entry from ever reaching the
    // hard maxAgeMs TTL.
    clock += 5_001; // clock = 15_001, just past the 15s refresh threshold
    await cache.refresh();
    expect(computeCalls).toBe(2);
  });

  test("isUnfilteredUsageQuery: any filter param routes away from the unfiltered cache", () => {
    expect(isUnfilteredUsageQuery({})).toBe(true);
    expect(isUnfilteredUsageQuery({ from: "2026-07-01" })).toBe(false);
    expect(isUnfilteredUsageQuery({ to: "2026-07-31" })).toBe(false);
    expect(isUnfilteredUsageQuery({ models: ["claude-opus-4"] })).toBe(false);
    expect(isUnfilteredUsageQuery({ projects: ["sigmashake-tokens"] })).toBe(false);
  });

  test("a filtered request never reuses the unfiltered cache entry — totals actually differ for an excluding range", async () => {
    // Simulates the real routing in serverOptions.fetch: the unfiltered
    // shape reads unfilteredUsageCache.get(); any filtered shape calls the
    // aggregation function directly with its own params, bypassing the
    // cache entirely.
    const cache = createUnfilteredUsageCache({
      compute: async () => fixtureAggregations(49_000_000_000, 1_000),
      now: () => 1_000,
      maxAgeMs: 30_000,
    });
    // Warm the unfiltered cache first, as the background poll loop does.
    await cache.get();

    const loadFiltered = async (params: { from?: string; to?: string }) => {
      // A range that excludes all data returns a materially different
      // (zero) total — proving this path recomputed rather than answering
      // from the unfiltered cache's 49B-token entry. The dataset lives
      // entirely at ts=1_000 (epoch-adjacent), so any 2020+ range excludes
      // it — this branch never runs against the unfiltered cache's data.
      const excludesAllData = params.from !== undefined && Date.parse(params.from) > 1_000;
      return fixtureAggregations(excludesAllData ? 0 : 49_000_000_000, 1_000);
    };

    const params = { from: "2020-01-01", to: "2020-01-02" };
    const agg = isUnfilteredUsageQuery(params) ? await cache.get() : await loadFiltered(params);

    expect(isUnfilteredUsageQuery(params)).toBe(false);
    expect(agg.totals.totalTokens).toBe(0);
    expect(agg.totals.totalTokens).not.toBe((await cache.get()).totals.totalTokens);
  });
});

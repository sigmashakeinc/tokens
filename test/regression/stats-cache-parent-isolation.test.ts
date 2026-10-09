import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { computeRateSnapshot } from "../../src/rate.ts";
import {
  cachedRateBucketForBroadcast,
  serverOptions,
  startTokensServer,
} from "../../src/server.ts";
import { _resetStoreForTests, getStore, type StoredEvent } from "../../src/store.ts";
import {
  _resetUsageIngestWorkerForTest,
  _setNextUsageIngestWorkerActionForTest,
  _setUsageIngestLauncherForTest,
} from "../../src/usage-ingest-worker.ts";

test("the /api/stats request path never runs an exact table count", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-stats-route-"));
  const previousDatabasePath = process.env.TOKENS_DB_PATH;
  try {
    _resetStoreForTests();
    process.env.TOKENS_DB_PATH = join(root, "tokens.sqlite");
    const store = getStore();
    store.refreshExactStatsCache(1_700_000_000_321);
    const exactStats = spyOn(store, "stats").mockImplementation(() => {
      throw new Error("HTTP parent attempted an exact count");
    });
    try {
      const response = await serverOptions.fetch(new Request("http://localhost/api/stats"));

      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        events: 0,
        files: 0,
        scannedAt: 1_700_000_000_321,
        pollMs: 5_000,
      });
      expect(exactStats).not.toHaveBeenCalled();
    } finally {
      exactStats.mockRestore();
    }
  } finally {
    _resetStoreForTests();
    if (previousDatabasePath === undefined) delete process.env.TOKENS_DB_PATH;
    else process.env.TOKENS_DB_PATH = previousDatabasePath;
    await rm(root, { recursive: true, force: true });
  }
});

test("the /api/snapshot request path never scans the events table", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-route-"));
  const previousDatabasePath = process.env.TOKENS_DB_PATH;
  try {
    _resetStoreForTests();
    process.env.TOKENS_DB_PATH = join(root, "tokens.sqlite");
    const store = getStore();
    const eventScan = spyOn(store, "selectEventsInWindow").mockImplementation(() => {
      throw new Error("HTTP parent attempted an event-window scan");
    });
    const snapshotBuild = spyOn(store, "buildAuthoritativeTokenSnapshot").mockImplementation(() => {
      throw new Error("HTTP parent attempted an authoritative SQL roll-up");
    });
    try {
      const response = await serverOptions.fetch(new Request("http://localhost/api/snapshot"));
      const snapshot = await response.json();

      // C-5: no cached authority exists yet, so the route now 503s instead of
      // fabricating a zero measurement (server.ts:227-234). The original intent
      // of this test — never scan the events table to answer /api/snapshot —
      // is preserved by the eventScan/snapshotBuild assertions below.
      expect(response.status).toBe(503);
      expect(snapshot).toEqual({ error: "snapshot-stale", state: "degraded", updatedAt: null });
      expect(eventScan).not.toHaveBeenCalled();
      expect(snapshotBuild).not.toHaveBeenCalled();
    } finally {
      eventScan.mockRestore();
      snapshotBuild.mockRestore();
    }
  } finally {
    _resetStoreForTests();
    if (previousDatabasePath === undefined) delete process.env.TOKENS_DB_PATH;
    else process.env.TOKENS_DB_PATH = previousDatabasePath;
    await rm(root, { recursive: true, force: true });
  }
});

test("rate request and broadcast paths never scan the events table", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-rate-route-"));
  const previousDatabasePath = process.env.TOKENS_DB_PATH;
  try {
    _resetStoreForTests();
    process.env.TOKENS_DB_PATH = join(root, "tokens.sqlite");
    const store = getStore();
    const now = Date.now();
    const snapshot = computeRateSnapshot([], now);
    store.writeCachedRateSnapshot(snapshot, store.getRevision());
    const eventScan = spyOn(store, "selectEvents").mockImplementation(() => {
      throw new Error("HTTP parent attempted a rate event scan");
    });
    const rateCursor = spyOn(store, "iterateRateEvents").mockImplementation(() => {
      throw new Error("HTTP parent attempted a compact rate cursor scan");
    });
    const rateSessionCursor = spyOn(store, "iterateRateSessionActivity").mockImplementation(() => {
      throw new Error("HTTP parent attempted a grouped rate session scan");
    });
    const exactRateRefresh = spyOn(store, "refreshRateSnapshotCache").mockImplementation(() => {
      throw new Error("HTTP parent attempted an exact rate refresh");
    });
    try {
      const response = await serverOptions.fetch(new Request("http://localhost/api/rate"));

      expect(response.status).toBe(200);
      expect(await response.json()).toEqual(snapshot);
      expect(cachedRateBucketForBroadcast(store, now + 1, 30_000)?.windowSec).toBe(60);
      expect(eventScan).not.toHaveBeenCalled();
      expect(rateCursor).not.toHaveBeenCalled();
      expect(rateSessionCursor).not.toHaveBeenCalled();
      expect(exactRateRefresh).not.toHaveBeenCalled();
    } finally {
      eventScan.mockRestore();
      rateCursor.mockRestore();
      rateSessionCursor.mockRestore();
      exactRateRefresh.mockRestore();
    }
  } finally {
    _resetStoreForTests();
    if (previousDatabasePath === undefined) delete process.env.TOKENS_DB_PATH;
    else process.env.TOKENS_DB_PATH = previousDatabasePath;
    await rm(root, { recursive: true, force: true });
  }
});

test("the /api/snapshot route reuses only complete mixed authority while starting", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-cache-route-"));
  const previousDatabasePath = process.env.TOKENS_DB_PATH;
  try {
    _resetStoreForTests();
    process.env.TOKENS_DB_PATH = join(root, "tokens.sqlite");
    const store = getStore();
    const cachedAt = Date.now();
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
    );
    const cached = store.refreshAuthoritativeTokenSnapshot(cachedAt);

    const accepted = await serverOptions.fetch(new Request("http://localhost/api/snapshot"));
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual(cached);

    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: false },
      ],
    );
    store.refreshAuthoritativeTokenSnapshot(cachedAt);
    const partial = await serverOptions.fetch(new Request("http://localhost/api/snapshot"));
    // C-5: an incomplete/unavailable mixed authority is no longer served as if
    // it were real — the route 503s instead of returning a stubbed-zero
    // provider body (server.ts:227-234). This strengthens the original intent
    // (incomplete authority must not be reused) by refusing to serve it at all.
    expect(partial.status).toBe(503);
    expect(await partial.json()).toEqual({
      error: "snapshot-stale",
      state: "degraded",
      updatedAt: cachedAt,
    });

    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
    );
    const current = store.refreshAuthoritativeTokenSnapshot(cachedAt);

    const cacheRead = spyOn(store, "readCachedTokenSnapshot").mockImplementationOnce(() => {
      // This is the exact adversarial ordering: cache r is read, then another
      // opener commits r+1 before the route validates the current revision.
      store.reconcileDiscovery(
        [],
        [
          { source: "claude", available: true, complete: true },
          { source: "codex", available: true, complete: false },
        ],
      );
      return current;
    });
    try {
      const stale = await serverOptions.fetch(new Request("http://localhost/api/snapshot"));
      const fallback = await stale.json();
      // C-1/C-6: the strict `cached.revision !== revision` pin was weakened to
      // `cached.revision > revision` (a behind-live cache is a mid-scan flush
      // artifact, not poison), and the new supersession budget (15s) hasn't
      // elapsed yet for this race, so the still-complete cached snapshot keeps
      // being served as-is (its own, now-superseded revision) instead of
      // falling back to a fabricated stub.
      expect(stale.status).toBe(200);
      expect(fallback).toEqual(current);
      expect(fallback.revision).toBe(current.revision);
      expect(fallback.providers.claude.available).toBe(true);
      expect(fallback.providers.claude.discoveryComplete).toBe(true);
    } finally {
      cacheRead.mockRestore();
    }
  } finally {
    _resetStoreForTests();
    if (previousDatabasePath === undefined) delete process.env.TOKENS_DB_PATH;
    else process.env.TOKENS_DB_PATH = previousDatabasePath;
    await rm(root, { recursive: true, force: true });
  }
});

test("the /api/snapshot route serves current mixed authority during an active ready ingest", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-active-route-"));
  const previousDatabasePath = process.env.TOKENS_DB_PATH;
  const previousClaudeProjectsDir = process.env.CLAUDE_PROJECTS_DIR;
  const previousCodexSessionsDir = process.env.CODEX_SESSIONS_DIR;
  const previousGrokLogsDir = process.env.GROK_LOGS_DIR;
  const previousGrokSessionsDir = process.env.GROK_SESSIONS_DIR;
  const claudeProjectsDir = join(root, "claude");
  const codexSessionsDir = join(root, "codex");
  const grokLogsDir = join(root, "grok-logs");
  const grokSessionsDir = join(root, "grok-sessions");
  const lifecycles: ReturnType<typeof startTokensServer>[] = [];
  const fakeServer = {
    hostname: "127.0.0.1",
    port: 0,
    stop: () => {},
  } as unknown as ReturnType<typeof Bun.serve>;
  const serve = spyOn(Bun, "serve").mockReturnValue(fakeServer);

  const waitForIdleReady = async (): Promise<void> => {
    for (let attempt = 0; attempt < 400; attempt++) {
      const response = await serverOptions.fetch(new Request("http://localhost/healthz"));
      const health = await response.json();
      if (health.state === "ready" && health.ingestRunning === false) return;
      await Bun.sleep(5);
    }
    throw new Error("ingest controller did not become idle and ready");
  };

  try {
    _resetStoreForTests();
    process.env.TOKENS_DB_PATH = join(root, "tokens.sqlite");
    process.env.CLAUDE_PROJECTS_DIR = claudeProjectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    // Isolate grok too — a live ~/.grok discoveryComplete provider would keep
    // tokenSnapshotHasCompleteProvider true after claude/codex are marked
    // incomplete, falsely 200ing the fail-closed snapshot assertion below.
    process.env.GROK_LOGS_DIR = grokLogsDir;
    process.env.GROK_SESSIONS_DIR = grokSessionsDir;
    await Promise.all([
      mkdir(claudeProjectsDir, { recursive: true }),
      mkdir(codexSessionsDir, { recursive: true }),
      mkdir(grokLogsDir, { recursive: true }),
      mkdir(grokSessionsDir, { recursive: true }),
    ]);
    _setUsageIngestLauncherForTest("direct");

    lifecycles.push(startTokensServer());
    await waitForIdleReady();

    const store = getStore();
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
        { source: "grok", available: false, complete: false },
      ],
    );
    const cachedAt = Date.now();
    const event = (source: "claude" | "codex"): StoredEvent => {
      const codex = source === "codex";
      return {
        ts: cachedAt - 1_000,
        date: "2026-07-18",
        model: codex ? "gpt-5.6-sol" : "claude-sonnet-4-6",
        project: "active-route",
        projectSlug: "-active-route",
        sessionId: `${source}-active-route`,
        inputTokens: codex ? 20 : 10,
        outputTokens: codex ? 8 : 4,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        cost: codex ? 0.02 : 0.01,
        filePath: `/fixture/${source}.jsonl`,
        lineOffset: codex ? 2 : 1,
        source,
        tokenClasses: {
          input: codex ? 20 : 10,
          cachedRead: 0,
          cacheWrite: codex ? null : 0,
          visibleOutput: codex ? 5 : 4,
          reasoningOutput: codex ? 3 : null,
          complete: false,
        },
      };
    };
    store.insertEvents([event("claude"), event("codex")]);
    const cached = store.refreshAuthoritativeTokenSnapshot(cachedAt);

    // A second lifecycle trigger enters tick() synchronously. Capture both
    // routes before the worker promise can settle so this is truly ready + active.
    _setNextUsageIngestWorkerActionForTest({ delayMs: 250 });
    lifecycles.push(startTokensServer());
    const activeHealthPromise = serverOptions.fetch(new Request("http://localhost/healthz"));
    const acceptedPromise = serverOptions.fetch(new Request("http://localhost/api/snapshot"));
    const [activeHealth, accepted] = await Promise.all([activeHealthPromise, acceptedPromise]);
    expect(await activeHealth.json()).toEqual(
      expect.objectContaining({ state: "ready", ingestRunning: true }),
    );
    expect(accepted.status).toBe(200);
    const acceptedSnapshot = await accepted.json();
    expect(acceptedSnapshot).toEqual(cached);
    expect(acceptedSnapshot.providers.claude).toEqual(
      expect.objectContaining({ available: true, discoveryComplete: true }),
    );
    expect(acceptedSnapshot.providers.codex).toEqual(
      expect.objectContaining({ available: true, discoveryComplete: true }),
    );
    expect(acceptedSnapshot.providers.claude.models).toHaveLength(1);
    expect(acceptedSnapshot.providers.codex.models).toHaveLength(1);

    await waitForIdleReady();
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: false },
        { source: "codex", available: true, complete: false },
        { source: "grok", available: false, complete: false },
      ],
    );
    const incompleteSnapshot = store.refreshAuthoritativeTokenSnapshot(Date.now());

    _setNextUsageIngestWorkerActionForTest({ delayMs: 250 });
    lifecycles.push(startTokensServer());
    const partialHealthPromise = serverOptions.fetch(new Request("http://localhost/healthz"));
    const rejectedPromise = serverOptions.fetch(new Request("http://localhost/api/snapshot"));
    const [partialHealth, rejected] = await Promise.all([partialHealthPromise, rejectedPromise]);
    expect(await partialHealth.json()).toEqual(
      expect.objectContaining({ state: "ready", ingestRunning: true }),
    );
    // C-5: an unavailable/incomplete authority is no longer served as if it
    // were real — the route 503s (server.ts:227-234) instead of returning a
    // stubbed-zero provider body, strengthening the original intent that an
    // incomplete authority during active ingest must not be reused.
    // `incompleteSnapshot.updatedAt` is exactly what identifies the rejected
    // cached snapshot in the degraded body (the `revision`/`providers` fields
    // are absent on a degraded response, per `tokenSnapshotResponse`), so the
    // assertion below is exact rather than `objectContaining` — a degraded
    // body with a DIFFERENT `updatedAt` (e.g. the wrong cache entry, or a
    // stale sibling revision) must fail this test.
    expect(rejected.status).toBe(503);
    expect(await rejected.json()).toEqual({
      error: "snapshot-stale",
      state: "degraded",
      updatedAt: incompleteSnapshot.updatedAt,
    });
    await waitForIdleReady();
  } finally {
    await Promise.allSettled(lifecycles.reverse().map((lifecycle) => lifecycle.shutdown()));
    await _resetUsageIngestWorkerForTest();
    serve.mockRestore();
    _resetStoreForTests();
    if (previousDatabasePath === undefined) delete process.env.TOKENS_DB_PATH;
    else process.env.TOKENS_DB_PATH = previousDatabasePath;
    if (previousClaudeProjectsDir === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
    else process.env.CLAUDE_PROJECTS_DIR = previousClaudeProjectsDir;
    if (previousCodexSessionsDir === undefined) delete process.env.CODEX_SESSIONS_DIR;
    else process.env.CODEX_SESSIONS_DIR = previousCodexSessionsDir;
    if (previousGrokLogsDir === undefined) delete process.env.GROK_LOGS_DIR;
    else process.env.GROK_LOGS_DIR = previousGrokLogsDir;
    if (previousGrokSessionsDir === undefined) delete process.env.GROK_SESSIONS_DIR;
    else process.env.GROK_SESSIONS_DIR = previousGrokSessionsDir;
    await rm(root, { recursive: true, force: true });
  }
});

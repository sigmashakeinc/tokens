// Tests for the pure rolling-window rate/throughput helper (src/rate.ts) and
// the events.source v1→v2 migration/backfill in src/store.ts. Kept PURE —
// no server.ts import (it boots Bun.serve at module load).

import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { computeRateSnapshot, selectRateSnapshot } from "../src/rate.ts";
import {
  _rateEventCursorQueryForTest,
  _rateSessionActivityCursorQueryForTest,
  _usageSessionCursorQueryForTest,
  type CapturedFileCommit,
  type StoredEvent,
  TokenStore,
} from "../src/store.ts";

function mkEvent(overrides: Partial<StoredEvent> = {}): StoredEvent {
  return {
    ts: 0,
    date: "2026-06-01",
    model: "claude-sonnet-4-6",
    project: "sigmashake-tokens",
    projectSlug: "-home-user-sigmashake-tokens",
    sessionId: "session-a",
    inputTokens: 100,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0.01,
    filePath: "/srv/example/.claude/projects/foo/bar.jsonl",
    lineOffset: 0,
    source: "claude",
    ...overrides,
  };
}

function sessionActivity(events: StoredEvent[]): Array<{ latestTs: number }> {
  const latestBySession = new Map<string, number>();
  for (const event of events) {
    latestBySession.set(
      event.sessionId,
      Math.max(latestBySession.get(event.sessionId) ?? Number.MIN_SAFE_INTEGER, event.ts),
    );
  }
  return [...latestBySession.values()].map((latestTs) => ({ latestTs }));
}

describe("computeRateSnapshot", () => {
  test("returns all four fixed windows with byModel + byHarness shape", () => {
    const now = 1_000_000_000;
    const events = [
      mkEvent({
        ts: now - 10_000,
        model: "claude-sonnet-4-6",
        source: "claude",
        sessionId: "s1",
        inputTokens: 1000,
      }),
      mkEvent({
        ts: now - 20_000,
        model: "gpt-5.5-cyber-preview",
        source: "codex",
        sessionId: "s2",
        inputTokens: 2000,
      }),
    ];
    const snap = computeRateSnapshot(events, now, sessionActivity(events));
    expect(snap.now).toBe(now);
    expect(snap.buckets.map((b) => b.windowSec)).toEqual([60, 300, 900, 3600]);

    const b60 = snap.buckets.find((b) => b.windowSec === 60)!;
    expect(b60.messages).toBe(2);
    expect(b60.tokens).toBe(3000);
    expect(b60.activeSessions).toBe(2);
    expect(b60.byModel.length).toBe(2);
    expect(b60.byHarness.length).toBe(2);

    const claudeHarness = b60.byHarness.find((h) => h.source === "claude")!;
    expect(claudeHarness.tokens).toBe(1000);
    expect(claudeHarness.messages).toBe(1);
    const codexHarness = b60.byHarness.find((h) => h.source === "codex")!;
    expect(codexHarness.tokens).toBe(2000);
  });

  test("tokensPerSecond = tokens ÷ windowSec (fixed denominator, never 0)", () => {
    const now = 2_000_000_000;
    const events = [mkEvent({ ts: now - 5_000, inputTokens: 600, outputTokens: 0 })];
    const snap = computeRateSnapshot(events, now, sessionActivity(events));
    for (const b of snap.buckets) {
      expect(b.tokensPerSecond).toBeCloseTo(600 / b.windowSec);
      expect(b.tokensPerMinute).toBeCloseTo(600 / (b.windowSec / 60));
      expect(b.byModel[0]?.tokensPerSecond).toBeCloseTo(600 / b.windowSec);
    }
  });

  test("excludes future-dated rows via its own now bound, independent of caller filtering", () => {
    const now = 3_000_000_000;
    const events = [
      mkEvent({ ts: now - 1_000, inputTokens: 100, outputTokens: 0 }),
      // Future-dated relative to `now` — must never inflate a live window.
      mkEvent({ ts: now + 5_000, inputTokens: 999_999, outputTokens: 0, sessionId: "future" }),
    ];
    const b60 = computeRateSnapshot(events, now, sessionActivity(events)).buckets.find(
      (b) => b.windowSec === 60,
    )!;
    expect(b60.tokens).toBe(100);
    expect(b60.messages).toBe(1);
  });

  test("excludes rows older than the window's lower bound", () => {
    const now = 4_000_000_000;
    const events = [
      mkEvent({ ts: now - 30_000, inputTokens: 100, outputTokens: 0 }), // inside 60s
      mkEvent({ ts: now - 120_000, inputTokens: 500, outputTokens: 0, sessionId: "old" }), // outside 60s
    ];
    const snapshot = computeRateSnapshot(events, now, sessionActivity(events));
    const b60 = snapshot.buckets.find((b) => b.windowSec === 60)!;
    expect(b60.tokens).toBe(100);
    const b300 = snapshot.buckets.find((b) => b.windowSec === 300)!;
    expect(b300.tokens).toBe(600);
  });

  test("empty event set yields zeroed buckets, never throws", () => {
    const snap = computeRateSnapshot([], Date.now());
    for (const b of snap.buckets) {
      expect(b.tokens).toBe(0);
      expect(b.messages).toBe(0);
      expect(b.tokensPerSecond).toBe(0);
      expect(b.byModel).toEqual([]);
      expect(b.byHarness).toEqual([]);
    }
  });

  test("selectRateSnapshot performs bounded event and grouped-session cursor reads", () => {
    const now = 5_000_000_000;
    const eventCalls: Array<{ fromTs: number; toTs: number }> = [];
    const activityCalls: Array<{ fromTs: number; toTs: number }> = [];
    const store = {
      iterateRateEvents(opts: { fromTs: number; toTs: number }) {
        eventCalls.push(opts);
        return [mkEvent({ ts: now - 30_000, inputTokens: 3600 })].values();
      },
      iterateRateSessionActivity(opts: { fromTs: number; toTs: number }) {
        activityCalls.push(opts);
        return [{ latestTs: now - 30_000 }].values();
      },
    };

    const snapshot = selectRateSnapshot(store, now);

    expect(eventCalls).toEqual([{ fromTs: now - 3_600_000, toTs: now }]);
    expect(activityCalls).toEqual([{ fromTs: now - 3_600_000, toTs: now }]);
    expect(snapshot.buckets.find((bucket) => bucket.windowSec === 60)?.tokens).toBe(3600);
    expect(snapshot.buckets.find((bucket) => bucket.windowSec === 3600)?.tokens).toBe(3600);
  });

  test("consumes a one-shot event cursor exactly once for all four windows", () => {
    const now = 5_500_000_000;
    let iterations = 0;
    const rows = [
      mkEvent({ ts: now - 30_000, sessionId: "recent", inputTokens: 60 }),
      mkEvent({ ts: now - 600_000, sessionId: "older", inputTokens: 900 }),
    ];
    const cursor: Iterable<StoredEvent> = {
      [Symbol.iterator]() {
        iterations += 1;
        if (iterations > 1) throw new Error("rate cursor was consumed more than once");
        return rows.values();
      },
    };
    let activityIterations = 0;
    const activityCursor: Iterable<{ latestTs: number }> = {
      [Symbol.iterator]() {
        activityIterations += 1;
        if (activityIterations > 1) throw new Error("session cursor was consumed more than once");
        return sessionActivity(rows).values();
      },
    };

    const snapshot = computeRateSnapshot(cursor, now, activityCursor);

    expect(iterations).toBe(1);
    expect(activityIterations).toBe(1);
    expect(snapshot.buckets.find((bucket) => bucket.windowSec === 60)).toMatchObject({
      tokens: 60,
      messages: 1,
      activeSessions: 1,
    });
    expect(snapshot.buckets.find((bucket) => bucket.windowSec === 3600)).toMatchObject({
      tokens: 960,
      messages: 2,
      activeSessions: 2,
    });
  });
});

describe("aggregateEventTotals", () => {
  test("rolls up exact totals and inclusive bounds without materializing event rows", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-sql-totals-"));
    const store = new TokenStore(join(root, "tokens.sqlite"));
    try {
      store.insertEvents([
        mkEvent({
          ts: 1_000,
          sessionId: "first",
          inputTokens: 10,
          outputTokens: 2,
          cacheReadTokens: 3,
          cacheWrite5mTokens: 4,
          cacheWrite1hTokens: 5,
          cost: 0.1,
          lineOffset: 1,
        }),
        mkEvent({
          ts: 2_000,
          sessionId: "second",
          inputTokens: 20,
          outputTokens: 4,
          cacheReadTokens: 6,
          cacheWrite5mTokens: 8,
          cacheWrite1hTokens: 10,
          cost: 0.2,
          lineOffset: 2,
        }),
      ]);

      expect(store.aggregateEventTotals()).toEqual({
        inputTokens: 30,
        outputTokens: 6,
        cacheReadTokens: 9,
        cacheWrite5mTokens: 12,
        cacheWrite1hTokens: 15,
        cost: 0.30000000000000004,
        messages: 2,
        minTs: 1_000,
        maxTs: 2_000,
      });
      expect(store.aggregateEventTotals({ fromTs: 2_000, toTs: 2_000 })).toMatchObject({
        inputTokens: 20,
        messages: 1,
        minTs: 2_000,
        maxTs: 2_000,
      });
    } finally {
      store.close();
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("events.source v1→v2 migration/backfill", () => {
  async function withRawV1Db(fn: (ctx: { dbPath: string }) => Promise<void>): Promise<void> {
    const root = await mkdtemp(join(tmpdir(), "tokens-migrate-"));
    const dbPath = join(root, "tokens.sqlite");
    try {
      // Build a pre-migration v1 DB by hand: no `source` column, user_version=1.
      const raw = new Database(dbPath);
      raw.run(
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, date TEXT NOT NULL, model TEXT NOT NULL, project TEXT NOT NULL, project_slug TEXT NOT NULL, session_id TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, cache_read_tokens INTEGER NOT NULL, cache_write_5m_tokens INTEGER NOT NULL, cache_write_1h_tokens INTEGER NOT NULL, cost REAL NOT NULL, file_path TEXT NOT NULL, line_offset INTEGER NOT NULL, UNIQUE(file_path, line_offset))",
      );
      raw.run(
        "CREATE TABLE file_state (path TEXT PRIMARY KEY, mtime_ms INTEGER NOT NULL, size INTEGER NOT NULL, last_byte_offset INTEGER NOT NULL, last_line_number INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
      );
      raw.run("PRAGMA user_version = 1");
      const insert = raw.prepare(
        "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      );
      insert.run(
        1000,
        "2026-01-01",
        "gpt-5.5-cyber-preview",
        "sigmashake-tokens",
        "-home-user-sigmashake-tokens",
        "sess-codex",
        10,
        5,
        0,
        0,
        0,
        0.001,
        "/srv/example/.codex/sessions/2026/rollout-x.jsonl",
        0,
      );
      insert.run(
        2000,
        "2026-01-01",
        "claude-sonnet-4-6",
        "sigmashake-tokens",
        "-home-user-sigmashake-tokens",
        "sess-claude",
        10,
        5,
        0,
        0,
        0,
        0.001,
        "/srv/example/.claude/projects/foo/bar.jsonl",
        1,
      );
      insert.run(
        3000,
        "2026-01-01",
        "gpt-5.5-cyber-preview",
        "sigmashake-tokens",
        "-home-user-sigmashake-tokens",
        "sess-custom-root",
        10,
        5,
        0,
        0,
        0,
        0.001,
        "/mnt/custom-codex-root/2026/rollout-y.jsonl",
        2,
      );
      raw.close();
      await fn({ dbPath });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }

  test("backfills source='codex' for /.codex/ file_paths and 'claude' otherwise", async () => {
    await withRawV1Db(async ({ dbPath }) => {
      const store = new TokenStore(dbPath);
      try {
        const events = store.selectEvents();
        const codexEvent = events.find((e) => e.sessionId === "sess-codex");
        const claudeEvent = events.find((e) => e.sessionId === "sess-claude");
        expect(codexEvent?.source).toBe("codex");
        expect(claudeEvent?.source).toBe("claude");
      } finally {
        store.close();
      }
    });
  });

  test("also backfills a configured CODEX_SESSIONS_DIR root, not just /.codex/", async () => {
    const prevRoot = process.env.CODEX_SESSIONS_DIR;
    process.env.CODEX_SESSIONS_DIR = "/mnt/custom-codex-root";
    try {
      await withRawV1Db(async ({ dbPath }) => {
        const store = new TokenStore(dbPath);
        try {
          const events = store.selectEvents();
          const customRootEvent = events.find((e) => e.sessionId === "sess-custom-root");
          expect(customRootEvent?.source).toBe("codex");
        } finally {
          store.close();
        }
      });
    } finally {
      if (prevRoot === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = prevRoot;
    }
  });

  test("migration is idempotent — safe to construct a second TokenStore against the same file", async () => {
    await withRawV1Db(async ({ dbPath }) => {
      const store1 = new TokenStore(dbPath);
      store1.close();
      const store2 = new TokenStore(dbPath);
      try {
        const events = store2.selectEvents();
        expect(events.length).toBe(3);
        expect(events.find((e) => e.sessionId === "sess-codex")?.source).toBe("codex");
      } finally {
        store2.close();
      }
    });
  });

  test("session aggregation migration streams in exact reducer order without a temp sort", async () => {
    await withRawV1Db(async ({ dbPath }) => {
      const first = new TokenStore(dbPath);
      first.close();
      const second = new TokenStore(dbPath);
      second.close();

      const inspected = new Database(dbPath, { readonly: true });
      try {
        const columns = inspected
          .query("PRAGMA index_info(idx_events_session_ts_id)")
          .all() as Array<{ name: string }>;
        expect(columns.map((column) => column.name)).toEqual(["session_id", "ts", "id"]);

        const plan = inspected
          .query(`EXPLAIN QUERY PLAN ${_usageSessionCursorQueryForTest("ts >= ? AND ts <= ?")}`)
          .all(0, Number.MAX_SAFE_INTEGER) as Array<{ detail: string }>;
        const details = plan.map((row) => row.detail);
        expect(details.some((detail) => detail.includes("idx_events_session_ts_id"))).toBe(true);
        expect(details.some((detail) => detail.includes("USE TEMP B-TREE"))).toBe(false);
      } finally {
        inspected.close();
      }
    });
  });

  test("rate reduction streams through the timestamp index without a full scan or temp sort", async () => {
    await withRawV1Db(async ({ dbPath }) => {
      const store = new TokenStore(dbPath);
      store.close();
      const inspected = new Database(dbPath, { readonly: true });
      try {
        const plan = inspected
          .query(`EXPLAIN QUERY PLAN ${_rateEventCursorQueryForTest()}`)
          .all(0, Number.MAX_SAFE_INTEGER) as Array<{ detail: string }>;
        const details = plan.map((row) => row.detail);
        expect(details.some((detail) => detail.includes("idx_events_ts"))).toBe(true);
        expect(details.some((detail) => detail.includes("SCAN events"))).toBe(false);
        expect(details.some((detail) => detail.includes("USE TEMP B-TREE"))).toBe(false);

        const activityPlan = inspected
          .query(`EXPLAIN QUERY PLAN ${_rateSessionActivityCursorQueryForTest()}`)
          .all(0, Number.MAX_SAFE_INTEGER) as Array<{ detail: string }>;
        expect(activityPlan.some((row) => row.detail.includes("idx_events_ts"))).toBe(true);
      } finally {
        inspected.close();
      }
    });
  });

  test("v4 migration preserves legacy billing rows and installs exact-once schema", async () => {
    await withRawV1Db(async ({ dbPath }) => {
      const store1 = new TokenStore(dbPath);
      const first = new Database(dbPath);
      try {
        const cols = first.query("PRAGMA table_info(events)").all() as Array<{ name: string }>;
        expect(cols.some((c) => c.name === "dedupe_key")).toBe(true);
        expect(cols.some((c) => c.name === "canonical_key")).toBe(true);
        expect(cols.some((c) => c.name === "class_reasoning_output_tokens")).toBe(true);
        const indexes = first.query("PRAGMA index_list(events)").all() as Array<{
          name: string;
          unique: number;
        }>;
        const dedupeIndex = indexes.find((i) => i.name === "idx_events_dedupe_key");
        const canonicalIndex = indexes.find((i) => i.name === "idx_events_canonical");
        expect(dedupeIndex?.unique).toBe(0);
        expect(canonicalIndex?.unique).toBe(1);
        const legacy = store1.selectEvents().find((event) => event.sessionId === "sess-codex");
        expect(legacy?.inputTokens).toBe(10);
        expect(legacy?.outputTokens).toBe(5);
        expect(legacy?.cost).toBe(0.001);
      } finally {
        first.close();
        store1.close();
      }

      const store2 = new TokenStore(dbPath);
      const second = new Database(dbPath);
      try {
        const version = (second.query("PRAGMA user_version").get() as { user_version: number })
          .user_version;
        expect(version).toBe(5);
        expect(store2.selectEvents().length).toBe(3);
      } finally {
        second.close();
        store2.close();
      }
    });
  });

  test("same captured occurrence retry is transactionally exact once", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-occurrence-retry-"));
    const store = new TokenStore(join(root, "tokens.sqlite"));
    try {
      const captured: CapturedFileCommit = {
        state: {
          generationKey: "claude:1:2:generation",
          source: "claude",
          device: "1",
          inode: "2",
          representativePath: "/fixture/session.jsonl",
          capturedSize: 128,
          mtimeMs: 10,
          mtimeNs: "10000000",
          ctimeNs: "10000000",
          lastByteOffset: 128,
          lastLineNumber: 1,
          committedPrefixSha256: "prefix",
          codexStateJson: null,
        },
        aliases: ["/fixture/session.jsonl"],
        events: [
          {
            event: mkEvent({
              ts: 1000,
              filePath: "/fixture/session.jsonl",
              lineOffset: 0,
            }),
            canonicalKey: "canonical-1",
            rawRecordSha256: "raw-1",
            immutableIdentity: null,
            logicalMessageKey: "request:one",
            byteStart: 0,
            byteEnd: 128,
            lineNumber: 0,
          },
        ],
      };
      const first = store.commitCapturedFile(captured);
      const revision = first.revision;
      const retry = store.commitCapturedFile(captured);
      expect(first.inserted).toBe(1);
      expect(retry.inserted).toBe(0);
      expect(retry.mutated).toBe(false);
      expect(retry.revision).toBe(revision);
      expect(store.selectEvents()).toHaveLength(1);

      const reconciled = store.reconcileDiscovery(
        [
          {
            path: "/fixture/session.jsonl",
            generationKey: captured.state.generationKey,
            source: "claude",
          },
        ],
        [{ source: "claude", available: true, complete: true }],
      );
      const reconciledRetry = store.reconcileDiscovery(
        [
          {
            path: "/fixture/session.jsonl",
            generationKey: captured.state.generationKey,
            source: "claude",
          },
        ],
        [{ source: "claude", available: true, complete: true }],
      );
      expect(reconciled.mutated).toBe(true);
      expect(reconciledRetry.mutated).toBe(false);
      expect(reconciledRetry.revision).toBe(reconciled.revision);
    } finally {
      store.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("future schema is rejected without lowering user_version", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-future-schema-"));
    const dbPath = join(root, "tokens.sqlite");
    try {
      const future = new Database(dbPath);
      future.run("PRAGMA user_version = 6");
      future.close();
      expect(() => new TokenStore(dbPath)).toThrow("newer than supported schema");
      const inspected = new Database(dbPath);
      try {
        const version = (
          inspected.query("PRAGMA user_version").get() as {
            user_version: number;
          }
        ).user_version;
        expect(version).toBe(6);
      } finally {
        inspected.close();
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

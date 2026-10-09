import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadSessionContextEstimate } from "../src/server.ts";
import { _resetStoreForTests, getStore, type StoredEvent } from "../src/store.ts";

function event(overrides: Partial<StoredEvent> & { ts: number; lineOffset: number }): StoredEvent {
  return {
    date: "2026-07-18",
    model: "claude-sonnet-4-6",
    project: "session-context",
    projectSlug: "-session-context",
    sessionId: "session-a",
    inputTokens: 100,
    outputTokens: 20,
    cacheReadTokens: 10,
    cacheWrite5mTokens: 5,
    cacheWrite1hTokens: 0,
    cost: 0.01,
    filePath: "/fixtures/session-context.jsonl",
    source: "claude",
    ...overrides,
  };
}

let root: string;
let previousDbPath: string | undefined;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "tokens-session-context-"));
  previousDbPath = process.env.TOKENS_DB_PATH;
  process.env.TOKENS_DB_PATH = join(root, "tokens.sqlite");
  _resetStoreForTests();
});

afterEach(async () => {
  _resetStoreForTests();
  if (previousDbPath === undefined) delete process.env.TOKENS_DB_PATH;
  else process.env.TOKENS_DB_PATH = previousDbPath;
  await rm(root, { recursive: true, force: true });
});

test("selectLatestEventForSession returns null for an unknown session", () => {
  expect(getStore().selectLatestEventForSession("no-such-session")).toBeNull();
});

test("selectLatestEventForSession returns the highest-ts event for the session", () => {
  getStore().insertEvents([
    event({ ts: 1_700_000_000_000, lineOffset: 1, inputTokens: 50 }),
    event({ ts: 1_700_000_000_500, lineOffset: 2, inputTokens: 999 }),
    event({ ts: 1_700_000_000_250, lineOffset: 3, inputTokens: 75 }),
    event({ ts: 1_700_000_000_500, lineOffset: 4, sessionId: "session-b", inputTokens: 1 }),
  ]);
  const latest = getStore().selectLatestEventForSession("session-a");
  expect(latest).not.toBeNull();
  expect(latest!.ts).toBe(1_700_000_000_500);
  expect(latest!.inputTokens).toBe(999);
});

test("selectLatestEventForSession breaks an exact-ts tie by highest id (most recently inserted)", () => {
  const tiedTs = 1_700_000_000_500;
  getStore().insertEvents([event({ ts: tiedTs, lineOffset: 1, inputTokens: 111 })]);
  getStore().insertEvents([event({ ts: tiedTs, lineOffset: 2, inputTokens: 222 })]);
  const latest = getStore().selectLatestEventForSession("session-a");
  expect(latest!.ts).toBe(tiedTs);
  expect(latest!.inputTokens).toBe(222);
});

test("regression: selectEventsForSession still returns full multi-event history in ts ASC order", () => {
  getStore().insertEvents([
    event({ ts: 1_700_000_000_300, lineOffset: 1, inputTokens: 10 }),
    event({ ts: 1_700_000_000_100, lineOffset: 2, inputTokens: 20 }),
    event({ ts: 1_700_000_000_200, lineOffset: 3, inputTokens: 30 }),
  ]);
  const events = getStore().selectEventsForSession("session-a");
  expect(events).toHaveLength(3);
  expect(events.map((e) => e.ts)).toEqual([
    1_700_000_000_100, 1_700_000_000_200, 1_700_000_000_300,
  ]);
  expect(events.map((e) => e.inputTokens)).toEqual([20, 30, 10]);
});

test("loadSessionContextEstimate: unknown session reports found:false with null fields", () => {
  const result = loadSessionContextEstimate("no-such-session");
  expect(result).toEqual({
    sessionId: "no-such-session",
    found: false,
    ts: null,
    model: null,
    estimatedContextTokens: null,
    components: null,
  });
});

test("loadSessionContextEstimate uses only the latest event's uncached-input + cache-read + cache-write (excludes output, not cumulative across the session)", () => {
  getStore().insertEvents([
    event({
      ts: 1_700_000_000_100,
      lineOffset: 1,
      inputTokens: 1000,
      cacheReadTokens: 500,
      cacheWrite5mTokens: 200,
      cacheWrite1hTokens: 100,
      outputTokens: 9999,
    }),
    event({
      ts: 1_700_000_000_200,
      lineOffset: 2,
      inputTokens: 40,
      cacheReadTokens: 30,
      cacheWrite5mTokens: 20,
      cacheWrite1hTokens: 10,
      outputTokens: 1,
    }),
  ]);
  const result = loadSessionContextEstimate("session-a");
  expect(result.found).toBe(true);
  expect(result.ts).toBe(1_700_000_000_200);
  // Latest event only (40+30+30=100) — never the two-event sum (1600), and
  // never includes outputTokens.
  expect(result.estimatedContextTokens).toBe(100);
  expect(result.components).toEqual({
    inputTokens: 40,
    cacheReadTokens: 30,
    cacheWriteTokens: 30,
  });
});

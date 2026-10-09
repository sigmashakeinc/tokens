import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type StoredEvent, TokenStore } from "../src/store.ts";

function event(lineOffset: number): StoredEvent {
  const ts = 1_700_000_000_000 + lineOffset;
  return {
    ts,
    date: "2023-11-14",
    model: "claude-sonnet-4-6",
    project: "stats-cache",
    projectSlug: "-stats-cache",
    sessionId: `session-${lineOffset}`,
    inputTokens: 10,
    outputTokens: 20,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0.01,
    filePath: "/fixtures/stats-cache.jsonl",
    lineOffset,
    source: "claude",
  };
}

test("exact stats cache is timestamped, stable, and durable across store reopen", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-stats-cache-"));
  const databasePath = join(root, "tokens.sqlite");
  let store: TokenStore | null = new TokenStore(databasePath);
  try {
    expect(store.readCachedStats()).toBeNull();
    store.insertEvents([event(1)]);
    const firstRevision = store.getRevision();
    expect(store.refreshExactStatsCache(1_700_000_000_100, firstRevision)).toEqual({
      revision: firstRevision,
      events: 1,
      files: 0,
      scannedAt: 1_700_000_000_100,
    });

    // Later writes do not masquerade as part of the older exact snapshot.
    store.insertEvents([event(2)]);
    expect(store.readCachedStats()).toEqual({
      revision: firstRevision,
      events: 1,
      files: 0,
      scannedAt: 1_700_000_000_100,
    });
    store.close();
    store = null;

    const reopened = new TokenStore(databasePath, { readonly: true });
    try {
      expect(reopened.readCachedStats()).toEqual({
        revision: firstRevision,
        events: 1,
        files: 0,
        scannedAt: 1_700_000_000_100,
      });
    } finally {
      reopened.close();
    }

    store = new TokenStore(databasePath);
    const currentRevision = store.getRevision();
    expect(store.refreshExactStatsCache(1_700_000_000_200, currentRevision)).toEqual({
      revision: currentRevision,
      events: 2,
      files: 0,
      scannedAt: 1_700_000_000_200,
    });
  } finally {
    store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("exact stats publication is revision-pinned and preserves newer same-revision scans", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-stats-cache-cas-"));
  const databasePath = join(root, "tokens.sqlite");
  const store = new TokenStore(databasePath);
  try {
    store.insertEvents([event(1)]);
    const revision = store.getRevision();
    const older = store.computeExactStatsSnapshot(1_700_000_000_100, revision);
    const newer = store.computeExactStatsSnapshot(1_700_000_000_200, revision);
    expect(store.publishExactStatsCache(newer)).toEqual(newer);
    expect(store.publishExactStatsCache(older)).toEqual(newer);
    expect(store.readCachedStats()).toEqual(newer);

    const stale = store.computeExactStatsSnapshot(1_700_000_000_300, revision);
    const concurrent = new TokenStore(databasePath);
    try {
      concurrent.insertEvents([event(2)]);
    } finally {
      concurrent.close();
    }
    expect(() => store.publishExactStatsCache(stale)).toThrow(
      "stats cache revision changed before publication",
    );
    expect(store.readCachedStats()).toEqual(newer);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

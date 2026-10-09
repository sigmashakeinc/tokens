import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { computeRateSnapshot, isRateSnapshot, RATE_MODEL_BUCKET_MAX } from "../src/rate.ts";
import { type StoredEvent, TokenStore } from "../src/store.ts";

function event(now: number, index: number): StoredEvent {
  return {
    ts: now - 1_000,
    date: "2026-07-15",
    model: `model-${index}`,
    project: "sigmashake-tokens",
    projectSlug: "-home-user-sigmashake-tokens",
    sessionId: `session-${index}`,
    inputTokens: index + 1,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
    filePath: `/tmp/rate-${index}.jsonl`,
    lineOffset: index,
    source: "claude",
  };
}

describe("isolated rate cache", () => {
  test("preserves exact totals while bounding high-cardinality model output", () => {
    const now = 1_800_000_000_000;
    const events = Array.from({ length: RATE_MODEL_BUCKET_MAX + 20 }, (_, index) =>
      event(now, index),
    );
    const expectedTokens = events.reduce((total, row) => total + row.inputTokens, 0);

    const snapshot = computeRateSnapshot(
      events,
      now,
      events.map((row) => ({ latestTs: row.ts })),
    );

    expect(isRateSnapshot(snapshot)).toBe(true);
    for (const bucket of snapshot.buckets) {
      expect(bucket.tokens).toBe(expectedTokens);
      expect(bucket.messages).toBe(events.length);
      expect(bucket.byModel.length).toBeLessThanOrEqual(RATE_MODEL_BUCKET_MAX);
      expect(bucket.byModel.some((model) => model.model === "other")).toBe(true);
      expect(bucket.byModel.reduce((total, model) => total + model.tokens, 0)).toBe(expectedTokens);
    }
  });

  test("streams high-cardinality session activity without retaining session identifiers", () => {
    const now = 1_800_000_000_000;
    const count = 10_000;
    function* events(): IterableIterator<StoredEvent> {
      for (let index = 0; index < count; index++) yield event(now, index);
    }
    function* activity(): IterableIterator<{ latestTs: number }> {
      for (let index = 0; index < count; index++) yield { latestTs: now - 1_000 };
    }

    const snapshot = computeRateSnapshot(events(), now, activity());
    const bucket = snapshot.buckets.find((candidate) => candidate.windowSec === 60)!;
    expect(bucket.messages).toBe(count);
    expect(bucket.activeSessions).toBe(count);
    expect(bucket.byModel.length).toBeLessThanOrEqual(RATE_MODEL_BUCKET_MAX);
    expect(isRateSnapshot(snapshot)).toBe(true);
  });

  test("persists a revision-pinned exact snapshot for bounded parent reads", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-rate-cache-"));
    const databasePath = join(root, "tokens.sqlite");
    const now = 1_800_000_000_000;
    let expectedCache: ReturnType<TokenStore["readCachedRateSnapshot"]> = null;
    try {
      const store = new TokenStore(databasePath);
      try {
        store.insertEvents([event(now, 0)]);
        const revision = store.getRevision();
        const snapshot = store.refreshRateSnapshotCache(now, revision);
        const completedAt = now + 120_000;
        expectedCache = store.touchCachedRateSnapshotPublication(
          revision,
          snapshot.now,
          completedAt,
        );
        expect(snapshot.buckets[0]?.tokens).toBe(1);
        expect(store.readCachedRateSnapshot()).toEqual(expectedCache);
        expect(expectedCache.publishedAt).toBe(completedAt);
        expect(expectedCache.snapshot.now).toBe(now);
        expect(() => store.refreshRateSnapshotCache(now, revision + 1)).toThrow(
          "rate snapshot revision changed before computation",
        );
      } finally {
        store.close();
      }

      const reopened = new TokenStore(databasePath, { readonly: true });
      try {
        expect(reopened.readCachedRateSnapshot()).toEqual(expectedCache);
      } finally {
        reopened.close();
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TokenStore } from "../src/store.ts";
import {
  createTokenSnapshotHeartbeat,
  issueTokenSnapshotHeartbeatTarget,
  scheduleTokenSnapshotHeartbeat,
  TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS,
} from "../src/token-snapshot-heartbeat.ts";

const STARTED_AT = Date.parse("2026-07-15T19:00:00.000Z");

async function withHealthySnapshot(
  run: (store: TokenStore) => void | Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "tokens-authority-heartbeat-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
    );
    store.refreshAuthoritativeTokenSnapshot(STARTED_AT);
    await run(store);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

test("pinned heartbeat advances healthy SQLite authority and permanently disarms on replacement", async () => {
  await withHealthySnapshot((store) => {
    const target = issueTokenSnapshotHeartbeatTarget(store, STARTED_AT);
    expect(target).not.toBeNull();
    const heartbeat = createTokenSnapshotHeartbeat(store, target, STARTED_AT);
    for (const elapsed of [5_000, 10_000, 15_000, 20_000]) {
      expect(heartbeat.beat(STARTED_AT + elapsed)).toBe(true);
      expect(store.readCachedTokenSnapshot()?.updatedAt).toBe(STARTED_AT + elapsed);
    }

    const current = store.readCachedTokenSnapshot()!;
    const replacement = { ...current, updatedAt: STARTED_AT + 20_001 };
    store.writeCachedTokenSnapshot(replacement);
    expect(heartbeat.beat(STARTED_AT + 25_000)).toBe(false);
    expect(heartbeat.active()).toBe(false);
    expect(store.readCachedTokenSnapshot()).toEqual(replacement);
    expect(heartbeat.beat(STARTED_AT + 30_000)).toBe(false);
  });
});

test("stale and future authority cannot be revived", async () => {
  await withHealthySnapshot((store) => {
    const cached = store.readCachedTokenSnapshot()!;
    store.writeCachedTokenSnapshot({
      ...cached,
      updatedAt: STARTED_AT - TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS - 1,
    });
    expect(issueTokenSnapshotHeartbeatTarget(store, STARTED_AT)).toBeNull();

    store.writeCachedTokenSnapshot({ ...cached, updatedAt: STARTED_AT + 1 });
    expect(issueTokenSnapshotHeartbeatTarget(store, STARTED_AT)).toBeNull();
  });
});

test("mixed provider authority heartbeats while all-incomplete authority stays disarmed", async () => {
  await withHealthySnapshot((store) => {
    const refresh = (
      claudeAvailable: boolean,
      claudeComplete: boolean,
      codexAvailable: boolean,
      codexComplete: boolean,
    ) => {
      store.reconcileDiscovery(
        [],
        [
          { source: "claude", available: claudeAvailable, complete: claudeComplete },
          { source: "codex", available: codexAvailable, complete: codexComplete },
        ],
      );
      store.refreshAuthoritativeTokenSnapshot(STARTED_AT);
    };

    for (const states of [
      [true, false, true, true],
      [true, true, true, false],
    ] as const) {
      refresh(...states);
      const target = issueTokenSnapshotHeartbeatTarget(store, STARTED_AT);
      expect(target).not.toBeNull();
      const heartbeat = createTokenSnapshotHeartbeat(store, target, STARTED_AT);
      expect(heartbeat.active()).toBe(true);
      expect(heartbeat.beat(STARTED_AT + 5_000)).toBe(true);
      expect(store.readCachedTokenSnapshot()?.updatedAt).toBe(STARTED_AT + 5_000);
    }

    refresh(true, false, true, false);
    expect(issueTokenSnapshotHeartbeatTarget(store, STARTED_AT)).toBeNull();
    refresh(false, false, false, false);
    expect(issueTokenSnapshotHeartbeatTarget(store, STARTED_AT)).toBeNull();
  });
});

test("scheduled heartbeat fires during an asynchronous phase and clear stops it", async () => {
  await withHealthySnapshot(async (store) => {
    let clockReads = 0;
    const target = issueTokenSnapshotHeartbeatTarget(store, STARTED_AT);
    expect(target).not.toBeNull();
    const scheduled = scheduleTokenSnapshotHeartbeat(store, {
      target,
      intervalMs: 10,
      now: () => STARTED_AT + clockReads++ * 5_000,
    });
    await Bun.sleep(35);
    expect(store.readCachedTokenSnapshot()!.updatedAt).toBeGreaterThanOrEqual(STARTED_AT + 5_000);
    scheduled.clear();
    expect(scheduled.active()).toBe(false);
    const stoppedAt = store.readCachedTokenSnapshot()!.updatedAt;
    await Bun.sleep(25);
    expect(store.readCachedTokenSnapshot()!.updatedAt).toBe(stoppedAt);
  });
});

test("transient SQLite contention retries while nontransient errors disarm", async () => {
  await withHealthySnapshot((store) => {
    let startedAt = STARTED_AT;
    for (const [code, errno] of [
      ["SQLITE_BUSY", 5],
      ["SQLITE_LOCKED_SHAREDCACHE", 262],
    ] as const) {
      const target = issueTokenSnapshotHeartbeatTarget(store, startedAt);
      expect(target).not.toBeNull();
      let transientFailures = 1;
      const retryingStore = {
        getRevision: () => store.getRevision(),
        readCachedTokenSnapshot: () => store.readCachedTokenSnapshot(),
        touchCachedTokenSnapshot: (...args: Parameters<TokenStore["touchCachedTokenSnapshot"]>) => {
          if (transientFailures-- > 0) {
            throw Object.assign(new Error(code), { name: "SQLiteError", code, errno });
          }
          return store.touchCachedTokenSnapshot(...args);
        },
      };
      const heartbeat = createTokenSnapshotHeartbeat(retryingStore, target, startedAt);

      expect(heartbeat.beat(startedAt + 5_000)).toBe(false);
      expect(heartbeat.active()).toBe(true);
      expect(store.readCachedTokenSnapshot()!.updatedAt).toBe(startedAt);
      expect(heartbeat.beat(startedAt + 10_000)).toBe(true);
      expect(heartbeat.active()).toBe(true);
      startedAt += 10_000;
    }

    const busyTarget = issueTokenSnapshotHeartbeatTarget(store, startedAt);
    expect(busyTarget).not.toBeNull();
    let busyAttempts = 0;
    const alwaysBusyStore = {
      getRevision: () => store.getRevision(),
      readCachedTokenSnapshot: () => store.readCachedTokenSnapshot(),
      touchCachedTokenSnapshot: () => {
        busyAttempts++;
        throw Object.assign(new Error("busy"), {
          name: "SQLiteError",
          code: "SQLITE_BUSY",
          errno: 5,
        });
      },
    };
    const agingHeartbeat = createTokenSnapshotHeartbeat(alwaysBusyStore, busyTarget, startedAt);
    expect(agingHeartbeat.beat(startedAt + 5_000)).toBe(false);
    expect(agingHeartbeat.active()).toBe(true);
    expect(agingHeartbeat.beat(startedAt + TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS + 1)).toBe(false);
    expect(agingHeartbeat.active()).toBe(false);
    expect(busyAttempts).toBe(1);

    const target = issueTokenSnapshotHeartbeatTarget(store, startedAt);
    expect(target).not.toBeNull();
    const failingStore = {
      getRevision: () => store.getRevision(),
      readCachedTokenSnapshot: () => store.readCachedTokenSnapshot(),
      touchCachedTokenSnapshot: () => {
        throw Object.assign(new Error("constraint"), {
          name: "SQLiteError",
          code: "SQLITE_CONSTRAINT",
          errno: 19,
        });
      },
    };
    const heartbeat = createTokenSnapshotHeartbeat(failingStore, target, startedAt);
    expect(heartbeat.beat(startedAt + 5_000)).toBe(false);
    expect(heartbeat.active()).toBe(false);
  });
});

test("heartbeat permanently disarms at Los Angeles midnight before touching SQLite", async () => {
  const beforeMidnight = Date.parse("2026-07-16T06:59:59.000Z");
  await withHealthySnapshot((store) => {
    store.refreshAuthoritativeTokenSnapshot(beforeMidnight);
    const target = issueTokenSnapshotHeartbeatTarget(store, beforeMidnight);
    expect(target).not.toBeNull();
    const heartbeat = createTokenSnapshotHeartbeat(store, target, beforeMidnight);
    const cachedBefore = store.readCachedTokenSnapshot();

    expect(heartbeat.beat(Date.parse("2026-07-16T07:00:00.000Z"))).toBe(false);
    expect(heartbeat.active()).toBe(false);
    expect(store.readCachedTokenSnapshot()).toEqual(cachedBefore);
    expect(heartbeat.beat(Date.parse("2026-07-16T07:00:05.000Z"))).toBe(false);
    expect(store.readCachedTokenSnapshot()).toEqual(cachedBefore);
  });
});

import { dailyTokenWindow } from "./rate.ts";
import {
  isTransientSqliteContentionError,
  type TokenStore,
  tokenSnapshotHasCompleteProvider,
} from "./store.ts";
import type { TokenSnapshotHeartbeatTarget } from "./usage-ingest-worker-protocol.ts";

export const TOKEN_SNAPSHOT_HEARTBEAT_INTERVAL_MS = 5_000;
export const TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS = 15_000;

type HeartbeatStore = Pick<
  TokenStore,
  "getRevision" | "readCachedTokenSnapshot" | "touchCachedTokenSnapshot"
>;

export interface TokenSnapshotHeartbeat {
  beat(now?: number): boolean;
  stop(): void;
  active(): boolean;
}

export function issueTokenSnapshotHeartbeatTarget(
  store: HeartbeatStore,
  now: number = Date.now(),
): TokenSnapshotHeartbeatTarget | null {
  const cached = store.readCachedTokenSnapshot();
  const ageMs = cached === null ? Number.NaN : now - cached.updatedAt;
  // Store-revision equality is intentionally NOT checked here: a mid-scan
  // flush can bump the store revision without publishing a new snapshot
  // (src/scanner.ts commitScan), and the cached snapshot's own revision is
  // what `touchCachedTokenSnapshot`'s CAS below pins against, not the live
  // store revision at issue time.
  return Number.isSafeInteger(now) &&
    now >= 0 &&
    cached !== null &&
    cached.window.windowKey === dailyTokenWindow(now).windowKey &&
    tokenSnapshotHasCompleteProvider(cached) &&
    Number.isFinite(ageMs) &&
    ageMs >= 0 &&
    ageMs <= TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS
    ? { revision: cached.revision, windowKey: cached.window.windowKey, updatedAt: cached.updatedAt }
    : null;
}

export function createTokenSnapshotHeartbeat(
  store: HeartbeatStore,
  issuedTarget: TokenSnapshotHeartbeatTarget | null,
  startedAt: number = Date.now(),
): TokenSnapshotHeartbeat {
  const cached = store.readCachedTokenSnapshot();
  const ageMs = issuedTarget === null ? Number.NaN : startedAt - issuedTarget.updatedAt;
  // Live store-revision equality is intentionally NOT re-checked here (see
  // issueTokenSnapshotHeartbeatTarget above) — only the cached snapshot's
  // own revision must match the target that was issued against it.
  let target =
    Number.isSafeInteger(startedAt) &&
    startedAt >= 0 &&
    issuedTarget !== null &&
    cached !== null &&
    cached.revision === issuedTarget.revision &&
    cached.window.windowKey === issuedTarget.windowKey &&
    cached.updatedAt === issuedTarget.updatedAt &&
    issuedTarget.windowKey === dailyTokenWindow(startedAt).windowKey &&
    tokenSnapshotHasCompleteProvider(cached) &&
    Number.isFinite(ageMs) &&
    ageMs >= 0 &&
    ageMs <= TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS
      ? { ...issuedTarget }
      : null;

  let consecutiveContentionFailures = 0;

  return {
    beat(now = Date.now()) {
      if (target === null) return false;
      const age = now - target.updatedAt;
      if (
        !Number.isSafeInteger(now) ||
        now < 0 ||
        !Number.isFinite(age) ||
        age < 0 ||
        age > TOKEN_SNAPSHOT_HEARTBEAT_MAX_AGE_MS ||
        target.windowKey !== dailyTokenWindow(now).windowKey
      ) {
        target = null;
        return false;
      }
      try {
        // Retrying transient SQLite contention lives inside
        // store.touchCachedTokenSnapshot itself (a short bounded retry on
        // the store's own connection) so it stays invisible here as a
        // single call/throw — this call site only decides what to do once
        // that bounded retry has already given up.
        store.touchCachedTokenSnapshot(now, target.revision, target.windowKey, target.updatedAt);
        consecutiveContentionFailures = 0;
        target = { ...target, updatedAt: now };
        return true;
      } catch (error) {
        if (isTransientSqliteContentionError(error)) {
          consecutiveContentionFailures += 1;
          console.error(
            `token snapshot heartbeat: transient SQLite contention (${consecutiveContentionFailures} consecutive) — will retry next tick`,
            error,
          );
          return false;
        }
        target = null;
        return false;
      }
    },
    stop() {
      target = null;
    },
    active() {
      return target !== null;
    },
  };
}

export function scheduleTokenSnapshotHeartbeat(
  store: HeartbeatStore,
  options: {
    target?: TokenSnapshotHeartbeatTarget | null;
    now?: () => number;
    intervalMs?: number;
  } = {},
): TokenSnapshotHeartbeat & { clear(): void } {
  const now = options.now ?? Date.now;
  const heartbeat = createTokenSnapshotHeartbeat(store, options.target ?? null, now());
  const intervalMs = options.intervalMs ?? TOKEN_SNAPSHOT_HEARTBEAT_INTERVAL_MS;
  const timer = heartbeat.active() ? setInterval(() => heartbeat.beat(now()), intervalMs) : null;
  timer?.unref?.();
  return {
    ...heartbeat,
    clear() {
      if (timer !== null) clearInterval(timer);
      heartbeat.stop();
    },
  };
}

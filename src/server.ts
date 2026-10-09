// Local HTTP server for sigmashake-tokens.
// Binds to 127.0.0.1:7913 by default. Serves the static dashboard at `/`,
// a JSON aggregation endpoint at `/api/usage`, and a live SSE feed at
// `/api/live` that pushes deltas whenever the background loop ingests new
// usage events.

import { resolve } from "node:path";
import { broadcast, buildUpdateFromDelta, buildUpdateFromEvents, sseResponse } from "./live.ts";
import {
  mapTokensRoute,
  recordAggregationRefreshSpan,
  recordApiRequestSpan,
  recordIngestCycleSpan,
} from "./otel-export.ts";
import { claudeProjectsDir, codexSessionsDir, grokLogsDir, grokSessionsDir } from "./paths.ts";
import appCssAsset from "./public/app.css" with { type: "file" };
import appJsAsset from "./public/app.js" with { type: "file" };
import indexHtmlAsset from "./public/index.html" with { type: "file" };
import {
  type AuthoritativeTokenSnapshot,
  buildAuthoritativeTokenSnapshot,
  dailyTokenWindow,
  type RateSnapshot,
  rolling24hTokenWindow,
} from "./rate.ts";
import { readSavings } from "./savings.ts";
import {
  type Aggregations,
  loadAggregations,
  loadDayDetail,
  loadSessionDetail,
} from "./scanner.ts";
import { getStore, type TokenStore, tokenSnapshotHasCompleteProvider } from "./store.ts";
import { issueTokenSnapshotHeartbeatTarget } from "./token-snapshot-heartbeat.ts";
import type { TokenomicsResult } from "./tokenomics.ts";
import { computeTokenomicsOffThread, shutdownTokenomicsWorker } from "./tokenomics-worker.ts";
import { shutdownUsageAggregationWorker } from "./usage-aggregation-worker.ts";
import {
  ingestUsageOffThread,
  shutdownUsageIngestWorker,
  UsageIngestWorkerShutdownError,
} from "./usage-ingest-worker.ts";

const PORT = Number(process.env.TOKENS_PORT || 7913);
const HOST = process.env.TOKENS_HOST || "127.0.0.1";
export const TOKENS_DEFAULT_POLL_MS = 5_000;
export const TOKENS_MIN_POLL_MS = 1_000;
export const TOKENS_MAX_POLL_MS = 300_000;

export function sanitizeTokensPollMs(raw: string | undefined): number {
  if (raw === undefined || raw.trim() === "") return TOKENS_DEFAULT_POLL_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return TOKENS_DEFAULT_POLL_MS;
  return Math.max(TOKENS_MIN_POLL_MS, Math.min(TOKENS_MAX_POLL_MS, Math.trunc(parsed)));
}

const POLL_MS = sanitizeTokensPollMs(process.env.TOKENS_POLL_MS);
/** Three full poll intervals, with a 30s floor for the default 5s cadence. */
export function tokensCacheMaxAgeMs(pollMs: number): number {
  return Math.max(30_000, pollMs * 3);
}

export const TOKENS_CACHE_MAX_AGE_MS = tokensCacheMaxAgeMs(POLL_MS);
// Dashboard assets are imported as files so `bun build --compile` embeds them
// in the single binary. In a source checkout the import resolves to the path on
// disk; in a compiled binary it resolves to the embedded copy. Either way
// Bun.file() reads it. The map is an allow-list, so no request path can reach
// any other file.
const STATIC_ASSETS: Record<string, string> = {
  "/index.html": indexHtmlAsset as unknown as string,
  "/app.js": appJsAsset,
  "/app.css": appCssAsset,
};

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json",
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
    },
  });
}

async function serveStatic(pathname: string): Promise<Response> {
  const rel = pathname === "/" ? "/index.html" : pathname;
  const path = Object.hasOwn(STATIC_ASSETS, rel) ? STATIC_ASSETS[rel] : undefined;
  if (path === undefined) return new Response("not found", { status: 404 });
  const file = Bun.file(path);
  if (!(await file.exists())) return new Response("not found", { status: 404 });
  const ext = `.${rel.split(".").pop() || ""}`;
  const mime = MIME[ext] || "application/octet-stream";
  return new Response(file, { headers: { "content-type": mime } });
}

type TokenSnapshotCacheReader = Pick<TokenStore, "getRevision" | "readCachedTokenSnapshot">;

export interface FreshTokenSnapshotRead {
  revision: number;
  snapshot: ReturnType<TokenStore["readCachedTokenSnapshot"]>;
}

/** How long a cached snapshot may keep being served after its revision falls
 * behind the store's live revision (a mid-scan flush bumped revision without
 * publishing) before it degrades. Named/exported so a persistently-behind
 * cache (the "ingest succeeds while committing nothing" signature) cannot
 * be served indefinitely just because its heartbeat keeps `updatedAt` fresh.
 *
 * EXTERNAL COUPLING: this value (15_000ms) silently equals
 * `TOKEN_SNAPSHOT_FRESHNESS_MS` in the OBS Boss HUD client of the SigmaShake
 * monorepo (`sigmashake-obs/overlay/token-authority-client.ts`) — that HUD's
 * hard freshness bound for the authoritative token snapshot it renders. The
 * two constants are NOT imported from a shared source; they just happen to
 * match today. If this budget is ever widened here without updating the
 * OBS-side constant to match, a scan that goes >15s between publishes will
 * make the HUD fall back to an em-dash even though this server is still
 * willing to serve the (now more-stale) cached snapshot as fresh — drift
 * here is a silent HUD regression, not a build failure. */
export const TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS = 15_000;

export interface SupersessionTracker {
  /** Returns the wall-clock time the given cached revision was first
   * observed behind `liveRevision`, or null if it isn't currently behind.
   * Tracks by revision identity (not by `updatedAt`), so a heartbeat touch
   * that only refreshes the timestamp doesn't reset the supersession clock. */
  observe(cachedRevision: number, liveRevision: number, now: number): number | null;
}

/** Two scalars, not a map keyed by snapshot identity — avoids unbounded
 * growth as the heartbeat republishes a new `updatedAt` every beat. */
export function createSupersessionTracker(): SupersessionTracker {
  let trackedRevision: number | null = null;
  let since: number | null = null;
  return {
    observe(cachedRevision, liveRevision, now) {
      if (cachedRevision >= liveRevision) {
        trackedRevision = null;
        since = null;
        return null;
      }
      if (trackedRevision !== cachedRevision) {
        trackedRevision = cachedRevision;
        since = now;
      }
      return since;
    },
  };
}

/** Per-store tracker registry, keyed by the store object's own identity
 * (WeakMap, so an abandoned/replaced store — e.g. `_resetStoreForTests()`
 * swapping in a fresh `TokenStore`, or a test passing a one-off object
 * literal — is GC'd along with its tracker automatically). This replaces a
 * single process-global tracker that was keyed only on a low-cardinality
 * revision integer: two independent stores whose revisions both happen to
 * pass through the same small integer (e.g. two fresh tmpdir DBs both at
 * revision 3, or any two long-lived stores after enough history) would
 * otherwise silently share supersession state and could reject a brand-new,
 * genuinely fresh snapshot in one store because of an unrelated observation
 * made earlier against a same-numbered revision in a different store. Scoping
 * by store identity makes that cross-store leak impossible by construction —
 * no reset hook required for correctness (tests may still call one for
 * hygiene/memory, but nothing depends on it being called). */
const supersessionTrackersByStore = new WeakMap<TokenSnapshotCacheReader, SupersessionTracker>();

function supersessionTrackerForStore(store: TokenSnapshotCacheReader): SupersessionTracker {
  let tracker = supersessionTrackersByStore.get(store);
  if (!tracker) {
    tracker = createSupersessionTracker();
    supersessionTrackersByStore.set(store, tracker);
  }
  return tracker;
}

/** Cache-first ordering prevents an old cache from being accepted when a
 * sibling commits a newer revision between the two SQLite reads. Revision
 * ahead of the live store (`cached.revision > revision`) is rejected as
 * future-poisoned; revision behind is accepted (a mid-scan flush can bump
 * the store revision without publishing a new snapshot) but only within the
 * supersession budget above. */
export function readFreshCachedTokenSnapshot(
  store: TokenSnapshotCacheReader,
  now: number = Date.now(),
  maxAgeMs: number = TOKENS_CACHE_MAX_AGE_MS,
  supersessionTracker: SupersessionTracker = supersessionTrackerForStore(store),
): FreshTokenSnapshotRead {
  const cached = store.readCachedTokenSnapshot();
  const revision = store.getRevision();
  if (cached === null) return { revision, snapshot: null };
  const ageMs = now - cached.updatedAt;
  const supersededSince = supersessionTracker.observe(cached.revision, revision, now);
  if (
    !Number.isSafeInteger(now) ||
    now < 0 ||
    !Number.isFinite(maxAgeMs) ||
    maxAgeMs < 0 ||
    cached.revision > revision ||
    !Number.isFinite(ageMs) ||
    ageMs < 0 ||
    ageMs > maxAgeMs ||
    cached.window.windowKey !== dailyTokenWindow(now).windowKey ||
    !tokenSnapshotHasCompleteProvider(cached) ||
    (supersededSince !== null && now - supersededSince > TOKEN_SNAPSHOT_SUPERSESSION_BUDGET_MS)
  ) {
    return { revision, snapshot: null };
  }
  return { revision, snapshot: cached };
}

export type AuthoritativeSnapshotState =
  | { state: "ready"; revision: number; snapshot: AuthoritativeTokenSnapshot }
  | { state: "degraded"; revision: number; updatedAt: number | null };

/** Single source of truth for whether the current cached authority may be
 * served to a caller. `/api/snapshot` (below) 503s on "degraded" rather than
 * fabricating a measurement; the SSE authority (also below) keeps serving a
 * valid all-zero incomplete-provider stub in that case — see the comment on
 * `authoritativeSnapshot` for why those two surfaces are allowed to differ.
 * Starting may reuse a complete mixed last-good cache; ready serves any fresh
 * complete cache from `readFreshCachedTokenSnapshot`. */
function authoritativeSnapshotState(now: number = Date.now()): AuthoritativeSnapshotState {
  const store = getStore();
  const current = readFreshCachedTokenSnapshot(store, now);
  const health = ingestController.health();
  const grok = current.snapshot?.providers.grok;
  const completeMixedProviderCache =
    current.snapshot !== null &&
    current.snapshot.providers.claude.available &&
    current.snapshot.providers.claude.discoveryComplete &&
    current.snapshot.providers.codex.available &&
    current.snapshot.providers.codex.discoveryComplete &&
    (grok === undefined || !grok.available || grok.discoveryComplete);
  if (
    current.snapshot !== null &&
    shouldServeCachedTokenSnapshot(health) &&
    (health.state === "ready" || completeMixedProviderCache)
  ) {
    return { state: "ready", revision: current.revision, snapshot: current.snapshot };
  }
  // readFreshCachedTokenSnapshot already returns null on any rejected guard
  // (staleness, window rollover, supersession budget, ...); re-read the raw
  // cache here only to surface its updatedAt for the degraded response body.
  const rawCached = store.readCachedTokenSnapshot();
  return { state: "degraded", revision: current.revision, updatedAt: rawCached?.updatedAt ?? null };
}

/** SSE_DECISION: keep-stub-with-comment. SSE (`/api/live` + broadcast deltas)
 * keeps returning a valid all-zero incomplete-provider authority when the
 * cache is degraded, rather than omitting the `snapshot` field — the field
 * has always been optional-but-usually-present on `LiveUpdate`, and OBS
 * treats a present-but-incomplete authority as "still warming" (falls back
 * to nothing extra), whereas dropping the field on every degraded tick would
 * be a larger behavioral change to the SSE contract than this bugfix calls
 * for. The one-shot HTTP `/api/snapshot` route intentionally does NOT reuse
 * this stub — see `tokenSnapshotResponse` — because a bare fetch response
 * rendered directly on stream must never show a fabricated zero measurement. */
function authoritativeSnapshot(now: number = Date.now()) {
  const state = authoritativeSnapshotState(now);
  if (state.state === "ready") return state.snapshot;
  return buildAuthoritativeTokenSnapshot([], {
    window: dailyTokenWindow(now),
    revision: state.revision,
    updatedAt: now,
    providerStates: [
      { source: "claude", available: false, complete: false },
      { source: "codex", available: false, complete: false },
      { source: "grok", available: false, complete: false },
    ],
  });
}

/** Backs `GET /api/snapshot` / `GET /api/token-snapshot`. Unlike the SSE
 * authority above, this never fabricates a zero measurement — a degraded
 * cache 503s with the same shape as `cachedStatsResponse`/`cachedRateResponse`. */
export function parseTokensSnapshotWindow(raw: string | null): "daily" | "rolling24h" | null {
  if (raw === null || raw === "" || raw === "daily") return "daily";
  if (raw === "rolling24h") return "rolling24h";
  return null;
}

export function tokenSnapshotResponse(
  now: number = Date.now(),
  windowParam: string | null = null,
): Response {
  const windowKind = parseTokensSnapshotWindow(windowParam);
  if (windowKind === null) {
    return jsonResponse({ error: "invalid-window", window: windowParam }, 400);
  }
  const state = authoritativeSnapshotState(now);
  if (state.state !== "ready") {
    return jsonResponse(
      { error: "snapshot-stale", state: "degraded", updatedAt: state.updatedAt },
      503,
    );
  }
  if (windowKind === "daily") return jsonResponse(state.snapshot);
  return jsonResponse(getStore().buildAuthoritativeTokenSnapshot(now, rolling24hTokenWindow(now)));
}

export interface IngestHealth {
  ready: boolean;
  state: "starting" | "ready" | "degraded";
  ingestRunning: boolean;
  consecutiveFailures: number;
  lastFailureAt: number | null;
}

export function shouldServeCachedTokenSnapshot(health: IngestHealth): boolean {
  // ready: live authority. starting: last-good complete mixed cache is
  // servable so cold-start does not 503 /api/snapshot. degraded stays
  // fail-closed on snapshot routes until the next successful ingest.
  return health.state === "ready" || health.state === "starting";
}

interface IngestControllerDependencies {
  getRevision(): number;
  ingest(previouslyReady: boolean): Promise<void>;
  onAuthorityChange?(): void;
  onFailure?(error: unknown): void;
  now?(): number;
}

export interface IngestController {
  tick(): Promise<void>;
  health(): IngestHealth;
}

const MAX_REPORTED_FAILURES = 999;

/** Single-flight ingestion lifecycle. Readiness begins only after one complete
 * isolated ingest/cache publication cycle succeeds. */
export function createIngestController(deps: IngestControllerDependencies): IngestController {
  let ingestRunning = false;
  let ready = false;
  let consecutiveFailures = 0;
  let lastFailureAt: number | null = null;

  const notifyFailure = (error: unknown): void => {
    try {
      deps.onFailure?.(error);
    } catch {
      // Diagnostics must never break the retry loop.
    }
  };

  const recordFailure = (error: unknown): void => {
    ready = false;
    consecutiveFailures = Math.min(MAX_REPORTED_FAILURES, consecutiveFailures + 1);
    const now = deps.now?.() ?? Date.now();
    lastFailureAt = Number.isFinite(now)
      ? Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.trunc(now)))
      : 0;
    notifyFailure(error);
  };

  return {
    async tick(): Promise<void> {
      if (ingestRunning) return;
      ingestRunning = true;
      let revisionBefore: number | undefined;
      const previouslyReady = ready;
      try {
        revisionBefore = deps.getRevision();
        await deps.ingest(previouslyReady);
        ready = true;
        consecutiveFailures = 0;
        lastFailureAt = null;
        if (!previouslyReady) {
          try {
            deps.onAuthorityChange?.();
          } catch (publicationError) {
            notifyFailure(publicationError);
          }
        }
      } catch (error) {
        if (error instanceof UsageIngestWorkerShutdownError) return;
        recordFailure(error);
        // A fail-closed record conflict commits provider incompleteness but no
        // staged file/event state. Push that authority change immediately.
        if (revisionBefore !== undefined) {
          try {
            if (deps.getRevision() !== revisionBefore) deps.onAuthorityChange?.();
          } catch (reconciliationError) {
            notifyFailure(reconciliationError);
          }
        }
      } finally {
        ingestRunning = false;
      }
    },
    health(): IngestHealth {
      return {
        ready,
        state: ready ? "ready" : consecutiveFailures > 0 ? "degraded" : "starting",
        ingestRunning,
        consecutiveFailures,
        lastFailureAt,
      };
    },
  };
}

export interface UsageHealthPayload {
  state: "ready" | "degraded";
  reason: string | null;
  consecutiveFailures: number;
  lastIngestAt: number;
  staleSec: number;
}

/** Maps the same IngestHealth /healthz reports onto the /api/usage
 *  `health` field of the OBS-consumed contract. `lastIngestAt`/`staleSec`
 *  are derived from the aggregation's own `scannedAt`, which is the ts of
 *  the ingest tick that produced the response being returned — not a
 *  separate, possibly-stale read of controller state. `reason` is always
 *  present (stable key set across both health states) — `null` when
 *  healthy, a string when degraded. */
export function usageHealthPayload(
  health: IngestHealth,
  scannedAt: number,
  now: number = Date.now(),
): UsageHealthPayload {
  const staleSec =
    Number.isFinite(now) && Number.isFinite(scannedAt) ? Math.max(0, (now - scannedAt) / 1000) : 0;
  return {
    state: health.ready ? "ready" : "degraded",
    reason: health.ready ? null : health.state === "starting" ? "ingest-starting" : "ingest-failed",
    consecutiveFailures: health.consecutiveFailures,
    lastIngestAt: scannedAt,
    staleSec,
  };
}

/** $mono treats non-200 /healthz as unreachable. Reachable when ingest is
 * ready OR a complete last-good snapshot can already be served — 503 only
 * when ingest-failed (or still starting) with no servable last-good cache. */
export function healthResponse(
  health: IngestHealth,
  hasCompleteLastGood: boolean = false,
): Response {
  const reachable = health.ready || hasCompleteLastGood;
  return jsonResponse(
    {
      ok: reachable,
      state: health.state,
      ingestRunning: health.ingestRunning,
      consecutiveFailures: health.consecutiveFailures,
      lastFailureAt: health.lastFailureAt,
      ...(!health.ready
        ? { reason: health.state === "starting" ? "ingest-starting" : "ingest-failed" }
        : {}),
    },
    reachable ? 200 : 503,
  );
}

/** True when /healthz may stay 200 on a complete last-good cache: snapshot
 * routes would already serve it, or ingest-failed still holds a fresh
 * complete-provider cache (reachability only — snapshot routes stay
 * fail-closed on degraded). */
export function hasCompleteLastGoodForHealthz(now: number = Date.now()): boolean {
  if (authoritativeSnapshotState(now).state === "ready") return true;
  return readFreshCachedTokenSnapshot(getStore(), now).snapshot !== null;
}

type StatsCacheReader = Pick<TokenStore, "getRevision" | "readCachedStats">;

/** Request-safe stats response: this dependency cannot issue exact table counts. */
export function cachedStatsResponse(store: StatsCacheReader, pollMs: number = POLL_MS): Response {
  const cached = store.readCachedStats();
  if (cached === null) {
    return jsonResponse({ error: "stats-warming", state: "warming", pollMs }, 503);
  }
  if (cached.revision !== store.getRevision()) {
    return jsonResponse(
      {
        error: "stats-stale",
        state: "degraded",
        updatedAt: cached.scannedAt,
        pollMs,
      },
      503,
    );
  }
  const { revision: _revision, ...stats } = cached;
  return jsonResponse({ ...stats, pollMs });
}

export interface SessionContextEstimate {
  sessionId: string;
  found: boolean;
  ts: number | null;
  model: string | null;
  /** ESTIMATE only: the latest single request's uncached-input + cache-read +
   * cache-write tokens. Deliberately excludes output tokens (generated, not
   * held in context) and is NOT cumulative across the session — it reflects
   * one turn's reported usage, not a running context-window total. Harness
   * limitation: only as fresh as the ~5s ingest poll lag, and only covers
   * whatever token fields the source harness (currently Claude Code) reports. */
  estimatedContextTokens: number | null;
  components: { inputTokens: number; cacheReadTokens: number; cacheWriteTokens: number } | null;
}

/** Backs the /api/session-context route consumed by the auto-compact nudge
 * hook. Reads the store's latest-event cursor directly — no aggregation, no
 * ingest trigger — so it stays cheap enough to call on every Bash tool use. */
export function loadSessionContextEstimate(sessionId: string): SessionContextEstimate {
  const event = getStore().selectLatestEventForSession(sessionId);
  if (!event) {
    return {
      sessionId,
      found: false,
      ts: null,
      model: null,
      estimatedContextTokens: null,
      components: null,
    };
  }
  const cacheWriteTokens = event.cacheWrite5mTokens + event.cacheWrite1hTokens;
  return {
    sessionId,
    found: true,
    ts: event.ts,
    model: event.model,
    estimatedContextTokens: event.inputTokens + event.cacheReadTokens + cacheWriteTokens,
    components: {
      inputTokens: event.inputTokens,
      cacheReadTokens: event.cacheReadTokens,
      cacheWriteTokens,
    },
  };
}

type RateCacheReader = Pick<TokenStore, "getRevision" | "readCachedRateSnapshot">;

export type CachedRateState =
  | { state: "ready"; snapshot: RateSnapshot }
  | { state: "warming" }
  | { state: "degraded"; updatedAt: number };

/** Reads only the bounded metadata cache and rejects revision drift, clock
 * reversal, and data older than the ingest cadence permits. */
export function readCachedRateState(
  store: RateCacheReader,
  now: number = Date.now(),
  maxAgeMs: number = TOKENS_CACHE_MAX_AGE_MS,
): CachedRateState {
  const cached = store.readCachedRateSnapshot();
  if (cached === null) return { state: "warming" };
  const ageMs = now - cached.publishedAt;
  if (
    cached.revision !== store.getRevision() ||
    !Number.isFinite(ageMs) ||
    ageMs < 0 ||
    ageMs > maxAgeMs
  ) {
    return { state: "degraded", updatedAt: cached.publishedAt };
  }
  return { state: "ready", snapshot: cached.snapshot };
}

export function cachedRateResponse(
  store: RateCacheReader,
  now: number = Date.now(),
  pollMs: number = POLL_MS,
  maxAgeMs: number = TOKENS_CACHE_MAX_AGE_MS,
): Response {
  const cached = readCachedRateState(store, now, maxAgeMs);
  if (cached.state === "warming") {
    return jsonResponse({ error: "rate-warming", state: "warming", pollMs }, 503);
  }
  if (cached.state === "degraded") {
    return jsonResponse(
      { error: "rate-stale", state: "degraded", updatedAt: cached.updatedAt, pollMs },
      503,
    );
  }
  return jsonResponse(cached.snapshot);
}

export function cachedRateBucketForBroadcast(
  store: RateCacheReader,
  now: number = Date.now(),
  maxAgeMs: number = TOKENS_CACHE_MAX_AGE_MS,
) {
  const cached = readCachedRateState(store, now, maxAgeMs);
  return cached.state === "ready"
    ? cached.snapshot.buckets.find((bucket) => bucket.windowSec === 60)
    : undefined;
}

export function shouldBroadcastIngestResult(result: {
  mutated: boolean;
  snapshotRefreshed: boolean;
}): boolean {
  return result.mutated || result.snapshotRefreshed;
}

// === Unfiltered /api/usage cache ===
//
// The OBS Boss HUD polls the unfiltered (no from/to/models/projects) shape
// of /api/usage every 5s. That aggregation recomputes the entire store
// (millions of events) via the off-thread aggregation worker, which takes
// ~8-14s on this box. Left uncached, overlapping polls pile up concurrent
// aggregation runs and each request blocks on a multi-second recompute.
//
// This cache is populated by the background poll loop (never by a request),
// mirroring the readCachedStats/readCachedRateSnapshot pattern: requests
// only ever read a value that was computed off the request path. Unlike
// that store-backed pattern, this cache lives in-process (no SQLite meta
// row) because the aggregation result itself — not just a small snapshot —
// is what's expensive, and there is exactly one Bun.serve process per
// systemd unit, so an in-memory cache needs no cross-process publication.
//
// Filtered/parameterized requests are intentionally NOT served from this
// cache — they always call loadAggregations() directly, which already has
// its own revision-gated, per-parameter-set cache in scanner.ts. Mixing
// filtered and unfiltered results here would risk answering a filtered
// request with the entire-store totals; keeping the two paths disjoint
// makes that impossible by construction rather than by careful bookkeeping.

export interface UnfilteredUsageCacheEntry {
  agg: Aggregations;
  computedAt: number;
}

/** Pure freshness check — no I/O, fully unit-testable. */
export function isUnfilteredUsageCacheFresh(
  entry: UnfilteredUsageCacheEntry | null,
  now: number,
  maxAgeMs: number,
): boolean {
  if (entry === null) return false;
  const ageMs = now - entry.computedAt;
  return Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= maxAgeMs;
}

export interface UnfilteredUsageCacheDeps {
  compute(): Promise<Aggregations>;
  now?(): number;
  maxAgeMs?: number;
  /** How stale (in ms) the entry may get before a background refresh()
   * call actually launches a recompute. Defaults to half of maxAgeMs.
   * This must stay well below maxAgeMs: the ~8-14s real compute time needs
   * headroom to finish before the hard TTL expires, and — critically — it
   * bounds how often the single off-thread aggregation worker slot gets
   * monopolized. Without this gate, calling refresh() every 5s poll tick
   * would launch a new ~10s aggregation almost back-to-back forever,
   * starving any concurrent filtered/drill-down query stuck behind it in
   * the worker's queue — the same pile-up this cache exists to prevent. */
  refreshAfterMs?: number;
}

export interface UnfilteredUsageCache {
  /** Serves the freshest cached aggregation; computes synchronously (and
   * single-flight-shares that computation across concurrent callers) only
   * when the cache is empty or has aged past maxAgeMs — e.g. right after
   * boot, or after the background refresher has been failing. */
  get(): Promise<Aggregations>;
  /** Best-effort background repopulation. A no-op once the entry is fresher
   * than refreshAfterMs — safe to call on every poll tick. Never rejects. */
  refresh(): Promise<void>;
  peek(): UnfilteredUsageCacheEntry | null;
}

/** Single-flight + wall-clock-TTL cache for the unfiltered aggregation.
 * Deliberately revision-agnostic (unlike scanner.ts's request-path cache):
 * on this box the store revision advances on effectively every poll tick
 * because many concurrent Claude Code sessions are always producing new
 * events, so a revision-gated cache would miss almost every time. A wall
 * clock TTL is the only cache key that can be honest about the tradeoff
 * we're actually making (bounded staleness, not "always exact"), and that
 * staleness is surfaced to callers via health.staleSec — never hidden. */
export function createUnfilteredUsageCache(deps: UnfilteredUsageCacheDeps): UnfilteredUsageCache {
  const maxAgeMs = deps.maxAgeMs ?? TOKENS_CACHE_MAX_AGE_MS;
  const refreshAfterMs = deps.refreshAfterMs ?? Math.floor(maxAgeMs / 2);
  const now = deps.now ?? Date.now;
  let entry: UnfilteredUsageCacheEntry | null = null;
  let inFlight: Promise<Aggregations> | null = null;

  const computeAndStore = (): Promise<Aggregations> => {
    if (inFlight) return inFlight;
    const pending = deps
      .compute()
      .then((agg) => {
        entry = { agg, computedAt: now() };
        return agg;
      })
      .finally(() => {
        if (inFlight === pending) inFlight = null;
      });
    inFlight = pending;
    return pending;
  };

  return {
    async get(): Promise<Aggregations> {
      if (isUnfilteredUsageCacheFresh(entry, now(), maxAgeMs)) {
        return (entry as UnfilteredUsageCacheEntry).agg;
      }
      return computeAndStore();
    },
    async refresh(): Promise<void> {
      if (isUnfilteredUsageCacheFresh(entry, now(), refreshAfterMs)) return;
      await computeAndStore();
    },
    peek(): UnfilteredUsageCacheEntry | null {
      return entry;
    },
  };
}

// === /api/tokenomics off-thread cache ===
//
// analyzeTokenomics()'s efficiency scan (bounded JSONL corpus walk + sha256
// hashing over up to 256 MiB) is CPU-bound and, computed synchronously in the
// HTTP handler, blocked this single-threaded Bun process's event loop long
// enough to get throttled under the sigmashake-tokens.slice CPUQuota=50% cap
// (verified: 86% of a 20s sample's 100ms cgroup periods throttled). This
// mirrors unfilteredUsageCache above exactly (wall-clock TTL, single-flight,
// background-refreshed by the poll loop) but the compute() delegate runs in
// a dedicated transient sibling process via computeTokenomicsOffThread —
// tokenomics-worker.ts, the same systemd-run-per-job pattern already used by
// usage-aggregation-worker.ts — so even the rare cold-cache compute never
// costs this process CPU time, only I/O-wait on the child's stdout pipe.

export interface TokenomicsCacheEntry {
  result: TokenomicsResult;
  computedAt: number;
}

export interface TokenomicsCache {
  /** Background-only. Single-flight; a no-op once the entry is fresher than
   * refreshAfterMs. Never rejects — swallows compute failures (including the
   * transient "revision changed mid-read" race that a concurrently-ingesting
   * store can produce) so the poll loop's fire-and-forget caller never needs
   * its own retry logic; the next tick simply tries again. */
  refresh(): Promise<void>;
  /** Synchronous, zero-I/O read of the last successfully computed entry.
   * NEVER triggers a compute — this is what makes /api/tokenomics prompt: a
   * request can only ever read what the background loop already finished,
   * it can never be made to wait on one. */
  peek(): TokenomicsCacheEntry | null;
}

/** compute() runs a full tokenomics pass in a dedicated sibling process (see
 * tokenomics-worker.ts). Observed wall-clock time for that off-thread call
 * ranges from sub-millisecond (steady state) up to ~20s (cold boot's bulk
 * ingest burst, or contention with the ingest/aggregation siblings sharing
 * this service's CPUQuota=50% slice — see systemd/sigmashake-tokens.slice,
 * never widened for this). A caller that awaited compute() directly would
 * occasionally block for that entire tail, which is exactly what tripped
 * Mission Control's 4s client-side /api/tokenomics timeout and produced the
 * degraded flap this cache exists to eliminate — so no code path here ever
 * awaits compute() from a request; only refresh() (background, always
 * fire-and-forget) calls it. */
export function createTokenomicsCache(deps: {
  compute(): Promise<TokenomicsResult>;
  now?(): number;
  refreshAfterMs?: number;
}): TokenomicsCache {
  const refreshAfterMs = deps.refreshAfterMs ?? 15_000;
  const now = deps.now ?? Date.now;
  let entry: TokenomicsCacheEntry | null = null;
  let inFlight: Promise<void> | null = null;

  return {
    async refresh(): Promise<void> {
      if (entry !== null) {
        const ageMs = now() - entry.computedAt;
        if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= refreshAfterMs) return;
      }
      if (inFlight) return inFlight;
      // A compute failure (most commonly the store's revision advancing
      // mid-read because ingest is actively writing, or an intentional
      // rejection from shutdownTokenomicsWorker()) is intentionally NOT
      // swallowed here — it propagates to this call's own caller (the poll
      // loop's fire-and-forget refresh().catch(), which already knows to
      // skip logging during shutdown, matching unfilteredUsageCache's
      // convention above). Either way, `entry` is only ever replaced by a
      // *successful* compute, so peek() keeps serving the prior good
      // reading — honestly aging toward "stale" — until a retry succeeds.
      const pending = deps
        .compute()
        .then((result) => {
          entry = { result, computedAt: now() };
        })
        .finally(() => {
          if (inFlight === pending) inFlight = null;
        });
      inFlight = pending;
      return pending;
    },
    peek(): TokenomicsCacheEntry | null {
      return entry;
    },
  };
}

const ingestController = createIngestController({
  getRevision: () => getStore().getRevision(),
  ingest: async (previouslyReady) => {
    const startMs = Date.now();
    let ok = false;
    let filesScanned = 0;
    let rowsIngested = 0;
    try {
      const store = getStore();
      const heartbeatTarget = previouslyReady
        ? (issueTokenSnapshotHeartbeatTarget(store) ?? undefined)
        : undefined;
      const summary = await ingestUsageOffThread(
        {
          databasePath: resolve(store.databasePath),
          claudeProjectsDir: resolve(claudeProjectsDir()),
          codexSessionsDir: resolve(codexSessionsDir()),
          grokLogsDir: resolve(grokLogsDir()),
          grokSessionsDir: resolve(grokSessionsDir()),
        },
        heartbeatTarget,
      );
      ok = true;
      filesScanned = summary.files;
      rowsIngested = summary.update.newEvents;
      if (!shouldBroadcastIngestResult(summary) || !previouslyReady) return;
      // The worker persisted the exact fixed windows after its scan. Reading the
      // tiny cache here keeps both SSE and /api/rate off the events table.
      const now = Date.now();
      const rate60s = cachedRateBucketForBroadcast(store, now);
      broadcast(buildUpdateFromDelta(summary.update, rate60s, authoritativeSnapshot(now)));
    } finally {
      // Fires on every path (success, early return, or a rethrown failure)
      // so the ingest.cycle span always reflects reality — `ok` only ever
      // flips true once ingestUsageOffThread genuinely resolves.
      recordIngestCycleSpan({ startMs, endMs: Date.now(), ok, filesScanned, rowsIngested });
    }
  },
  onAuthorityChange: () => {
    broadcast(buildUpdateFromEvents([], undefined, authoritativeSnapshot()));
  },
  onFailure: (error) => console.error("ingest tick failed:", error),
});

const unfilteredUsageCache = createUnfilteredUsageCache({
  compute: () => loadAggregations({ rescan: false }),
});

const tokenomicsCache = createTokenomicsCache({
  compute: () => computeTokenomicsOffThread(resolve(getStore().databasePath)),
});

/** True only when none of the four /api/usage filter params are present —
 * the exact shape the OBS Boss HUD polls, and the only shape ever served
 * from unfilteredUsageCache. Extracted as a pure predicate so routing can
 * be asserted without standing up a live server. */
export function isUnfilteredUsageQuery(params: {
  from?: string;
  to?: string;
  models?: string[];
  projects?: string[];
}): boolean {
  return !params.from && !params.to && !params.models && !params.projects;
}

async function handleTokensRequest(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const p = url.pathname;
  if (p === "/healthz") {
    const health = ingestController.health();
    return healthResponse(health, hasCompleteLastGoodForHealthz());
  }
  if (p === "/api/live") return sseResponse(authoritativeSnapshot());
  if (p === "/api/snapshot" || p === "/api/token-snapshot") {
    return tokenSnapshotResponse(Date.now(), url.searchParams.get("window"));
  }
  if (p === "/api/stats") return cachedStatsResponse(getStore());
  if (p === "/api/rate") return cachedRateResponse(getStore());
  if (p === "/api/usage") {
    const from = url.searchParams.get("from") || undefined;
    const to = url.searchParams.get("to") || undefined;
    const modelsParam = url.searchParams.get("models") || "";
    const projectsParam = url.searchParams.get("projects") || "";
    const models = modelsParam ? modelsParam.split(",").filter(Boolean) : undefined;
    const projects = projectsParam ? projectsParam.split(",").filter(Boolean) : undefined;
    const isUnfiltered = isUnfilteredUsageQuery({ from, to, models, projects });
    // Never run an ingest from the request path — the background loop owns
    // ingestion + broadcasting. Triggering an ingest here would broadcast
    // the same events the response already includes, double-counting any
    // open SSE clients.
    try {
      // The unfiltered shape is the OBS Boss HUD's hot poll path — serve it
      // from the wall-clock-TTL cache populated by the background loop.
      // Filtered/parameterized queries always compute directly so a cache
      // entry for one parameter set can never answer a different one.
      const agg = isUnfiltered
        ? await unfilteredUsageCache.get()
        : await loadAggregations({ from, to, rescan: false, models, projects });
      return jsonResponse({
        ...agg,
        health: usageHealthPayload(ingestController.health(), agg.scannedAt),
      });
    } catch (err) {
      return jsonResponse({ error: String(err) }, 500);
    }
  }
  if (p === "/api/day") {
    const date = url.searchParams.get("date") || "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return jsonResponse({ error: "date param must be YYYY-MM-DD" }, 400);
    }
    try {
      const detail = await loadDayDetail(date);
      return jsonResponse(detail);
    } catch (err) {
      return jsonResponse({ error: String(err) }, 500);
    }
  }
  if (p === "/api/session") {
    const id = url.searchParams.get("id") || "";
    if (!id) return jsonResponse({ error: "id required" }, 400);
    try {
      const detail = await loadSessionDetail(id);
      return jsonResponse(detail);
    } catch (err) {
      return jsonResponse({ error: String(err) }, 500);
    }
  }
  if (p === "/api/session-context") {
    const id = url.searchParams.get("id") || "";
    if (!id) return jsonResponse({ error: "id required" }, 400);
    try {
      return jsonResponse(loadSessionContextEstimate(id));
    } catch (err) {
      return jsonResponse({ error: String(err) }, 500);
    }
  }
  if (p === "/api/savings") {
    try {
      const savings = await readSavings();
      return jsonResponse(savings);
    } catch (err) {
      return jsonResponse({ error: String(err) }, 500);
    }
  }
  if (p === "/api/tokenomics") {
    // Zero-I/O read — see createTokenomicsCache. This route can never wait
    // on a compute, so it can never be the reason a caller (e.g. Mission
    // Control's 4s /api/tokenomics client timeout) sees a slow response.
    const cached = tokenomicsCache.peek();
    if (cached === null) {
      // Only reachable in the brief window before the first successful
      // background compute completes (cold boot) — never a recurring
      // steady-state state, and never a silent fabricated zero reading.
      return jsonResponse({ error: "tokenomics-warming", state: "warming" }, 503);
    }
    const ageMs = Date.now() - cached.computedAt;
    return jsonResponse({
      ...cached.result,
      computedAt: cached.computedAt,
      ageMs,
      stale: ageMs > TOKENS_CACHE_MAX_AGE_MS,
    });
  }
  if (p.startsWith("/api/")) return jsonResponse({ error: "not found" }, 404);
  return serveStatic(p);
}

/** Buckets a request pathname the same way handleTokensRequest's own
 * branching does — known API/health paths pass through as-is, an
 * unrecognized `/api/*` path collapses to "other" inside
 * mapTokensRoute(), and everything else (dashboard HTML/JS/CSS) is the
 * single "static" bucket. Keeps the otel span's route cardinality bounded
 * without duplicating the route table in two places. */
function tokensRequestRouteLabel(pathname: string): string {
  const isApiPath = pathname === "/healthz" || pathname.startsWith("/api/");
  return mapTokensRoute(isApiPath ? pathname : "static");
}

export const serverOptions = {
  hostname: HOST,
  port: PORT,
  idleTimeout: 0, // disable for SSE
  async fetch(req) {
    const startMs = Date.now();
    const route = tokensRequestRouteLabel(new URL(req.url).pathname);
    const response = await handleTokensRequest(req);
    recordApiRequestSpan({ startMs, endMs: Date.now(), status: response.status, route });
    return response;
  },
} satisfies Parameters<typeof Bun.serve>[0];

export interface TokensServerLifecycle {
  shutdown(): Promise<void>;
}

export function startTokensServer(): TokensServerLifecycle {
  // Open/migrate the durable store before accepting traffic. Ingestion itself
  // starts only after the listener is ready and cannot block this event loop.
  getStore();
  // Declared before triggerIngest so a refresh still draining during
  // shutdown can tell an expected worker-teardown rejection apart from a
  // real failure and skip logging it as one.
  let shuttingDown = false;
  // At-most-one transient of {ingest, aggregation, tokenomics} from the poll
  // path — their MemoryMax values cannot all fit under the 512M slice with the
  // HTTP owner. Overlapping setInterval ticks queue on pollChain.
  // When idle, tick() is entered synchronously so same-turn /healthz readers
  // observe ingestRunning=true (createIngestController sets the flag before
  // its first await).
  let pollChain: Promise<void> = Promise.resolve();
  let pollInFlight = false;
  const runPostIngestWorkers = async (): Promise<void> => {
    if (shuttingDown) return;
    const usageRefreshStartMs = Date.now();
    await unfilteredUsageCache
      .refresh()
      .then(() => {
        recordAggregationRefreshSpan({
          startMs: usageRefreshStartMs,
          endMs: Date.now(),
          ok: true,
          kind: "usage",
        });
      })
      .catch((error) => {
        recordAggregationRefreshSpan({
          startMs: usageRefreshStartMs,
          endMs: Date.now(),
          ok: false,
          kind: "usage",
        });
        if (shuttingDown) return;
        console.error("usage cache refresh failed:", error);
      });
    if (shuttingDown) return;
    const tokenomicsRefreshStartMs = Date.now();
    await tokenomicsCache
      .refresh()
      .then(() => {
        recordAggregationRefreshSpan({
          startMs: tokenomicsRefreshStartMs,
          endMs: Date.now(),
          ok: true,
          kind: "tokenomics",
        });
      })
      .catch((error) => {
        recordAggregationRefreshSpan({
          startMs: tokenomicsRefreshStartMs,
          endMs: Date.now(),
          ok: false,
          kind: "tokenomics",
        });
        if (shuttingDown) return;
        console.error("tokenomics cache refresh failed:", error);
      });
  };
  const runSerializedCycle = async (ingestTick: Promise<void>): Promise<void> => {
    try {
      if (shuttingDown) return;
      await ingestTick.catch((error) => console.error("ingest scheduler failed:", error));
      await runPostIngestWorkers();
    } finally {
      pollInFlight = false;
    }
  };
  const triggerIngest = (): void => {
    if (pollInFlight) {
      pollChain = pollChain
        .catch(() => undefined)
        .then(async () => {
          if (shuttingDown || pollInFlight) return;
          pollInFlight = true;
          await runSerializedCycle(ingestController.tick());
        });
      return;
    }
    pollInFlight = true;
    // Synchronous tick entry — ingestRunning flips before this returns.
    const ingestTick = ingestController.tick();
    pollChain = runSerializedCycle(ingestTick);
  };
  const pollTimer = setInterval(triggerIngest, POLL_MS);
  const server = Bun.serve(serverOptions);
  triggerIngest();
  const shutdown = async (): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    clearInterval(pollTimer);
    process.off("SIGINT", requestShutdown);
    process.off("SIGTERM", requestShutdown);
    server.stop(true);
    await Promise.all([
      shutdownUsageIngestWorker(),
      shutdownUsageAggregationWorker(),
      shutdownTokenomicsWorker(),
    ]);
  };
  const requestShutdown = (): void => {
    void shutdown().catch((error) => {
      console.error("tokens shutdown failed:", error);
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", requestShutdown);
  process.once("SIGTERM", requestShutdown);
  console.log(
    `sigmashake-tokens listening on http://${server.hostname}:${server.port} (poll=${POLL_MS}ms)`,
  );
  return { shutdown };
}

if (import.meta.main) {
  startTokensServer();
}

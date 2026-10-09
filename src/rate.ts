// Pure rolling-window rate/throughput computation. The cross-package daily
// snapshot contract is re-exported from token-usage-contract.ts below.

export type {
  AuthoritativeTokenSnapshot,
  DailyTokenWindow,
  Rolling24hTokenWindow,
  SnapshotModel,
  SnapshotProvider,
  SnapshotRawTotals,
  SnapshotTokenClass,
  SnapshotTokenClasses,
  TokenWindow,
} from "./token-usage-contract.ts";
export {
  buildAuthoritativeTokenSnapshot,
  composeProviderTokenClasses,
  composeSnapshotTokenClasses,
  dailyTokenWindow,
  HUD_TIME_ZONE,
  isAuthoritativeTokenSnapshot,
  ROLLING_24H_MS,
  rolling24hTokenWindow,
  shouldResetAuthoritativeHud,
  TOKEN_SOURCES,
} from "./token-usage-contract.ts";

export const RATE_WINDOWS_SEC = [60, 5 * 60, 15 * 60, 60 * 60] as const;
export const RATE_MODEL_BUCKET_MAX = 32;
export const RATE_HARNESS_BUCKET_MAX = 4;
export const RATE_BUCKET_LABEL_MAX_BYTES = 256;

const RATE_MODEL_OVERFLOW = "other";
const RATE_HARNESS_OVERFLOW = "other";
const textEncoder = new TextEncoder();

export interface RateModelBucket {
  model: string;
  tokens: number;
  /** tokens ÷ windowSec — a fixed denominator, so this is never divide-by-0. */
  tokensPerSecond: number;
  messages: number;
}

export interface RateHarnessBucket {
  source: string;
  tokens: number;
  tokensPerSecond: number;
  messages: number;
}

export interface RateWindowBucket {
  windowSec: number;
  messages: number;
  tokens: number;
  cost: number;
  tokensPerSecond: number;
  tokensPerMinute: number;
  costPerHour: number;
  activeSessions: number;
  byModel: RateModelBucket[];
  byHarness: RateHarnessBucket[];
}

export interface RateSnapshot {
  now: number;
  buckets: RateWindowBucket[];
}

/** Compact row required by the rolling-rate reducer. Keeping this contract
 * independent from StoredEvent prevents the worker from materializing large
 * project/path/dedupe payloads that rate calculations never use. */
export interface RateEvent {
  ts: number;
  model: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  source: string;
}

/** One row per session, grouped inside SQLite. The reducer consumes this
 * cursor without retaining session identifiers in the worker heap. */
export interface RateSessionActivity {
  latestTs: number;
}

export interface RateEventStore {
  iterateRateEvents(opts: { fromTs: number; toTs: number }): Iterable<RateEvent>;
  iterateRateSessionActivity(opts: { fromTs: number; toTs: number }): Iterable<RateSessionActivity>;
}

interface RateDimensionTally {
  tokens: number;
  messages: number;
}

function boundedRateLabel(value: string, overflow: string): string {
  return value.length > 0 &&
    value.length <= RATE_BUCKET_LABEL_MAX_BYTES &&
    textEncoder.encode(value).byteLength <= RATE_BUCKET_LABEL_MAX_BYTES
    ? value
    : overflow;
}

/** Keep breakdown memory and wire cardinality fixed while preserving exact
 * window totals. Once a dimension fills, all additional labels roll into an
 * explicit `other` bucket rather than being silently discarded. */
function addBoundedDimension(
  tally: Map<string, RateDimensionTally>,
  boundedLabel: string,
  overflowLabel: string,
  maxBuckets: number,
  tokens: number,
): void {
  const label =
    tally.has(boundedLabel) || tally.size < maxBuckets - 1 ? boundedLabel : overflowLabel;
  const current = tally.get(label) ?? { tokens: 0, messages: 0 };
  current.tokens += tokens;
  current.messages += 1;
  tally.set(label, current);
}

function compareRateDimensions(
  left: { tokens: number; label: string },
  right: { tokens: number; label: string },
): number {
  return right.tokens - left.tokens || left.label.localeCompare(right.label);
}

function totalTokensFor(event: RateEvent): number {
  return (
    event.inputTokens +
    event.outputTokens +
    event.cacheReadTokens +
    event.cacheWrite5mTokens +
    event.cacheWrite1hTokens
  );
}

interface MutableRateWindow {
  windowSec: number;
  fromTs: number;
  tokens: number;
  cost: number;
  messages: number;
  activeSessions: number;
  modelTally: Map<string, RateDimensionTally>;
  harnessTally: Map<string, RateDimensionTally>;
}

function finalizeWindowBucket(window: MutableRateWindow): RateWindowBucket {
  const { windowSec, tokens, cost, messages, activeSessions, modelTally, harnessTally } = window;

  const byModel: RateModelBucket[] = [...modelTally.entries()]
    .map(([model, value]) => ({
      model,
      tokens: value.tokens,
      tokensPerSecond: value.tokens / windowSec,
      messages: value.messages,
    }))
    .sort((a, b) =>
      compareRateDimensions(
        { tokens: a.tokens, label: a.model },
        { tokens: b.tokens, label: b.model },
      ),
    );

  const byHarness: RateHarnessBucket[] = [...harnessTally.entries()]
    .map(([source, value]) => ({
      source,
      tokens: value.tokens,
      tokensPerSecond: value.tokens / windowSec,
      messages: value.messages,
    }))
    .sort((a, b) =>
      compareRateDimensions(
        { tokens: a.tokens, label: a.source },
        { tokens: b.tokens, label: b.source },
      ),
    );

  return {
    windowSec,
    messages,
    tokens,
    cost,
    tokensPerSecond: tokens / windowSec,
    tokensPerMinute: tokens / (windowSec / 60),
    costPerHour: cost * (3600 / windowSec),
    activeSessions,
    byModel,
    byHarness,
  };
}

/** Fixed-window burn-rate snapshot. Each bucket applies its own [from, now]
 * bound and never claims per-message generation speed. The input is consumed
 * exactly once so a SQLite iterator remains bounded regardless of event count. */
export function computeRateSnapshot(
  events: Iterable<RateEvent>,
  now: number,
  sessionActivity: Iterable<RateSessionActivity> = [],
): RateSnapshot {
  const windows: MutableRateWindow[] = RATE_WINDOWS_SEC.map((windowSec) => ({
    windowSec,
    fromTs: now - windowSec * 1000,
    tokens: 0,
    cost: 0,
    messages: 0,
    activeSessions: 0,
    modelTally: new Map(),
    harnessTally: new Map(),
  }));
  const widestFromTs = windows[windows.length - 1].fromTs;

  for (const event of events) {
    if (event.ts < widestFromTs || event.ts > now) continue;
    const total = totalTokensFor(event);
    const model = boundedRateLabel(event.model, RATE_MODEL_OVERFLOW);
    const harness = boundedRateLabel(event.source, RATE_HARNESS_OVERFLOW);
    for (const window of windows) {
      if (event.ts < window.fromTs) continue;
      window.tokens += total;
      window.cost += event.cost;
      window.messages += 1;
      addBoundedDimension(
        window.modelTally,
        model,
        RATE_MODEL_OVERFLOW,
        RATE_MODEL_BUCKET_MAX,
        total,
      );
      addBoundedDimension(
        window.harnessTally,
        harness,
        RATE_HARNESS_OVERFLOW,
        RATE_HARNESS_BUCKET_MAX,
        total,
      );
    }
  }

  for (const session of sessionActivity) {
    if (session.latestTs < widestFromTs || session.latestTs > now) continue;
    for (const window of windows) {
      if (session.latestTs >= window.fromTs) window.activeSessions += 1;
    }
  }

  return {
    now,
    buckets: windows.map(finalizeWindowBucket),
  };
}

/** Streams only compact rate columns across the widest rolling window once;
 * narrower buckets are accumulated during the same cursor pass. */
export function selectRateSnapshot(store: RateEventStore, now: number = Date.now()): RateSnapshot {
  const widestWindowSec = RATE_WINDOWS_SEC[RATE_WINDOWS_SEC.length - 1];
  const bounds = {
    fromTs: now - widestWindowSec * 1000,
    toTs: now,
  };
  return computeRateSnapshot(
    store.iterateRateEvents(bounds),
    now,
    store.iterateRateSessionActivity(bounds),
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isNonNegativeFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function isBoundedRateLabel(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= RATE_BUCKET_LABEL_MAX_BYTES &&
    textEncoder.encode(value).byteLength <= RATE_BUCKET_LABEL_MAX_BYTES
  );
}

function nearlyEqual(actual: number, expected: number): boolean {
  return Math.abs(actual - expected) <= Math.max(1, Math.abs(expected)) * 1e-12;
}

function isRateDimensionArray(
  value: unknown,
  labelKey: "model" | "source",
  maxBuckets: number,
  windowSec: number,
  expectedTokens: number,
  expectedMessages: number,
): boolean {
  if (!Array.isArray(value) || value.length > maxBuckets) return false;
  const labels = new Set<string>();
  let tokens = 0;
  let messages = 0;
  for (const entry of value) {
    if (!isRecord(entry)) return false;
    const label = entry[labelKey];
    if (!isBoundedRateLabel(label)) return false;
    if (labels.has(label)) return false;
    labels.add(label);
    if (!isNonNegativeSafeInteger(entry.tokens) || !isNonNegativeSafeInteger(entry.messages)) {
      return false;
    }
    if (
      !isNonNegativeFiniteNumber(entry.tokensPerSecond) ||
      !nearlyEqual(entry.tokensPerSecond, entry.tokens / windowSec)
    ) {
      return false;
    }
    tokens += entry.tokens;
    messages += entry.messages;
    if (!Number.isSafeInteger(tokens) || !Number.isSafeInteger(messages)) return false;
  }
  return tokens === expectedTokens && messages === expectedMessages;
}

/** Strict validation for the persisted and cross-process rate contract. */
export function isRateSnapshot(value: unknown): value is RateSnapshot {
  if (!isRecord(value) || !isNonNegativeSafeInteger(value.now)) return false;
  if (!Array.isArray(value.buckets) || value.buckets.length !== RATE_WINDOWS_SEC.length) {
    return false;
  }
  return value.buckets.every((bucket, index) => {
    if (!isRecord(bucket) || bucket.windowSec !== RATE_WINDOWS_SEC[index]) return false;
    const windowSec = RATE_WINDOWS_SEC[index];
    if (
      !isNonNegativeSafeInteger(bucket.messages) ||
      !isNonNegativeSafeInteger(bucket.tokens) ||
      !isNonNegativeFiniteNumber(bucket.cost) ||
      !isNonNegativeFiniteNumber(bucket.tokensPerSecond) ||
      !isNonNegativeFiniteNumber(bucket.tokensPerMinute) ||
      !isNonNegativeFiniteNumber(bucket.costPerHour) ||
      !isNonNegativeSafeInteger(bucket.activeSessions) ||
      bucket.activeSessions > bucket.messages
    ) {
      return false;
    }
    if (
      !nearlyEqual(bucket.tokensPerSecond, bucket.tokens / windowSec) ||
      !nearlyEqual(bucket.tokensPerMinute, bucket.tokens / (windowSec / 60)) ||
      !nearlyEqual(bucket.costPerHour, bucket.cost * (3600 / windowSec))
    ) {
      return false;
    }
    return (
      isRateDimensionArray(
        bucket.byModel,
        "model",
        RATE_MODEL_BUCKET_MAX,
        windowSec,
        bucket.tokens,
        bucket.messages,
      ) &&
      isRateDimensionArray(
        bucket.byHarness,
        "source",
        RATE_HARNESS_BUCKET_MAX,
        windowSec,
        bucket.tokens,
        bucket.messages,
      )
    );
  });
}

// otel-export.ts — detached, fail-open OTLP/HTTP JSON exporter for
// sigmashake-tokens (the token-spend observability daemon itself).
//
// Mirrors the proven zero-dependency pattern already shipped in
// sigmashake-orch/src/common/otel-export.ts (spans) and
// sigmashake-mesh/src/otel-export.ts (bounded coalescing, injectable
// fetch, gated by OTEL_EXPORTER_OTLP_ENDPOINT). This module adds metrics
// export (POST {endpoint}/v1/metrics) alongside spans (POST
// {endpoint}/v1/traces), since sigmashake-tokens has no logs pipeline
// worth exporting but does have real counters and a heartbeat gauge.
//
// Hard invariants (do not relax):
//  - Fully OFF (zero work: no timers, no buffering, no allocation beyond
//    the env read) when OTEL_EXPORTER_OTLP_ENDPOINT is unset/blank. The
//    env is read at call time, never frozen in a module-level const.
//  - Bounded span buffer: max 256 spans AND max 256 KiB approx serialized
//    size. A full buffer drops the incoming span and counts it.
//  - Exactly one in-flight span flush and one in-flight metrics flush at a
//    time (independent `flushingSpans`/`flushingMetrics` gates).
//  - Every failure mode (network error, timeout, non-2xx, bad JSON) is
//    swallowed — this module must never throw into the HTTP request path,
//    the ingest loop, or the SSE broadcaster.
//  - service.name is the literal string "tokens" — this MUST match the
//    otelServiceName mission-control's REQUIRED_OTEL_SERVICES roster
//    expects for this daemon's row (see
//    sigmashake-mission-control/src/collectors/otelRemoteHost.ts).

export const OTEL_SERVICE_NAME = "tokens";

const MAX_SPANS = 256;
const MAX_SPAN_BUFFER_BYTES = 256 * 1024;
const MAX_BODY_BYTES = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 4 * 1024;
const FLUSH_INTERVAL_MS = 5_000;
const FETCH_TIMEOUT_MS = 2_000;

function endpoint(): string | null {
  const raw = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  if (raw == null) return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  return trimmed.replace(/\/+$/, "");
}

export function isOtelExportEnabled(): boolean {
  return endpoint() !== null;
}

// ── Fetch seam (dependency injection for tests — no network required) ──────

export interface OtelFetchResponse {
  ok: boolean;
  status: number;
  text?: () => Promise<string>;
  body?: ReadableStream<Uint8Array> | null;
}

export type OtelFetchLike = (
  url: string,
  init: {
    method: string;
    headers: Record<string, string>;
    body: string;
    signal: AbortSignal;
  },
) => Promise<OtelFetchResponse>;

let fetchImpl: OtelFetchLike = globalThis.fetch as unknown as OtelFetchLike;

/** setOtelFetch: inject a fake fetch for tests. Pass null to restore global fetch. */
export function setOtelFetch(fn: OtelFetchLike | null): void {
  fetchImpl = fn ?? (globalThis.fetch as unknown as OtelFetchLike);
}

// ── Allowlisted vocabularies ─────────────────────────────────────────────

export type TokensSpanKind = "ingest.cycle" | "api.request" | "aggregation.refresh";

const KNOWN_ROUTES = new Set([
  "/healthz",
  "/api/live",
  "/api/snapshot",
  "/api/token-snapshot",
  "/api/stats",
  "/api/rate",
  "/api/usage",
  "/api/day",
  "/api/session",
  "/api/session-context",
  "/api/savings",
  "/api/tokenomics",
  "static",
]);

/** Maps an arbitrary request pathname through the known-route allowlist.
 * Unknown paths (404s, probes, typos) serialize as the literal "other" —
 * never verbatim, so this can never become an open text sink. */
export function mapTokensRoute(pathname: string): string {
  return KNOWN_ROUTES.has(pathname) ? pathname : "other";
}

// ── OTLP/JSON projection ────────────────────────────────────────────────

interface OtlpAttribute {
  key: string;
  value: { stringValue?: string; intValue?: string; boolValue?: boolean };
}

interface OtlpSpan {
  traceId: string;
  spanId: string;
  name: TokensSpanKind;
  kind: number;
  startTimeUnixNano: string;
  endTimeUnixNano: string;
  attributes: OtlpAttribute[];
  status: { code: number };
}

function strAttr(key: string, value: string): OtlpAttribute {
  return { key, value: { stringValue: value } };
}

function intAttr(key: string, value: number): OtlpAttribute {
  return { key, value: { intValue: String(Math.trunc(value)) } };
}

function boolAttr(key: string, value: boolean): OtlpAttribute {
  return { key, value: { boolValue: value } };
}

function nanos(ms: number): string {
  const safeMs = Number.isFinite(ms) ? Math.max(0, Math.trunc(ms)) : 0;
  return (BigInt(safeMs) * 1_000_000n).toString();
}

function randomHex(bytes: number): string {
  const arr = new Uint8Array(bytes);
  crypto.getRandomValues(arr);
  return Array.from(arr, (b) => b.toString(16).padStart(2, "0")).join("");
}

function buildSpan(
  name: TokensSpanKind,
  startMs: number,
  endMs: number,
  ok: boolean,
  attributes: OtlpAttribute[],
): OtlpSpan {
  const statusCode = ok ? 1 : 2; // OTLP StatusCode OK / ERROR
  return {
    traceId: randomHex(16),
    spanId: randomHex(8),
    name,
    kind: 1, // INTERNAL
    startTimeUnixNano: nanos(startMs),
    endTimeUnixNano: nanos(Math.max(startMs, endMs)),
    attributes,
    status: { code: statusCode },
  };
}

function tracesPayload(spans: OtlpSpan[]): unknown {
  return {
    resourceSpans: [
      {
        resource: { attributes: [strAttr("service.name", OTEL_SERVICE_NAME)] },
        scopeSpans: [{ scope: { name: "sigmashake-tokens" }, spans }],
      },
    ],
  };
}

// ── Counters (present as OTLP cumulative-sum metrics on each flush) ───────

const processStartMs = Date.now();
let ingestCyclesTotal = 0;
let ingestRowsTotal = 0;
let apiRequestsTotal = 0;

interface OtlpMetric {
  name: string;
  unit: string;
  gauge?: { dataPoints: Array<{ timeUnixNano: string; asDouble: number }> };
  sum?: {
    aggregationTemporality: number;
    isMonotonic: boolean;
    dataPoints: Array<{ startTimeUnixNano: string; timeUnixNano: string; asInt: string }>;
  };
}

const AGGREGATION_TEMPORALITY_CUMULATIVE = 2;

function counterMetric(name: string, value: number, nowNano: string, startNano: string): OtlpMetric {
  return {
    name,
    unit: "1",
    sum: {
      aggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
      isMonotonic: true,
      dataPoints: [{ startTimeUnixNano: startNano, timeUnixNano: nowNano, asInt: String(Math.max(0, Math.trunc(value))) }],
    },
  };
}

function metricsPayload(now: number): unknown {
  const nowNano = nanos(now);
  const startNano = nanos(processStartMs);
  const uptimeSeconds = Math.max(0, (now - processStartMs) / 1000);
  return {
    resourceMetrics: [
      {
        resource: { attributes: [strAttr("service.name", OTEL_SERVICE_NAME)] },
        scopeMetrics: [
          {
            scope: { name: "sigmashake-tokens" },
            metrics: [
              {
                name: "tokens_uptime_seconds",
                unit: "s",
                gauge: { dataPoints: [{ timeUnixNano: nowNano, asDouble: uptimeSeconds }] },
              },
              counterMetric("tokens_ingest_cycles_total", ingestCyclesTotal, nowNano, startNano),
              counterMetric("tokens_ingest_rows_total", ingestRowsTotal, nowNano, startNano),
              counterMetric("tokens_api_requests_total", apiRequestsTotal, nowNano, startNano),
            ],
          },
        ],
      },
    ],
  };
}

// ── Bounded response reader (mirrors orch/mesh) ─────────────────────────

async function readBounded(res: OtelFetchResponse, maxBytes: number): Promise<string> {
  const body = res.body;
  if (body && typeof body.getReader === "function") {
    const reader = body.getReader();
    let received = 0;
    const chunks: Uint8Array[] = [];
    try {
      while (received < maxBytes) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) {
          const remaining = maxBytes - received;
          const chunk = value.byteLength > remaining ? value.slice(0, remaining) : value;
          chunks.push(chunk);
          received += chunk.byteLength;
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {
        // ignore
      }
    }
    return Buffer.concat(chunks.map((c) => Buffer.from(c))).toString("utf-8");
  }
  if (typeof res.text === "function") {
    const text = await res.text();
    return text.length > maxBytes ? text.slice(0, maxBytes) : text;
  }
  return "";
}

class ExportFailure extends Error {}

/** Posts one OTLP/JSON payload with a bounded timeout, bounded body size,
 * and a bounded response read. Never returns partial success info to the
 * caller — non-2xx and network errors both surface as ExportFailure so
 * every call site's accounting stays uniform. */
async function postOtlp(path: string, payload: unknown): Promise<void> {
  const exportEndpoint = endpoint();
  if (!exportEndpoint) throw new ExportFailure("otel disabled");
  const body = JSON.stringify(payload);
  if (Buffer.byteLength(body, "utf-8") > MAX_BODY_BYTES) {
    throw new ExportFailure("body exceeds max size");
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetchImpl(`${exportEndpoint}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
      signal: controller.signal,
    });
    if (!response.ok) throw new ExportFailure(`http ${response.status}`);
    await readBounded(response, MAX_RESPONSE_BYTES);
  } finally {
    clearTimeout(timeout);
  }
}

// ── Span buffer + flush ─────────────────────────────────────────────────

let spanBuffer: OtlpSpan[] = [];
let spanBufferBytes = 0;
let flushingSpans = false;
let flushingMetrics = false;
let interval: ReturnType<typeof setInterval> | undefined;

export interface OtelExportStats {
  spansExported: number;
  spansDropped: number;
  metricsExported: number;
  metricsDropped: number;
}

const stats: OtelExportStats = {
  spansExported: 0,
  spansDropped: 0,
  metricsExported: 0,
  metricsDropped: 0,
};

export function getOtelExportStats(): OtelExportStats {
  return { ...stats };
}

async function flushSpans(): Promise<void> {
  if (flushingSpans || spanBuffer.length === 0) return;
  if (!endpoint()) return;
  flushingSpans = true;
  const batch = spanBuffer;
  spanBuffer = [];
  spanBufferBytes = 0;
  try {
    await postOtlp("/v1/traces", tracesPayload(batch));
    stats.spansExported += batch.length;
  } catch {
    stats.spansDropped += batch.length;
  } finally {
    flushingSpans = false;
  }
}

async function flushMetrics(): Promise<void> {
  if (flushingMetrics) return;
  if (!endpoint()) return;
  flushingMetrics = true;
  try {
    await postOtlp("/v1/metrics", metricsPayload(Date.now()));
    stats.metricsExported += 1;
  } catch {
    stats.metricsDropped += 1;
  } finally {
    flushingMetrics = false;
  }
}

function detachedFlush(): void {
  void flushSpans().catch(() => {
    // flushSpans never throws (all paths caught internally) — belt and suspenders.
  });
  void flushMetrics().catch(() => {
    // same
  });
}

function ensureInterval(): void {
  if (interval) return;
  interval = setInterval(detachedFlush, FLUSH_INTERVAL_MS);
  interval.unref?.();
}

function enqueueSpan(span: OtlpSpan): void {
  const bytes = Buffer.byteLength(JSON.stringify(span), "utf-8");
  if (
    bytes > MAX_SPAN_BUFFER_BYTES ||
    spanBuffer.length >= MAX_SPANS ||
    spanBufferBytes + bytes > MAX_SPAN_BUFFER_BYTES
  ) {
    stats.spansDropped += 1;
    return;
  }
  spanBuffer.push(span);
  spanBufferBytes += bytes;
  ensureInterval();
  if (spanBuffer.length >= MAX_SPANS || spanBufferBytes >= MAX_SPAN_BUFFER_BYTES) detachedFlush();
}

// ── Producer entry points ───────────────────────────────────────────────
//
// Each function is a strict projection: no caller-controlled free-text
// attribute ever reaches the wire. Every call is wrapped so a defect in
// this module can never throw into the caller (ingest loop / HTTP handler).

export function recordIngestCycleSpan(input: {
  startMs: number;
  endMs: number;
  ok: boolean;
  filesScanned: number;
  rowsIngested: number;
}): void {
  if (!isOtelExportEnabled()) return;
  try {
    ingestCyclesTotal += 1;
    ingestRowsTotal += Math.max(0, Math.trunc(input.rowsIngested));
    const span = buildSpan("ingest.cycle", input.startMs, input.endMs, input.ok, [
      boolAttr("ok", input.ok),
      intAttr("filesScanned", Math.max(0, Math.trunc(input.filesScanned))),
      intAttr("rowsIngested", Math.max(0, Math.trunc(input.rowsIngested))),
    ]);
    enqueueSpan(span);
  } catch {
    stats.spansDropped += 1;
  }
}

export function recordApiRequestSpan(input: { startMs: number; endMs: number; status: number; route: string }): void {
  if (!isOtelExportEnabled()) return;
  try {
    apiRequestsTotal += 1;
    const ok = input.status >= 200 && input.status < 500;
    const span = buildSpan("api.request", input.startMs, input.endMs, ok, [
      strAttr("route", mapTokensRoute(input.route)),
      intAttr("status", Math.max(0, Math.trunc(input.status))),
    ]);
    enqueueSpan(span);
  } catch {
    stats.spansDropped += 1;
  }
}

export function recordAggregationRefreshSpan(input: {
  startMs: number;
  endMs: number;
  ok: boolean;
  kind: "usage" | "tokenomics";
}): void {
  if (!isOtelExportEnabled()) return;
  try {
    const span = buildSpan("aggregation.refresh", input.startMs, input.endMs, input.ok, [
      strAttr("kind", input.kind),
      boolAttr("ok", input.ok),
    ]);
    enqueueSpan(span);
  } catch {
    stats.spansDropped += 1;
  }
}

// ── Test-only seams ──────────────────────────────────────────────────────

/** Test seam: flush both pipelines without exposing a rejecting promise. */
export async function forceFlushOtelForTests(): Promise<void> {
  await flushSpans().catch(() => {});
  await flushMetrics().catch(() => {});
}

/** Test seam: isolate module-global state between test cases. */
export function resetOtelForTests(): void {
  if (interval) clearInterval(interval);
  interval = undefined;
  spanBuffer = [];
  spanBufferBytes = 0;
  flushingSpans = false;
  flushingMetrics = false;
  stats.spansExported = 0;
  stats.spansDropped = 0;
  stats.metricsExported = 0;
  stats.metricsDropped = 0;
  ingestCyclesTotal = 0;
  ingestRowsTotal = 0;
  apiRequestsTotal = 0;
  fetchImpl = globalThis.fetch as unknown as OtelFetchLike;
}

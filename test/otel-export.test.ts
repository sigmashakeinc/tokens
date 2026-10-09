// otel-export.test.ts — unit tests for the sigmashake-tokens OTLP exporter.
//
// All network I/O is faked via setOtelFetch() — no real network access.
// Every test resets module-singleton state in beforeEach/afterEach.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  getOtelExportStats,
  isOtelExportEnabled,
  mapTokensRoute,
  OTEL_SERVICE_NAME,
  forceFlushOtelForTests,
  recordAggregationRefreshSpan,
  recordApiRequestSpan,
  recordIngestCycleSpan,
  resetOtelForTests,
  setOtelFetch,
  type OtelFetchResponse,
} from "../src/otel-export.ts";

const ENDPOINT = "http://otel.test:4318";

function okResponse(body = "{}"): OtelFetchResponse {
  return { ok: true, status: 200, text: async () => body };
}

beforeEach(() => {
  delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  resetOtelForTests();
});

afterEach(() => {
  resetOtelForTests();
  delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
});

describe("gating", () => {
  it("endpoint unset => isOtelExportEnabled is false, no fetch, stats stay zero", async () => {
    let fetchCalls = 0;
    setOtelFetch(async () => {
      fetchCalls++;
      return okResponse();
    });

    expect(isOtelExportEnabled()).toBe(false);

    recordApiRequestSpan({ startMs: 1, endMs: 2, status: 200, route: "/healthz" });
    recordIngestCycleSpan({ startMs: 1, endMs: 2, ok: true, filesScanned: 3, rowsIngested: 5 });
    recordAggregationRefreshSpan({ startMs: 1, endMs: 2, ok: true, kind: "usage" });
    await forceFlushOtelForTests();

    expect(fetchCalls).toBe(0);
    expect(getOtelExportStats()).toEqual({
      spansExported: 0,
      spansDropped: 0,
      metricsExported: 0,
      metricsDropped: 0,
    });
  });

  it("endpoint blank/whitespace => treated as unset", async () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = "   ";
    let fetchCalls = 0;
    setOtelFetch(async () => {
      fetchCalls++;
      return okResponse();
    });

    expect(isOtelExportEnabled()).toBe(false);
    recordApiRequestSpan({ startMs: 1, endMs: 2, status: 200, route: "/healthz" });
    await forceFlushOtelForTests();

    expect(fetchCalls).toBe(0);
  });
});

describe("happy path — spans", () => {
  it("posts a span batch to /v1/traces with the exact service.name mission-control expects", async () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = ENDPOINT;
    expect(isOtelExportEnabled()).toBe(true);
    expect(OTEL_SERVICE_NAME).toBe("tokens");

    const calls: Array<{ url: string; body: unknown }> = [];
    setOtelFetch(async (url, init) => {
      calls.push({ url, body: JSON.parse(init.body) });
      return okResponse();
    });

    recordIngestCycleSpan({ startMs: 1000, endMs: 1250, ok: true, filesScanned: 12, rowsIngested: 34 });
    await forceFlushOtelForTests();

    // Both /v1/traces (this span) and /v1/metrics (unconditional heartbeat)
    // post on the same flush tick — isolate the traces call.
    const tracesCall = calls.find((c) => c.url === `${ENDPOINT}/v1/traces`);
    expect(tracesCall).toBeDefined();
    const seenBody = tracesCall!.body;
    const resourceSpans = (seenBody as { resourceSpans: unknown[] }).resourceSpans;
    expect(resourceSpans).toHaveLength(1);
    const resource = (resourceSpans[0] as { resource: { attributes: Array<{ key: string; value: { stringValue?: string } }> } }).resource;
    expect(resource.attributes).toContainEqual({
      key: "service.name",
      value: { stringValue: "tokens" },
    });
    const spans = (resourceSpans[0] as { scopeSpans: Array<{ spans: unknown[] }> }).scopeSpans[0].spans;
    expect(spans).toHaveLength(1);
    const span = spans[0] as {
      name: string;
      traceId: string;
      spanId: string;
      startTimeUnixNano: string;
      endTimeUnixNano: string;
      status: { code: number };
      attributes: Array<{ key: string; value: Record<string, unknown> }>;
    };
    expect(span.name).toBe("ingest.cycle");
    expect(span.traceId).toMatch(/^[0-9a-f]{32}$/);
    expect(span.spanId).toMatch(/^[0-9a-f]{16}$/);
    expect(span.startTimeUnixNano).toBe("1000000000");
    expect(span.endTimeUnixNano).toBe("1250000000");
    expect(span.status.code).toBe(1); // OK
    expect(span.attributes).toContainEqual({ key: "filesScanned", value: { intValue: "12" } });
    expect(span.attributes).toContainEqual({ key: "rowsIngested", value: { intValue: "34" } });

    expect(getOtelExportStats().spansExported).toBe(1);
  });

  it("api.request span maps an unrecognized route through the allowlist to 'other'", async () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = ENDPOINT;
    const calls: Array<{ url: string; body: unknown }> = [];
    setOtelFetch(async (url, init) => {
      calls.push({ url, body: JSON.parse(init.body) });
      return okResponse();
    });

    recordApiRequestSpan({ startMs: 0, endMs: 10, status: 500, route: "/api/does-not-exist" });
    await forceFlushOtelForTests();

    const tracesCall = calls.find((c) => c.url === `${ENDPOINT}/v1/traces`);
    expect(tracesCall).toBeDefined();
    const resourceSpans = (tracesCall!.body as { resourceSpans: Array<{ scopeSpans: Array<{ spans: unknown[] }> }> })
      .resourceSpans;
    const span = resourceSpans[0].scopeSpans[0].spans[0] as {
      status: { code: number };
      attributes: Array<{ key: string; value: Record<string, unknown> }>;
    };
    expect(span.attributes).toContainEqual({ key: "route", value: { stringValue: "other" } });
    expect(span.status.code).toBe(2); // ERROR — 500 is not ok
  });

  it("mapTokensRoute is a pure allowlist projection", () => {
    expect(mapTokensRoute("/api/usage")).toBe("/api/usage");
    expect(mapTokensRoute("/healthz")).toBe("/healthz");
    expect(mapTokensRoute("static")).toBe("static");
    expect(mapTokensRoute("/anything/else")).toBe("other");
  });
});

describe("happy path — metrics", () => {
  it("posts an uptime gauge plus cumulative counters to /v1/metrics", async () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = ENDPOINT;
    let seenUrl = "";
    let seenBody: unknown;
    setOtelFetch(async (url, init) => {
      seenUrl = url;
      seenBody = JSON.parse(init.body);
      return okResponse();
    });

    // A span must be recorded first to start the flush timer/enable the
    // pipeline the same way the real ingest loop does on its first tick.
    recordIngestCycleSpan({ startMs: 0, endMs: 1, ok: true, filesScanned: 1, rowsIngested: 7 });
    recordApiRequestSpan({ startMs: 0, endMs: 1, status: 200, route: "/healthz" });
    await forceFlushOtelForTests();

    expect(seenUrl).toBe(`${ENDPOINT}/v1/metrics`);
    const resourceMetrics = (seenBody as { resourceMetrics: Array<{ scopeMetrics: Array<{ metrics: Array<Record<string, unknown>> }> }> })
      .resourceMetrics;
    const metrics = resourceMetrics[0].scopeMetrics[0].metrics;
    const byName = new Map(metrics.map((m) => [m.name as string, m]));

    const uptime = byName.get("tokens_uptime_seconds") as {
      gauge: { dataPoints: Array<{ asDouble: number }> };
    };
    expect(uptime).toBeDefined();
    expect(uptime.gauge.dataPoints[0].asDouble).toBeGreaterThanOrEqual(0);

    const cycles = byName.get("tokens_ingest_cycles_total") as {
      sum: { isMonotonic: boolean; dataPoints: Array<{ asInt: string }> };
    };
    expect(cycles.sum.isMonotonic).toBe(true);
    expect(cycles.sum.dataPoints[0].asInt).toBe("1");

    const rows = byName.get("tokens_ingest_rows_total") as { sum: { dataPoints: Array<{ asInt: string }> } };
    expect(rows.sum.dataPoints[0].asInt).toBe("7");

    const apiHits = byName.get("tokens_api_requests_total") as {
      sum: { dataPoints: Array<{ asInt: string }> };
    };
    expect(apiHits.sum.dataPoints[0].asInt).toBe("1");

    expect(getOtelExportStats().metricsExported).toBe(1);
  });
});

describe("fail-open", () => {
  it("a network error is swallowed — never throws, counts as dropped", async () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = ENDPOINT;
    setOtelFetch(async () => {
      throw new Error("simulated network failure");
    });

    recordIngestCycleSpan({ startMs: 0, endMs: 1, ok: true, filesScanned: 1, rowsIngested: 1 });
    await expect(forceFlushOtelForTests()).resolves.toBeUndefined();

    const stats = getOtelExportStats();
    expect(stats.spansDropped).toBe(1);
    expect(stats.metricsDropped).toBe(1);
    expect(stats.spansExported).toBe(0);
    expect(stats.metricsExported).toBe(0);
  });

  it("a non-2xx response is treated as a failure, never thrown", async () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = ENDPOINT;
    setOtelFetch(async () => ({ ok: false, status: 503, text: async () => "" }));

    recordApiRequestSpan({ startMs: 0, endMs: 1, status: 200, route: "/healthz" });
    await expect(forceFlushOtelForTests()).resolves.toBeUndefined();

    expect(getOtelExportStats().spansDropped).toBe(1);
  });

  it("record functions never throw even if called with pathological input", () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = ENDPOINT;
    setOtelFetch(async () => okResponse());
    expect(() =>
      recordApiRequestSpan({ startMs: Number.NaN, endMs: Number.NaN, status: -1, route: "" }),
    ).not.toThrow();
    expect(() =>
      recordIngestCycleSpan({ startMs: -5, endMs: -10, ok: false, filesScanned: -1, rowsIngested: -1 }),
    ).not.toThrow();
  });
});

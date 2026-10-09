// Live-update broadcaster + SSE handler.
// Server-Sent Events stream at /api/live; the ingestion loop calls broadcast()
// whenever new events land. Clients render KPI deltas without a full refetch.

import type { AuthoritativeTokenSnapshot, RateWindowBucket } from "./rate.ts";
import type { StoredEvent } from "./store.ts";

export interface LiveTotalsDelta {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
}

export interface LiveRecentEvent {
  ts: number;
  model: string;
  project: string;
  sessionId: string;
  totalTokens: number;
  cost: number;
}

/** The bounded portion of an ingest update that may cross a worker boundary. */
export interface LiveEventDelta {
  type: "events";
  newEvents: number;
  totalsDelta: LiveTotalsDelta;
  recent: LiveRecentEvent[];
  scannedAt: number;
}

export interface LiveUpdate extends LiveEventDelta {
  /** Additive — never renamed/removed. Rolling 60s throughput snapshot from
   *  src/rate.ts (computeRateSnapshot), computed from the STORE at broadcast
   *  time (not derived from this update's `recent`/`totalsDelta`, which are
   *  only an ingest-tick delta and can't represent a fixed wall-clock
   *  window). Absent when the caller didn't supply one. */
  rate?: RateWindowBucket;
  /** Full daily authority snapshot. Unlike totalsDelta, this may decrease
   * after exact-once reconciliation and carries its own reset contract. */
  snapshot?: AuthoritativeTokenSnapshot;
}

type Subscriber = (msg: LiveUpdate) => void;
const subscribers = new Set<Subscriber>();

export function subscribe(fn: Subscriber): () => void {
  subscribers.add(fn);
  return () => subscribers.delete(fn);
}

export function broadcast(update: LiveUpdate): void {
  for (const fn of subscribers) {
    try {
      fn(update);
    } catch {
      // Subscriber dropped; ignore.
    }
  }
}

export function buildUpdateFromEvents(
  newEvents: StoredEvent[],
  rate?: RateWindowBucket,
  snapshot?: AuthoritativeTokenSnapshot,
): LiveUpdate {
  const totalsDelta = {
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
  };
  for (const e of newEvents) {
    totalsDelta.inputTokens += e.inputTokens;
    totalsDelta.outputTokens += e.outputTokens;
    totalsDelta.cacheReadTokens += e.cacheReadTokens;
    totalsDelta.cacheWrite5mTokens += e.cacheWrite5mTokens;
    totalsDelta.cacheWrite1hTokens += e.cacheWrite1hTokens;
    totalsDelta.cost += e.cost;
  }
  const recent = newEvents.slice(-10).map((e) => ({
    ts: e.ts,
    model: e.model,
    project: e.project,
    sessionId: e.sessionId,
    totalTokens:
      e.inputTokens +
      e.outputTokens +
      e.cacheReadTokens +
      e.cacheWrite5mTokens +
      e.cacheWrite1hTokens,
    cost: e.cost,
  }));
  return {
    type: "events",
    newEvents: newEvents.length,
    totalsDelta,
    recent,
    scannedAt: Date.now(),
    ...(rate ? { rate } : {}),
    ...(snapshot ? { snapshot } : {}),
  };
}

/** Adds parent-owned authority to a compact worker result without expanding it. */
export function buildUpdateFromDelta(
  delta: LiveEventDelta,
  rate?: RateWindowBucket,
  snapshot?: AuthoritativeTokenSnapshot,
): LiveUpdate {
  return {
    ...delta,
    recent: delta.recent.slice(-10),
    ...(rate ? { rate } : {}),
    ...(snapshot ? { snapshot } : {}),
  };
}

// Bun ReadableStream-based SSE response. Keeps a heartbeat so reverse-proxies
// (none expected for 127.0.0.1, but still) don't kill idle connections.
export function sseResponse(initialSnapshot?: AuthoritativeTokenSnapshot): Response {
  let unsubscribe: (() => void) | null = null;
  let heartbeat: ReturnType<typeof setInterval> | null = null;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      const send = (event: string, data: unknown): void => {
        try {
          controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
        } catch {
          // Controller already closed.
        }
      };
      send("hello", { ok: true, ts: Date.now() });
      if (initialSnapshot) send("snapshot", initialSnapshot);
      unsubscribe = subscribe((msg) => send("update", msg));
      heartbeat = setInterval(() => send("ping", { ts: Date.now() }), 25_000);
    },
    cancel() {
      if (unsubscribe) unsubscribe();
      if (heartbeat) clearInterval(heartbeat);
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    },
  });
}

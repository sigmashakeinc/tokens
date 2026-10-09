// API: TokenStore HTTP-shaped responses round-trip through the live snapshot path.

import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { type StoredEvent, TokenStore } from "../../src/store.ts";
import {
  type AuthoritativeTokenSnapshot,
  buildAuthoritativeTokenSnapshot,
  claudeProviderRecordIdentity,
  dailyTokenWindow,
  isAuthoritativeTokenSnapshot,
  rolling24hTokenWindow,
  shouldResetAuthoritativeHud,
  shouldResetTokenSnapshot,
} from "../../src/token-usage-contract.ts";

test("selectEvents returns empty on a fresh store", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-api-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    const events = store.selectEvents();
    expect(Array.isArray(events)).toBe(true);
    expect(events.length).toBe(0);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

function event(overrides: Partial<StoredEvent>): StoredEvent {
  return {
    ts: 1783796400000,
    date: "2026-07-11",
    model: "claude-sonnet-4-6",
    project: "sigmashake-tokens",
    projectSlug: "-home-user-ssd-sigmashake-tokens",
    sessionId: "golden",
    inputTokens: 100,
    outputTokens: 40,
    cacheReadTokens: 500,
    cacheWrite5mTokens: 25,
    cacheWrite1hTokens: 10,
    cost: 0.007,
    filePath: "/fixture/claude.jsonl",
    lineOffset: 0,
    source: "claude",
    tokenClasses: {
      input: 100,
      cachedRead: 500,
      cacheWrite: 35,
      visibleOutput: 40,
      reasoningOutput: null,
      complete: false,
    },
    ...overrides,
  };
}

async function goldenSnapshot(): Promise<AuthoritativeTokenSnapshot> {
  const fixturePath = fileURLToPath(
    new URL("../fixtures/authoritative-token-snapshot.golden.json", import.meta.url),
  );
  const golden = JSON.parse(await readFile(fixturePath, "utf8")) as {
    snapshot: AuthoritativeTokenSnapshot;
  };
  return golden.snapshot;
}

test("TokenSource grok is a first-class snapshot provider", () => {
  const now = Date.parse("2026-08-29T18:00:00.000Z");
  const grokEvent = event({
    ts: now - 60_000,
    model: "grok-4.6",
    sessionId: "golden-grok",
    inputTokens: 800,
    outputTokens: 50,
    cacheReadTokens: 200,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
    filePath: "/fixture/grok/unified.jsonl",
    source: "grok",
    tokenClasses: {
      input: 800,
      cachedRead: 200,
      cacheWrite: null,
      visibleOutput: 40,
      reasoningOutput: 10,
      complete: false,
    },
  });
  const snapshot = buildAuthoritativeTokenSnapshot([grokEvent], {
    window: rolling24hTokenWindow(now),
    revision: 7,
    updatedAt: now,
    providerStates: [
      { source: "claude", available: false, complete: false },
      { source: "codex", available: false, complete: false },
      { source: "grok", available: true, complete: true },
    ],
  });
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(true);
  expect(snapshot.window.kind).toBe("rolling24h");
  expect(snapshot.providers.grok.models.map((row) => row.model)).toEqual(["grok-4.6"]);
  expect(snapshot.providers.grok.rawTotals.inputTokens).toBe(800);
  expect(snapshot.providers.grok.rawTotals.rawInputTokens).toBe(1000);
  expect(snapshot.providers.grok.rawTotals.cost).toBe(0);
  expect(snapshot.providers.grok.classes.reasoningOutput).toEqual({ tokens: 10, complete: true });
  expect(snapshot.hud.in).toEqual({ tokens: 800, complete: true });
});

test("dailyTokenWindow is LA-local, half-open, and DST-safe", () => {
  const spring = dailyTokenWindow(Date.parse("2026-03-08T19:00:00.000Z"));
  expect(spring.windowKey).toBe("2026-03-08");
  expect(spring.endMs - spring.startMs).toBe(23 * 60 * 60 * 1000);

  const fall = dailyTokenWindow(Date.parse("2026-11-01T20:00:00.000Z"));
  expect(fall.windowKey).toBe("2026-11-01");
  expect(fall.endMs - fall.startMs).toBe(25 * 60 * 60 * 1000);
});

test("shared Claude identity precedence collapses revisions without correlation drift", () => {
  const record = {
    type: "assistant",
    uuid: "uuid-1",
    requestId: "request-1",
    timestamp: "2026-07-11T10:00:00.000Z",
    message: {
      id: "response-1",
      role: "assistant",
      model: "claude-sonnet-4-6",
      content: "stable",
      usage: { input_tokens: 10, output_tokens: 20 },
    },
  };
  const uuid = claudeProviderRecordIdentity(record, { sessionId: "session-1" });
  const revised = claudeProviderRecordIdentity(
    {
      ...record,
      timestamp: "2026-07-11T10:00:01.000Z",
      message: {
        ...record.message,
        model: "claude-opus-4-8",
        usage: { input_tokens: 30, output_tokens: 40 },
      },
    },
    { sessionId: "session-1" },
  );
  expect(uuid.kind).toBe("uuid");
  expect(revised.canonicalIdentity).toBe(uuid.canonicalIdentity);
  expect(revised.immutablePayload).toBe(uuid.immutablePayload);

  const responseA = claudeProviderRecordIdentity(
    { ...record, uuid: undefined, message: { ...record.message, id: "response-a" } },
    { sessionId: "session-1" },
  );
  const responseB = claudeProviderRecordIdentity(
    { ...record, uuid: undefined, message: { ...record.message, id: "response-b" } },
    { sessionId: "session-1" },
  );
  expect(responseA.kind).toBe("response");
  expect(responseA.canonicalIdentity).not.toBe(responseB.canonicalIdentity);

  const requestA = claudeProviderRecordIdentity(
    { ...record, uuid: undefined, message: { ...record.message, id: undefined } },
    { sessionId: "session-1" },
  );
  const requestB = claudeProviderRecordIdentity(
    {
      ...record,
      uuid: undefined,
      timestamp: "2026-07-11T10:00:02.000Z",
      message: {
        ...record.message,
        id: undefined,
        usage: { input_tokens: 99, output_tokens: 88 },
      },
    },
    { sessionId: "session-1" },
  );
  expect(requestA.kind).toBe("request");
  expect(requestA.canonicalIdentity).toBe(requestB.canonicalIdentity);
});

test("authoritative snapshot matches the shared OBS golden contract", async () => {
  const fixturePath = fileURLToPath(
    new URL("../fixtures/authoritative-token-snapshot.golden.json", import.meta.url),
  );
  const golden = JSON.parse(await readFile(fixturePath, "utf8")) as {
    snapshot: AuthoritativeTokenSnapshot;
  };
  expect(isAuthoritativeTokenSnapshot(golden.snapshot)).toBe(true);
  const codex = event({
    model: "gpt-5.5-cyber-preview",
    sessionId: "golden-codex",
    inputTokens: 200,
    outputTokens: 60,
    cacheReadTokens: 300,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0.0053,
    filePath: "/fixture/codex.jsonl",
    source: "codex",
    tokenClasses: {
      input: 200,
      cachedRead: 300,
      cacheWrite: null,
      visibleOutput: 40,
      reasoningOutput: 20,
      complete: false,
    },
  });
  const snapshot = buildAuthoritativeTokenSnapshot([event({}), codex], {
    window: dailyTokenWindow(1783796400000),
    revision: 42,
    updatedAt: 1783796400000,
    providerStates: [
      { source: "claude", available: true, complete: true, updatedAt: 1 },
      { source: "codex", available: true, complete: true, updatedAt: 1 },
    ],
  });
  expect(snapshot).toEqual(golden.snapshot);
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(true);
  expect(snapshot.hud.in).toEqual({ tokens: 300, complete: true });
  expect(snapshot.hud.out).toEqual({ tokens: 80, complete: true });
  expect(snapshot.totalCost).toBe(snapshot.rawTotals.cost);
  expect(snapshot.calls).toBe(snapshot.rawTotals.messages);
  expect(snapshot.providers.claude.models.map((row) => row.model)).toEqual(["claude-sonnet-4-6"]);
  expect(snapshot.providers.codex.models.map((row) => row.model)).toEqual([
    "gpt-5.5-cyber-preview",
  ]);
});

test("builder cost totals remain self-valid across large mixed-model histories", () => {
  const events: StoredEvent[] = [];
  for (let index = 0; index < 1_000; index++) {
    events.push(
      event({ model: "a-large", lineOffset: index * 2, cost: 100_000_000 }),
      event({ model: "z-small", lineOffset: index * 2 + 1, cost: 0.00001 }),
    );
  }

  const snapshot = buildAuthoritativeTokenSnapshot(events, {
    window: dailyTokenWindow(1783796400000),
    revision: 1,
    updatedAt: 1783796400000,
    providerStates: [
      { source: "claude", available: true, complete: true, updatedAt: 1 },
      { source: "codex", available: false, complete: false, updatedAt: 1 },
    ],
  });

  expect(snapshot.providers.claude.models.map((model) => model.model)).toEqual([
    "a-large",
    "z-small",
  ]);
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(true);
});

test("builder normalizes model whitespace and gates completeness on availability", () => {
  const snapshot = buildAuthoritativeTokenSnapshot(
    [
      event({ model: "\tclaude-sonnet-4-6\u00a0", cost: 0.1 }),
      event({ model: "claude-sonnet-4-6", lineOffset: 1, cost: 0.2 }),
    ],
    {
      window: dailyTokenWindow(1783796400000),
      revision: 1,
      updatedAt: 1783796400000,
      providerStates: [
        { source: "claude", available: false, complete: true, updatedAt: 1 },
        { source: "codex", available: false, complete: false, updatedAt: 1 },
      ],
    },
  );

  expect(snapshot.providers.claude.discoveryComplete).toBe(false);
  expect(snapshot.providers.claude.models).toHaveLength(1);
  expect(snapshot.providers.claude.models[0]?.model).toBe("claude-sonnet-4-6");
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(true);
});

test("validator rejects an LA windowKey mismatch", async () => {
  const snapshot = structuredClone(await goldenSnapshot());
  snapshot.window.windowKey = "2026-07-12";
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(false);
});

test("validator rejects ISO text and millisecond-bound mismatches", async () => {
  const badIso = structuredClone(await goldenSnapshot());
  badIso.window.start = "2026-07-11T07:00:00.001Z";
  expect(isAuthoritativeTokenSnapshot(badIso)).toBe(false);

  const badBounds = structuredClone(await goldenSnapshot());
  badBounds.window.endMs += 1;
  badBounds.window.end = new Date(badBounds.window.endMs).toISOString();
  expect(isAuthoritativeTokenSnapshot(badBounds)).toBe(false);
});

test("validator rejects a 24-hour bound on a 23-hour DST day", async () => {
  const snapshot = structuredClone(await goldenSnapshot());
  snapshot.window = dailyTokenWindow(Date.parse("2026-03-08T19:00:00.000Z"));
  snapshot.window.endMs = snapshot.window.startMs + 24 * 60 * 60 * 1000;
  snapshot.window.end = new Date(snapshot.window.endMs).toISOString();
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(false);
});

test("validator rejects top-level raw totals that do not compose from providers", async () => {
  const snapshot = structuredClone(await goldenSnapshot());
  snapshot.rawTotals.inputTokens += 1;
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(false);

  const harmlessFloat = structuredClone(await goldenSnapshot());
  harmlessFloat.rawTotals.cost =
    harmlessFloat.providers.claude.rawTotals.cost + harmlessFloat.providers.codex.rawTotals.cost;
  expect(isAuthoritativeTokenSnapshot(harmlessFloat)).toBe(true);

  const materialCost = structuredClone(await goldenSnapshot());
  materialCost.rawTotals.cost += 0.001;
  expect(isAuthoritativeTokenSnapshot(materialCost)).toBe(false);
});

test("validator enforces cost/call aliases and per-model provider composition", async () => {
  const badCostAlias = structuredClone(await goldenSnapshot());
  badCostAlias.totalCost += 0.001;
  expect(isAuthoritativeTokenSnapshot(badCostAlias)).toBe(false);

  const badCallsAlias = structuredClone(await goldenSnapshot());
  badCallsAlias.calls += 1;
  expect(isAuthoritativeTokenSnapshot(badCallsAlias)).toBe(false);

  const badModelTotals = structuredClone(await goldenSnapshot());
  badModelTotals.providers.claude.models[0]!.rawTotals.inputTokens += 1;
  expect(isAuthoritativeTokenSnapshot(badModelTotals)).toBe(false);

  const duplicateModel = structuredClone(await goldenSnapshot());
  duplicateModel.providers.claude.models.push(
    structuredClone(duplicateModel.providers.claude.models[0]!),
  );
  expect(isAuthoritativeTokenSnapshot(duplicateModel)).toBe(false);

  const emptyModel = structuredClone(await goldenSnapshot());
  emptyModel.providers.codex.models[0]!.model = "";
  expect(isAuthoritativeTokenSnapshot(emptyModel)).toBe(false);
});

test("missing ingestion model is retained in an explicit unknown bucket", () => {
  const { model: _model, ...source } = event({});
  expect(_model).toBe("claude-sonnet-4-6");
  const snapshot = buildAuthoritativeTokenSnapshot([source], {
    window: dailyTokenWindow(1783796400000),
    revision: 1,
    updatedAt: 1783796400000,
    providerStates: [
      { source: "claude", available: true, complete: true, updatedAt: 1 },
      { source: "codex", available: false, complete: false, updatedAt: 1 },
    ],
  });
  expect(snapshot.providers.claude.models[0]?.model).toBe("unknown");
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(true);
});

test("validator rejects provider class and completeness composition mismatches", async () => {
  const badClass = structuredClone(await goldenSnapshot());
  badClass.providers.codex.classes.input.tokens = 201;
  expect(isAuthoritativeTokenSnapshot(badClass)).toBe(false);

  const badCompleteness = structuredClone(await goldenSnapshot());
  badCompleteness.providers.codex.discoveryComplete = false;
  expect(isAuthoritativeTokenSnapshot(badCompleteness)).toBe(false);
});

test("validator rejects HUD values that differ from composed classes", async () => {
  const snapshot = structuredClone(await goldenSnapshot());
  snapshot.hud.out.tokens = 81;
  expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(false);
});

test("HUD reset predicate covers window, completeness, and authoritative decrease", async () => {
  const fixturePath = fileURLToPath(
    new URL("../fixtures/authoritative-token-snapshot.golden.json", import.meta.url),
  );
  const golden = JSON.parse(await readFile(fixturePath, "utf8")) as {
    snapshot: AuthoritativeTokenSnapshot;
  };
  const same = structuredClone(golden.snapshot);
  expect(shouldResetAuthoritativeHud(golden.snapshot, same)).toBe(false);

  const nextWindow = structuredClone(golden.snapshot);
  nextWindow.window.windowKey = "2026-07-12";
  expect(shouldResetAuthoritativeHud(golden.snapshot, nextWindow)).toBe(true);

  const incomplete = structuredClone(golden.snapshot);
  incomplete.classes.input = { tokens: null, complete: false };
  expect(shouldResetAuthoritativeHud(golden.snapshot, incomplete)).toBe(true);

  const decreased = structuredClone(golden.snapshot);
  decreased.classes.input.tokens = 299;
  expect(shouldResetAuthoritativeHud(golden.snapshot, decreased)).toBe(true);

  const invalid = structuredClone(golden.snapshot);
  invalid.hud.in.tokens = 0;
  expect(isAuthoritativeTokenSnapshot(invalid)).toBe(false);
});

test("snapshot reset compares provider availability, discovery, classes, and raw totals", async () => {
  const baseline = await goldenSnapshot();

  const availability = structuredClone(baseline);
  availability.providers.codex.available = false;
  expect(shouldResetTokenSnapshot(baseline, availability)).toBe(true);

  const discovery = structuredClone(baseline);
  discovery.providers.codex.discoveryComplete = false;
  expect(shouldResetTokenSnapshot(baseline, discovery)).toBe(true);

  const completeness = structuredClone(baseline);
  completeness.providers.codex.classes.input = { tokens: null, complete: false };
  expect(shouldResetTokenSnapshot(baseline, completeness)).toBe(true);

  const rawDecrease = structuredClone(baseline);
  rawDecrease.providers.claude.rawTotals.cacheReadTokens -= 1;
  expect(shouldResetTokenSnapshot(baseline, rawDecrease)).toBe(true);

  const modelIdentity = structuredClone(baseline);
  modelIdentity.providers.claude.models[0]!.model = "claude-opus-4-7";
  expect(shouldResetTokenSnapshot(baseline, modelIdentity)).toBe(true);

  const modelClassDecrease = structuredClone(baseline);
  modelClassDecrease.providers.codex.models[0]!.classes.visibleOutput.tokens! -= 1;
  expect(shouldResetTokenSnapshot(baseline, modelClassDecrease)).toBe(true);

  const modelRawDecrease = structuredClone(baseline);
  modelRawDecrease.providers.codex.models[0]!.rawTotals.messages -= 1;
  expect(shouldResetTokenSnapshot(baseline, modelRawDecrease)).toBe(true);

  const costAliasDecrease = structuredClone(baseline);
  costAliasDecrease.totalCost -= 0.001;
  expect(shouldResetTokenSnapshot(baseline, costAliasDecrease)).toBe(true);

  const callsAliasDecrease = structuredClone(baseline);
  callsAliasDecrease.calls -= 1;
  expect(shouldResetTokenSnapshot(baseline, callsAliasDecrease)).toBe(true);
});

test("known provider decrease resets even while combined classes are incomplete", async () => {
  const previous = structuredClone(await goldenSnapshot());
  previous.providers.codex.available = false;
  previous.providers.codex.discoveryComplete = false;
  previous.providers.codex.classes = {
    input: { tokens: null, complete: false },
    cachedRead: { tokens: null, complete: false },
    cacheWrite: { tokens: null, complete: false },
    visibleOutput: { tokens: null, complete: false },
    reasoningOutput: { tokens: null, complete: false },
    complete: false,
  };
  previous.classes = structuredClone(previous.providers.codex.classes);
  previous.hud.in = { tokens: null, complete: false };
  previous.hud.out = { tokens: null, complete: false };

  const current = structuredClone(previous);
  current.providers.claude.classes.input.tokens = 99;
  current.providers.claude.rawTotals.inputTokens = 99;
  current.providers.claude.rawTotals.rawInputTokens = 634;
  expect(previous.classes.input.complete).toBe(false);
  expect(current.classes.input.complete).toBe(false);
  expect(shouldResetTokenSnapshot(previous, current)).toBe(true);
});

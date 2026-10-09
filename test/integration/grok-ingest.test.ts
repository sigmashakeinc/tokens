import { expect, spyOn, test } from "bun:test";
import { cp, mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { _resetScannerStateForTest, ingestOnce } from "../../src/scanner.ts";
import {
  type CapturedEvent,
  type CapturedFileCommit,
  type ScanCommit,
  type StoredEvent,
  TokenStore,
} from "../../src/store.ts";
import { rolling24hTokenWindow } from "../../src/token-usage-contract.ts";

const FIXTURE_GROK = fileURLToPath(new URL("../fixtures/grok", import.meta.url));
const NOW = Date.parse("2026-08-29T18:00:00.000Z");

test("grok ingest maps billed turns, ignores updates.jsonl, and rolling24h drops stale turns", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-grok-"));
  const projectsDir = join(root, "projects");
  const codexSessionsDir = join(root, "codex-sessions");
  const grokRoot = join(root, "grok");
  await mkdir(projectsDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });
  await cp(FIXTURE_GROK, grokRoot, { recursive: true });

  process.env.CLAUDE_PROJECTS_DIR = projectsDir;
  process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
  process.env.GROK_LOGS_DIR = join(grokRoot, "logs");
  process.env.GROK_SESSIONS_DIR = join(grokRoot, "sessions");
  _resetScannerStateForTest();
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    await ingestOnce(store);
    const events = store.selectEvents();
    expect(events).toHaveLength(2);
    expect(events.every((event) => event.source === "grok")).toBe(true);
    expect(events.reduce((sum, event) => sum + event.cost, 0)).toBe(0);
    expect(
      events.some((event) => event.inputTokens === 37332 || event.outputTokens === 37332),
    ).toBe(false);

    const fresh = events.find(
      (event) => event.sessionId === "11111111-1111-4111-8111-111111111111",
    );
    expect(fresh?.model).toBe("grok-4.6");
    expect(fresh?.inputTokens).toBe(800);
    expect(fresh?.cacheReadTokens).toBe(200);
    expect(fresh?.outputTokens).toBe(50);
    expect(fresh?.tokenClasses).toEqual({
      input: 800,
      cachedRead: 200,
      cacheWrite: null,
      visibleOutput: 40,
      reasoningOutput: 10,
      complete: false,
    });

    const daily = store.buildAuthoritativeTokenSnapshot(NOW);
    expect(daily.window.kind).toBe("daily");
    const grokDaily = daily.providers.grok;
    expect(grokDaily.available).toBe(true);
    expect(grokDaily.discoveryComplete).toBe(true);
    expect(grokDaily.models.map((row) => row.model)).toEqual(["grok-4.6"]);
    expect(grokDaily.rawTotals.cost).toBe(0);
    expect(grokDaily.rawTotals.rawInputTokens).toBe(1000);
    expect(grokDaily.classes.input.tokens).toBe(800);

    const rolling = store.buildAuthoritativeTokenSnapshot(NOW, rolling24hTokenWindow(NOW));
    expect(rolling.window.kind).toBe("rolling24h");
    expect(rolling.providers.grok.rawTotals.messages).toBe(1);
    expect(rolling.providers.grok.rawTotals.inputTokens).toBe(800);
    expect(rolling.providers.grok.models).toHaveLength(1);
    expect(rolling.providers.grok.models[0]?.rawTotals.messages).toBe(1);
  } finally {
    store.close();
    _resetScannerStateForTest();
    await rm(root, { recursive: true, force: true });
    delete process.env.CLAUDE_PROJECTS_DIR;
    delete process.env.CODEX_SESSIONS_DIR;
    delete process.env.GROK_LOGS_DIR;
    delete process.env.GROK_SESSIONS_DIR;
  }
});

function grokStoredEvent(overrides: Partial<StoredEvent> = {}): StoredEvent {
  return {
    ts: Date.parse("2026-08-29T17:00:01.000Z"),
    date: "2026-08-29",
    model: "grok-4.6",
    project: "grok",
    projectSlug: "grok",
    sessionId: "11111111-1111-4111-8111-111111111111",
    inputTokens: 800,
    outputTokens: 50,
    cacheReadTokens: 200,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
    filePath: "/tmp/unified.jsonl",
    lineOffset: 10912,
    source: "grok",
    tokenClasses: {
      input: 800,
      cachedRead: 200,
      cacheWrite: null,
      visibleOutput: 40,
      reasoningOutput: 10,
      complete: false,
    },
    ...overrides,
  };
}

function grokCaptured(event: StoredEvent, overrides: Partial<CapturedEvent> = {}): CapturedEvent {
  return {
    event,
    canonicalKey: "c3d4c7b5fddf8053c262925c595bceec4dc4d5faf9a1ce1b61313589e43260ba",
    rawRecordSha256: "raw-a",
    immutableIdentity: null,
    logicalMessageKey: "grok:a",
    byteStart: 10900,
    byteEnd: 11000,
    lineNumber: 12,
    ...overrides,
  };
}

function grokBatch(
  events: CapturedEvent[],
  lastByteOffset: number,
  lastLineNumber: number,
): CapturedFileCommit {
  return {
    state: {
      generationKey: "grok:1:2:unified",
      source: "grok",
      device: "1",
      inode: "2",
      representativePath: "/tmp/unified.jsonl",
      capturedSize: lastByteOffset,
      mtimeMs: 10,
      mtimeNs: "10000000",
      ctimeNs: "10000000",
      lastByteOffset,
      lastLineNumber,
      committedPrefixSha256: "prefix",
      codexStateJson: null,
    },
    aliases: ["/tmp/unified.jsonl"],
    events,
  };
}

function grokScan(batch: CapturedFileCommit): ScanCommit {
  return {
    batches: [batch],
    aliases: [
      {
        path: "/tmp/unified.jsonl",
        generationKey: batch.state.generationKey,
        source: "grok",
      },
    ],
    providers: [
      { source: "claude", available: false, complete: true },
      { source: "codex", available: false, complete: true },
      { source: "grok", available: true, complete: true },
    ],
  };
}

test("drifted grok offset/class metadata skips the occurrence without aborting ingest", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-grok-canonical-skip-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  const sibling = grokStoredEvent({
    sessionId: "22222222-2222-4222-8222-222222222222",
    inputTokens: 400,
    outputTokens: 20,
    cacheReadTokens: 0,
    tokenClasses: {
      input: 400,
      cachedRead: 0,
      cacheWrite: null,
      visibleOutput: 15,
      reasoningOutput: 5,
      complete: false,
    },
  });
  const siblingCaptured = grokCaptured(sibling, {
    canonicalKey: "sibling-canonical",
    rawRecordSha256: "raw-b",
    logicalMessageKey: "grok:b",
    byteStart: 12000,
    byteEnd: 12100,
    lineNumber: 13,
  });
  const healthy = grokStoredEvent({
    sessionId: "33333333-3333-4333-8333-333333333333",
    inputTokens: 100,
    outputTokens: 10,
    cacheReadTokens: 0,
    lineOffset: 14000,
    tokenClasses: {
      input: 100,
      cachedRead: 0,
      cacheWrite: null,
      visibleOutput: 10,
      reasoningOutput: null,
      complete: false,
    },
  });
  try {
    store.commitScan(
      grokScan(grokBatch([grokCaptured(grokStoredEvent()), siblingCaptured], 12100, 14)),
    );
    expect(store.selectEvents()).toHaveLength(2);

    const classDrifted = grokCaptured(
      grokStoredEvent({
        lineOffset: 10940,
        tokenClasses: {
          input: 790,
          cachedRead: 190,
          cacheWrite: null,
          visibleOutput: 39,
          reasoningOutput: 11,
          complete: true,
        },
      }),
    );
    expect(() =>
      store.commitScan(
        grokScan(
          grokBatch(
            [
              classDrifted,
              siblingCaptured,
              grokCaptured(healthy, {
                canonicalKey: "healthy-canonical",
                rawRecordSha256: "raw-c",
                logicalMessageKey: "grok:c",
                byteStart: 14000,
                byteEnd: 14100,
                lineNumber: 20,
              }),
            ],
            14100,
            21,
          ),
        ),
      ),
    ).not.toThrow();

    const afterClassDrift = store.selectEvents();
    expect(afterClassDrift).toHaveLength(3);
    const original = afterClassDrift.find(
      (event) => event.sessionId === grokStoredEvent().sessionId,
    );
    expect(original?.model).toBe("grok-4.6");
    expect(original?.inputTokens).toBe(800);
    expect(original?.lineOffset).toBe(10912);
    expect(original?.tokenClasses).toEqual({
      input: 800,
      cachedRead: 200,
      cacheWrite: null,
      visibleOutput: 40,
      reasoningOutput: 10,
      complete: false,
    });
    expect(afterClassDrift.some((event) => event.sessionId === healthy.sessionId)).toBe(true);

    const identityDrifted = grokCaptured(
      grokStoredEvent({
        model: "unknown",
        inputTokens: 1,
        outputTokens: 1,
        cacheReadTokens: 0,
        lineOffset: 10912,
        tokenClasses: {
          input: 1,
          cachedRead: 0,
          cacheWrite: null,
          visibleOutput: 1,
          reasoningOutput: null,
          complete: false,
        },
      }),
    );
    const extra = grokStoredEvent({
      sessionId: "44444444-4444-4444-8444-444444444444",
      inputTokens: 50,
      outputTokens: 5,
      cacheReadTokens: 0,
      lineOffset: 15000,
      tokenClasses: {
        input: 50,
        cachedRead: 0,
        cacheWrite: null,
        visibleOutput: 5,
        reasoningOutput: null,
        complete: false,
      },
    });
    const conflictLog = spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(() =>
        store.commitScan(
          grokScan(
            grokBatch(
              [
                identityDrifted,
                grokCaptured(extra, {
                  canonicalKey: "extra-canonical",
                  rawRecordSha256: "raw-d",
                  logicalMessageKey: "grok:d",
                  byteStart: 15000,
                  byteEnd: 15100,
                  lineNumber: 21,
                }),
              ],
              15100,
              22,
            ),
          ),
        ),
      ).not.toThrow();
      expect(conflictLog).toHaveBeenCalled();
      expect(
        conflictLog.mock.calls.some((args) =>
          String(args[0]).includes(
            "canonical token record conflict skipped: c3d4c7b5fddf8053c262925c595bceec4dc4d5faf9a1ce1b61313589e43260ba",
          ),
        ),
      ).toBe(true);
    } finally {
      conflictLog.mockRestore();
    }

    const events = store.selectEvents();
    expect(events).toHaveLength(4);
    const kept = events.find((event) => event.sessionId === grokStoredEvent().sessionId);
    expect(kept?.model).toBe("grok-4.6");
    expect(kept?.inputTokens).toBe(800);
    expect(kept?.cost).toBe(0);
    expect(events.some((event) => event.sessionId === extra.sessionId)).toBe(true);
    expect(store.getProviderStates().find((state) => state.source === "grok")?.complete).toBe(true);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

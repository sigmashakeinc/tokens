import { Database } from "bun:sqlite";
import { describe, expect, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
import { appendFile, link, mkdir, mkdtemp, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { costFor, priceFor, priceForWithSource } from "../src/pricing.ts";
import {
  _codexSessionTimestampFromUuidForTest,
  _metadataFilesystemTrustedForTest,
  _parseCodexExplicitTimestampForTest,
  _resetScannerStateForTest,
  _scannerCodexCacheKeysForTest,
  _scannerCodexCacheSizeForTest,
  aggregateEvents,
  ingestOnce,
} from "../src/scanner.ts";
import {
  type CapturedFileCommit,
  type CapturedFileCommitResult,
  CLAUDE_USAGE_PARSER_VERSION,
  CODEX_USAGE_PARSER_VERSION,
  type ScanCommit,
  type StoredEvent,
  TokenStore,
} from "../src/store.ts";
import {
  _resetUsageAggregationWorkerForTest,
  _setUsageAggregationControlSpawnForTest,
  _setUsageAggregationLauncherForTest,
  _setUsageAggregationSpawnForTest,
  aggregateUsageOffThread,
  shutdownUsageAggregationWorker,
  type UsageAggregationTestControlSubprocess,
  type UsageAggregationTestSubprocess,
} from "../src/usage-aggregation-worker.ts";

/** Minimal StoredEvent fixture builder for pure aggregateEvents tests — no
 * JSONL/ingest pipeline involved. */
function mkEvent(overrides: Partial<StoredEvent> = {}): StoredEvent {
  return {
    ts: Date.now(),
    date: "2026-06-01",
    model: "claude-sonnet-4-6",
    project: "sigmashake-tokens",
    projectSlug: "-home-user-sigmashake-tokens",
    sessionId: "session-a",
    inputTokens: 100,
    outputTokens: 50,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0.01,
    filePath: "/srv/example/.claude/projects/foo/bar.jsonl",
    lineOffset: 0,
    source: "claude",
    ...overrides,
  };
}

class FailFirstCommitStore extends TokenStore {
  private failNextCommit = true;

  failNextCapturedCommit(): void {
    this.failNextCommit = true;
  }

  override commitCapturedFile(batch: CapturedFileCommit): CapturedFileCommitResult {
    if (this.failNextCommit) {
      this.failNextCommit = false;
      throw new Error("injected captured-file commit failure");
    }
    return super.commitCapturedFile(batch);
  }
}

class FailLaterCommitStore extends TokenStore {
  private commitCount = 0;
  private failAt = 2;

  override commitCapturedFile(batch: CapturedFileCommit): CapturedFileCommitResult {
    this.commitCount += 1;
    if (this.commitCount === this.failAt) {
      this.failAt = -1;
      throw new Error("injected later captured-file commit failure");
    }
    return super.commitCapturedFile(batch);
  }
}

class ArmedFailLaterCommitStore extends TokenStore {
  private commitCount = 0;
  private failAt: number | null = null;

  armCapturedCommitFailure(failAt: number): void {
    this.commitCount = 0;
    this.failAt = failAt;
  }

  override commitCapturedFile(batch: CapturedFileCommit): CapturedFileCommitResult {
    this.commitCount += 1;
    if (this.failAt !== null && this.commitCount === this.failAt) {
      this.failAt = null;
      throw new Error("injected armed captured-file commit failure");
    }
    return super.commitCapturedFile(batch);
  }
}

interface LegacySeed {
  path: string;
  input: number;
  output: number;
  cost: number;
  size: number;
  offset: number;
  line: number;
  mtimeMs: number;
  cursor?: boolean;
}

function seedLegacyV3Database(dbPath: string, seeds: LegacySeed[]): void {
  const db = new Database(dbPath);
  db.run(
    "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, date TEXT NOT NULL, model TEXT NOT NULL, project TEXT NOT NULL, project_slug TEXT NOT NULL, session_id TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, cache_read_tokens INTEGER NOT NULL, cache_write_5m_tokens INTEGER NOT NULL, cache_write_1h_tokens INTEGER NOT NULL, cost REAL NOT NULL, file_path TEXT NOT NULL, line_offset INTEGER NOT NULL, dedupe_key TEXT, source TEXT NOT NULL DEFAULT 'claude', UNIQUE(file_path, line_offset))",
  );
  db.run(
    "CREATE UNIQUE INDEX idx_events_dedupe_key ON events(dedupe_key) WHERE dedupe_key IS NOT NULL",
  );
  db.run(
    "CREATE TABLE file_state (path TEXT PRIMARY KEY, mtime_ms INTEGER NOT NULL, size INTEGER NOT NULL, last_byte_offset INTEGER NOT NULL, last_line_number INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
  );
  const insertEvent = db.prepare(
    "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, dedupe_key, source) VALUES (?, '2026-05-17', 'claude-sonnet-4-6', 'ssd-demo', '-home-user-ssd-demo', ?, ?, ?, 0, 0, 0, ?, ?, 0, NULL, 'claude')",
  );
  const insertCursor = db.prepare(
    "INSERT INTO file_state (path, mtime_ms, size, last_byte_offset, last_line_number, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
  );
  for (const seed of seeds) {
    const sessionId =
      seed.path
        .split("/")
        .pop()
        ?.replace(/\.jsonl$/, "") ?? "legacy";
    insertEvent.run(
      Date.parse("2026-05-17T10:00:00.000Z"),
      sessionId,
      seed.input,
      seed.output,
      seed.cost,
      seed.path,
    );
    if (seed.cursor !== false) {
      insertCursor.run(seed.path, seed.mtimeMs, seed.size, seed.offset, seed.line, Date.now());
    }
  }
  db.run("PRAGMA user_version = 3");
  db.close();
}

function legacyPendingReason(dbPath: string, path: string): string | null {
  const db = new Database(dbPath, { readonly: true });
  try {
    const row = db
      .query("SELECT reason FROM legacy_reconciliation_pending WHERE path = ?")
      .get(path) as { reason: string } | null;
    return row?.reason ?? null;
  } finally {
    db.close();
  }
}

describe("pricing", () => {
  test("opus-4-7 resolves to opus tier", () => {
    const p = priceFor("claude-opus-4-7");
    expect(p.input).toBe(15);
    expect(p.output).toBe(75);
  });

  test("sonnet-4-6 resolves to sonnet tier", () => {
    const p = priceFor("claude-sonnet-4-6");
    expect(p.input).toBe(3);
    expect(p.output).toBe(15);
  });

  test("haiku-4-5 resolves to haiku-4 tier", () => {
    const p = priceFor("claude-haiku-4-5-20251001");
    expect(p.input).toBe(1);
    expect(p.output).toBe(5);
  });

  test("gpt and codex models resolve to explicit OpenAI tiers", () => {
    expect(priceFor("gpt-5.5").input).toBe(5);
    expect(priceFor("gpt-5.5-cyber-preview").output).toBe(30);
    expect(priceFor("gpt-5.4-mini").cacheRead).toBe(0.075);
    expect(priceFor("gpt-5.3-codex-spark").input).toBe(1.75);
    expect(priceFor("gpt-5.2-codex").output).toBe(14);
    expect(priceFor("gpt-5.1-codex-max").input).toBe(1.75);
    expect(priceFor("gpt-5").input).toBe(2.5);
  });

  test("unknown model falls back to sonnet tier", () => {
    const p = priceFor("future-model-x");
    expect(p.input).toBe(3);
  });

  // ─── Claude 5 family (Fable 5 / Sonnet 5) — 2026-07-01 pricing add ───────

  test("fable-5 resolves from the table at the Mythos-tier rate", () => {
    const p = priceForWithSource("claude-fable-5");
    expect(p.source).toBe("table");
    expect(p.price.input).toBe(10);
    expect(p.price.output).toBe(50);
    expect(p.price.cacheRead).toBe(1);
    expect(p.price.cacheWrite5m).toBe(12.5);
    expect(p.price.cacheWrite1h).toBe(20);
  });

  test("fable-5 dated/suffixed model IDs still resolve from the table", () => {
    const p = priceForWithSource("claude-fable-5-20260101");
    expect(p.source).toBe("table");
    expect(p.price.input).toBe(10);
    expect(p.price.output).toBe(50);
  });

  test("sonnet-5 resolves from the table at the introductory rate", () => {
    const p = priceForWithSource("claude-sonnet-5");
    expect(p.source).toBe("table");
    expect(p.price.input).toBe(2);
    expect(p.price.output).toBe(10);
    expect(p.price.cacheRead).toBe(0.2);
    expect(p.price.cacheWrite5m).toBe(2.5);
    expect(p.price.cacheWrite1h).toBe(4);
  });

  test("sonnet-5 dated/suffixed model IDs still resolve from the table", () => {
    const p = priceForWithSource("claude-sonnet-5-20260615");
    expect(p.source).toBe("table");
    expect(p.price.input).toBe(2);
    expect(p.price.output).toBe(10);
  });

  test("sonnet-5 does not collide with the sonnet-4 entry (longest-prefix)", () => {
    // claude-sonnet-4-6 must keep resolving to the sonnet-4 rate, not sonnet-5.
    expect(priceForWithSource("claude-sonnet-4-6").price.input).toBe(3);
    expect(priceForWithSource("claude-sonnet-5").price.input).toBe(2);
  });

  test('unknown model reports source "fallback" alongside the fallback price', () => {
    const p = priceForWithSource("claude-unknown-9");
    expect(p.source).toBe("fallback");
    expect(p.price.input).toBe(3);
    expect(p.price.output).toBe(15);
  });

  test("priceFor() is unchanged — thin wrapper over priceForWithSource().price", () => {
    for (const model of [
      "claude-fable-5",
      "claude-sonnet-5",
      "claude-opus-4-7",
      "claude-unknown-9",
    ]) {
      expect(priceFor(model)).toEqual(priceForWithSource(model).price);
    }
  });

  test("regression: existing longest-prefix resolution is unaffected by the new entries", () => {
    expect(priceForWithSource("gpt-5.5").source).toBe("table");
    expect(priceFor("gpt-5.5").input).toBe(5);
    expect(priceForWithSource("claude-opus-4-7").source).toBe("table");
    expect(priceFor("claude-opus-4-7").input).toBe(15);
    expect(priceForWithSource("claude-haiku-4-5-20251001").source).toBe("table");
    expect(priceFor("claude-haiku-4-5-20251001").input).toBe(1);
  });

  test("cost math matches manual calculation", () => {
    const c = costFor("claude-opus-4-7", {
      inputTokens: 1_000_000,
      outputTokens: 0,
      cacheReadTokens: 0,
      cacheWrite5mTokens: 0,
      cacheWrite1hTokens: 0,
    });
    expect(c).toBeCloseTo(15);
  });
});

describe("aggregateEvents — pricing provenance in the payload", () => {
  test("Aggregations.pricing carries source per model: table for known, fallback for unknown", () => {
    const events = [
      mkEvent({ model: "claude-fable-5-20260101", sessionId: "s1" }),
      mkEvent({ model: "claude-sonnet-5-20260615", sessionId: "s2" }),
      mkEvent({ model: "claude-unknown-9", sessionId: "s3" }),
    ];
    const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });

    expect(agg.pricing["claude-fable-5-20260101"]?.source).toBe("table");
    expect(agg.pricing["claude-fable-5-20260101"]?.input).toBe(10);

    expect(agg.pricing["claude-sonnet-5-20260615"]?.source).toBe("table");
    expect(agg.pricing["claude-sonnet-5-20260615"]?.input).toBe(2);

    // Unknown model must still surface a positive fallback price, but with
    // source "fallback" — never silently indistinguishable from a table hit.
    expect(agg.pricing["claude-unknown-9"]?.source).toBe("fallback");
    expect(agg.pricing["claude-unknown-9"]?.input).toBe(3);
    expect(agg.pricing["claude-unknown-9"]?.output).toBe(15);
  });
});

async function withFixture(
  fn: (ctx: {
    projectsDir: string;
    codexSessionsDir: string;
    store: TokenStore;
    storePath: string;
  }) => Promise<void>,
  storeFactory: (storePath: string) => TokenStore = (storePath) => new TokenStore(storePath),
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "tokens-test-"));
  const projectsDir = join(root, "projects");
  const codexSessionsDir = join(root, "codex-sessions");
  const storePath = join(root, "tokens.sqlite");
  await mkdir(projectsDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });
  process.env.CLAUDE_PROJECTS_DIR = projectsDir;
  process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
  const store = storeFactory(storePath);
  try {
    await fn({ projectsDir, codexSessionsDir, store, storePath });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
    delete process.env.CLAUDE_PROJECTS_DIR;
    delete process.env.CODEX_SESSIONS_DIR;
  }
}

function expectStructurallyHealthyWindowIncomplete(
  store: TokenStore,
  source: "claude" | "codex",
): void {
  expect(store.getProviderStates().find((state) => state.source === source)?.complete).toBe(true);
  expect(
    store.buildAuthoritativeTokenSnapshot(Date.now()).providers[source].discoveryComplete,
  ).toBe(false);
}

function singleCodexReplacementRows(sessionId: string): string {
  return `${[
    JSON.stringify({
      type: "session_meta",
      timestamp: "2026-07-15T16:00:00.000Z",
      payload: {
        id: sessionId,
        session_id: sessionId,
        forked_from_id: null,
        cwd: "/srv/example/sigmashake-tokens",
        model: "gpt-5.5-codex",
      },
    }),
    JSON.stringify({
      type: "event_msg",
      timestamp: "2026-07-15T16:00:01.000Z",
      payload: {
        type: "token_count",
        info: {
          total_token_usage: {
            input_tokens: 100,
            cached_input_tokens: 20,
            output_tokens: 10,
          },
        },
      },
    }),
  ].join("\n")}\n`;
}

function codexThreadSourceRows(sessionId: string, threadSource: unknown = "user"): string {
  return `${[
    JSON.stringify({
      type: "session_meta",
      timestamp: "2026-07-15T16:00:00.000Z",
      payload: {
        id: sessionId,
        session_id: sessionId,
        forked_from_id: null,
        thread_source: threadSource,
        model: "gpt-5.5-codex",
      },
    }),
    JSON.stringify({
      type: "event_msg",
      timestamp: "2026-07-15T16:00:01.000Z",
      payload: {
        type: "token_count",
        info: {
          total_token_usage: {
            input_tokens: 10,
            cached_input_tokens: 2,
            output_tokens: 1,
          },
        },
      },
    }),
  ].join("\n")}\n`;
}

function asOldThreadSourceAmbiguity(checkpointJson: string): Record<string, unknown> {
  const state = JSON.parse(checkpointJson) as Record<string, unknown>;
  delete state.threadSourceRoleRepaired;
  state.phase = "ambiguous";
  state.usageComplete = false;
  state.totals = null;
  state.boundaryByte = null;
  state.boundaryLine = null;
  state.firstLiveRowPending = false;
  state.tokenCountRows = 1;
  state.billedUsageRows = 0;
  state.inheritedSuppressedRows = 0;
  state.ambiguousQuarantinedRows = 1;
  state.zeroNoopRows = 0;
  state.unrecoverableResetRows = 0;
  state.invalidUsageRows = 0;
  return state;
}

function codexDepthOneLineageRows(
  rootId: string,
  childId: string,
  includeCopiedRoot = true,
): string {
  const total = (timestamp: string, input: number) =>
    JSON.stringify({
      type: "event_msg",
      timestamp,
      payload: {
        type: "token_count",
        info: {
          total_token_usage: { input_tokens: input, cached_input_tokens: 0, output_tokens: 1 },
        },
      },
    });
  const childMeta = JSON.stringify({
    type: "session_meta",
    timestamp: "2026-07-15T16:00:00.000Z",
    payload: {
      id: childId,
      session_id: rootId,
      thread_source: "subagent",
      parent_thread_id: rootId,
      source: { subagent: { thread_spawn: { parent_thread_id: rootId, depth: 1 } } },
    },
  });
  const rootMeta = JSON.stringify({
    type: "session_meta",
    timestamp: "2026-07-15T15:00:00.000Z",
    payload: {
      id: rootId,
      session_id: rootId,
      thread_source: "user",
    },
  });
  return `${[
    childMeta,
    ...(includeCopiedRoot ? [rootMeta] : []),
    total("2026-07-15T16:00:01.000Z", 10),
    JSON.stringify({
      type: "inter_agent_communication_metadata",
      timestamp: "2026-07-15T16:00:02.000Z",
      payload: { trigger_turn: true },
    }),
    total("2026-07-15T16:00:03.000Z", 20),
  ].join("\n")}\n`;
}

function codexDepthTwoLineageRows(rootId: string, parentId: string, childId: string): string {
  const depthOneRows = codexDepthOneLineageRows(rootId, parentId).trimEnd().split("\n");
  const childMeta = JSON.stringify({
    type: "session_meta",
    timestamp: "2026-07-15T17:00:00.000Z",
    payload: {
      id: childId,
      session_id: rootId,
      forked_from_id: parentId,
      thread_source: "subagent",
      parent_thread_id: parentId,
      source: { subagent: { thread_spawn: { parent_thread_id: parentId, depth: 2 } } },
    },
  });
  const copiedParent = JSON.parse(depthOneRows[0]!) as { payload: Record<string, unknown> };
  copiedParent.payload.forked_from_id = rootId;
  return `${[childMeta, JSON.stringify(copiedParent), ...depthOneRows.slice(1)].join("\n")}\n`;
}

function asOldCodexLineageAmbiguity(checkpointJson: string): Record<string, unknown> {
  const state = JSON.parse(checkpointJson) as Record<string, unknown>;
  const tokenCountRows = state.tokenCountRows as number;
  delete state.codexLineageRepaired;
  state.phase = "ambiguous";
  state.usageComplete = false;
  state.totals = null;
  state.boundaryByte = null;
  state.boundaryLine = null;
  state.firstLiveRowPending = false;
  state.billedUsageRows = 0;
  state.inheritedSuppressedRows = 0;
  state.ambiguousQuarantinedRows = tokenCountRows;
  state.zeroNoopRows = 0;
  state.unrecoverableResetRows = 0;
  state.invalidUsageRows = 0;
  return state;
}

function asFailedDepthOneDirectAmbiguity(checkpointJson: string): Record<string, unknown> {
  const state = asOldCodexLineageAmbiguity(checkpointJson);
  state.codexLineageRepaired = true;
  delete state.depthOneDirectLineageRepaired;
  state.lineageDepth = 1;
  state.lineageReplayStage = "await-root";
  return state;
}

function asFailedObservedLineageShape(
  checkpointJson: string,
  shape: "unclassified-depth-one" | "stranded-depth-two",
): Record<string, unknown> {
  const state = JSON.parse(checkpointJson) as Record<string, unknown>;
  const tokenCountRows = state.tokenCountRows as number;
  delete state.codexLineageShapeRepaired;
  state.phase = "ambiguous";
  state.usageComplete = false;
  state.totals = null;
  state.boundaryByte = null;
  state.boundaryLine = null;
  state.firstLiveRowPending = false;
  state.billedUsageRows = 0;
  state.inheritedSuppressedRows = 0;
  state.ambiguousQuarantinedRows = tokenCountRows;
  state.zeroNoopRows = 0;
  state.unrecoverableResetRows = 0;
  state.invalidUsageRows = 0;
  if (shape === "unclassified-depth-one") {
    state.childSessionId = null;
    state.forkParentId = null;
    state.lineageDepth = null;
    state.rootSessionId = null;
    state.lineageReplayStage = null;
  } else {
    delete state.depthOneDirectLineageRepaired;
    state.lineageReplayStage = "await-parent";
  }
  return state;
}

function asDeployedMinimalV3(checkpointJson: string): Record<string, unknown> {
  const state = JSON.parse(checkpointJson) as Record<string, unknown>;
  return {
    sessionId: state.sessionId,
    model: state.model,
    cwd: state.cwd,
    project: state.project,
    projectSlug: state.projectSlug,
    totals: state.totals,
    sessionTimestampMs: state.sessionTimestampMs,
    usageParserVersion: 3,
    usageComplete: false,
    phase: "ambiguous",
    firstLiveRowPending: false,
  };
}

function asDeployedAllAmbiguous(
  checkpointJson: string,
  shape: "depth-two-without-ancestors" | "depth-one-ready",
): Record<string, unknown> {
  const state = JSON.parse(checkpointJson) as Record<string, unknown>;
  const rows = state.tokenCountRows as number;
  delete state.codexDeployedCheckpointRepaired;
  state.phase = "ambiguous";
  state.usageComplete = false;
  state.totals = null;
  state.boundaryByte = null;
  state.boundaryLine = null;
  state.firstLiveRowPending = false;
  state.billedUsageRows = 0;
  state.inheritedSuppressedRows = 0;
  state.ambiguousQuarantinedRows = rows;
  state.zeroNoopRows = 0;
  state.unrecoverableResetRows = 0;
  state.invalidUsageRows = 0;
  if (shape === "depth-one-ready") delete state.codexLineageShapeRepaired;
  return state;
}

function withDepthOneMatchingFork(rows: string, rootId: string): string {
  const lines = rows.trimEnd().split("\n");
  const child = JSON.parse(lines[0]!) as { payload: Record<string, unknown> };
  child.payload.forked_from_id = rootId;
  lines[0] = JSON.stringify(child);
  return `${lines.join("\n")}\n`;
}

function codexDepthTwoWithoutAncestorsRows(
  rootId: string,
  parentId: string,
  childId: string,
): string {
  const rows = codexDepthTwoLineageRows(rootId, parentId, childId).trimEnd().split("\n");
  return `${[rows[0], ...rows.slice(3)].join("\n")}\n`;
}

function prepareCodexReplacement(
  store: TokenStore,
  storePath: string,
  path: string,
  sourceState: NonNullable<ReturnType<TokenStore["getLatestSourceFile"]>>,
): { batch: CapturedFileCommit; scan: ScanCommit } {
  const originalCheckpoint = JSON.parse(sourceState.codexStateJson!) as Record<string, unknown>;
  const staleCheckpoint = { ...originalCheckpoint, usageParserVersion: 2 };
  let occurrence: {
    canonical_key: string;
    byte_start: number;
    byte_end: number;
    line_number: number;
    raw_record_sha256: string;
    logical_message_key: string | null;
  };
  const db = new Database(storePath);
  try {
    db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
      JSON.stringify(staleCheckpoint),
      sourceState.generationKey,
    );
    occurrence = db
      .query(
        "SELECT canonical_key, byte_start, byte_end, line_number, raw_record_sha256, logical_message_key FROM event_occurrences WHERE generation_key = ? LIMIT 1",
      )
      .get(sourceState.generationKey) as typeof occurrence;
  } finally {
    db.close();
  }
  if (!occurrence) throw new Error("replacement fixture occurrence is missing");
  const expectedState = store.getLatestSourceFile("codex", sourceState.device, sourceState.inode)!;
  const event = store.selectEvents().find((candidate) => candidate.filePath === path);
  if (!event) throw new Error("replacement fixture event is missing");
  const batch: CapturedFileCommit = {
    state: {
      ...expectedState,
      codexStateJson: JSON.stringify(originalCheckpoint),
    },
    aliases: [path],
    events: [
      {
        event,
        canonicalKey: occurrence.canonical_key,
        rawRecordSha256: occurrence.raw_record_sha256,
        immutableIdentity: null,
        immutablePayloadKey: null,
        logicalMessageKey: occurrence.logical_message_key,
        collapseRevisions: false,
        byteStart: occurrence.byte_start,
        byteEnd: occurrence.byte_end,
        lineNumber: occurrence.line_number,
      },
    ],
    replacement: { expectedState, expectedAliases: [path] },
  };
  return {
    batch,
    scan: {
      batches: [batch],
      aliases: [{ path, generationKey: sourceState.generationKey, source: "codex" }],
      providers: store
        .getProviderStates()
        .map(({ source, available, complete }) => ({ source, available, complete })),
      sourceSnapshotRevision: store.getRevision(),
      unchangedBatchIndexes: [],
    },
  };
}

function replacementAuthoritySnapshot(
  store: TokenStore,
  storePath: string,
  generationKey: string,
): unknown {
  const db = new Database(storePath, { readonly: true });
  try {
    return {
      revision: store.getRevision(),
      source: db
        .query("SELECT generation_key, codex_state_json FROM source_files WHERE generation_key = ?")
        .get(generationKey),
      events: db
        .query(
          "SELECT id, session_id, input_tokens, output_tokens, cache_read_tokens, file_path, line_offset, canonical_key FROM events ORDER BY id",
        )
        .all(),
      occurrences: db
        .query(
          "SELECT event_id, canonical_key, generation_key, observed_path, byte_start, byte_end, line_number, raw_record_sha256 FROM event_occurrences ORDER BY event_id, observed_path",
        )
        .all(),
    };
  } finally {
    db.close();
  }
}

async function withLegacyStoreRowsForReingest(
  fn: (ctx: {
    projectsDir: string;
    codexSessionsDir: string;
    dbPath: string;
    missingPath: string;
  }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "tokens-reingest-"));
  const projectsDir = join(root, "projects");
  const codexSessionsDir = join(root, "codex-sessions");
  await mkdir(projectsDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });

  const projectDir = join(projectsDir, "-home-user-ssd-sigmashake-demo");
  await mkdir(projectDir, { recursive: true });

  const presentA = join(projectDir, "a.jsonl");
  const presentB = join(projectDir, "b.jsonl");
  const missingPath = join(projectsDir, "-home-user-missing", "missing.jsonl");

  const requestId = "req-live-reingest";
  const rowsA = [
    JSON.stringify({
      type: "assistant",
      requestId,
      timestamp: "2026-05-17T10:00:00.000Z",
      cwd: "/srv/example/demo",
      message: {
        role: "assistant",
        model: "claude-sonnet-4-6",
        usage: { input_tokens: 10, output_tokens: 20 },
      },
    }),
    JSON.stringify({
      type: "assistant",
      requestId,
      timestamp: "2026-05-17T10:00:01.000Z",
      cwd: "/srv/example/demo",
      message: {
        role: "assistant",
        model: "claude-sonnet-4-6",
        usage: { input_tokens: 30, output_tokens: 40 },
      },
    }),
  ];
  const rowsB = [
    JSON.stringify({
      type: "assistant",
      requestId,
      timestamp: "2026-05-17T11:00:00.000Z",
      cwd: "/srv/example/demo",
      message: {
        role: "assistant",
        model: "claude-sonnet-4-6",
        usage: { input_tokens: 50, output_tokens: 60 },
      },
    }),
  ];
  await writeFile(presentA, `${rowsA.join("\n")}\n`);
  await writeFile(presentB, `${rowsB.join("\n")}\n`);

  const dbPath = join(root, "tokens.sqlite");
  const raw = new Database(dbPath);
  raw.exec(
    "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, date TEXT NOT NULL, model TEXT NOT NULL, project TEXT NOT NULL, project_slug TEXT NOT NULL, session_id TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, cache_read_tokens INTEGER NOT NULL, cache_write_5m_tokens INTEGER NOT NULL, cache_write_1h_tokens INTEGER NOT NULL, cost REAL NOT NULL, file_path TEXT NOT NULL, line_offset INTEGER NOT NULL, source TEXT NOT NULL DEFAULT 'claude', UNIQUE(file_path, line_offset))",
  );
  raw.exec(
    "CREATE TABLE file_state (path TEXT PRIMARY KEY, mtime_ms INTEGER NOT NULL, size INTEGER NOT NULL, last_byte_offset INTEGER NOT NULL, last_line_number INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
  );
  raw.run("PRAGMA user_version = 2");
  const insert = raw.prepare(
    "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  );
  insert.run(
    1_700_000_000_000,
    "2026-05-17",
    "claude-sonnet-4-6",
    "sigmashake-tokens",
    "-home-user-ssd-sigmashake-demo",
    "legacy-session-a",
    10,
    20,
    0,
    0,
    0,
    0.1,
    presentA,
    0,
    "claude",
  );
  insert.run(
    1_700_000_001_000,
    "2026-05-17",
    "claude-sonnet-4-6",
    "sigmashake-tokens",
    "-home-user-ssd-sigmashake-demo",
    "legacy-session-a",
    30,
    40,
    0,
    0,
    0,
    0.2,
    presentA,
    1,
    "claude",
  );
  insert.run(
    1_700_000_002_000,
    "2026-05-17",
    "claude-sonnet-4-6",
    "sigmashake-tokens",
    "-home-user-ssd-sigmashake-demo",
    "legacy-session-b",
    50,
    60,
    0,
    0,
    0,
    0.3,
    presentB,
    0,
    "claude",
  );
  insert.run(
    1_700_000_003_000,
    "2026-05-17",
    "claude-sonnet-4-6",
    "sigmashake-tokens",
    "-home-user-ssd-sigmashake-demo",
    "legacy-session-missing",
    100,
    200,
    0,
    0,
    0,
    1,
    missingPath,
    0,
    "claude",
  );
  raw.close();

  const prevProjects = process.env.CLAUDE_PROJECTS_DIR;
  const prevCodex = process.env.CODEX_SESSIONS_DIR;
  process.env.CLAUDE_PROJECTS_DIR = projectsDir;
  process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
  try {
    await fn({ projectsDir, codexSessionsDir, dbPath, missingPath });
  } finally {
    if (prevProjects === undefined) {
      delete process.env.CLAUDE_PROJECTS_DIR;
    } else {
      process.env.CLAUDE_PROJECTS_DIR = prevProjects;
    }
    if (prevCodex === undefined) {
      delete process.env.CODEX_SESSIONS_DIR;
    } else {
      process.env.CODEX_SESSIONS_DIR = prevCodex;
    }
    await rm(root, { recursive: true, force: true });
  }
}

async function withWalRowsOnlyReingestFixture(
  fn: (ctx: {
    projectsDir: string;
    codexSessionsDir: string;
    dbPath: string;
    missingPath: string;
  }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "tokens-reingest-wal-"));
  const projectsDir = join(root, "projects");
  const codexSessionsDir = join(root, "codex-sessions");
  await mkdir(projectsDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });

  const missingDir = join(projectsDir, "-home-user-missing");
  await mkdir(missingDir, { recursive: true });
  const missingPath = join(missingDir, "missing.jsonl");

  const dbPath = join(root, "tokens.sqlite");
  const store = new TokenStore(dbPath);
  store.close();

  const raw = new Database(dbPath);
  raw.exec("PRAGMA journal_mode = WAL");
  raw.exec("PRAGMA wal_autocheckpoint = 0");
  const insert = raw.prepare(
    "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  );
  insert.run(
    1_700_000_010_000,
    "2026-05-17",
    "claude-sonnet-4-6",
    "sigmashake-tokens",
    "-home-user-ssd-sigmashake-demo",
    "wal-session",
    7,
    8,
    0,
    0,
    0,
    0.15,
    missingPath,
    0,
    "claude",
  );
  raw.close();

  const prevProjects = process.env.CLAUDE_PROJECTS_DIR;
  const prevCodex = process.env.CODEX_SESSIONS_DIR;
  process.env.CLAUDE_PROJECTS_DIR = projectsDir;
  process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
  try {
    await fn({ projectsDir, codexSessionsDir, dbPath, missingPath });
  } finally {
    if (prevProjects === undefined) {
      delete process.env.CLAUDE_PROJECTS_DIR;
    } else {
      process.env.CLAUDE_PROJECTS_DIR = prevProjects;
    }
    if (prevCodex === undefined) {
      delete process.env.CODEX_SESSIONS_DIR;
    } else {
      process.env.CODEX_SESSIONS_DIR = prevCodex;
    }
    await rm(root, { recursive: true, force: true });
  }
}

describe("scanner", () => {
  test("Codex UUIDv7 timestamps require canonical version and RFC variant bits", () => {
    const timestampMs = Date.parse("2026-07-05T11:00:00.000Z");
    const prefix = timestampMs.toString(16).padStart(12, "0");
    const valid = `${prefix.slice(0, 8)}-${prefix.slice(8)}-7000-8000-000000000000`;
    expect(_codexSessionTimestampFromUuidForTest(valid)).toBe(timestampMs);
    expect(_codexSessionTimestampFromUuidForTest(valid.toUpperCase())).toBe(timestampMs);
    for (const invalid of [
      valid.replace("-7", "-6"),
      valid.replace("-8000-", "-7000-"),
      valid.replaceAll("-", ""),
      `${valid}0`,
      "not-a-uuid",
      null,
    ]) {
      expect(_codexSessionTimestampFromUuidForTest(invalid)).toBeNull();
    }
  });

  test("Codex explicit timestamps require a valid date and numeric offset or Z", () => {
    for (const [value, expected] of [
      ["2026-07-05T11:00:00.123Z", Date.parse("2026-07-05T11:00:00.123Z")],
      ["2026-07-05T04:00:00-07:00", Date.parse("2026-07-05T11:00:00Z")],
      ["2026-07-05T18:30:00+07:30", Date.parse("2026-07-05T11:00:00Z")],
    ] as const) {
      expect(_parseCodexExplicitTimestampForTest(value)).toBe(expected);
    }
    for (const invalid of [
      "2026-07-05T11:00:00",
      "2026-02-30T11:00:00Z",
      "2026-07-05T24:00:00Z",
      "2026-07-05T11:00:00+24:00",
      "2026-07-05T11:00:00+07:60",
      "not-a-date",
    ]) {
      expect(_parseCodexExplicitTimestampForTest(invalid)).toBeNull();
    }
  });

  test("readable v3 state is purged then rebuilt from zero without cost inflation", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-cursor-migrate-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const sessionPath = join(projectDir, "legacy-session.jsonl");
    const row = JSON.stringify({
      type: "assistant",
      timestamp: "2026-05-17T10:00:00.000Z",
      cwd: "/srv/example/demo",
      message: {
        role: "assistant",
        model: "claude-sonnet-4-6",
        usage: { input_tokens: 10, output_tokens: 20 },
      },
    });
    const content = `${row}\n`;
    await writeFile(sessionPath, content);
    const sourceStat = await stat(sessionPath);
    const dbPath = join(root, "tokens.sqlite");
    const raw = new Database(dbPath);
    raw.run(
      "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, date TEXT NOT NULL, model TEXT NOT NULL, project TEXT NOT NULL, project_slug TEXT NOT NULL, session_id TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, cache_read_tokens INTEGER NOT NULL, cache_write_5m_tokens INTEGER NOT NULL, cache_write_1h_tokens INTEGER NOT NULL, cost REAL NOT NULL, file_path TEXT NOT NULL, line_offset INTEGER NOT NULL, dedupe_key TEXT, source TEXT NOT NULL DEFAULT 'claude', UNIQUE(file_path, line_offset))",
    );
    raw.run(
      "CREATE UNIQUE INDEX idx_events_dedupe_key ON events(dedupe_key) WHERE dedupe_key IS NOT NULL",
    );
    raw.run(
      "CREATE TABLE file_state (path TEXT PRIMARY KEY, mtime_ms INTEGER NOT NULL, size INTEGER NOT NULL, last_byte_offset INTEGER NOT NULL, last_line_number INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
    );
    const legacyCost = costFor("claude-sonnet-4-6", {
      inputTokens: 10,
      outputTokens: 20,
      cacheReadTokens: 0,
      cacheWrite5mTokens: 0,
      cacheWrite1hTokens: 0,
    });
    raw
      .prepare(
        "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, dedupe_key, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'claude')",
      )
      .run(
        Date.parse("2026-05-17T10:00:00.000Z"),
        "2026-05-17",
        "claude-sonnet-4-6",
        "ssd-demo",
        "-home-user-ssd-demo",
        "legacy-session",
        10,
        20,
        0,
        0,
        0,
        legacyCost,
        sessionPath,
        0,
      );
    raw
      .prepare(
        "INSERT INTO file_state (path, mtime_ms, size, last_byte_offset, last_line_number, updated_at) VALUES (?, ?, ?, ?, 1, ?)",
      )
      .run(
        sessionPath,
        sourceStat.mtimeMs,
        Buffer.byteLength(content),
        Buffer.byteLength(content),
        Date.now(),
      );
    raw.run("PRAGMA user_version = 3");
    raw.close();

    const previousClaude = process.env.CLAUDE_PROJECTS_DIR;
    const previousCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new TokenStore(dbPath);
    try {
      expect(store.selectEvents()).toEqual([]);
      const rescan = await ingestOnce(store);
      const after = store.selectEvents();
      expect(rescan.newEvents).toBe(1);
      expect(after).toHaveLength(1);
      expect(after[0]?.cost).toBe(legacyCost);
      expect(after[0]?.inputTokens).toBe(10);
      expect(after[0]?.outputTokens).toBe(20);
    } finally {
      store.close();
      if (previousClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = previousClaude;
      if (previousCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = previousCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("v4 source rows are verified once, upgraded with nanoseconds, and fast after reopen", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-ns-migrate-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    const sessionPath = join(projectDir, "legacy-v4.jsonl");
    const storePath = join(root, "tokens.sqlite");
    const previousClaude = process.env.CLAUDE_PROJECTS_DIR;
    const previousCodex = process.env.CODEX_SESSIONS_DIR;
    let activeStore: TokenStore | null = null;
    try {
      await mkdir(projectDir, { recursive: true });
      await mkdir(codexSessionsDir, { recursive: true });
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "assistant",
          timestamp: "2026-05-17T10:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20 },
          },
        })}\n`,
      );
      process.env.CLAUDE_PROJECTS_DIR = projectsDir;
      process.env.CODEX_SESSIONS_DIR = codexSessionsDir;

      activeStore = new TokenStore(storePath);
      await ingestOnce(activeStore, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath, { bigint: true });
      const device = sourceStat.dev.toString();
      const inode = sourceStat.ino.toString();
      expect(activeStore.getLatestSourceFile("claude", device, inode)?.mtimeNs).toBe(
        sourceStat.mtimeNs.toString(),
      );
      expect(activeStore.getLatestSourceFile("claude", device, inode)?.ctimeNs).toBe(
        sourceStat.ctimeNs.toString(),
      );
      activeStore.close();
      activeStore = null;

      // Recreate the exact v4 shape: the idempotent v5 migration must add both
      // nullable TEXT columns without trusting an unverifiable checkpoint.
      const legacy = new Database(storePath);
      try {
        legacy.run("ALTER TABLE source_files DROP COLUMN mtime_ns");
        legacy.run("ALTER TABLE source_files DROP COLUMN ctime_ns");
        legacy.run("PRAGMA user_version = 4");
      } finally {
        legacy.close();
      }

      activeStore = new TokenStore(storePath);
      expect(activeStore.getLatestSourceFile("claude", device, inode)?.mtimeNs).toBeNull();
      expect(activeStore.getLatestSourceFile("claude", device, inode)?.ctimeNs).toBeNull();
      let capturedReads = 0;
      const upgraded = await ingestOnce(activeStore, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(upgraded.newEvents).toBe(0);
      expect(upgraded.mutated).toBe(true);
      expect(activeStore.getLatestSourceFile("claude", device, inode)?.mtimeNs).toBe(
        sourceStat.mtimeNs.toString(),
      );
      expect(activeStore.getLatestSourceFile("claude", device, inode)?.ctimeNs).toBe(
        sourceStat.ctimeNs.toString(),
      );
      activeStore.close();
      activeStore = null;

      activeStore = new TokenStore(storePath);
      capturedReads = 0;
      const reopenedNoop = await ingestOnce(activeStore, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(reopenedNoop.newEvents).toBe(0);
      expect(reopenedNoop.mutated).toBe(false);

      const inspected = new Database(storePath);
      try {
        const version = (inspected.query("PRAGMA user_version").get() as { user_version: number })
          .user_version;
        const metadataColumns = (
          inspected.query("PRAGMA table_info(source_files)").all() as Array<{ name: string }>
        ).filter((column) => column.name === "mtime_ns" || column.name === "ctime_ns");
        expect(version).toBe(5);
        expect(metadataColumns.map((column) => column.name).sort()).toEqual([
          "ctime_ns",
          "mtime_ns",
        ]);
      } finally {
        inspected.close();
      }
    } finally {
      activeStore?.close();
      if (previousClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = previousClaude;
      if (previousCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = previousCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("repairs a pre-release user_version=5 ctime_ms schema independently of version", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-prerelease-v5-"));
    const storePath = join(root, "tokens.sqlite");
    try {
      new TokenStore(storePath).close();
      const prerelease = new Database(storePath);
      try {
        prerelease.run("ALTER TABLE source_files DROP COLUMN mtime_ns");
        prerelease.run("ALTER TABLE source_files DROP COLUMN ctime_ns");
        prerelease.run("ALTER TABLE source_files ADD COLUMN ctime_ms REAL");
        prerelease.run("PRAGMA user_version = 5");
      } finally {
        prerelease.close();
      }

      new TokenStore(storePath).close();
      new TokenStore(storePath).close();
      const inspected = new Database(storePath);
      try {
        const columns = inspected.query("PRAGMA table_info(source_files)").all() as Array<{
          name: string;
          type: string;
        }>;
        expect(columns.filter((column) => column.name === "mtime_ns")).toEqual([
          expect.objectContaining({ type: "TEXT" }),
        ]);
        expect(columns.filter((column) => column.name === "ctime_ns")).toEqual([
          expect.objectContaining({ type: "TEXT" }),
        ]);
        // The unused pre-release artifact is tolerated; correctness never
        // depends on DROP COLUMN support during service startup.
        expect(columns.filter((column) => column.name === "ctime_ms")).toHaveLength(1);
        expect(
          (inspected.query("PRAGMA user_version").get() as { user_version: number }).user_version,
        ).toBe(5);
      } finally {
        inspected.close();
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("concurrent v5 migration adds nanosecond columns exactly once", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-concurrent-v5-"));
    const storePath = join(root, "tokens.sqlite");
    try {
      new TokenStore(storePath).close();
      const legacy = new Database(storePath);
      try {
        legacy.run("ALTER TABLE source_files DROP COLUMN mtime_ns");
        legacy.run("ALTER TABLE source_files DROP COLUMN ctime_ns");
        legacy.run("PRAGMA user_version = 4");
      } finally {
        legacy.close();
      }

      const storeModule = new URL("../src/store.ts", import.meta.url).href;
      const program = `import {TokenStore} from ${JSON.stringify(storeModule)}; const store = new TokenStore(${JSON.stringify(storePath)}); store.close();`;
      const processes = Array.from({ length: 4 }, () =>
        Bun.spawn([process.execPath, "-e", program], { stdout: "pipe", stderr: "pipe" }),
      );
      const results = await Promise.all(
        processes.map(async (child) => ({
          exitCode: await child.exited,
          stderr: await new Response(child.stderr).text(),
        })),
      );
      expect(results).toEqual(Array.from({ length: 4 }, () => ({ exitCode: 0, stderr: "" })));

      new TokenStore(storePath).close();
      const inspected = new Database(storePath);
      try {
        const columns = inspected.query("PRAGMA table_info(source_files)").all() as Array<{
          name: string;
        }>;
        expect(columns.filter((column) => column.name === "mtime_ns")).toHaveLength(1);
        expect(columns.filter((column) => column.name === "ctime_ns")).toHaveLength(1);
      } finally {
        inspected.close();
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("readable event-only rewritten history rebuilds from current content", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-legacy-rewrite-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const sessionPath = join(projectDir, "rewritten.jsonl");
    const current = `${JSON.stringify({
      type: "assistant",
      timestamp: "2026-05-17T11:00:00.000Z",
      cwd: "/srv/example/demo",
      message: {
        role: "assistant",
        model: "claude-sonnet-4-6",
        usage: { input_tokens: 30, output_tokens: 40 },
      },
    })}\n`;
    await writeFile(sessionPath, current);
    const currentStat = await stat(sessionPath);
    const dbPath = join(root, "tokens.sqlite");
    seedLegacyV3Database(dbPath, [
      {
        path: sessionPath,
        input: 10,
        output: 20,
        cost: 0.25,
        size: Buffer.byteLength(current),
        offset: Buffer.byteLength(current),
        line: 1,
        mtimeMs: currentStat.mtimeMs,
        cursor: false,
      },
    ]);
    const oldClaude = process.env.CLAUDE_PROJECTS_DIR;
    const oldCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new TokenStore(dbPath);
    try {
      expect(store.selectEvents()).toEqual([]);
      const rebuilt = await ingestOnce(store);
      expect(rebuilt.newEvents).toBe(1);
      const event = store.selectEvents()[0];
      expect(event?.inputTokens).toBe(30);
      expect(event?.outputTokens).toBe(40);
      expect(event?.cost).not.toBe(0.25);
    } finally {
      store.close();
      if (oldClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = oldClaude;
      if (oldCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = oldCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("hardlinked legacy cursors purge as one identity and rebuild once", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-legacy-hardlink-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const pathA = join(projectDir, "hardlink-a.jsonl");
    const pathB = join(projectDir, "hardlink-b.jsonl");
    const current = `${JSON.stringify({
      type: "assistant",
      timestamp: "2026-05-17T11:00:00.000Z",
      cwd: "/srv/example/demo",
      message: {
        role: "assistant",
        model: "claude-sonnet-4-6",
        usage: { input_tokens: 50, output_tokens: 60 },
      },
    })}\n`;
    await writeFile(pathA, current);
    await link(pathA, pathB);
    const currentStat = await stat(pathA);
    const dbPath = join(root, "tokens.sqlite");
    seedLegacyV3Database(dbPath, [
      {
        path: pathA,
        input: 1,
        output: 2,
        cost: 0.1,
        size: 5,
        offset: 5,
        line: 1,
        mtimeMs: currentStat.mtimeMs,
      },
      {
        path: pathB,
        input: 3,
        output: 4,
        cost: 0.2,
        size: Buffer.byteLength(current),
        offset: Buffer.byteLength(current),
        line: 9,
        mtimeMs: currentStat.mtimeMs,
      },
    ]);
    const oldClaude = process.env.CLAUDE_PROJECTS_DIR;
    const oldCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new TokenStore(dbPath);
    try {
      expect(store.selectEvents()).toEqual([]);
      expect(store.getFileState(pathA)).toBeNull();
      expect(store.getFileState(pathB)).toBeNull();
      const rebuilt = await ingestOnce(store);
      expect(rebuilt.files).toBe(1);
      expect(rebuilt.newEvents).toBe(1);
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.selectEvents()[0]?.inputTokens).toBe(50);
    } finally {
      store.close();
      if (oldClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = oldClaude;
      if (oldCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = oldCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("missing legacy path expires, then returning content replaces it exactly once", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-legacy-missing-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const missingPath = join(projectDir, "missing.jsonl");
    const dbPath = join(root, "tokens.sqlite");
    seedLegacyV3Database(dbPath, [
      {
        path: missingPath,
        input: 10,
        output: 20,
        cost: 0.25,
        size: 100,
        offset: 100,
        line: 1,
        mtimeMs: 1,
        cursor: false,
      },
    ]);
    const oldClaude = process.env.CLAUDE_PROJECTS_DIR;
    const oldCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new TokenStore(dbPath);
    try {
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.getFileState(missingPath)).toBeNull();
      await ingestOnce(store);
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.getFileState(missingPath)).toBeNull();
      expect(legacyPendingReason(dbPath, missingPath)).toBe("expired-missing");
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        true,
      );

      const revisionAfterExpiry = store.getRevision();
      const expiryLog = spyOn(console, "error").mockImplementation(() => {});
      try {
        const steadyState = await ingestOnce(store);
        expect(steadyState.newEvents).toBe(0);
        expect(store.getRevision()).toBe(revisionAfterExpiry);
        expect(expiryLog).not.toHaveBeenCalled();
      } finally {
        expiryLog.mockRestore();
      }

      const current = {
        type: "assistant",
        uuid: "pending-replacement",
        timestamp: "2026-05-17T12:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          content: "current",
          usage: { input_tokens: 30, output_tokens: 40 },
        },
      };
      const conflict = {
        ...current,
        message: { ...current.message, content: "conflict" },
      };
      await writeFile(missingPath, `${JSON.stringify(current)}\n${JSON.stringify(conflict)}\n`);
      await expect(ingestOnce(store)).rejects.toThrow("immutable token record conflict");
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.selectEvents()[0]?.inputTokens).toBe(10);
      expect(legacyPendingReason(dbPath, missingPath)).toBe("expired-missing");
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        false,
      );

      await writeFile(missingPath, `${JSON.stringify(current)}\n`);
      const replaced = await ingestOnce(store);
      expect(replaced.newEvents).toBe(1);
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.selectEvents()[0]?.inputTokens).toBe(30);
      expect(legacyPendingReason(dbPath, missingPath)).toBeNull();
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        true,
      );

      const noop = await ingestOnce(store);
      expect(noop.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.selectEvents()[0]?.inputTokens).toBe(30);
    } finally {
      store.close();
      if (oldClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = oldClaude;
      if (oldCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = oldCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("non-ENOENT legacy stat failure remains pending and incomplete", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-legacy-eacces-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    await mkdir(projectsDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const missingPath = join(projectsDir, "-home-user-ssd-demo", "missing.jsonl");
    const dbPath = join(root, "tokens.sqlite");
    seedLegacyV3Database(dbPath, [
      {
        path: missingPath,
        input: 10,
        output: 20,
        cost: 0.25,
        size: 100,
        offset: 100,
        line: 1,
        mtimeMs: 1,
        cursor: false,
      },
    ]);
    const oldClaude = process.env.CLAUDE_PROJECTS_DIR;
    const oldCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new TokenStore(dbPath);
    try {
      await ingestOnce(store, {
        legacyReconciliationStat() {
          throw Object.assign(new Error("permission denied"), { code: "EACCES" });
        },
      });
      expect(legacyPendingReason(dbPath, missingPath)).toBe("unreadable");
      expectStructurallyHealthyWindowIncomplete(store, "claude");
    } finally {
      store.close();
      if (oldClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = oldClaude;
      if (oldCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = oldCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("missing legacy expiry is skipped when the current provider scan is incomplete", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-legacy-incomplete-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const missingPath = join(projectDir, "missing.jsonl");
    const activePath = join(projectDir, "active.jsonl");
    await writeFile(
      activePath,
      `${JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T12:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 30, output_tokens: 40 },
        },
      })}\n`,
    );
    const dbPath = join(root, "tokens.sqlite");
    seedLegacyV3Database(dbPath, [
      {
        path: missingPath,
        input: 10,
        output: 20,
        cost: 0.25,
        size: 100,
        offset: 100,
        line: 1,
        mtimeMs: 1,
        cursor: false,
      },
    ]);
    const oldClaude = process.env.CLAUDE_PROJECTS_DIR;
    const oldCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new TokenStore(dbPath);
    try {
      await ingestOnce(store, {
        afterCapturedRead() {
          throw new Error("injected incomplete provider scan");
        },
      });
      expect(legacyPendingReason(dbPath, missingPath)).toBe("unreadable");
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        false,
      );
    } finally {
      store.close();
      if (oldClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = oldClaude;
      if (oldCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = oldCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("aggregates assistant usage from a fixture directory", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-srv-example-example-repo-sigmashake-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "11111111-2222-3333-4444-555555555555.jsonl");
      const lines = [
        JSON.stringify({
          type: "user",
          timestamp: "2026-05-17T10:00:00.000Z",
          message: { role: "user", content: "hi" },
        }),
        JSON.stringify({
          type: "assistant",
          timestamp: "2026-05-17T10:00:01.000Z",
          cwd: "/srv/example/example-repo/sigmashake-demo",
          message: {
            role: "assistant",
            model: "claude-opus-4-7",
            usage: {
              input_tokens: 100,
              output_tokens: 500,
              cache_read_input_tokens: 20000,
              cache_creation_input_tokens: 5000,
              cache_creation: {
                ephemeral_5m_input_tokens: 1000,
                ephemeral_1h_input_tokens: 3000,
              },
            },
          },
        }),
        JSON.stringify({
          type: "assistant",
          timestamp: "2026-05-18T11:00:00.000Z",
          cwd: "/srv/example/example-repo/sigmashake-demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 200, output_tokens: 800 },
          },
        }),
      ].join("\n");
      await writeFile(sessionPath, `${lines}\n`);

      const summary = await ingestOnce(store);
      expect(summary.files).toBe(1);
      expect(summary.newEvents).toBe(2);

      const events = store.selectEvents();
      const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
      expect(agg.totals.messages).toBe(2);
      expect(agg.totals.inputTokens).toBe(300);
      expect(agg.totals.outputTokens).toBe(1300);
      expect(agg.totals.cacheReadTokens).toBe(20000);
      expect(agg.totals.cacheWrite5mTokens).toBe(2000);
      expect(agg.totals.cacheWrite1hTokens).toBe(3000);
      expect(agg.byModel.length).toBe(2);
      expect(agg.byProject[0]?.project).toBe("sigmashake-demo");
      expect(agg.daily.length).toBe(2);
      expect(agg.topSessions[0]?.messages).toBe(2);
      expect(agg.totals.cost).toBeGreaterThan(0);
    });
  });

  test("aggregates codex rollout usage from a fixture directory", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "06", "30");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "rollout-2026-06-30T01-02-03-codex-session.jsonl");
      const lines = [
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-06-30T01:02:03.000Z",
          payload: {
            session_id: "codex-session",
            cwd: "/srv/example/example-repo/sigmashake-tokens",
          },
        }),
        JSON.stringify({
          type: "turn_context",
          timestamp: "2026-06-30T01:02:04.000Z",
          payload: {
            model: "gpt-5.5-cyber-preview",
            cwd: "/srv/example/example-repo/sigmashake-tokens",
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-06-30T01:02:04.500Z",
          payload: {
            type: "token_count",
            info: null,
            rate_limits: {
              primary: { used_percent: 10 },
              secondary: { used_percent: 20 },
            },
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-06-30T01:02:05.000Z",
          payload: {
            type: "token_count",
            info: {
              last_token_usage: {
                input_tokens: 1000,
                cached_input_tokens: 400,
                output_tokens: 20,
                reasoning_output_tokens: 5,
                total_tokens: 1020,
              },
              total_token_usage: {
                input_tokens: 1000,
                cached_input_tokens: 400,
                output_tokens: 20,
                reasoning_output_tokens: 5,
                total_tokens: 1020,
              },
            },
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-06-30T01:02:06.000Z",
          payload: {
            type: "token_count",
            info: {
              last_token_usage: {
                input_tokens: 1000,
                cached_input_tokens: 400,
                output_tokens: 20,
                reasoning_output_tokens: 5,
                total_tokens: 1020,
              },
              total_token_usage: {
                input_tokens: 1000,
                cached_input_tokens: 400,
                output_tokens: 20,
                reasoning_output_tokens: 5,
                total_tokens: 1020,
              },
            },
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-06-30T01:02:07.000Z",
          payload: {
            type: "token_count",
            info: {
              last_token_usage: {
                input_tokens: 200,
                cached_input_tokens: 50,
                output_tokens: 20,
                reasoning_output_tokens: 7,
                total_tokens: 220,
              },
              total_token_usage: {
                input_tokens: 1200,
                cached_input_tokens: 450,
                output_tokens: 40,
                reasoning_output_tokens: 12,
                total_tokens: 1240,
              },
            },
          },
        }),
      ].join("\n");
      await writeFile(sessionPath, `${lines}\n`);

      const summary = await ingestOnce(store);
      expect(summary.files).toBe(1);
      expect(summary.newEvents).toBe(2);

      const noop = await ingestOnce(store);
      expect(noop.newEvents).toBe(0);

      const next = JSON.stringify({
        type: "event_msg",
        timestamp: "2026-06-30T01:02:08.000Z",
        payload: {
          type: "token_count",
          info: {
            last_token_usage: {
              input_tokens: 100,
              cached_input_tokens: 25,
              output_tokens: 10,
              reasoning_output_tokens: 3,
              total_tokens: 110,
            },
            total_token_usage: {
              input_tokens: 1300,
              cached_input_tokens: 475,
              output_tokens: 50,
              reasoning_output_tokens: 15,
              total_tokens: 1350,
            },
          },
        },
      });
      await appendFile(sessionPath, `${next}\n`);
      _resetScannerStateForTest();
      const resumed = await ingestOnce(store);
      expect(resumed.newEvents).toBe(1);

      const events = store.selectEvents();
      const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
      expect(agg.totals.messages).toBe(3);
      expect(agg.totals.inputTokens).toBe(825);
      expect(agg.totals.cacheReadTokens).toBe(475);
      expect(agg.totals.outputTokens).toBe(50);
      expect(agg.totals.totalTokens).toBe(1350);
      expect(agg.byModel[0]?.model).toBe("gpt-5.5-cyber-preview");
      expect(agg.byProject[0]?.project).toBe("sigmashake-tokens");
      expect(agg.topSessions[0]?.sessionId).toBe("codex-session");
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
      expect(agg.totals.cost).toBeCloseTo(
        costFor("gpt-5.5-cyber-preview", {
          inputTokens: 825,
          outputTokens: 50,
          cacheReadTokens: 475,
          cacheWrite5mTokens: 0,
          cacheWrite1hTokens: 0,
        }),
      );
    });
  });

  test("Codex fork replay is suppressed through the first top-level child trigger", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const parentId = "019f6500-0000-7000-8000-000000000001";
      const childId = "019f6501-0000-7000-8000-000000000002";
      const parentPath = join(sessionDir, "a-parent.jsonl");
      const forkPath = join(sessionDir, "b-fork.jsonl");
      const tokenLine = (timestamp: string, input: number, cached: number, output: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              last_token_usage: {
                input_tokens: input,
                cached_input_tokens: cached,
                output_tokens: output,
              },
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: cached,
                output_tokens: output,
              },
            },
          },
        });
      const parentRows = [
        tokenLine("2026-07-15T10:00:01.000Z", 100, 20, 10),
        tokenLine("2026-07-15T10:00:02.000Z", 200, 40, 30),
      ];
      await writeFile(
        parentPath,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T10:00:00.000Z",
            payload: {
              id: parentId,
              session_id: parentId,
              thread_source: "user",
              cwd: "/srv/example/demo",
            },
          }),
          ...parentRows,
        ].join("\n")}\n`,
      );
      await writeFile(
        forkPath,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T11:00:00.000Z",
            payload: {
              id: childId,
              session_id: parentId,
              forked_from_id: parentId,
              thread_source: "subagent",
              parent_thread_id: parentId,
              source: {
                subagent: { thread_spawn: { parent_thread_id: parentId, depth: 1 } },
              },
              cwd: "/srv/example/demo",
            },
          }),
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T10:00:00.000Z",
            payload: {
              id: parentId,
              session_id: parentId,
              thread_source: "user",
              cwd: "/srv/example/demo",
            },
          }),
          tokenLine("2026-07-15T11:00:01.000Z", 100, 20, 10),
          JSON.stringify({
            type: "inter_agent_communication_metadata",
            timestamp: "2026-07-15T11:00:01.100Z",
            payload: { trigger_turn: false },
          }),
          tokenLine("2026-07-15T11:00:02.000Z", 200, 40, 30),
          JSON.stringify({
            type: "event_msg",
            timestamp: "2026-07-15T11:00:02.100Z",
            payload: {
              type: "inter_agent_communication_metadata",
              trigger_turn: true,
            },
          }),
          JSON.stringify({
            type: "inter_agent_communication_metadata",
            timestamp: "2026-07-15T11:00:03.000Z",
            payload: { trigger_turn: true },
          }),
          tokenLine("2026-07-15T11:00:04.000Z", 230, 45, 35),
        ].join("\n")}\n`,
      );

      const initial = await ingestOnce(store);
      expect(initial.newEvents).toBe(3);
      const initialEvents = store.selectEvents();
      expect(initialEvents.filter((event) => event.sessionId === parentId)).toHaveLength(2);
      expect(initialEvents.filter((event) => event.sessionId === childId)).toEqual([
        expect.objectContaining({ inputTokens: 25, cacheReadTokens: 5, outputTokens: 5 }),
      ]);
      const forkStat = await stat(forkPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(forkStat.dev),
        String(forkStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          childSessionId: childId,
          forkParentId: parentId,
          phase: "live",
          tokenCountRows: 3,
          billedUsageRows: 1,
          inheritedSuppressedRows: 2,
          ambiguousQuarantinedRows: 0,
          zeroNoopRows: 0,
          unrecoverableResetRows: 0,
          invalidUsageRows: 0,
        }),
      );

      _resetScannerStateForTest();
      const appended = tokenLine("2026-07-15T11:00:05.000Z", 250, 50, 40);
      await appendFile(forkPath, appended);
      const partial = await ingestOnce(store);
      expect(partial.newEvents).toBe(0);
      await appendFile(forkPath, "\n");
      const resumed = await ingestOnce(store);
      expect(resumed.newEvents).toBe(1);
      expect(store.selectEvents().filter((event) => event.sessionId === childId)).toEqual([
        expect.objectContaining({ inputTokens: 25, cacheReadTokens: 5, outputTokens: 5 }),
        expect.objectContaining({ inputTokens: 15, cacheReadTokens: 5, outputTokens: 5 }),
      ]);
      const resumedCheckpoint = store.getLatestSourceFile(
        "codex",
        String(forkStat.dev),
        String(forkStat.ino),
      )!;
      const state = JSON.parse(resumedCheckpoint.codexStateJson!) as Record<string, unknown>;
      expect(state.tokenCountRows).toBe(4);
      expect(state.billedUsageRows).toBe(2);
      expect(
        Number(state.billedUsageRows) +
          Number(state.inheritedSuppressedRows) +
          Number(state.ambiguousQuarantinedRows) +
          Number(state.zeroNoopRows) +
          Number(state.unrecoverableResetRows) +
          Number(state.invalidUsageRows),
      ).toBe(state.tokenCountRows);
    });
  });

  test("Codex user thread_source remains billable and provider-complete", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionId = "019f6502-0000-7000-8000-000000000003";
      await writeFile(
        join(sessionDir, "normal-user.jsonl"),
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T12:00:00.000Z",
            payload: {
              id: sessionId,
              session_id: sessionId,
              forked_from_id: null,
              thread_source: "user",
              cwd: "/srv/example/demo",
              model: "gpt-5.5-codex",
            },
          }),
          JSON.stringify({
            type: "event_msg",
            timestamp: "2026-07-15T12:00:01.000Z",
            payload: {
              type: "token_count",
              info: {
                last_token_usage: {
                  input_tokens: 100,
                  cached_input_tokens: 20,
                  output_tokens: 10,
                },
                total_token_usage: {
                  input_tokens: 100,
                  cached_input_tokens: 20,
                  output_tokens: 10,
                },
              },
            },
          }),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({
          sessionId,
          inputTokens: 80,
          cacheReadTokens: 20,
          outputTokens: 10,
        }),
      ]);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
    });
  });

  test("legacy object thread_source corroborates a strong Codex fork", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const parentId = "019f6507-0000-7000-8000-000000000008";
      const childId = "019f6508-0000-7000-8000-000000000009";
      const path = join(sessionDir, "legacy-object-thread-source.jsonl");
      await writeFile(
        path,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T12:30:00.000Z",
            payload: {
              id: childId,
              session_id: parentId,
              forked_from_id: parentId,
              parent_thread_id: parentId,
              thread_source: { parent_thread_id: parentId },
              source: {
                subagent: { thread_spawn: { parent_thread_id: parentId, depth: 1 } },
              },
            },
          }),
          JSON.stringify({
            type: "event_msg",
            timestamp: "2026-07-15T12:30:01.000Z",
            payload: {
              type: "token_count",
              info: {
                total_token_usage: {
                  input_tokens: 10,
                  cached_input_tokens: 0,
                  output_tokens: 1,
                },
              },
            },
          }),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      const sourceStat = await stat(path);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          phase: "inherited",
          childSessionId: childId,
          forkParentId: parentId,
          inheritedSuppressedRows: 1,
          ambiguousQuarantinedRows: 0,
          usageComplete: true,
        }),
      );
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
    });
  });

  test("Codex actual depth-one absent-fork and depth-two ancestor chains suppress inherited usage", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const rootId = "019f6520-0000-7000-8000-000000000021";
      const parentId = "019f6521-0000-7000-8000-000000000022";
      const childId = "019f6522-0000-7000-8000-000000000023";
      const token = (timestamp: string, input: number, output: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: 0,
                output_tokens: output,
              },
            },
          },
        });
      const rootMeta = JSON.stringify({
        type: "session_meta",
        timestamp: "2026-07-15T10:00:00.000Z",
        payload: {
          id: rootId,
          session_id: rootId,
          thread_source: "user",
        },
      });
      const parentMeta = JSON.stringify({
        type: "session_meta",
        timestamp: "2026-07-15T11:00:00.000Z",
        payload: {
          id: parentId,
          session_id: rootId,
          forked_from_id: rootId,
          thread_source: "subagent",
          parent_thread_id: rootId,
          source: { subagent: { thread_spawn: { parent_thread_id: rootId, depth: 1 } } },
        },
      });
      const trigger = (timestamp: string) =>
        JSON.stringify({
          type: "inter_agent_communication_metadata",
          timestamp,
          payload: { trigger_turn: true },
        });
      const depthOnePath = join(sessionDir, "depth-one-matching-fork.jsonl");
      await writeFile(
        depthOnePath,
        `${[
          parentMeta,
          rootMeta,
          token("2026-07-15T11:00:01.000Z", 100, 10),
          trigger("2026-07-15T11:00:02.000Z"),
          token("2026-07-15T11:00:03.000Z", 130, 13),
        ].join("\n")}\n`,
      );
      const depthTwoPath = join(sessionDir, "depth-two-chain.jsonl");
      await writeFile(
        depthTwoPath,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T12:00:00.000Z",
            payload: {
              id: childId,
              session_id: rootId,
              forked_from_id: parentId,
              thread_source: "subagent",
              parent_thread_id: parentId,
              source: {
                subagent: { thread_spawn: { parent_thread_id: parentId, depth: 2 } },
              },
            },
          }),
          parentMeta,
          rootMeta,
          token("2026-07-15T12:00:01.000Z", 200, 20),
          trigger("2026-07-15T12:00:02.000Z"),
          token("2026-07-15T12:00:03.000Z", 250, 25),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(2);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ sessionId: parentId, inputTokens: 30, outputTokens: 3 }),
        expect.objectContaining({ sessionId: childId, inputTokens: 50, outputTokens: 5 }),
      ]);
      for (const path of [depthOnePath, depthTwoPath]) {
        const sourceStat = await stat(path);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            codexLineageRepaired: true,
            phase: "live",
            inheritedSuppressedRows: 1,
            billedUsageRows: 1,
            ambiguousQuarantinedRows: 0,
            usageComplete: true,
          }),
        );
      }
    });
  });

  test("Codex lineage rejects missing or contradictory observed ancestry", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const rootId = "019f6540-0000-7000-8000-000000000041";
      const parentId = "019f6541-0000-7000-8000-000000000042";
      const childId = "019f6542-0000-7000-8000-000000000043";
      const depthOne: Record<string, unknown> = {
        id: parentId,
        session_id: rootId,
        thread_source: "subagent",
        parent_thread_id: rootId,
        source: { subagent: { thread_spawn: { parent_thread_id: rootId, depth: 1 } } },
      };
      const depthTwo: Record<string, unknown> = {
        id: childId,
        session_id: rootId,
        forked_from_id: parentId,
        thread_source: "subagent",
        parent_thread_id: parentId,
        source: { subagent: { thread_spawn: { parent_thread_id: parentId, depth: 2 } } },
      };
      const root: Record<string, unknown> = {
        id: rootId,
        session_id: rootId,
        thread_source: "user",
      };
      const copiedParent = { ...depthOne, forked_from_id: rootId };
      const omit = (payload: Record<string, unknown>, field: string) => {
        const copy = { ...payload };
        delete copy[field];
        return copy;
      };
      const meta = (payload: Record<string, unknown>) =>
        JSON.stringify({ type: "session_meta", timestamp: "2026-07-15T14:00:00.000Z", payload });
      const token = JSON.stringify({
        type: "event_msg",
        timestamp: "2026-07-15T14:00:01.000Z",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: { input_tokens: 10, cached_input_tokens: 0, output_tokens: 1 },
          },
        },
      });
      const fixtures = [
        ["depth1-wrong-fork", [{ ...depthOne, forked_from_id: childId }, root]],
        ["depth1-null-fork", [{ ...depthOne, forked_from_id: null }, root]],
        ["depth1-no-parent", [omit(depthOne, "parent_thread_id"), root]],
        ["depth2-no-fork", [omit(depthTwo, "forked_from_id"), copiedParent, root]],
        ["depth2-no-parent", [omit(depthTwo, "parent_thread_id"), copiedParent, root]],
        ["copied-parent-no-fork", [depthTwo, omit(copiedParent, "forked_from_id"), root]],
        ["copied-parent-null-fork", [depthTwo, { ...copiedParent, forked_from_id: null }, root]],
        ["copied-parent-no-parent", [depthTwo, omit(copiedParent, "parent_thread_id"), root]],
        ["copied-root-wrong-fork", [depthOne, { ...root, forked_from_id: parentId }]],
        ["copied-root-wrong-parent", [depthOne, { ...root, parent_thread_id: parentId }]],
        [
          "copied-root-null-lineage",
          [depthOne, { ...root, forked_from_id: null, parent_thread_id: null }],
        ],
      ] as const;
      const paths: string[] = [];
      for (const [name, payloads] of fixtures) {
        const path = join(sessionDir, `${name}.jsonl`);
        paths.push(path);
        await writeFile(path, `${[...payloads.map(meta), token].join("\n")}\n`);
      }

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      for (const path of paths) {
        const sourceStat = await stat(path);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({ phase: "ambiguous", usageComplete: false }),
        );
      }
    });
  });

  test("Codex proven depth-one lineage is ready without a copied root envelope", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const rootId = "019f6550-0000-7000-8000-000000000051";
      const childId = "019f6551-0000-7000-8000-000000000052";
      const path = join(sessionDir, "depth-one-direct.jsonl");
      await writeFile(path, codexDepthOneLineageRows(rootId, childId, false));

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ sessionId: childId, inputTokens: 10, outputTokens: 0 }),
      ]);
      const sourceStat = await stat(path);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          depthOneDirectLineageRepaired: true,
          lineageDepth: 1,
          lineageReplayStage: "ready",
          phase: "live",
          inheritedSuppressedRows: 1,
          billedUsageRows: 1,
          ambiguousQuarantinedRows: 0,
          usageComplete: true,
        }),
      );
    });
  });

  test("unknown or malformed Codex thread_source remains fail-closed", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionId = "019f6503-0000-7000-8000-000000000004";
      const path = join(sessionDir, "unknown-thread-source.jsonl");
      await writeFile(
        path,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T13:00:00.000Z",
            payload: {
              id: sessionId,
              session_id: sessionId,
              forked_from_id: null,
              thread_source: "future-kind",
            },
          }),
          JSON.stringify({
            type: "event_msg",
            timestamp: "2026-07-15T13:00:01.000Z",
            payload: {
              type: "token_count",
              info: {
                total_token_usage: {
                  input_tokens: 10,
                  cached_input_tokens: 0,
                  output_tokens: 1,
                },
              },
            },
          }),
        ].join("\n")}\n`,
      );
      const contradictoryPaths = [
        {
          path: join(sessionDir, "user-strong-fork.jsonl"),
          payload: {
            id: "019f6504-0000-7000-8000-000000000005",
            session_id: sessionId,
            forked_from_id: sessionId,
            thread_source: "user",
            source: {
              subagent: { thread_spawn: { parent_thread_id: sessionId, depth: 1 } },
            },
          },
        },
        {
          path: join(sessionDir, "null-thread-source.jsonl"),
          payload: {
            id: "019f6505-0000-7000-8000-000000000006",
            session_id: sessionId,
            forked_from_id: sessionId,
            parent_thread_id: sessionId,
            thread_source: null,
            source: {
              subagent: { thread_spawn: { parent_thread_id: sessionId, depth: 1 } },
            },
          },
        },
        {
          path: join(sessionDir, "subagent-without-fork.jsonl"),
          payload: {
            id: "019f6506-0000-7000-8000-000000000007",
            session_id: "019f6506-0000-7000-8000-000000000007",
            forked_from_id: null,
            thread_source: "subagent",
          },
        },
        {
          path: join(sessionDir, "strong-fork-without-spawn.jsonl"),
          payload: {
            id: "019f6506-0000-7000-8000-000000000007",
            session_id: sessionId,
            forked_from_id: sessionId,
            thread_source: "subagent",
          },
        },
        {
          path: join(sessionDir, "depth-two-missing-ancestor-chain.jsonl"),
          payload: {
            id: "019f6507-0000-7000-8000-000000000008",
            session_id: sessionId,
            forked_from_id: "019f6508-0000-7000-8000-000000000009",
            thread_source: "subagent",
            parent_thread_id: "019f6508-0000-7000-8000-000000000009",
            source: {
              subagent: {
                thread_spawn: {
                  parent_thread_id: "019f6508-0000-7000-8000-000000000009",
                  depth: 2,
                },
              },
            },
          },
        },
      ];
      for (const contradiction of contradictoryPaths) {
        await writeFile(
          contradiction.path,
          `${[
            JSON.stringify({
              type: "session_meta",
              timestamp: "2026-07-15T13:01:00.000Z",
              payload: contradiction.payload,
            }),
            JSON.stringify({
              type: "event_msg",
              timestamp: "2026-07-15T13:01:01.000Z",
              payload: {
                type: "token_count",
                info: {
                  total_token_usage: {
                    input_tokens: 10,
                    cached_input_tokens: 0,
                    output_tokens: 1,
                  },
                },
              },
            }),
          ].join("\n")}\n`,
        );
      }

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
      const sourceStat = await stat(path);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({ phase: "ambiguous", usageComplete: false }),
      );
      for (const contradiction of contradictoryPaths) {
        const contradictionStat = await stat(contradiction.path);
        const contradictionCheckpoint = store.getLatestSourceFile(
          "codex",
          String(contradictionStat.dev),
          String(contradictionStat.ino),
        )!;
        expect(JSON.parse(contradictionCheckpoint.codexStateJson!)).toEqual(
          expect.objectContaining({ phase: "ambiguous", usageComplete: false }),
        );
      }
    });
  });

  test("Codex fork classification stays ambiguous for weak, conflicting, deep, or late evidence", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const parentId = "019f6511-0000-7000-8000-000000000012";
      const otherParentId = "019f6512-0000-7000-8000-000000000013";
      const childId = (suffix: string) => `019f6513-0000-7000-8000-0000000000${suffix}`;
      const token = (timestamp: string, input: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: 0,
                output_tokens: 1,
              },
            },
          },
        });
      const forkMeta = (
        id: string,
        options: {
          sourceParent?: string;
          topLevelParent?: unknown;
          threadSource?: unknown;
          depth?: number;
        } = {},
      ) =>
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-15T14:00:00.000Z",
          payload: {
            id,
            session_id: parentId,
            forked_from_id: parentId,
            ...(Object.hasOwn(options, "topLevelParent")
              ? { parent_thread_id: options.topLevelParent }
              : {}),
            ...(Object.hasOwn(options, "threadSource")
              ? { thread_source: options.threadSource }
              : {}),
            ...(options.sourceParent
              ? {
                  source: {
                    subagent: {
                      thread_spawn: {
                        parent_thread_id: options.sourceParent,
                        depth: options.depth ?? 1,
                      },
                    },
                  },
                }
              : {}),
          },
        });
      const selfMeta = JSON.stringify({
        type: "session_meta",
        timestamp: "2026-07-15T14:00:00.000Z",
        payload: { id: parentId, session_id: parentId, forked_from_id: null },
      });
      const fixtures = [
        {
          name: "weak-then-strong.jsonl",
          rows: [selfMeta, forkMeta(childId("14")), token("2026-07-15T14:00:01.000Z", 10)],
        },
        {
          name: "conflicting-parents.jsonl",
          rows: [
            forkMeta(childId("15"), {
              sourceParent: parentId,
              topLevelParent: otherParentId,
              threadSource: "subagent",
            }),
            token("2026-07-15T14:01:01.000Z", 10),
          ],
        },
        {
          name: "depth-two.jsonl",
          rows: [
            forkMeta(childId("16"), {
              sourceParent: parentId,
              topLevelParent: parentId,
              threadSource: "subagent",
              depth: 2,
            }),
            token("2026-07-15T14:02:01.000Z", 10),
          ],
        },
        {
          name: "token-before-fork.jsonl",
          rows: [
            token("2026-07-15T14:03:00.000Z", 5),
            forkMeta(childId("17"), {
              sourceParent: parentId,
              topLevelParent: parentId,
              threadSource: "subagent",
            }),
            token("2026-07-15T14:03:01.000Z", 10),
          ],
        },
        {
          name: "malformed-top-level-parent.jsonl",
          rows: [
            forkMeta(childId("18"), {
              sourceParent: parentId,
              topLevelParent: { parent_thread_id: parentId },
              threadSource: "subagent",
            }),
            token("2026-07-15T14:04:01.000Z", 10),
          ],
        },
      ];
      for (const fixture of fixtures) {
        await writeFile(join(sessionDir, fixture.name), `${fixture.rows.join("\n")}\n`);
      }

      const initial = await ingestOnce(store);
      expect(initial.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      for (const fixture of fixtures) {
        const path = join(sessionDir, fixture.name);
        const sourceStat = await stat(path);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({ phase: "ambiguous", usageComplete: false }),
        );
      }

      const conflictPath = join(sessionDir, "conflicting-parents.jsonl");
      await appendFile(
        conflictPath,
        `${forkMeta(childId("15"), {
          sourceParent: parentId,
          topLevelParent: parentId,
          threadSource: "subagent",
        })}\n${token("2026-07-15T14:01:02.000Z", 20)}\n`,
      );
      const sticky = await ingestOnce(store);
      expect(sticky.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      const conflictStat = await stat(conflictPath);
      const conflictState = store.getLatestSourceFile(
        "codex",
        String(conflictStat.dev),
        String(conflictStat.ino),
      )!;
      expect(JSON.parse(conflictState.codexStateJson!)).toEqual(
        expect.objectContaining({
          childSessionId: null,
          phase: "ambiguous",
          tokenCountRows: 2,
          ambiguousQuarantinedRows: 2,
          usageComplete: false,
        }),
      );
    });
  });

  test("Codex v2 replacement preserves a shared event and is revision-idempotent", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionId = "019f6502-0000-7000-8000-000000000003";
      const rows = `${[
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-15T12:00:00.000Z",
          payload: {
            id: sessionId,
            session_id: sessionId,
            forked_from_id: null,
            cwd: "/srv/example/demo",
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-15T12:00:01.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: 100,
                cached_input_tokens: 20,
                output_tokens: 10,
              },
            },
          },
        }),
      ].join("\n")}\n`;
      const firstPath = join(sessionDir, "shared-a.jsonl");
      const secondPath = join(sessionDir, "shared-b.jsonl");
      await writeFile(firstPath, rows);
      await writeFile(secondPath, rows);

      const initial = await ingestOnce(store);
      expect(initial.newEvents).toBe(1);
      const firstStat = await stat(firstPath);
      const secondStat = await stat(secondPath);
      const firstState = store.getLatestSourceFile(
        "codex",
        String(firstStat.dev),
        String(firstStat.ino),
      )!;
      const secondState = store.getLatestSourceFile(
        "codex",
        String(secondStat.dev),
        String(secondStat.ino),
      )!;
      let sharedEventId = 0;
      const db = new Database(storePath);
      try {
        const event = db.query("SELECT id FROM events").get() as { id: number };
        sharedEventId = event.id;
        const checkpoint = JSON.parse(firstState.codexStateJson!) as Record<string, unknown>;
        checkpoint.usageParserVersion = 2;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(checkpoint),
          firstState.generationKey,
        );
        const secondCheckpoint = JSON.parse(secondState.codexStateJson!) as Record<string, unknown>;
        secondCheckpoint.usageParserVersion = 2;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(secondCheckpoint),
          secondState.generationKey,
        );
      } finally {
        db.close();
      }

      const repaired = await ingestOnce(store);
      expect(repaired.mutated).toBe(true);
      expect(repaired.newEvents).toBe(0);
      expect(repaired.revision).toBe(initial.revision + 1);
      const inspected = new Database(storePath, { readonly: true });
      try {
        expect(inspected.query("SELECT id FROM events").get()).toEqual({ id: sharedEventId });
        expect(inspected.query("SELECT COUNT(*) AS count FROM event_occurrences").get()).toEqual({
          count: 2,
        });
      } finally {
        inspected.close();
      }
      const migrated = store.getLatestSourceFile(
        "codex",
        String(firstStat.dev),
        String(firstStat.ino),
      )!;
      expect(JSON.parse(migrated.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
          usageComplete: true,
          tokenCountRows: 1,
          billedUsageRows: 1,
        }),
      );
      const secondMigrated = store.getLatestSourceFile(
        "codex",
        String(secondStat.dev),
        String(secondStat.ino),
      )!;
      expect(JSON.parse(secondMigrated.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
          usageComplete: true,
        }),
      );

      const revision = repaired.revision;
      const noop = await ingestOnce(store, { metadataFastPathSupported: () => true });
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
      expect(noop.newEvents).toBe(0);
    });
  });

  test("Codex replacement guards fail closed without partial authority changes", async () => {
    const scenarios: Array<{
      name: string;
      error: string;
      mutate: (ctx: {
        store: TokenStore;
        storePath: string;
        path: string;
        generationKey: string;
        device: string;
        inode: string;
        lastByteOffset: number;
        batch: CapturedFileCommit;
      }) => void;
    }> = [
      {
        name: "missing repair marker",
        error: "Codex replacement result is incomplete or non-conserving",
        mutate: ({ batch }) => {
          const state = JSON.parse(batch.state.codexStateJson!) as Record<string, unknown>;
          delete state.threadSourceRoleRepaired;
          batch.state.codexStateJson = JSON.stringify(state);
        },
      },
      {
        name: "ambiguous rebuilt phase",
        error: "Codex replacement result is incomplete or non-conserving",
        mutate: ({ batch }) => {
          const state = JSON.parse(batch.state.codexStateJson!) as Record<string, unknown>;
          state.phase = "ambiguous";
          batch.state.codexStateJson = JSON.stringify(state);
        },
      },
      {
        name: "source snapshot revision",
        error: "Codex replacement source snapshot revision changed",
        mutate: ({ store }) => {
          store.insertEvents([
            mkEvent({
              filePath: "/fixture/revision-race.jsonl",
              lineOffset: 901,
              sessionId: "revision-race",
            }),
          ]);
        },
      },
      {
        name: "alias drift",
        error: "Codex replacement active aliases changed",
        mutate: ({ storePath, path }) => {
          const db = new Database(storePath);
          try {
            db.prepare("UPDATE source_aliases SET active = 0 WHERE path = ?").run(path);
          } finally {
            db.close();
          }
        },
      },
      {
        name: "multiple generation",
        error: "Codex replacement has multiple source generations",
        mutate: ({ storePath, generationKey, device, inode }) => {
          const db = new Database(storePath);
          try {
            db.prepare(
              "INSERT INTO source_files (generation_key, source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json, updated_at) SELECT ?, source, ?, ?, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json, updated_at FROM source_files WHERE generation_key = ?",
            ).run(`${generationKey}:extra`, device, inode, generationKey);
          } finally {
            db.close();
          }
        },
      },
      {
        name: "occurrence coverage",
        error: "Codex replacement occurrence coverage is invalid",
        mutate: ({ storePath, generationKey, lastByteOffset }) => {
          const db = new Database(storePath);
          try {
            db.prepare("UPDATE event_occurrences SET byte_end = ? WHERE generation_key = ?").run(
              lastByteOffset + 1,
              generationKey,
            );
          } finally {
            db.close();
          }
        },
      },
      {
        name: "orphan event",
        error: "Codex replacement found an orphan event",
        mutate: ({ storePath, path }) => {
          const db = new Database(storePath);
          try {
            db.prepare(
              "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, source) VALUES (?, '2026-07-15', 'gpt-5.5-codex', 'sigmashake-tokens', '-home-user-ssd-sigmashake-tokens', 'orphan', 1, 1, 0, 0, 0, 0, ?, 999, 'codex')",
            ).run(Date.parse("2026-07-15T16:00:02.000Z"), path);
          } finally {
            db.close();
          }
        },
      },
    ];

    for (const [index, scenario] of scenarios.entries()) {
      await withFixture(async ({ codexSessionsDir, store, storePath }) => {
        const sessionDir = join(codexSessionsDir, "2026", "07", "15");
        await mkdir(sessionDir, { recursive: true });
        const sessionId = `019f653${index}-0000-7000-8000-00000000003${index}`;
        const path = join(sessionDir, `${scenario.name.replaceAll(" ", "-")}.jsonl`);
        await writeFile(path, singleCodexReplacementRows(sessionId));
        expect((await ingestOnce(store)).newEvents).toBe(1);
        const sourceStat = await stat(path);
        const sourceState = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const prepared = prepareCodexReplacement(store, storePath, path, sourceState);
        scenario.mutate({
          store,
          storePath,
          path,
          generationKey: sourceState.generationKey,
          device: sourceState.device,
          inode: sourceState.inode,
          lastByteOffset: sourceState.lastByteOffset,
          batch: prepared.batch,
        });
        const before = replacementAuthoritySnapshot(store, storePath, sourceState.generationKey);

        expect(() => store.commitScan(prepared.scan)).toThrow(scenario.error);
        expect(replacementAuthoritySnapshot(store, storePath, sourceState.generationKey)).toEqual(
          before,
        );
        const checkpoint = store.getLatestSourceFile(
          "codex",
          sourceState.device,
          sourceState.inode,
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({ usageParserVersion: 2 }),
        );
      });
    }
  });

  test("a late batch failure rolls back an earlier Codex replacement", async () => {
    await withFixture(
      async ({ codexSessionsDir, store, storePath }) => {
        const armedStore = store as ArmedFailLaterCommitStore;
        const sessionDir = join(codexSessionsDir, "2026", "07", "15");
        await mkdir(sessionDir, { recursive: true });
        const replacementPath = join(sessionDir, "a-replacement.jsonl");
        const laterPath = join(sessionDir, "z-later.jsonl");
        await writeFile(
          replacementPath,
          singleCodexReplacementRows("019f6540-0000-7000-8000-000000000040"),
        );
        await writeFile(
          laterPath,
          singleCodexReplacementRows("019f6541-0000-7000-8000-000000000041"),
        );
        expect((await ingestOnce(store)).newEvents).toBe(2);
        const replacementStat = await stat(replacementPath);
        const laterStat = await stat(laterPath);
        const replacementState = store.getLatestSourceFile(
          "codex",
          String(replacementStat.dev),
          String(replacementStat.ino),
        )!;
        const laterState = store.getLatestSourceFile(
          "codex",
          String(laterStat.dev),
          String(laterStat.ino),
        )!;
        const prepared = prepareCodexReplacement(
          store,
          storePath,
          replacementPath,
          replacementState,
        );
        prepared.scan.batches.push({ state: laterState, aliases: [laterPath], events: [] });
        prepared.scan.aliases.push({
          path: laterPath,
          generationKey: laterState.generationKey,
          source: "codex",
        });
        const before = replacementAuthoritySnapshot(
          store,
          storePath,
          replacementState.generationKey,
        );
        armedStore.armCapturedCommitFailure(2);

        expect(() => store.commitScan(prepared.scan)).toThrow(
          "injected armed captured-file commit failure",
        );
        expect(
          replacementAuthoritySnapshot(store, storePath, replacementState.generationKey),
        ).toEqual(before);
        expect(store.selectEvents()).toHaveLength(2);
        expect(
          JSON.parse(
            store.getLatestSourceFile("codex", replacementState.device, replacementState.inode)!
              .codexStateJson!,
          ),
        ).toEqual(expect.objectContaining({ usageParserVersion: 2 }));
      },
      (storePath) => new ArmedFailLaterCommitStore(storePath),
    );
  });

  test("historical Codex ambiguity persists quarantine without replacing authority", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "historical-ambiguous.jsonl");
      const rootId = "019f6509-0000-7000-8000-000000000010";
      const token = (timestamp: string, input: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: 0,
                output_tokens: 1,
              },
            },
          },
        });
      await writeFile(
        sessionPath,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T12:30:00.000Z",
            payload: { id: rootId, session_id: rootId, forked_from_id: null },
          }),
          token("2026-07-15T12:30:01.000Z", 10),
        ].join("\n")}\n`,
      );
      const initial = await ingestOnce(store);
      expect(initial.newEvents).toBe(1);
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const stale = JSON.parse(checkpoint.codexStateJson!) as Record<string, unknown>;
        stale.usageParserVersion = 2;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(stale),
          checkpoint.generationKey,
        );
      } finally {
        db.close();
      }
      const conflictingId = "019f6510-0000-7000-8000-000000000011";
      await appendFile(
        sessionPath,
        `${JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-15T12:30:02.000Z",
          payload: {
            id: conflictingId,
            session_id: conflictingId,
            forked_from_id: null,
          },
        })}\n${token("2026-07-15T12:30:03.000Z", 20)}\n`,
      );

      const quarantined = await ingestOnce(store);
      expect(quarantined.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(1);
      const persisted = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(persisted.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
          usageComplete: false,
          phase: "ambiguous",
          tokenCountRows: 2,
          billedUsageRows: 1,
          ambiguousQuarantinedRows: 1,
        }),
      );
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("Codex fork reset, ambiguity, and zero rows have exhaustive dispositions", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const parentId = "019f6503-0000-7000-8000-000000000004";
      const meta = (childId: string) =>
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-15T13:00:00.000Z",
          payload: {
            id: childId,
            session_id: parentId,
            forked_from_id: parentId,
            thread_source: "subagent",
            parent_thread_id: parentId,
            source: {
              subagent: { thread_spawn: { parent_thread_id: parentId, depth: 1 } },
            },
            cwd: "/srv/example/demo",
          },
        });
      const copiedRootMeta = JSON.stringify({
        type: "session_meta",
        timestamp: "2026-07-15T12:59:59.000Z",
        payload: {
          id: parentId,
          session_id: parentId,
          thread_source: "user",
        },
      });
      const usage = (
        timestamp: string,
        total: {
          input_tokens: number;
          cached_input_tokens: number;
          output_tokens: number;
          reasoning_output_tokens?: number;
        },
        last?: {
          input_tokens: number;
          cached_input_tokens: number;
          output_tokens: number;
          reasoning_output_tokens?: number;
        },
      ) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              ...(last ? { last_token_usage: last } : {}),
              total_token_usage: total,
            },
          },
        });
      const trigger = JSON.stringify({
        type: "inter_agent_communication_metadata",
        timestamp: "2026-07-15T13:00:02.000Z",
        payload: { trigger_turn: true },
      });
      const inherited = usage("2026-07-15T13:00:01.000Z", {
        input_tokens: 100,
        cached_input_tokens: 20,
        output_tokens: 10,
      });
      const goodId = "019f6504-0000-7000-8000-000000000005";
      const missingId = "019f6505-0000-7000-8000-000000000006";
      const ambiguousId = "019f6506-0000-7000-8000-000000000007";
      const oversizedId = "019f6514-0000-7000-8000-000000000018";
      const crossedId = "019f6515-0000-7000-8000-000000000019";
      const goodPath = join(sessionDir, "reset-good.jsonl");
      const missingPath = join(sessionDir, "reset-missing.jsonl");
      const ambiguousPath = join(sessionDir, "ambiguous.jsonl");
      const oversizedPath = join(sessionDir, "reset-oversized-last.jsonl");
      const crossedPath = join(sessionDir, "reset-crossed-last.jsonl");
      const zeroPath = join(sessionDir, "zero.jsonl");
      await writeFile(
        goodPath,
        `${[
          meta(goodId),
          copiedRootMeta,
          inherited,
          trigger,
          usage(
            "2026-07-15T13:00:03.000Z",
            { input_tokens: 10, cached_input_tokens: 2, output_tokens: 3 },
            { input_tokens: 10, cached_input_tokens: 2, output_tokens: 3 },
          ),
        ].join("\n")}\n`,
      );
      await writeFile(
        missingPath,
        `${[
          meta(missingId),
          copiedRootMeta,
          inherited,
          trigger,
          usage("2026-07-15T13:00:03.000Z", {
            input_tokens: 10,
            cached_input_tokens: 2,
            output_tokens: 3,
          }),
        ].join("\n")}\n`,
      );
      await writeFile(
        ambiguousPath,
        `${[
          meta(ambiguousId),
          copiedRootMeta,
          meta("019f6507-0000-7000-8000-000000000008"),
          inherited,
        ].join("\n")}\n`,
      );
      await writeFile(
        oversizedPath,
        `${[
          meta(oversizedId),
          copiedRootMeta,
          inherited,
          trigger,
          usage(
            "2026-07-15T13:00:03.000Z",
            { input_tokens: 10, cached_input_tokens: 2, output_tokens: 3 },
            { input_tokens: 11, cached_input_tokens: 2, output_tokens: 3 },
          ),
          usage("2026-07-15T13:00:04.000Z", {
            input_tokens: 12,
            cached_input_tokens: 3,
            output_tokens: 4,
          }),
        ].join("\n")}\n`,
      );
      await writeFile(
        crossedPath,
        `${[
          meta(crossedId),
          copiedRootMeta,
          inherited,
          trigger,
          usage(
            "2026-07-15T13:00:03.000Z",
            {
              input_tokens: 10,
              cached_input_tokens: 8,
              output_tokens: 10,
              reasoning_output_tokens: 8,
            },
            {
              input_tokens: 9,
              cached_input_tokens: 0,
              output_tokens: 9,
              reasoning_output_tokens: 0,
            },
          ),
          usage("2026-07-15T13:00:04.000Z", {
            input_tokens: 12,
            cached_input_tokens: 9,
            output_tokens: 12,
            reasoning_output_tokens: 9,
          }),
        ].join("\n")}\n`,
      );
      const rootId = "019f6508-0000-7000-8000-000000000009";
      await writeFile(
        zeroPath,
        `${[
          JSON.stringify({
            type: "session_meta",
            timestamp: "2026-07-15T13:00:00.000Z",
            payload: { id: rootId, session_id: rootId, forked_from_id: null },
          }),
          usage("2026-07-15T13:00:01.000Z", {
            input_tokens: 0,
            cached_input_tokens: 0,
            output_tokens: 0,
          }),
          usage("2026-07-15T13:00:02.000Z", {
            input_tokens: 10,
            cached_input_tokens: 2,
            output_tokens: 3,
          }),
          usage("2026-07-15T13:00:03.000Z", {
            input_tokens: 10,
            cached_input_tokens: 2,
            output_tokens: 3,
          }),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(4);
      expect(store.selectEvents().find((event) => event.sessionId === goodId)).toEqual(
        expect.objectContaining({ inputTokens: 8, cacheReadTokens: 2, outputTokens: 3 }),
      );
      expect(store.selectEvents().filter((event) => event.sessionId === oversizedId)).toEqual([
        expect.objectContaining({
          lineOffset: 5,
          inputTokens: 1,
          cacheReadTokens: 1,
          outputTokens: 1,
        }),
      ]);
      expect(store.selectEvents().filter((event) => event.sessionId === crossedId)).toEqual([
        expect.objectContaining({
          lineOffset: 5,
          inputTokens: 1,
          cacheReadTokens: 1,
          outputTokens: 2,
          tokenClasses: expect.objectContaining({ visibleOutput: 1, reasoningOutput: 1 }),
        }),
      ]);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
      const stateFor = async (path: string): Promise<Record<string, number | string | boolean>> => {
        const sourceStat = await stat(path);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        return JSON.parse(checkpoint.codexStateJson!) as Record<string, number | string | boolean>;
      };
      const good = await stateFor(goodPath);
      const missing = await stateFor(missingPath);
      const ambiguous = await stateFor(ambiguousPath);
      const oversized = await stateFor(oversizedPath);
      const crossed = await stateFor(crossedPath);
      const zero = await stateFor(zeroPath);
      expect(good).toEqual(
        expect.objectContaining({
          tokenCountRows: 2,
          inheritedSuppressedRows: 1,
          billedUsageRows: 1,
        }),
      );
      expect(missing).toEqual(
        expect.objectContaining({
          tokenCountRows: 2,
          inheritedSuppressedRows: 1,
          unrecoverableResetRows: 1,
          usageComplete: false,
        }),
      );
      expect(ambiguous).toEqual(
        expect.objectContaining({
          phase: "ambiguous",
          tokenCountRows: 1,
          ambiguousQuarantinedRows: 1,
          usageComplete: false,
        }),
      );
      for (const incoherent of [oversized, crossed]) {
        expect(incoherent).toEqual(
          expect.objectContaining({
            tokenCountRows: 3,
            inheritedSuppressedRows: 1,
            invalidUsageRows: 1,
            billedUsageRows: 1,
            usageComplete: false,
          }),
        );
      }
      expect(zero).toEqual(
        expect.objectContaining({ tokenCountRows: 3, billedUsageRows: 1, zeroNoopRows: 2 }),
      );
      for (const state of [good, missing, ambiguous, oversized, crossed, zero]) {
        expect(
          Number(state.billedUsageRows) +
            Number(state.inheritedSuppressedRows) +
            Number(state.ambiguousQuarantinedRows) +
            Number(state.zeroNoopRows) +
            Number(state.unrecoverableResetRows) +
            Number(state.invalidUsageRows),
        ).toBe(state.tokenCountRows);
      }
    });
  });

  test("Codex timestamp-less total-only deltas use the observed session UUIDv7 instant", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const timestampMs = Date.parse("2026-07-05T11:00:00.000Z");
      const prefix = timestampMs.toString(16).padStart(12, "0");
      const sessionId = `${prefix.slice(0, 8)}-${prefix.slice(8)}-7000-8000-000000000001`;
      const total = (input: number, cached: number, output: number) =>
        JSON.stringify({
          type: "event_msg",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: cached,
                output_tokens: output,
              },
            },
          },
        });
      await writeFile(
        join(sessionDir, "timestamp-less.jsonl"),
        `${[
          JSON.stringify({ type: "session_meta", payload: { session_id: sessionId } }),
          JSON.stringify({
            type: "turn_context",
            payload: { model: "gpt-5.5-cyber-preview", cwd: "/srv/example/demo" },
          }),
          total(10, 2, 1),
          total(30, 7, 4),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(2);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({
          ts: timestampMs,
          date: "2026-07-05",
          sessionId,
          inputTokens: 8,
          cacheReadTokens: 2,
          outputTokens: 1,
        }),
        expect.objectContaining({
          ts: timestampMs,
          date: "2026-07-05",
          sessionId,
          inputTokens: 15,
          cacheReadTokens: 5,
          outputTokens: 3,
        }),
      ]);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
    });
  });

  test("Codex filename and payload.id UUIDs never authorize timestamp fallback", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionId = "0197db0e-7d80-7000-8000-000000000001";
      const tokenLine = JSON.stringify({
        type: "event_msg",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 10,
              cached_input_tokens: 2,
              output_tokens: 1,
            },
          },
        },
      });
      await writeFile(join(sessionDir, `${sessionId}.jsonl`), `${tokenLine}\n`);
      await writeFile(
        join(sessionDir, "payload-id.jsonl"),
        `${JSON.stringify({ type: "session_meta", payload: { id: sessionId } })}\n${tokenLine}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("Codex timezone-less timestamps fall back only to an observed session UUIDv7", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const timestampMs = Date.parse("2026-07-05T11:00:00.000Z");
      const prefix = timestampMs.toString(16).padStart(12, "0");
      const sessionId = `${prefix.slice(0, 8)}-${prefix.slice(8)}-7000-8000-000000000002`;
      const tokenLine = JSON.stringify({
        type: "event_msg",
        timestamp: "2026-07-05T04:00:00",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 10,
              cached_input_tokens: 2,
              output_tokens: 1,
            },
          },
        },
      });
      await writeFile(
        join(sessionDir, "with-session.jsonl"),
        `${JSON.stringify({ type: "session_meta", payload: { session_id: sessionId } })}\n${tokenLine}\n`,
      );
      await writeFile(join(sessionDir, "without-session.jsonl"), `${tokenLine}\n`);

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ ts: timestampMs, date: "2026-07-05", sessionId }),
      ]);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("Codex rejects malformed totals without advancing baseline and persists incompleteness", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "02");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "malformed.jsonl");
      const tokenLine = (
        timestamp: string,
        total: Record<string, number>,
        last: Record<string, number> = total,
      ) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: { last_token_usage: last, total_token_usage: total },
          },
        });
      const prefix = [
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-02T10:00:00.000Z",
          payload: { session_id: "malformed-codex", cwd: "/srv/example/demo" },
        }),
        JSON.stringify({
          type: "turn_context",
          timestamp: "2026-07-02T10:00:01.000Z",
          payload: { model: "gpt-5.5-cyber-preview", cwd: "/srv/example/demo" },
        }),
        tokenLine("2026-07-02T10:00:02.000Z", {
          input_tokens: 100,
          cached_input_tokens: 20,
          output_tokens: 10,
          reasoning_output_tokens: 2,
        }),
        tokenLine(
          "2026-07-02T10:00:03.000Z",
          {
            input_tokens: 120,
            cached_input_tokens: 121,
            output_tokens: 12,
            reasoning_output_tokens: 2,
          },
          {
            input_tokens: 20,
            cached_input_tokens: 5,
            output_tokens: 2,
            reasoning_output_tokens: 1,
          },
        ),
      ];
      await writeFile(sessionPath, `${prefix.join("\n")}\n`);

      const malformed = await ingestOnce(store);
      expect(malformed.newEvents).toBe(1);
      expect(store.selectEvents()).toHaveLength(1);
      expectStructurallyHealthyWindowIncomplete(store, "codex");

      await appendFile(
        sessionPath,
        `${tokenLine("2026-07-02T10:00:04.000Z", {
          input_tokens: 150,
          cached_input_tokens: 30,
          output_tokens: 15,
          reasoning_output_tokens: 3,
        })}\n`,
      );
      const resumed = await ingestOnce(store);
      expect(resumed.newEvents).toBe(1);
      const events = store.selectEvents();
      expect(events[1]?.inputTokens).toBe(40);
      expect(events[1]?.cacheReadTokens).toBe(10);
      expect(events[1]?.outputTokens).toBe(5);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
      const revision = resumed.revision;
      let capturedReads = 0;
      const noop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
      const sourceStat = await stat(sessionPath);
      const state = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      );
      expect(JSON.parse(state!.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageComplete: false,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
        }),
      );

      // A checkpoint written by a newer parser remains fail-closed and must
      // not be destructively downgraded or reparsed by this binary.
      const db = new Database(storePath);
      try {
        const futureState = JSON.parse(state!.codexStateJson!) as Record<string, unknown>;
        futureState.usageParserVersion = CODEX_USAGE_PARSER_VERSION + 1;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(futureState),
          state!.generationKey,
        );
      } finally {
        db.close();
      }
      await appendFile(
        sessionPath,
        `${tokenLine("2026-07-02T10:00:05.000Z", {
          input_tokens: 200,
          cached_input_tokens: 40,
          output_tokens: 20,
          reasoning_output_tokens: 4,
        })}\n`,
      );
      capturedReads = 0;
      const futureNoop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(futureNoop.mutated).toBe(false);
      expect(futureNoop.revision).toBe(revision);
      const futureCheckpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      );
      expect(JSON.parse(futureCheckpoint!.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageComplete: false,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION + 1,
        }),
      );
      expect(futureCheckpoint!.capturedSize).toBe(state!.capturedSize);
      expect(futureCheckpoint!.lastByteOffset).toBe(state!.lastByteOffset);
    });
  });

  test("Codex invalid required/optional counters never produce usage", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "03");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "invalid-components.jsonl");
      const totals = [
        { input_tokens: -1, cached_input_tokens: 0, output_tokens: 1 },
        { input_tokens: 10.5, cached_input_tokens: 1, output_tokens: 1 },
        { input_tokens: 10, cached_input_tokens: 11, output_tokens: 1 },
        {
          input_tokens: 10,
          cached_input_tokens: 1,
          output_tokens: 2,
          reasoning_output_tokens: 3,
        },
        { input_tokens: 10, cached_input_tokens: 1, output_tokens: 2, total_tokens: 999 },
        { input_tokens: 10, cached_input_tokens: 1 },
      ];
      const lines = totals.map((total, index) =>
        JSON.stringify({
          type: "event_msg",
          timestamp: `2026-07-03T10:00:0${index}.000Z`,
          payload: {
            type: "token_count",
            info: { total_token_usage: total },
          },
        }),
      );
      await writeFile(sessionPath, `${lines.join("\n")}\n`);
      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toEqual([]);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("Codex trailing partial token_count remains retryable", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "03");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "partial-token-count.jsonl");
      const line = JSON.stringify({
        type: "event_msg",
        timestamp: "2026-07-03T11:00:00.000Z",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 10,
              cached_input_tokens: 2,
              output_tokens: 1,
            },
          },
        },
      });
      await writeFile(sessionPath, line);
      const partial = await ingestOnce(store);
      expect(partial.newEvents).toBe(0);
      await appendFile(sessionPath, "\n");
      const completed = await ingestOnce(store);
      expect(completed.newEvents).toBe(1);
      expect(store.selectEvents()[0]?.inputTokens).toBe(8);
    });
  });

  test("Codex asymmetric cumulative rollback is baseline-only", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "04");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "counter-reset.jsonl");
      const line = (timestamp: string, input: number, cached: number, output: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: cached,
                output_tokens: output,
              },
            },
          },
        });
      await writeFile(
        sessionPath,
        `${[
          line("2026-07-04T10:00:00.000Z", 100, 20, 10),
          line("2026-07-04T10:00:01.000Z", 120, 10, 15),
          line("2026-07-04T10:00:02.000Z", 140, 20, 20),
        ].join("\n")}\n`,
      );
      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(2);
      const events = store.selectEvents();
      expect(events.map((event) => event.inputTokens)).toEqual([80, 10]);
      expect(events.map((event) => event.cacheReadTokens)).toEqual([20, 10]);
      expect(events.map((event) => event.outputTokens)).toEqual([10, 5]);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
    });
  });

  test("Codex last-only rows seed a later cumulative baseline without double-counting", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "04");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "last-then-total.jsonl");
      const line = (timestamp: string, usage: Record<string, number>, kind: "last" | "total") =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: { [kind === "last" ? "last_token_usage" : "total_token_usage"]: usage },
          },
        });
      await writeFile(
        sessionPath,
        `${[
          line(
            "2026-07-04T11:00:00.000Z",
            {
              input_tokens: 10,
              cached_input_tokens: 2,
              output_tokens: 3,
              reasoning_output_tokens: 1,
              total_tokens: 13,
            },
            "last",
          ),
          line(
            "2026-07-04T11:00:01.000Z",
            {
              input_tokens: 20,
              cached_input_tokens: 5,
              output_tokens: 7,
              reasoning_output_tokens: 2,
              total_tokens: 27,
            },
            "last",
          ),
          line(
            "2026-07-04T11:00:02.000Z",
            {
              input_tokens: 40,
              cached_input_tokens: 10,
              output_tokens: 15,
              reasoning_output_tokens: 4,
              total_tokens: 55,
            },
            "total",
          ),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(3);
      const events = store.selectEvents();
      expect(events.map((event) => event.inputTokens)).toEqual([8, 15, 7]);
      expect(events.map((event) => event.cacheReadTokens)).toEqual([2, 5, 3]);
      expect(events.map((event) => event.outputTokens)).toEqual([3, 7, 5]);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
    });
  });

  test("missing persisted Codex state ignores cache and revalidates prefix timestamps", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "legacy-state.jsonl");
      const valid = JSON.stringify({
        type: "event_msg",
        timestamp: "2026-07-05T10:00:00.000Z",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 10,
              cached_input_tokens: 2,
              output_tokens: 1,
            },
          },
        },
      });
      const invalid = JSON.stringify({
        type: "event_msg",
        timestamp: "not-a-timestamp",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 20,
              cached_input_tokens: 4,
              output_tokens: 2,
            },
          },
        },
      });
      await writeFile(sessionPath, `${valid}\n`);
      await ingestOnce(store);
      expect(_scannerCodexCacheSizeForTest()).toBeGreaterThan(0);

      // Simulate a pre-contract checkpoint that already covers an invalid
      // timestamp while the process cache still holds the earlier valid
      // state. The DB must force prefix replay instead of trusting the cache.
      const fullPrefix = `${valid}\n${invalid}\n`;
      await writeFile(sessionPath, fullPrefix);
      const sourceStat = await stat(sessionPath);
      const db = new Database(storePath);
      try {
        const row = db.query("SELECT generation_key, codex_state_json FROM source_files").get() as {
          generation_key: string;
          codex_state_json: string;
        };
        const oldState = JSON.parse(row.codex_state_json) as Record<string, unknown>;
        delete oldState.usageComplete;
        db.prepare(
          "UPDATE source_files SET captured_size = ?, mtime_ms = ?, last_byte_offset = ?, last_line_number = ?, committed_prefix_sha256 = ?, codex_state_json = ? WHERE generation_key = ?",
        ).run(
          sourceStat.size,
          sourceStat.mtimeMs,
          sourceStat.size,
          2,
          createHash("sha256").update(fullPrefix).digest("hex"),
          JSON.stringify(oldState),
          row.generation_key,
        );
        db.run("UPDATE provider_state SET complete = 1 WHERE source = 'codex'");
      } finally {
        db.close();
      }
      await ingestOnce(store);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("marked v3 Codex lineage ambiguity repairs exactly once with or without occurrences", async () => {
    for (const retainOccurrences of [false, true]) {
      await withFixture(async ({ codexSessionsDir, store, storePath }) => {
        const sessionDir = join(codexSessionsDir, "2026", "07", "15");
        await mkdir(sessionDir, { recursive: true });
        const rootId = "019f6530-0000-7000-8000-000000000031";
        const childId = retainOccurrences
          ? "019f6531-0000-7000-8000-000000000032"
          : "019f6532-0000-7000-8000-000000000033";
        const sessionPath = join(
          sessionDir,
          retainOccurrences ? "lineage-with-occurrence.jsonl" : "lineage-zero-occurrence.jsonl",
        );
        await writeFile(sessionPath, codexDepthOneLineageRows(rootId, childId));
        await ingestOnce(store, { metadataFastPathSupported: () => true });
        const sourceStat = await stat(sessionPath);
        const current = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const db = new Database(storePath);
        try {
          if (!retainOccurrences) {
            db.prepare("DELETE FROM event_occurrences WHERE generation_key = ?").run(
              current.generationKey,
            );
            db.run("DELETE FROM events");
          }
          db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
            JSON.stringify(asOldCodexLineageAmbiguity(current.codexStateJson!)),
            current.generationKey,
          );
          db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
        } finally {
          db.close();
        }

        let capturedReads = 0;
        const repaired = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(1);
        expect(repaired.newEvents).toBe(retainOccurrences ? 0 : 1);
        expect(store.selectEvents()).toEqual([
          expect.objectContaining({ sessionId: childId, inputTokens: 10 }),
        ]);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            codexLineageRepaired: true,
            phase: "live",
            inheritedSuppressedRows: 1,
            billedUsageRows: 1,
            usageComplete: true,
          }),
        );

        capturedReads = 0;
        const noop = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(0);
        expect(noop.newEvents).toBe(0);
      });
    }
  });

  test("failed marked depth-one await-root checkpoints repair exactly once", async () => {
    for (const retainOccurrences of [false, true]) {
      await withFixture(async ({ codexSessionsDir, store, storePath }) => {
        const sessionDir = join(codexSessionsDir, "2026", "07", "15");
        await mkdir(sessionDir, { recursive: true });
        const rootId = "019f6560-0000-7000-8000-000000000061";
        const childId = retainOccurrences
          ? "019f6561-0000-7000-8000-000000000062"
          : "019f6562-0000-7000-8000-000000000063";
        const path = join(
          sessionDir,
          retainOccurrences ? "direct-repair-occurrence.jsonl" : "direct-repair-zero.jsonl",
        );
        await writeFile(path, codexDepthOneLineageRows(rootId, childId, false));
        await ingestOnce(store, { metadataFastPathSupported: () => true });
        const sourceStat = await stat(path);
        const current = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const db = new Database(storePath);
        try {
          if (!retainOccurrences) {
            db.prepare("DELETE FROM event_occurrences WHERE generation_key = ?").run(
              current.generationKey,
            );
            db.run("DELETE FROM events");
          }
          db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
            JSON.stringify(asFailedDepthOneDirectAmbiguity(current.codexStateJson!)),
            current.generationKey,
          );
          db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
        } finally {
          db.close();
        }

        let capturedReads = 0;
        const repaired = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(1);
        expect(repaired.newEvents).toBe(retainOccurrences ? 0 : 1);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            depthOneDirectLineageRepaired: true,
            lineageReplayStage: "ready",
            phase: "live",
            inheritedSuppressedRows: 1,
            billedUsageRows: 1,
            ambiguousQuarantinedRows: 0,
            usageComplete: true,
          }),
        );
        capturedReads = 0;
        const noop = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(0);
        expect(noop.newEvents).toBe(0);
      });
    }
  });

  test("observed lineage-shape failures replace occurrence-backed rows exactly once", async () => {
    for (const shape of ["unclassified-depth-one", "stranded-depth-two"] as const) {
      await withFixture(async ({ codexSessionsDir, store, storePath }) => {
        const sessionDir = join(codexSessionsDir, "2026", "07", "15");
        await mkdir(sessionDir, { recursive: true });
        const rootId = "019f6570-0000-7000-8000-000000000071";
        const parentId = "019f6571-0000-7000-8000-000000000072";
        const childId = "019f6572-0000-7000-8000-000000000073";
        const path = join(sessionDir, `${shape}.jsonl`);
        await writeFile(
          path,
          shape === "unclassified-depth-one"
            ? codexDepthOneLineageRows(rootId, parentId, false)
            : codexDepthTwoLineageRows(rootId, parentId, childId),
        );
        await ingestOnce(store, { metadataFastPathSupported: () => true });
        const sourceStat = await stat(path);
        const current = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const db = new Database(storePath);
        try {
          db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
            JSON.stringify(asFailedObservedLineageShape(current.codexStateJson!, shape)),
            current.generationKey,
          );
          db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
        } finally {
          db.close();
        }

        let capturedReads = 0;
        const repaired = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(1);
        expect(repaired.newEvents).toBe(0);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            threadSourceRoleRepaired: true,
            codexLineageRepaired: true,
            depthOneDirectLineageRepaired: true,
            codexLineageShapeRepaired: true,
            phase: "live",
            ambiguousQuarantinedRows: 0,
            usageComplete: true,
          }),
        );
        capturedReads = 0;
        await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(0);
      });
    }
  });

  test("four deployed Codex v3 checkpoint shapes repair exactly once", async () => {
    const cases = [
      { name: "13-17-minimal-matching-fork", shape: "legacy-matching" },
      { name: "13-42-minimal-absent-fork", shape: "legacy-absent" },
      { name: "13-56-depth-two-without-ancestors", shape: "depth-two" },
      { name: "16-14-depth-one-ready", shape: "depth-one-ready" },
    ] as const;
    for (const fixture of cases) {
      await withFixture(async ({ codexSessionsDir, store, storePath }) => {
        const sessionDir = join(codexSessionsDir, "2026", "07", "15");
        await mkdir(sessionDir, { recursive: true });
        const rootId = "019f6580-0000-7000-8000-000000000081";
        const parentId = "019f6581-0000-7000-8000-000000000082";
        const childId = "019f6582-0000-7000-8000-000000000083";
        const path = join(sessionDir, `${fixture.name}.jsonl`);
        const depthOne = codexDepthOneLineageRows(rootId, parentId, false);
        const rows =
          fixture.shape === "depth-two"
            ? codexDepthTwoWithoutAncestorsRows(rootId, parentId, childId)
            : fixture.shape === "legacy-matching"
              ? withDepthOneMatchingFork(depthOne, rootId)
              : depthOne;
        await writeFile(path, rows);
        await ingestOnce(store, { metadataFastPathSupported: () => true });
        const sourceStat = await stat(path);
        const current = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const db = new Database(storePath);
        try {
          if (fixture.shape === "depth-one-ready") {
            db.prepare("DELETE FROM event_occurrences WHERE generation_key = ?").run(
              current.generationKey,
            );
            db.run("DELETE FROM events");
          }
          const deployed =
            fixture.shape === "legacy-matching" || fixture.shape === "legacy-absent"
              ? asDeployedMinimalV3(current.codexStateJson!)
              : asDeployedAllAmbiguous(
                  current.codexStateJson!,
                  fixture.shape === "depth-two" ? "depth-two-without-ancestors" : "depth-one-ready",
                );
          db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
            JSON.stringify(deployed),
            current.generationKey,
          );
          db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
        } finally {
          db.close();
        }

        let capturedReads = 0;
        const repaired = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(1);
        expect(repaired.newEvents).toBe(
          fixture.shape === "legacy-matching" || fixture.shape === "legacy-absent" ? 0 : 1,
        );
        expect(store.selectEvents()).toHaveLength(1);
        const checkpoint = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            threadSourceRoleRepaired: true,
            codexLineageRepaired: true,
            depthOneDirectLineageRepaired: true,
            codexLineageShapeRepaired: true,
            codexDeployedCheckpointRepaired: true,
            phase: "live",
            ambiguousQuarantinedRows: 0,
            usageComplete: true,
          }),
        );

        capturedReads = 0;
        const noop = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(0);
        expect(noop.newEvents).toBe(0);
      });
    }
  });

  test("same-inode prefix rewrite cannot inherit deployed depth-two repair privilege", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const rootId = "019f6590-0000-7000-8000-000000000091";
      const parentId = "019f6591-0000-7000-8000-000000000092";
      const oldChildId = "019f6592-0000-7000-8000-000000000093";
      const newChildId = "019f6593-0000-7000-8000-000000000094";
      const path = join(sessionDir, "same-inode-depth-two-rewrite.jsonl");
      await writeFile(path, codexDepthTwoWithoutAncestorsRows(rootId, parentId, oldChildId));
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const beforeStat = await stat(path);
      const prior = store.getLatestSourceFile(
        "codex",
        String(beforeStat.dev),
        String(beforeStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const deployed = asDeployedAllAmbiguous(
          prior.codexStateJson!,
          "depth-two-without-ancestors",
        );
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(deployed),
          prior.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }

      await writeFile(path, codexDepthTwoWithoutAncestorsRows(rootId, parentId, newChildId));
      const afterStat = await stat(path);
      expect(String(afterStat.ino)).toBe(String(beforeStat.ino));
      let capturedReads = 0;
      const rewritten = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads++;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(rewritten.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      const current = store.getLatestSourceFile(
        "codex",
        String(afterStat.dev),
        String(afterStat.ino),
      )!;
      expect(current.generationKey).not.toBe(prior.generationKey);
      expect(JSON.parse(current.codexStateJson!)).toEqual(
        expect.objectContaining({
          childSessionId: newChildId,
          lineageDepth: 2,
          lineageReplayStage: "await-parent",
          phase: "ambiguous",
          billedUsageRows: 0,
          ambiguousQuarantinedRows: 2,
          usageComplete: false,
        }),
      );
    });
  });

  test("old zero-occurrence user-role ambiguity repairs once to billed and complete", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "old-user-role.jsonl");
      const sessionId = "019f6510-0000-7000-8000-000000000011";
      await writeFile(sessionPath, codexThreadSourceRows(sessionId));
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        db.prepare("DELETE FROM event_occurrences WHERE generation_key = ?").run(
          current.generationKey,
        );
        db.run("DELETE FROM events");
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(asOldThreadSourceAmbiguity(current.codexStateJson!)),
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const repaired = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads++;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(repaired.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ sessionId, inputTokens: 8, cacheReadTokens: 2 }),
      ]);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({ threadSourceRoleRepaired: true, usageComplete: true }),
      );
    });
  });

  test("genuine role ambiguity replays once then marker presence stays metadata-fast", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "genuine-role-ambiguity.jsonl");
      const sessionId = "019f6511-0000-7000-8000-000000000012";
      await writeFile(sessionPath, codexThreadSourceRows(sessionId, "future-kind"));
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const state = JSON.parse(current.codexStateJson!) as Record<string, unknown>;
        delete state.threadSourceRoleRepaired;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(state),
          current.generationKey,
        );
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const replayed = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads++;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(replayed.newEvents).toBe(0);
      const repaired = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(repaired.codexStateJson!)).toEqual(
        expect.objectContaining({
          threadSourceRoleRepaired: true,
          phase: "ambiguous",
          usageComplete: false,
        }),
      );

      for (const guarded of [true, false, null]) {
        const guardedDb = new Database(storePath);
        try {
          const state = JSON.parse(repaired.codexStateJson!) as Record<string, unknown>;
          state.threadSourceRoleRepaired = guarded;
          guardedDb
            .prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?")
            .run(JSON.stringify(state), repaired.generationKey);
        } finally {
          guardedDb.close();
        }
        capturedReads = 0;
        const noop = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads++;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads).toBe(0);
        expect(noop.newEvents).toBe(0);
      }
    });
  });

  test("old role ambiguity with an occurrence is atomically replaced once", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "15");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "role-occurrence.jsonl");
      const sessionId = "019f6512-0000-7000-8000-000000000013";
      await writeFile(sessionPath, codexThreadSourceRows(sessionId));
      const initial = await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(asOldThreadSourceAmbiguity(current.codexStateJson!)),
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const repaired = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads++;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(repaired.newEvents).toBe(0);
      expect(repaired.mutated).toBe(true);
      expect(repaired.revision).toBe(initial.revision + 1);
      expect(store.selectEvents()).toHaveLength(1);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({ threadSourceRoleRepaired: true, usageComplete: true }),
      );

      capturedReads = 0;
      const noop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads++;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(noop.newEvents).toBe(0);
      expect(noop.revision).toBe(repaired.revision);
    });
  });

  test("legacy incomplete Codex checkpoint replays once under the current parser", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "legacy-incomplete.jsonl");
      const lines = [
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-05T11:00:00.000Z",
          payload: { session_id: "legacy-incomplete", cwd: "/srv/example/demo" },
        }),
        JSON.stringify({
          type: "turn_context",
          timestamp: "2026-07-05T11:00:01.000Z",
          payload: { model: "gpt-5.5-cyber-preview", cwd: "/srv/example/demo" },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-05T11:00:02.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: 10,
                cached_input_tokens: 2,
                output_tokens: 1,
              },
            },
          },
        }),
      ];
      await writeFile(sessionPath, `${lines.join("\n")}\n`);
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;

      // Model the old parser accurately: it reached EOF and persisted an
      // incomplete checkpoint, but never emitted an event or occurrence for
      // the total-only record that the current parser supports.
      const db = new Database(storePath);
      try {
        const legacyState = JSON.parse(checkpoint.codexStateJson!) as Record<string, unknown>;
        legacyState.totals = null;
        legacyState.usageComplete = false;
        legacyState.usageParserVersion = 1;
        delete legacyState.sessionTimestampMs;
        db.prepare("DELETE FROM event_occurrences WHERE generation_key = ?").run(
          checkpoint.generationKey,
        );
        db.run("DELETE FROM events");
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(legacyState),
          checkpoint.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const recovered = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(recovered.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ inputTokens: 8, cacheReadTokens: 2, outputTokens: 1 }),
      ]);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
      const repaired = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(repaired.generationKey).toBe(checkpoint.generationKey);
      expect(JSON.parse(repaired.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageComplete: true,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
        }),
      );
      const inspected = new Database(storePath);
      try {
        const occurrenceCount = inspected
          .query("SELECT COUNT(*) AS count FROM event_occurrences")
          .get() as { count: number };
        expect(occurrenceCount.count).toBe(1);
      } finally {
        inspected.close();
      }

      const revision = recovered.revision;
      capturedReads = 0;
      const noop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(noop.newEvents).toBe(0);
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
    });
  });

  test("partial Codex v1 generation with an occurrence never full-replays", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "partial-v1.jsonl");
      const total = (timestamp: string, input: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp,
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: 0,
                output_tokens: 1,
              },
            },
          },
        });
      await writeFile(
        sessionPath,
        `${total("2026-07-05T11:30:00.000Z", 10)}\n${total("invalid", 20)}\n`,
      );
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const v1State = JSON.parse(current.codexStateJson!) as Record<string, unknown>;
        v1State.usageParserVersion = 1;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(v1State),
          current.generationKey,
        );
      } finally {
        db.close();
      }
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      await appendFile(sessionPath, `${total("2026-07-05T11:30:02.000Z", 30)}\n`);

      let capturedReads = 0;
      const revision = store.getRevision();
      const rejected = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(rejected.newEvents).toBe(0);
      expect(rejected.mutated).toBe(false);
      expect(rejected.revision).toBe(revision);
      expect(
        store.getLatestSourceFile("codex", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(checkpoint);
      expect(store.selectEvents()).toHaveLength(1);
    });
  });

  test("unchanged zero-occurrence Codex v1 with no baseline repairs from zero", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "zero-occurrence-v1.jsonl");
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-05T11:40:00.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: 10,
                cached_input_tokens: 2,
                output_tokens: 1,
              },
            },
          },
        })}\n`,
      );
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const v1State = JSON.parse(current.codexStateJson!) as Record<string, unknown>;
        v1State.usageComplete = true;
        v1State.usageParserVersion = 1;
        v1State.totals = null;
        db.run("DELETE FROM event_occurrences");
        db.run("DELETE FROM events");
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(v1State),
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const repaired = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(repaired.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ inputTokens: 8, cacheReadTokens: 2, outputTokens: 1 }),
      ]);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageComplete: true,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
        }),
      );
    });
  });

  test("genuine empty Codex v3 checkpoint remains complete and metadata-fast", async () => {
    await withFixture(async ({ codexSessionsDir, store }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "empty-v2.jsonl");
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-05T11:42:00.000Z",
          payload: { session_id: "empty-v2", cwd: "/srv/example/demo" },
        })}\n`,
      );
      const initial = await ingestOnce(store, { metadataFastPathSupported: () => true });
      expect(initial.newEvents).toBe(0);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          totals: null,
          usageComplete: true,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
        }),
      );

      let capturedReads = 0;
      const revision = initial.revision;
      const noop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(noop.newEvents).toBe(0);
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
      expect(
        store.getLatestSourceFile("codex", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(checkpoint);
      expect(store.getProviderStates().find((state) => state.source === "codex")?.complete).toBe(
        true,
      );
    });
  });

  test("current Codex v3 with occurrences and no baseline stays fail-closed", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "complete-v1-missing-baseline.jsonl");
      const total = (input: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-05T11:45:00.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: 0,
                output_tokens: 1,
              },
            },
          },
        });
      await writeFile(sessionPath, `${total(10)}\n`);
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const unsafe = JSON.parse(current.codexStateJson!) as Record<string, unknown>;
        unsafe.usageComplete = true;
        unsafe.usageParserVersion = 3;
        unsafe.totals = null;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(unsafe),
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      await appendFile(sessionPath, `${total(30)}\n`);

      let capturedReads = 0;
      const revision = store.getRevision();
      const rejected = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(rejected.newEvents).toBe(0);
      expect(rejected.mutated).toBe(true);
      expect(rejected.revision).toBe(revision + 1);
      expect(
        store.getLatestSourceFile("codex", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(checkpoint);
      expect(store.selectEvents()).toHaveLength(1);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("incomplete Codex v3 with occurrences and no baseline stays fail-closed", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "incomplete-v2-missing-baseline.jsonl");
      const total = (input: number) =>
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-05T11:50:00.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: 0,
                output_tokens: 1,
              },
            },
          },
        });
      await writeFile(sessionPath, `${total(10)}\n`);
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const retainedEvent = store.selectEvents()[0]!;
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const unsafe = JSON.parse(current.codexStateJson!) as Record<string, unknown>;
        unsafe.usageComplete = false;
        unsafe.usageParserVersion = 3;
        unsafe.totals = null;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(unsafe),
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      await appendFile(sessionPath, `${total(30)}\n`);

      let capturedReads = 0;
      const revision = store.getRevision();
      const rejected = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(rejected.newEvents).toBe(0);
      expect(rejected.mutated).toBe(true);
      expect(rejected.revision).toBe(revision + 1);
      expect(
        store.getLatestSourceFile("codex", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(checkpoint);
      expect(store.selectEvents()).toEqual([retainedEvent]);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("Codex migration decision matrix preserves or replaces atomically and is idempotent", async () => {
    const cases = [
      {
        name: "t3-occurrence",
        occurrences: true,
        expectedRead: 1,
        expectedNewEvents: 0,
        mutate(checkpoint: Record<string, unknown>) {
          checkpoint.usageParserVersion = 3;
          checkpoint.usageComplete = false;
          delete checkpoint.semanticUsageComplete;
          delete checkpoint.defectLineCount;
          delete checkpoint.defectLineNumbers;
        },
      },
      {
        name: "t3-zero-occurrence",
        occurrences: false,
        expectedRead: 1,
        expectedNewEvents: 1,
        mutate(checkpoint: Record<string, unknown>) {
          checkpoint.usageParserVersion = 3;
          checkpoint.usageComplete = false;
          delete checkpoint.semanticUsageComplete;
          delete checkpoint.defectLineCount;
          delete checkpoint.defectLineNumbers;
        },
      },
      {
        name: "lifecycle-current-marker",
        occurrences: true,
        expectedRead: 1,
        expectedNewEvents: 0,
        mutate(checkpoint: Record<string, unknown>) {
          delete checkpoint.threadSourceRoleRepaired;
          Object.assign(checkpoint, asOldThreadSourceAmbiguity(JSON.stringify(checkpoint)));
        },
      },
      {
        name: "stale-version-new-honesty-shape",
        occurrences: true,
        expectedRead: 0,
        expectedNewEvents: 0,
        mutate(checkpoint: Record<string, unknown>) {
          checkpoint.usageParserVersion = 1;
          checkpoint.usageComplete = false;
        },
      },
      {
        name: "stale-no-baseline",
        occurrences: true,
        expectedRead: 0,
        expectedNewEvents: 0,
        mutate(checkpoint: Record<string, unknown>) {
          checkpoint.usageParserVersion = 3;
          checkpoint.usageComplete = false;
          checkpoint.totals = null;
        },
      },
      {
        name: "future-version",
        occurrences: true,
        expectedRead: 0,
        expectedNewEvents: 0,
        mutate(checkpoint: Record<string, unknown>) {
          checkpoint.usageParserVersion = CODEX_USAGE_PARSER_VERSION + 1;
          checkpoint.usageComplete = false;
        },
      },
    ] as const;

    for (const [caseIndex, fixture] of cases.entries()) {
      await withFixture(async ({ codexSessionsDir, store, storePath }) => {
        const sessionDir = join(codexSessionsDir, "2026", "07", "16");
        await mkdir(sessionDir, { recursive: true });
        const sessionId = `019f7100-0000-7000-8000-${String(caseIndex + 1).padStart(12, "0")}`;
        const sessionPath = join(sessionDir, `${fixture.name}.jsonl`);
        await writeFile(sessionPath, codexThreadSourceRows(sessionId));
        const initial = await ingestOnce(store, { metadataFastPathSupported: () => true });
        const sourceStat = await stat(sessionPath);
        const current = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const checkpoint = JSON.parse(current.codexStateJson!) as Record<string, unknown>;
        fixture.mutate(checkpoint);
        const seededJson = JSON.stringify(checkpoint);
        const db = new Database(storePath);
        try {
          if (!fixture.occurrences) {
            db.prepare("DELETE FROM event_occurrences WHERE generation_key = ?").run(
              current.generationKey,
            );
            db.run("DELETE FROM events");
          }
          db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
            seededJson,
            current.generationKey,
          );
          db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
        } finally {
          db.close();
        }
        const seeded = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        const revision = store.getRevision();
        let capturedReads = 0;
        const decided = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads += 1;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads, fixture.name).toBe(fixture.expectedRead);
        expect(decided.newEvents, fixture.name).toBe(fixture.expectedNewEvents);
        expect(decided.mutated, fixture.name).toBe(true);
        expect(decided.revision, fixture.name).toBe(revision + 1);
        expect(
          store.getProviderStates().find((state) => state.source === "codex")?.complete,
          fixture.name,
        ).toBe(true);
        expect(
          store.buildAuthoritativeTokenSnapshot(Date.now()).providers.codex.discoveryComplete,
          fixture.name,
        ).toBe(fixture.expectedRead === 1);

        const after = store.getLatestSourceFile(
          "codex",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(after.generationKey, fixture.name).toBe(current.generationKey);
        if (fixture.expectedRead === 1) {
          expect(after.codexStateJson, fixture.name).not.toBe(seededJson);
          expect(JSON.parse(after.codexStateJson!), fixture.name).toEqual(
            expect.objectContaining({
              usageParserVersion: CODEX_USAGE_PARSER_VERSION,
              usageComplete: true,
            }),
          );
          expect(store.selectEvents(), fixture.name).toHaveLength(1);
        } else {
          expect(after, fixture.name).toEqual(seeded);
        }

        capturedReads = 0;
        const settledRevision = decided.revision;
        const second = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads += 1;
          },
          metadataFastPathSupported: () => true,
        });
        expect(capturedReads, fixture.name).toBe(0);
        expect(second.mutated, fixture.name).toBe(false);
        expect(second.revision, fixture.name).toBe(settledRevision);
        expect(second.newEvents, fixture.name).toBe(0);
        expect(initial.newEvents, fixture.name).toBe(1);
      });
    }
  });

  test("complete Codex v1 checkpoint replays one appended row then becomes bounded", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "05");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "complete-v1-append.jsonl");
      const timestampMs = Date.parse("2026-07-05T12:00:00.000Z");
      const prefix = timestampMs.toString(16).padStart(12, "0");
      const sessionId = `${prefix.slice(0, 8)}-${prefix.slice(8)}-7000-8000-000000000003`;
      const total = (input: number, cached: number, output: number, timestamp?: string) =>
        JSON.stringify({
          type: "event_msg",
          ...(timestamp ? { timestamp } : {}),
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: input,
                cached_input_tokens: cached,
                output_tokens: output,
              },
            },
          },
        });
      await writeFile(
        sessionPath,
        `${[
          JSON.stringify({ type: "session_meta", payload: { session_id: sessionId } }),
          JSON.stringify({
            type: "turn_context",
            payload: { model: "gpt-5.5-cyber-preview", cwd: "/srv/example/demo" },
          }),
          total(10, 2, 1, "2026-07-05T12:00:01.000Z"),
        ].join("\n")}\n`,
      );
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const originalEvent = store.selectEvents()[0]!;
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const v1State = JSON.parse(checkpoint.codexStateJson!) as Record<string, unknown>;
        v1State.usageParserVersion = 1;
        delete v1State.sessionTimestampMs;
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify(v1State),
          checkpoint.generationKey,
        );
      } finally {
        db.close();
      }
      await appendFile(sessionPath, `${total(30, 7, 4)}\n`);

      let capturedReads = 0;
      const upgraded = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(upgraded.newEvents).toBe(1);
      const events = store.selectEvents();
      expect(events).toHaveLength(2);
      expect(events.find((event) => event.lineOffset === originalEvent.lineOffset)).toEqual(
        originalEvent,
      );
      expect(events).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ inputTokens: 8, cacheReadTokens: 2, outputTokens: 1 }),
          expect.objectContaining({
            ts: timestampMs,
            date: "2026-07-05",
            inputTokens: 15,
            cacheReadTokens: 5,
            outputTokens: 3,
          }),
        ]),
      );
      const repaired = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(repaired.generationKey).toBe(checkpoint.generationKey);
      expect(JSON.parse(repaired.codexStateJson!)).toEqual(
        expect.objectContaining({
          sessionTimestampMs: timestampMs,
          usageComplete: true,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
        }),
      );

      const revision = upgraded.revision;
      capturedReads = 0;
      const noop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(noop.newEvents).toBe(0);
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
    });
  });

  test("invalid Codex checkpoint with an existing occurrence stays fail-closed", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "06");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "malformed-checkpoint.jsonl");
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-06T10:00:00.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: 10,
                cached_input_tokens: 2,
                output_tokens: 1,
              },
            },
          },
        })}\n`,
      );
      await ingestOnce(store, { metadataFastPathSupported: () => true });

      const db = new Database(storePath);
      try {
        db.run(`UPDATE source_files SET codex_state_json = '{"usageComplete":true}'`);
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      await appendFile(sessionPath, "{}\n");

      let capturedReads = 0;
      const revision = store.getRevision();
      const rejected = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(rejected.newEvents).toBe(0);
      expect(rejected.mutated).toBe(true);
      expect(rejected.revision).toBe(revision + 1);
      expect(
        store.getLatestSourceFile("codex", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(checkpoint);
      expectStructurallyHealthyWindowIncomplete(store, "codex");
    });
  });

  test("invalid Codex checkpoint without occurrences replays and repairs from zero", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "06");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "invalid-empty-generation.jsonl");
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-06T11:00:00.000Z",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: 10,
                cached_input_tokens: 2,
                output_tokens: 1,
              },
            },
          },
        })}\n`,
      );
      await ingestOnce(store, { metadataFastPathSupported: () => true });
      const db = new Database(storePath);
      try {
        db.run("DELETE FROM event_occurrences");
        db.run("DELETE FROM events");
        db.run(`UPDATE source_files SET codex_state_json = '{"usageComplete":true}'`);
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'codex'");
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const repaired = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(repaired.newEvents).toBe(1);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ inputTokens: 8, cacheReadTokens: 2, outputTokens: 1 }),
      ]);
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "codex",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          usageComplete: true,
          usageParserVersion: CODEX_USAGE_PARSER_VERSION,
        }),
      );
    });
  });

  test("does not dedupe codex token_count rows by request_id", async () => {
    await withFixture(async ({ codexSessionsDir, store, storePath }) => {
      const sessionDir = join(codexSessionsDir, "2026", "07", "01");
      await mkdir(sessionDir, { recursive: true });
      const sessionPath = join(sessionDir, "codex-request-id.jsonl");
      const requestId = "req-shared-codex";
      const lines = [
        JSON.stringify({
          type: "session_meta",
          timestamp: "2026-07-01T10:00:00.000Z",
          payload: {
            session_id: "codex-repeat-session",
            cwd: "/srv/example/example-repo/sigmashake-tokens",
          },
        }),
        JSON.stringify({
          type: "turn_context",
          timestamp: "2026-07-01T10:00:01.000Z",
          payload: {
            model: "gpt-5.5-cyber-preview",
            cwd: "/srv/example/example-repo/sigmashake-tokens",
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-01T10:00:02.000Z",
          payload: {
            request_id: requestId,
            type: "token_count",
            info: {
              last_token_usage: {
                input_tokens: 100,
                cached_input_tokens: 20,
                output_tokens: 10,
                total_tokens: 110,
              },
              total_token_usage: {
                input_tokens: 100,
                cached_input_tokens: 20,
                output_tokens: 10,
                total_tokens: 110,
              },
            },
          },
        }),
        JSON.stringify({
          type: "event_msg",
          timestamp: "2026-07-01T10:00:03.000Z",
          payload: {
            request_id: requestId,
            type: "token_count",
            info: {
              last_token_usage: {
                input_tokens: 200,
                cached_input_tokens: 40,
                output_tokens: 30,
                total_tokens: 230,
              },
              total_token_usage: {
                input_tokens: 200,
                cached_input_tokens: 40,
                output_tokens: 30,
                total_tokens: 230,
              },
            },
          },
        }),
      ].join("\n");
      await writeFile(sessionPath, `${lines}\n`);

      const summary = await ingestOnce(store);
      expect(summary.files).toBe(1);
      expect(summary.newEvents).toBe(2);

      const db = new Database(storePath);
      try {
        const dedupeRows = db
          .query("SELECT dedupe_key FROM events WHERE file_path = ? ORDER BY line_offset")
          .all(sessionPath) as Array<{ dedupe_key: string | null }>;
        expect(dedupeRows.length).toBe(2);
        expect(dedupeRows.every((r) => r.dedupe_key === null)).toBe(true);
      } finally {
        db.close();
      }

      const events = store.selectEvents();
      const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
      expect(agg.byHarness.find((h) => h.harness === "codex")?.messages).toBe(2);
      expect(agg.totals.messages).toBe(2);
      expect(agg.totals.totalTokens).toBe(230);
    });
  });

  test("incremental ingest only parses new appended lines", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "abc.jsonl");
      const a = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 50 },
        },
      });
      await writeFile(sessionPath, `${a}\n`);
      let capturedReads = 0;
      const hooks = {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      };
      const first = await ingestOnce(store, hooks);
      expect(first.newEvents).toBe(1);
      expect(capturedReads).toBe(1);

      // A committed complete file takes the metadata-only path. No content is
      // read, even after the database checkpoint has become authoritative.
      const noop = await ingestOnce(store, hooks);
      expect(noop.newEvents).toBe(0);
      expect(capturedReads).toBe(1);

      // Append a second message — only the new bytes should be parsed.
      const b = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T11:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 30, output_tokens: 200 },
        },
      });
      await appendFile(sessionPath, `${b}\n`);
      const second = await ingestOnce(store, hooks);
      expect(second.newEvents).toBe(1);
      expect(second.events[0]?.outputTokens).toBe(200);
      expect(capturedReads).toBe(2);

      const events = store.selectEvents();
      expect(events.length).toBe(2);
    });
  });

  test("filesystem trust predicate is an explicit local Linux whitelist", () => {
    expect(_metadataFilesystemTrustedForTest(0xef53n, "ext4", "linux")).toBe(true);
    expect(_metadataFilesystemTrustedForTest(0x58465342n, "xfs", "linux")).toBe(true);
    expect(_metadataFilesystemTrustedForTest(0x9123683en, "btrfs", "linux")).toBe(true);
    expect(_metadataFilesystemTrustedForTest(0x01021994n, "tmpfs", "linux")).toBe(true);
    expect(_metadataFilesystemTrustedForTest(0xef53n, "ext3", "linux")).toBe(false);
    expect(_metadataFilesystemTrustedForTest(0x65735546n, "fuse", "linux")).toBe(false);
    expect(_metadataFilesystemTrustedForTest(0x6969n, "nfs", "linux")).toBe(false);
    expect(_metadataFilesystemTrustedForTest(0x794c7630n, "overlay", "linux")).toBe(false);
    expect(_metadataFilesystemTrustedForTest(0xef53n, "ext4", "darwin")).toBe(false);
    expect(_metadataFilesystemTrustedForTest(0x58465342n, "ext4", "linux")).toBe(false);
  });

  test("unsupported filesystem always verifies the committed prefix", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      await writeFile(
        join(projectDir, "unsupported-fs.jsonl"),
        `${JSON.stringify({
          type: "assistant",
          timestamp: "2026-05-17T10:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20 },
          },
        })}\n`,
      );
      let capturedReads = 0;
      const hooks = {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => false,
      };
      expect((await ingestOnce(store, hooks)).newEvents).toBe(1);
      const revision = store.getRevision();
      const verifiedNoop = await ingestOnce(store, hooks);
      expect(capturedReads).toBe(2);
      expect(verifiedNoop.newEvents).toBe(0);
      expect(verifiedNoop.mutated).toBe(false);
      expect(verifiedNoop.revision).toBe(revision);
    });
  });

  test("same-size rewrite is reconciled when millisecond timestamps match", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "same-size-rewrite.jsonl");
      const row = (inputTokens: number): string =>
        JSON.stringify({
          type: "assistant",
          timestamp: "2026-05-17T10:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: inputTokens, output_tokens: 20 },
          },
        });
      const original = `${row(10)}\n`;
      const rewritten = `${row(90)}\n`;
      expect(Buffer.byteLength(rewritten)).toBe(Buffer.byteLength(original));
      await writeFile(sessionPath, original);
      const forcedMtime = new Date("2026-05-17T09:00:00.123Z");
      await utimes(sessionPath, forcedMtime, forcedMtime);
      await ingestOnce(store, { metadataFastPathSupported: () => true });

      const originalStat = await stat(sessionPath, { bigint: true });
      const originalState = store.getLatestSourceFile(
        "claude",
        originalStat.dev.toString(),
        originalStat.ino.toString(),
      );
      if (!originalState) throw new Error("missing committed source state");
      if (originalState.ctimeNs === null) throw new Error("missing committed source ctime");
      expect(originalState.mtimeNs).toBe(originalStat.mtimeNs.toString());
      expect(originalState.ctimeNs).toBe(originalStat.ctimeNs.toString());

      await new Promise((resolve) => setTimeout(resolve, 10));
      await writeFile(sessionPath, rewritten);
      await utimes(sessionPath, forcedMtime, forcedMtime);
      const rewrittenStat = await stat(sessionPath, { bigint: true });
      expect(rewrittenStat.size).toBe(originalStat.size);
      expect(rewrittenStat.mtimeNs).toBe(originalStat.mtimeNs);
      expect(rewrittenStat.ctimeNs).not.toBe(BigInt(originalState.ctimeNs));

      let capturedReads = 0;
      const reconciled = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(1);
      expect(reconciled.newEvents).toBe(1);
      // Rewrites create a new immutable source generation; the prior observed
      // generation remains historical truth instead of being silently
      // overwritten or double-read under the old checkpoint.
      expect(store.selectEvents().map((event) => event.inputTokens)).toEqual([10, 90]);
      expect(
        store.getLatestSourceFile(
          "claude",
          rewrittenStat.dev.toString(),
          rewrittenStat.ino.toString(),
        )?.generationKey,
      ).not.toBe(originalState.generationKey);
    });
  });

  test("collapses hardlink aliases while retaining both occurrence paths", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const pathA = join(projectDir, "alias-a.jsonl");
      const pathB = join(projectDir, "alias-b.jsonl");
      const row = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          content: "immutable content",
          usage: { input_tokens: 10, output_tokens: 20 },
        },
      });
      await writeFile(pathA, `${row}\n`);
      await link(pathA, pathB);

      const summary = await ingestOnce(store);
      expect(summary.files).toBe(1);
      expect(store.selectEvents().length).toBe(1);

      const db = new Database(storePath);
      try {
        const occurrences = db
          .query("SELECT observed_path FROM event_occurrences ORDER BY observed_path")
          .all() as Array<{ observed_path: string }>;
        expect(occurrences.map((entry) => entry.observed_path)).toEqual([pathA, pathB]);
      } finally {
        db.close();
      }
    });
  });

  test("aggregates an exact canonical copy once but preserves both generations", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectA = join(projectsDir, "copy-a");
      const projectB = join(projectsDir, "copy-b");
      await mkdir(projectA, { recursive: true });
      await mkdir(projectB, { recursive: true });
      const row = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 20 },
        },
      });
      await writeFile(join(projectA, "same-session.jsonl"), `${row}\n`);
      await writeFile(join(projectB, "same-session.jsonl"), `${row}\n`);

      await ingestOnce(store);
      expect(store.selectEvents().length).toBe(1);
      const db = new Database(storePath);
      try {
        const count = db.query("SELECT COUNT(*) AS c FROM event_occurrences").get() as {
          c: number;
        };
        expect(count.c).toBe(2);
      } finally {
        db.close();
      }
    });
  });

  test("immutable record conflict rolls back events and checkpoint", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const base = {
        type: "assistant",
        uuid: "immutable-record-1",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 20 },
        },
      };
      const conflict = {
        ...base,
        message: {
          ...base.message,
          content: "conflicting immutable content",
          usage: { input_tokens: 11, output_tokens: 20 },
        },
      };
      await writeFile(
        join(projectDir, "conflict.jsonl"),
        `${JSON.stringify(base)}\n${JSON.stringify(conflict)}\n`,
      );

      await expect(ingestOnce(store)).rejects.toThrow("immutable token record conflict");
      expect(store.selectEvents()).toEqual([]);
      expect(store.stats().files).toBe(0);
      const claude = store.getProviderStates().find((state) => state.source === "claude");
      expect(claude?.available).toBe(true);
      expect(claude?.complete).toBe(false);
    });
  });

  test("retries a trailing partial line and does not revise a true no-op scan", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "partial.jsonl");
      const row = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/démø",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 20 },
        },
      });
      await writeFile(sessionPath, row);
      let capturedReads = 0;
      const hooks = {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      };
      const partial = await ingestOnce(store, hooks);
      expect(partial.newEvents).toBe(0);
      expect(capturedReads).toBe(1);

      // A partial final line cannot use the no-content-read fast path: the
      // same bytes must remain eligible for completion by a later newline.
      const unchangedPartial = await ingestOnce(store, hooks);
      expect(unchangedPartial.newEvents).toBe(0);
      expect(capturedReads).toBe(2);

      await appendFile(sessionPath, "\n");
      const completed = await ingestOnce(store, hooks);
      expect(completed.newEvents).toBe(1);
      expect(capturedReads).toBe(3);
      const revision = completed.revision;
      const noop = await ingestOnce(store, hooks);
      expect(noop.newEvents).toBe(0);
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
      expect(capturedReads).toBe(3);
    });
  });

  test("truncate then regrow creates a generation without double-billing repeated history", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "rewrite.jsonl");
      const row = (timestamp: string, input: number): string =>
        JSON.stringify({
          type: "assistant",
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: input, output_tokens: 20 },
          },
        });
      const original = row("2026-05-17T10:00:00.000Z", 10);
      await writeFile(sessionPath, `${original}\n`);
      await ingestOnce(store);

      await writeFile(sessionPath, "");
      const truncated = await ingestOnce(store);
      expect(truncated.newEvents).toBe(0);

      const added = row("2026-05-17T11:00:00.000Z", 30);
      await writeFile(sessionPath, `${original}\n${added}\n`);
      const regrown = await ingestOnce(store);
      expect(regrown.newEvents).toBe(1);
      const events = store.selectEvents();
      expect(events).toHaveLength(2);
      expect(events.reduce((sum, event) => sum + event.inputTokens, 0)).toBe(40);
    });
  });

  test("pre/post fstat rejects a file mutated after its captured-size read", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "moving.jsonl");
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "assistant",
          timestamp: "2026-05-17T10:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20 },
          },
        })}\n`,
      );
      let changed = false;
      const summary = await ingestOnce(store, {
        afterCapturedRead: async (path) => {
          if (path === sessionPath && !changed) {
            changed = true;
            await appendFile(path, "{}\n");
          }
        },
      });
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toEqual([]);
      expect(store.stats().files).toBe(0);
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        false,
      );
    });
  });

  test("a later batch failure rolls back earlier events and checkpoints atomically", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-scan-atomic-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const projectDir = join(projectsDir, "-home-user-ssd-demo");
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexSessionsDir, { recursive: true });
    const row = (id: string, input: number) =>
      JSON.stringify({
        type: "assistant",
        requestId: id,
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: input, output_tokens: 20 },
        },
      });
    await writeFile(join(projectDir, "a.jsonl"), `${row("atomic-a", 10)}\n`);
    await writeFile(join(projectDir, "b.jsonl"), `${row("atomic-b", 30)}\n`);
    const oldClaude = process.env.CLAUDE_PROJECTS_DIR;
    const oldCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    const store = new FailLaterCommitStore(join(root, "tokens.sqlite"));
    try {
      const failed = await ingestOnce(store);
      expect(failed.newEvents).toBe(0);
      expect(store.selectEvents()).toEqual([]);
      expect(store.stats().files).toBe(0);
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        false,
      );

      const retried = await ingestOnce(store);
      expect(retried.newEvents).toBe(2);
      expect(
        store
          .selectEvents()
          .map((event) => event.inputTokens)
          .sort(),
      ).toEqual([10, 30]);
      expect(store.stats().files).toBe(2);
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        true,
      );
    } finally {
      store.close();
      if (oldClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = oldClaude;
      if (oldCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = oldCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("Codex parser state is published only after a successful DB commit", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-codex-rollback-"));
    const projectsDir = join(root, "projects");
    const codexSessionsDir = join(root, "codex-sessions");
    const sessionDir = join(codexSessionsDir, "2026", "07", "11");
    await mkdir(projectsDir, { recursive: true });
    await mkdir(sessionDir, { recursive: true });
    const sessionPath = join(sessionDir, "rollback.jsonl");
    const lines = [
      {
        type: "session_meta",
        timestamp: "2026-07-11T10:00:00.000Z",
        payload: { session_id: "rollback-session", cwd: "/srv/example/demo" },
      },
      {
        type: "turn_context",
        timestamp: "2026-07-11T10:00:01.000Z",
        payload: { model: "gpt-5.5-cyber-preview", cwd: "/srv/example/demo" },
      },
      {
        type: "event_msg",
        timestamp: "2026-07-11T10:00:02.000Z",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 100,
              cached_input_tokens: 20,
              output_tokens: 10,
              reasoning_output_tokens: 2,
            },
          },
        },
      },
    ];
    await writeFile(sessionPath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`);
    const previousClaude = process.env.CLAUDE_PROJECTS_DIR;
    const previousCodex = process.env.CODEX_SESSIONS_DIR;
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
    _resetScannerStateForTest();
    const store = new FailFirstCommitStore(join(root, "tokens.sqlite"));
    try {
      const failed = await ingestOnce(store);
      expect(failed.newEvents).toBe(0);
      expect(store.selectEvents()).toEqual([]);
      expect(_scannerCodexCacheSizeForTest()).toBe(0);
      expect(_scannerCodexCacheKeysForTest()).toEqual([]);

      const retried = await ingestOnce(store);
      expect(retried.newEvents).toBe(1);
      expect(_scannerCodexCacheSizeForTest()).toBe(1);
      expect(store.selectEvents()[0]?.inputTokens).toBe(80);
      expect(store.selectEvents()[0]?.cacheReadTokens).toBe(20);
      const originalStat = await stat(sessionPath);
      const originalGeneration = store.getLatestSourceFile(
        "codex",
        String(originalStat.dev),
        String(originalStat.ino),
      )?.generationKey;
      expect(originalGeneration).toBeDefined();
      expect(_scannerCodexCacheKeysForTest()).toEqual([originalGeneration]);

      const unrelatedPath = join(sessionDir, "z-unrelated.jsonl");
      const unrelated = lines.map((line, index) =>
        index === 0
          ? {
              ...line,
              payload: { session_id: "unrelated-session", cwd: "/srv/example/unrelated" },
            }
          : line,
      );
      await writeFile(
        unrelatedPath,
        `${unrelated.map((line) => JSON.stringify(line)).join("\n")}\n`,
      );
      await ingestOnce(store);
      const unrelatedStat = await stat(unrelatedPath);
      const unrelatedGeneration = store.getLatestSourceFile(
        "codex",
        String(unrelatedStat.dev),
        String(unrelatedStat.ino),
      )?.generationKey;
      expect(unrelatedGeneration).toBeDefined();
      expect(_scannerCodexCacheKeysForTest()).toEqual(
        [originalGeneration, unrelatedGeneration].sort(),
      );

      const rewritten = lines.map((line, index) =>
        index === 0
          ? {
              ...line,
              payload: { session_id: "rollback-session-2", cwd: "/srv/example/demo" },
            }
          : line,
      );
      await writeFile(sessionPath, `${rewritten.map((line) => JSON.stringify(line)).join("\n")}\n`);
      store.failNextCapturedCommit();
      await ingestOnce(store);
      expect(_scannerCodexCacheKeysForTest()).toEqual(
        [originalGeneration, unrelatedGeneration].sort(),
      );

      await ingestOnce(store);
      const rewrittenStat = await stat(sessionPath);
      const currentGeneration = store.getLatestSourceFile(
        "codex",
        String(rewrittenStat.dev),
        String(rewrittenStat.ino),
      )?.generationKey;
      expect(currentGeneration).toBeDefined();
      expect(currentGeneration).not.toBe(originalGeneration);
      expect(_scannerCodexCacheKeysForTest()).toEqual(
        [currentGeneration, unrelatedGeneration].sort(),
      );
      expect(_scannerCodexCacheKeysForTest()).not.toContain(originalGeneration);
    } finally {
      _resetScannerStateForTest();
      store.close();
      if (previousClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
      else process.env.CLAUDE_PROJECTS_DIR = previousClaude;
      if (previousCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
      else process.env.CODEX_SESSIONS_DIR = previousCodex;
      await rm(root, { recursive: true, force: true });
    }
  });

  test("date range filter trims aggregated events", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "abc.jsonl");
      const rows = [
        { ts: "2026-05-01T00:00:00.000Z", out: 100 },
        { ts: "2026-05-10T00:00:00.000Z", out: 200 },
        { ts: "2026-05-20T00:00:00.000Z", out: 400 },
      ].map((r) =>
        JSON.stringify({
          type: "assistant",
          timestamp: r.ts,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 1, output_tokens: r.out },
          },
        }),
      );
      await writeFile(sessionPath, `${rows.join("\n")}\n`);
      await ingestOnce(store);

      const fromTs = Date.parse("2026-05-05T00:00:00Z");
      const toTs = Date.parse("2026-05-15T23:59:59Z");
      const events = store.selectEvents({ fromTs, toTs });
      const ranged = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
      expect(ranged.totals.outputTokens).toBe(200);
    });
  });

  test("Claude derives cache creation when zero declared total contradicts a positive breakdown", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "cache-creation-zero-total.jsonl");
      const row = (requestId: string, timestamp: string, usage: Record<string, unknown>): string =>
        JSON.stringify({
          type: "assistant",
          requestId,
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20, ...usage },
          },
        });
      await writeFile(
        sessionPath,
        `${[
          row("observed-zero-total", "2026-07-12T10:00:00.000Z", {
            cache_creation_input_tokens: 0,
            cache_creation: { ephemeral_1h_input_tokens: 6296 },
          }),
          row("absent-total", "2026-07-12T10:00:01.000Z", {
            cache_creation: {
              ephemeral_5m_input_tokens: 2,
              ephemeral_1h_input_tokens: 3,
            },
          }),
        ].join("\n")}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(2);
      expect(store.selectEvents()).toEqual([
        expect.objectContaining({ cacheWrite5mTokens: 0, cacheWrite1hTokens: 6296 }),
        expect.objectContaining({ cacheWrite5mTokens: 2, cacheWrite1hTokens: 3 }),
      ]);
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        true,
      );
    });
  });

  test("Claude keeps nonzero cache-creation contradictions fail-closed", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      await writeFile(
        join(projectDir, "cache-creation-contradiction.jsonl"),
        `${JSON.stringify({
          type: "assistant",
          requestId: "contradictory-total",
          timestamp: "2026-07-12T10:10:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: {
              input_tokens: 10,
              output_tokens: 20,
              cache_creation_input_tokens: 5,
              cache_creation: {
                ephemeral_5m_input_tokens: 4,
                ephemeral_1h_input_tokens: 6,
              },
            },
          },
        })}\n`,
      );

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(0);
      expectStructurallyHealthyWindowIncomplete(store, "claude");
    });
  });

  test("old incomplete Claude checkpoint replays atomically and heals discovery", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-parser-upgrade.jsonl");
      const row = (requestId: string, timestamp: string, usage: Record<string, unknown>) =>
        JSON.stringify({
          type: "assistant",
          requestId,
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20, ...usage },
          },
        });
      await writeFile(
        sessionPath,
        `${[
          row("upgrade-restored", "2026-07-12T11:00:00.000Z", {
            cache_creation_input_tokens: 0,
            cache_creation: { ephemeral_1h_input_tokens: 6296 },
          }),
          row("upgrade-accepted-a", "2026-07-12T11:00:01.000Z", {}),
          row("upgrade-accepted-b", "2026-07-12T11:00:02.000Z", { input_tokens: 20 }),
        ].join("\n")}\n`,
      );
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const missingLegacyPath = join(projectDir, "expired-missing.jsonl");
      const db = new Database(storePath);
      try {
        const dropped = db
          .query("SELECT id FROM events WHERE file_path = ? AND line_offset = 0")
          .get(sessionPath) as { id: number };
        db.prepare("DELETE FROM event_occurrences WHERE event_id = ?").run(dropped.id);
        db.prepare("DELETE FROM events WHERE id = ?").run(dropped.id);
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify({ usageParserVersion: 1, usageComplete: false }),
          checkpoint.generationKey,
        );
        db.prepare(
          "INSERT INTO legacy_reconciliation_pending (path, source, reason) VALUES (?, 'claude', 'unreadable')",
        ).run(missingLegacyPath);
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'claude'");
      } finally {
        db.close();
      }

      const healed = await ingestOnce(store);
      expect(healed.newEvents).toBe(1);
      const events = store.selectEvents();
      expect(events).toHaveLength(3);
      expect(events.filter((event) => event.cacheWrite1hTokens === 6296)).toHaveLength(1);
      expect(events.filter((event) => event.inputTokens === 10)).toHaveLength(2);
      expect(events.filter((event) => event.inputTokens === 20)).toHaveLength(1);
      const repaired = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(JSON.parse(repaired.codexStateJson!)).toEqual({
        usageParserVersion: CLAUDE_USAGE_PARSER_VERSION,
        semanticUsageComplete: true,
        defectLineCount: 0,
        defectLineNumbers: [],
        usageComplete: true,
      });
      expect(legacyPendingReason(storePath, missingLegacyPath)).toBe("expired-missing");
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        true,
      );
      const inspected = new Database(storePath, { readonly: true });
      try {
        expect(
          inspected
            .query("SELECT COUNT(*) AS count FROM event_occurrences WHERE generation_key = ?")
            .get(checkpoint.generationKey),
        ).toEqual({ count: 3 });
      } finally {
        inspected.close();
      }
    });
  });

  test("pre-contract Claude checkpoint with absent completeness replays exactly once", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-pre-contract.jsonl");
      const affected = JSON.stringify({
        type: "assistant",
        requestId: "pre-contract-restored",
        timestamp: "2026-07-12T12:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: {
            input_tokens: 10,
            output_tokens: 20,
            cache_creation_input_tokens: 0,
            cache_creation: { ephemeral_1h_input_tokens: 6296 },
          },
        },
      });
      const accepted = JSON.stringify({
        type: "assistant",
        requestId: "pre-contract-accepted",
        timestamp: "2026-07-12T12:00:01.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 30, output_tokens: 40 },
        },
      });
      await writeFile(sessionPath, `${affected}\n${accepted}\n`);
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        db.run("DELETE FROM event_occurrences");
        db.run("DELETE FROM events");
        db.prepare("UPDATE source_files SET codex_state_json = '{}' WHERE generation_key = ?").run(
          checkpoint.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'claude'");
      } finally {
        db.close();
      }

      const healed = await ingestOnce(store);
      expect(healed.newEvents).toBe(2);
      expect(store.selectEvents()).toHaveLength(2);
      expect(
        store.selectEvents().filter((event) => event.cacheWrite1hTokens === 6296),
      ).toHaveLength(1);
      const retry = await ingestOnce(store, { metadataFastPathSupported: () => true });
      expect(retry.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(2);
    });
  });

  test("corrupt Claude checkpoint with occurrences replays once and heals", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-corrupt-checkpoint.jsonl");
      const rows = [
        JSON.stringify({
          type: "assistant",
          requestId: "corrupt-restored",
          timestamp: "2026-07-12T12:10:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: {
              input_tokens: 10,
              output_tokens: 20,
              cache_creation_input_tokens: 0,
              cache_creation: { ephemeral_1h_input_tokens: 6296 },
            },
          },
        }),
        JSON.stringify({
          type: "assistant",
          requestId: "corrupt-existing",
          timestamp: "2026-07-12T12:10:01.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 30, output_tokens: 40 },
          },
        }),
      ];
      await writeFile(sessionPath, `${rows.join("\n")}\n`);
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          "{corrupt checkpoint",
          current.generationKey,
        );
      } finally {
        db.close();
      }

      let capturedReads = 0;
      const healed = await ingestOnce(store, {
        afterCapturedRead() {
          capturedReads += 1;
        },
      });
      expect(capturedReads).toBe(1);
      expect(healed.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(2);
      expect(
        JSON.parse(
          store.getLatestSourceFile("claude", String(sourceStat.dev), String(sourceStat.ino))!
            .codexStateJson!,
        ),
      ).toEqual({
        usageParserVersion: CLAUDE_USAGE_PARSER_VERSION,
        semanticUsageComplete: true,
        defectLineCount: 0,
        defectLineNumbers: [],
        usageComplete: true,
      });

      const retry = await ingestOnce(store, {
        metadataFastPathSupported: () => true,
        afterCapturedRead() {
          capturedReads += 1;
        },
      });
      expect(retry.newEvents).toBe(0);
      expect(capturedReads).toBe(1);
      expect(store.selectEvents()).toHaveLength(2);
    });
  });

  test("complete old-version Claude checkpoint is exempt from replay", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-complete-old-checkpoint.jsonl");
      await writeFile(
        sessionPath,
        `${JSON.stringify({
          type: "assistant",
          requestId: "complete-old-existing",
          timestamp: "2026-07-12T12:20:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20 },
          },
        })}\n`,
      );
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const oldCompleteCheckpoint = JSON.stringify({
        usageParserVersion: 1,
        usageComplete: true,
      });
      const db = new Database(storePath);
      try {
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          oldCompleteCheckpoint,
          current.generationKey,
        );
      } finally {
        db.close();
      }
      const eventsBefore = store.selectEvents();
      let capturedReads = 0;
      let stagedLines = 0;
      const skipped = await ingestOnce(store, {
        metadataFastPathSupported: () => true,
        afterCapturedRead() {
          capturedReads += 1;
        },
        afterStagedLine() {
          stagedLines += 1;
        },
      });

      expect(skipped.newEvents).toBe(0);
      expect(capturedReads).toBe(0);
      expect(stagedLines).toBe(0);
      expect(store.selectEvents()).toEqual(eventsBefore);
      expect(
        store.getLatestSourceFile("claude", String(sourceStat.dev), String(sourceStat.ino))!
          .codexStateJson,
      ).toBe(oldCompleteCheckpoint);
    });
  });

  test("future Claude parser checkpoint skips appended content without rewriting", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-future-checkpoint.jsonl");
      const row = (requestId: string, timestamp: string) =>
        JSON.stringify({
          type: "assistant",
          requestId,
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 20 },
          },
        });
      await writeFile(sessionPath, `${row("future-existing", "2026-07-12T13:00:00.000Z")}\n`);
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const futureCheckpoint = JSON.stringify({
        usageParserVersion: CLAUDE_USAGE_PARSER_VERSION + 1,
        usageComplete: false,
        futureField: "preserve-byte-for-byte",
      });
      const db = new Database(storePath);
      try {
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          futureCheckpoint,
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'claude'");
      } finally {
        db.close();
      }
      await appendFile(sessionPath, `${row("future-appended", "2026-07-12T13:00:01.000Z")}\n`);
      const before = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      let capturedReads = 0;
      const skipped = await ingestOnce(store, {
        afterCapturedRead() {
          capturedReads += 1;
        },
      });
      expect(capturedReads).toBe(0);
      expect(skipped.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(1);
      expect(
        store.getLatestSourceFile("claude", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(before);
      expect(before.codexStateJson).toBe(futureCheckpoint);
    });
  });

  test("failed Claude rebuild preserves the old checkpoint and retries", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-rebuild-retry.jsonl");
      const rows = [
        JSON.stringify({
          type: "assistant",
          requestId: "retry-restored",
          timestamp: "2026-07-12T14:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: {
              input_tokens: 10,
              output_tokens: 20,
              cache_creation_input_tokens: 0,
              cache_creation: { ephemeral_1h_input_tokens: 6296 },
            },
          },
        }),
        JSON.stringify({
          type: "assistant",
          requestId: "retry-accepted",
          timestamp: "2026-07-12T14:00:01.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 30, output_tokens: 40 },
          },
        }),
      ];
      await writeFile(sessionPath, `${rows.join("\n")}\n`);
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        const dropped = db
          .query("SELECT id FROM events WHERE file_path = ? AND line_offset = 0")
          .get(sessionPath) as { id: number };
        db.prepare("DELETE FROM event_occurrences WHERE event_id = ?").run(dropped.id);
        db.prepare("DELETE FROM events WHERE id = ?").run(dropped.id);
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify({ usageParserVersion: 1, usageComplete: false }),
          current.generationKey,
        );
      } finally {
        db.close();
      }
      const oldCheckpoint = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const errorLog = spyOn(console, "error").mockImplementation(() => {});
      try {
        await ingestOnce(store, {
          afterStagedLine(_path, lineNumber) {
            if (lineNumber === 1) throw new Error("injected Claude rebuild stage failure");
          },
        });
      } finally {
        errorLog.mockRestore();
      }
      expect(
        store.getLatestSourceFile("claude", String(sourceStat.dev), String(sourceStat.ino)),
      ).toEqual(oldCheckpoint);
      expect(store.selectEvents()).toHaveLength(1);

      const retried = await ingestOnce(store);
      expect(retried.newEvents).toBe(1);
      expect(store.selectEvents()).toHaveLength(2);
      expect(
        JSON.parse(
          store.getLatestSourceFile("claude", String(sourceStat.dev), String(sourceStat.ino))!
            .codexStateJson!,
        ),
      ).toEqual({
        usageParserVersion: CLAUDE_USAGE_PARSER_VERSION,
        semanticUsageComplete: true,
        defectLineCount: 0,
        defectLineNumbers: [],
        usageComplete: true,
      });
    });
  });

  test("Claude rebuild restores two collapsed revisions as two source occurrences", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "claude-collapsed-rebuild.jsonl");
      const revision = (timestamp: string, input: number) =>
        JSON.stringify({
          type: "assistant",
          requestId: "collapsed-rebuild-request",
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: input, output_tokens: 20 },
          },
        });
      await writeFile(
        sessionPath,
        `${revision("2026-07-12T15:00:00.000Z", 10)}\n${revision(
          "2026-07-12T15:00:01.000Z",
          30,
        )}\n`,
      );
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const current = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      const db = new Database(storePath);
      try {
        db.prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?").run(
          JSON.stringify({ usageParserVersion: 1, usageComplete: false }),
          current.generationKey,
        );
        db.run("UPDATE provider_state SET complete = 0 WHERE source = 'claude'");
      } finally {
        db.close();
      }

      const healed = await ingestOnce(store);
      expect(healed.newEvents).toBe(0);
      expect(store.selectEvents()).toHaveLength(1);
      const inspected = new Database(storePath, { readonly: true });
      try {
        expect(
          inspected
            .query("SELECT COUNT(*) AS count FROM event_occurrences WHERE generation_key = ?")
            .get(current.generationKey),
        ).toEqual({ count: 2 });
      } finally {
        inspected.close();
      }
      expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
        true,
      );
    });
  });

  test("Claude malformed usage is sticky incomplete while usage-absent assistant rows are benign", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "malformed-claude.jsonl");
      const contentOnly = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T09:00:00.000Z",
        message: { role: "assistant", content: "no usage on this record" },
      });
      const valid = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 20 },
        },
      });
      const malformed = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T11:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 10, output_tokens: 20, cache_read_input_tokens: -1 },
        },
      });
      await writeFile(sessionPath, `${contentOnly}\n${valid}\n${malformed}\n`);
      const first = await ingestOnce(store);
      expect(first.newEvents).toBe(1);
      expect(store.selectEvents()).toHaveLength(1);
      expectStructurallyHealthyWindowIncomplete(store, "claude");
      const revision = first.revision;
      const noop = await ingestOnce(store);
      expect(noop.mutated).toBe(false);
      expect(noop.revision).toBe(revision);
      expectStructurallyHealthyWindowIncomplete(store, "claude");
    });
  });

  test("Claude authority selects the earliest candidate independent of path order", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const row = (timestamp: string, input: number) =>
        JSON.stringify({
          type: "assistant",
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            id: "path-order-response",
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: input, output_tokens: 20 },
          },
        });
      await writeFile(
        join(projectDir, "a-later.jsonl"),
        `${row("2026-05-17T11:00:00.000Z", 90)}\n`,
      );
      const earlierPath = join(projectDir, "z-earlier.jsonl");
      await writeFile(earlierPath, `${row("2026-05-17T10:00:00.000Z", 10)}\n`);

      const summary = await ingestOnce(store);
      expect(summary.newEvents).toBe(1);
      expect(summary.events[0]?.inputTokens).toBe(10);
      const winner = store.selectEvents()[0];
      expect(winner?.inputTokens).toBe(10);
      expect(winner?.filePath).toBe(earlierPath);
      const db = new Database(storePath);
      try {
        const occurrenceCount = db
          .query("SELECT COUNT(*) AS count FROM event_occurrences")
          .get() as {
          count: number;
        };
        expect(occurrenceCount.count).toBe(2);
      } finally {
        db.close();
      }
    });
  });

  test("an earlier incremental Claude candidate replaces the winner in place", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const row = (timestamp: string, input: number) =>
        JSON.stringify({
          type: "assistant",
          requestId: "incremental-winner",
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: input, output_tokens: 20 },
          },
        });
      await writeFile(
        join(projectDir, "z-later.jsonl"),
        `${row("2026-05-17T11:00:00.000Z", 90)}\n`,
      );
      await ingestOnce(store);
      const db = new Database(storePath);
      try {
        const before = db.query("SELECT id FROM events").get() as { id: number };
        await writeFile(
          join(projectDir, "a-earlier.jsonl"),
          `${row("2026-05-17T10:00:00.000Z", 10)}\n`,
        );
        const replacement = await ingestOnce(store);
        expect(replacement.newEvents).toBe(0);
        expect(replacement.mutated).toBe(true);
        const after = db.query("SELECT id, input_tokens FROM events").get() as {
          id: number;
          input_tokens: number;
        };
        expect(after.id).toBe(before.id);
        expect(after.input_tokens).toBe(10);
      } finally {
        db.close();
      }
    });
  });

  test("equal-time Claude candidates use raw SHA and a true retry is a no-op", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const row = (input: number) =>
        JSON.stringify({
          type: "assistant",
          requestId: "sha-winner",
          timestamp: "2026-05-17T10:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: input, output_tokens: 20 },
          },
        });
      const left = row(11);
      const right = row(22);
      await writeFile(join(projectDir, "a.jsonl"), `${left}\n`);
      await writeFile(join(projectDir, "z.jsonl"), `${right}\n`);
      const expected =
        createHash("sha256").update(left).digest("hex") <
        createHash("sha256").update(right).digest("hex")
          ? 11
          : 22;

      const first = await ingestOnce(store);
      expect(first.events[0]?.inputTokens).toBe(expected);
      expect(store.selectEvents()[0]?.inputTokens).toBe(expected);
      const retry = await ingestOnce(store);
      expect(retry.newEvents).toBe(0);
      expect(retry.mutated).toBe(false);
      expect(retry.revision).toBe(first.revision);
    });
  });

  test("authority collapses message and UUID revisions but preserves unidentified repeats", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const revision = (identity: Record<string, string>, timestamp: string, input: number) =>
        JSON.stringify({
          type: "assistant",
          ...identity,
          timestamp,
          cwd: "/srv/example/demo",
          message: {
            id: identity.messageId,
            role: "assistant",
            model: "claude-sonnet-4-6",
            content: "stable response",
            usage: { input_tokens: input, output_tokens: 20 },
          },
        });
      const messageA = revision(
        { messageId: "message-revision-1" },
        "2026-05-17T10:00:00.000Z",
        10,
      );
      const messageB = revision(
        { messageId: "message-revision-1" },
        "2026-05-17T10:00:01.000Z",
        30,
      );
      const uuidA = revision({ uuid: "uuid-revision-1" }, "2026-05-17T11:00:00.000Z", 40);
      const uuidB = revision({ uuid: "uuid-revision-1" }, "2026-05-17T11:00:01.000Z", 60);
      const unidentified = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T12:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 70, output_tokens: 20 },
        },
      });
      await writeFile(join(projectDir, "message-revisions.jsonl"), `${messageA}\n${messageB}\n`);
      await writeFile(join(projectDir, "uuid-revisions.jsonl"), `${uuidA}\n${uuidB}\n`);
      await writeFile(
        join(projectDir, "unidentified-repeated.jsonl"),
        `${unidentified}\n${unidentified}\n`,
      );

      await ingestOnce(store);
      const events = store.selectEvents();
      expect(events.filter((event) => event.sessionId === "message-revisions")).toHaveLength(1);
      expect(events.filter((event) => event.sessionId === "uuid-revisions")).toHaveLength(1);
      expect(events.filter((event) => event.sessionId === "unidentified-repeated")).toHaveLength(2);
    });
  });

  test("collapses mutable revisions sharing a Claude requestId", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "dedupe-request.jsonl");
      const requestId = "req-dedupe-1";
      const rows = [
        JSON.stringify({
          type: "assistant",
          requestId,
          timestamp: "2026-05-17T10:00:00.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-sonnet-4-6",
            usage: { input_tokens: 10, output_tokens: 40 },
          },
        }),
        JSON.stringify({
          type: "assistant",
          requestId,
          timestamp: "2026-05-17T10:00:01.000Z",
          cwd: "/srv/example/demo",
          message: {
            role: "assistant",
            model: "claude-opus-4-8",
            usage: { input_tokens: 20, output_tokens: 80 },
          },
        }),
      ];
      await writeFile(sessionPath, `${rows.join("\n")}\n`);

      await ingestOnce(store);
      const events = store.selectEvents();
      expect(events.length).toBe(1);
      const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
      expect(agg.totals.messages).toBe(1);
      expect(agg.totals.totalTokens).toBe(50);
      expect(agg.byModel[0]?.model).toBe("claude-sonnet-4-6");
    });
  });

  test("keeps distinct response IDs sharing a requestId across files", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const requestId = "req-dedupe-across-files";
      const sessionPathA = join(projectDir, "a.jsonl");
      const sessionPathB = join(projectDir, "b.jsonl");
      const rowA = JSON.stringify({
        type: "assistant",
        requestId,
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          id: "response-a",
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 5, output_tokens: 10 },
        },
      });
      const rowB = JSON.stringify({
        type: "assistant",
        requestId,
        timestamp: "2026-05-17T11:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          id: "response-b",
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 6, output_tokens: 11 },
        },
      });
      await writeFile(sessionPathA, `${rowA}\n`);
      await writeFile(sessionPathB, `${rowB}\n`);

      await ingestOnce(store);
      expect(store.selectEvents().length).toBe(2);
    });
  });

  test("keeps NULL-key rows as legacy behavior", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const rowA = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T10:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 1, output_tokens: 2 },
        },
      });
      const rowB = JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T11:00:00.000Z",
        cwd: "/srv/example/demo",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: { input_tokens: 3, output_tokens: 4 },
        },
      });
      await writeFile(join(projectDir, "null-a.jsonl"), `${rowA}\n`);
      await writeFile(join(projectDir, "null-b.jsonl"), `${rowB}\n`);

      await ingestOnce(store);
      expect(store.selectEvents().length).toBe(2);
    });
  });

  test("reingest-dedupe preserves missing-source rows and dedupes present rows", async () => {
    await withLegacyStoreRowsForReingest(
      async ({ projectsDir, codexSessionsDir, dbPath, missingPath }) => {
        const proc = Bun.spawnSync({
          cmd: ["bun", "src/cli.ts", "reingest-dedupe"],
          cwd: join(import.meta.dir, ".."),
          stdout: "pipe",
          stderr: "pipe",
          env: {
            ...process.env,
            TOKENS_DB_PATH: dbPath,
            TOKENS_HOST: "127.0.0.1",
            TOKENS_PORT: "7999",
            CLAUDE_PROJECTS_DIR: projectsDir,
            CODEX_SESSIONS_DIR: codexSessionsDir,
          },
        });
        expect(proc.exitCode).toBe(0);
        const output = new TextDecoder().decode(proc.stdout || new Uint8Array());
        expect(output).toContain("rows after: 2");

        const migrated = new TokenStore(dbPath);
        try {
          const events = migrated.selectEvents();
          expect(events).toHaveLength(2);
          const missing = events.find((event) => event.filePath === missingPath);
          expect(missing?.inputTokens).toBe(100);
          expect(missing?.outputTokens).toBe(200);
          expect(missing?.cost).toBe(1);

          const rebuilt = events.find((event) => event.filePath !== missingPath);
          expect(rebuilt?.inputTokens).toBe(10);
          expect(rebuilt?.outputTokens).toBe(20);
          const rebuiltCost = costFor("claude-sonnet-4-6", {
            inputTokens: 10,
            outputTokens: 20,
            cacheReadTokens: 0,
            cacheWrite5mTokens: 0,
            cacheWrite1hTokens: 0,
          });
          expect(rebuilt?.cost).toBeCloseTo(rebuiltCost);
          expect(events.reduce((sum, event) => sum + event.inputTokens, 0)).toBe(110);
          expect(events.reduce((sum, event) => sum + event.outputTokens, 0)).toBe(220);
          expect(events.reduce((sum, event) => sum + event.cost, 0)).toBeCloseTo(1 + rebuiltCost);
          // The missing path is permanently gone (ENOENT), so reconciliation
          // tombstones it as expired-missing and completeness recovers; the
          // preserved missing-source event rows above are the retained data.
          expect(
            migrated.getProviderStates().find((state) => state.source === "claude")?.complete,
          ).toBe(true);
          const tombstone = migrated
            .captureSourceReconciliationSnapshot()
            .pendingLegacyReconciliations.find((row) => row.path === missingPath);
          expect(tombstone?.reason).toBe("expired-missing");
        } finally {
          migrated.close();
        }
      },
    );
  });

  test("reingest-dedupe backup includes rows that are still only in WAL", async () => {
    await withWalRowsOnlyReingestFixture(
      async ({ projectsDir, codexSessionsDir, dbPath, missingPath }) => {
        const proc = Bun.spawnSync({
          cmd: ["bun", "src/cli.ts", "reingest-dedupe"],
          cwd: join(import.meta.dir, ".."),
          stdout: "pipe",
          stderr: "pipe",
          env: {
            ...process.env,
            TOKENS_DB_PATH: dbPath,
            TOKENS_HOST: "127.0.0.1",
            TOKENS_PORT: "7999",
            CLAUDE_PROJECTS_DIR: projectsDir,
            CODEX_SESSIONS_DIR: codexSessionsDir,
          },
        });
        expect(proc.exitCode).toBe(0);
        const output = new TextDecoder().decode(proc.stdout || new Uint8Array());
        expect(output).toContain("rows before: 1");
        expect(output).toContain("rows deleted: 0");
        expect(output).toContain("rows preserved: 1");
        expect(output).toContain("rows after: 1");
        const drop = output.match(/dedupe drop %: ([0-9.]+)/);
        expect(drop).not.toBeNull();
        expect(Number(drop?.[1])).toBeCloseTo(0);

        const backupPath = output.match(/backup:\s+(.+?)\s*$/m)?.[1];
        expect(backupPath).toBeTruthy();

        const backupDb = new Database(backupPath as string);
        try {
          const rows = backupDb
            .query("SELECT file_path FROM events WHERE file_path = ?")
            .all(missingPath) as Array<{ file_path: string }>;
          expect(rows.length).toBe(1);
          expect(rows[0]?.file_path).toBe(missingPath);
        } finally {
          backupDb.close();
        }
      },
    );
  });
});

function defectClaudeLine(timestamp: string, inputTokens: number): string {
  return JSON.stringify({
    type: "assistant",
    timestamp,
    cwd: "/srv/example/demo",
    message: {
      role: "assistant",
      model: "claude-sonnet-4-6",
      usage: { input_tokens: inputTokens, output_tokens: 1 },
    },
  });
}

describe("permanent JSONL defect completeness", () => {
  test("a mid-file torn line is tombstoned once and later scans fast-path", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "mid-file-defect.jsonl");
      const first = defectClaudeLine("2026-07-16T10:00:00.000Z", 10);
      const second = defectClaudeLine("2026-07-16T10:00:01.000Z", 20);
      await writeFile(sessionPath, `${first}\n{"torn"\n${second}\n`);

      const errorLog = spyOn(console, "error").mockImplementation(() => {});
      try {
        const initial = await ingestOnce(store, { metadataFastPathSupported: () => true });
        expect(initial.newEvents).toBe(2);
        expect(store.getProviderStates().find((state) => state.source === "claude")).toEqual(
          expect.objectContaining({ complete: true, defectLines: 1 }),
        );
        const sourceStat = await stat(sessionPath);
        const checkpoint = store.getLatestSourceFile(
          "claude",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            semanticUsageComplete: true,
            usageComplete: true,
            defectLineCount: 1,
            defectLineNumbers: [1],
          }),
        );
        expect(
          errorLog.mock.calls.filter(([message]) =>
            String(message).includes("permanent JSONL defects"),
          ),
        ).toHaveLength(1);

        const revision = initial.revision;
        let capturedReads = 0;
        const noop = await ingestOnce(store, {
          afterCapturedRead: () => {
            capturedReads += 1;
          },
          metadataFastPathSupported: () => true,
        });
        expect(noop.mutated).toBe(false);
        expect(noop.revision).toBe(revision);
        expect(capturedReads).toBe(0);
        expect(
          errorLog.mock.calls.filter(([message]) =>
            String(message).includes("permanent JSONL defects"),
          ),
        ).toHaveLength(1);
      } finally {
        errorLog.mockRestore();
      }
    });
  });

  test("newline and unterminated malformed suffixes hold the cursor then heal on append", async () => {
    for (const suffix of ['{"torn"\n', '{"torn"']) {
      await withFixture(async ({ projectsDir, store }) => {
        const projectDir = join(projectsDir, "-home-user-ssd-demo");
        await mkdir(projectDir, { recursive: true });
        const sessionPath = join(
          projectDir,
          `terminal-${suffix.endsWith("\n") ? "newline" : "tail"}.jsonl`,
        );
        const first = defectClaudeLine("2026-07-16T11:00:00.000Z", 10);
        const second = defectClaudeLine("2026-07-16T11:00:01.000Z", 20);
        const committedPrefix = `${first}\n`;
        await writeFile(sessionPath, `${committedPrefix}${suffix}`);

        const incomplete = await ingestOnce(store);
        expect(incomplete.newEvents).toBe(1);
        const sourceStat = await stat(sessionPath);
        const held = store.getLatestSourceFile(
          "claude",
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(held.lastByteOffset).toBe(Buffer.byteLength(committedPrefix));
        expect(held.lastLineNumber).toBe(1);
        expect(JSON.parse(held.codexStateJson!)).toEqual(
          expect.objectContaining({
            semanticUsageComplete: true,
            usageComplete: false,
            defectLineCount: 0,
          }),
        );
        expectStructurallyHealthyWindowIncomplete(store, "claude");

        await appendFile(sessionPath, `${suffix.endsWith("\n") ? "" : "\n"}${second}\n`);
        const healed = await ingestOnce(store);
        expect(healed.newEvents).toBe(1);
        expect(store.selectEvents()).toHaveLength(2);
        expect(store.getProviderStates().find((state) => state.source === "claude")).toEqual(
          expect.objectContaining({ complete: true, defectLines: 1 }),
        );
        const completedStat = await stat(sessionPath);
        const completed = store.getLatestSourceFile(
          "claude",
          String(completedStat.dev),
          String(completedStat.ino),
        )!;
        expect(completed.lastByteOffset).toBe(completed.capturedSize);
        expect(JSON.parse(completed.codexStateJson!)).toEqual(
          expect.objectContaining({
            usageComplete: true,
            defectLineCount: 1,
            defectLineNumbers: [1],
          }),
        );
      });
    }
  });

  test("semantic usage violations remain fail-closed without syntax tombstones", async () => {
    await withFixture(async ({ projectsDir, store }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const sessionPath = join(projectDir, "semantic-invalid.jsonl");
      const semanticInvalid = JSON.stringify({
        type: "assistant",
        timestamp: "2026-07-16T12:00:00.000Z",
        message: {
          model: "claude-sonnet-4-6",
          usage: { input_tokens: -1, output_tokens: 1 },
        },
      });
      await writeFile(sessionPath, `${semanticInvalid}\n`);
      await ingestOnce(store);
      const sourceStat = await stat(sessionPath);
      const checkpoint = store.getLatestSourceFile(
        "claude",
        String(sourceStat.dev),
        String(sourceStat.ino),
      )!;
      expect(checkpoint.lastByteOffset).toBe(checkpoint.capturedSize);
      expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
        expect.objectContaining({
          semanticUsageComplete: false,
          usageComplete: false,
          defectLineCount: 0,
        }),
      );
      expectStructurallyHealthyWindowIncomplete(store, "claude");
    });
  });

  test("pre-amendment incomplete Claude v2 and Codex v3 checkpoints replay once", async () => {
    await withFixture(async ({ projectsDir, codexSessionsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      const codexDir = join(codexSessionsDir, "2026", "07", "16");
      await mkdir(projectDir, { recursive: true });
      await mkdir(codexDir, { recursive: true });
      const claudePath = join(projectDir, "claude-replay.jsonl");
      const codexPath = join(codexDir, "codex-replay.jsonl");
      const claudeRows = `${defectClaudeLine("2026-07-16T13:00:00.000Z", 10)}\n{"torn"\n${defectClaudeLine("2026-07-16T13:00:01.000Z", 20)}\n`;
      const codexRows = singleCodexReplacementRows("019f7000-0000-7000-8000-000000000001")
        .trimEnd()
        .split("\n");
      await writeFile(claudePath, claudeRows);
      await writeFile(codexPath, `${codexRows[0]}\n{"torn"\n${codexRows[1]}\n`);
      await ingestOnce(store);

      const claudeStat = await stat(claudePath);
      const codexStat = await stat(codexPath);
      const seeded = new Database(storePath);
      try {
        for (const [source, device, inode, version] of [
          ["claude", String(claudeStat.dev), String(claudeStat.ino), 2],
          ["codex", String(codexStat.dev), String(codexStat.ino), 3],
        ] as const) {
          const state = store.getLatestSourceFile(source, device, inode)!;
          const checkpoint = JSON.parse(state.codexStateJson!) as Record<string, unknown>;
          checkpoint.usageParserVersion = version;
          checkpoint.usageComplete = false;
          delete checkpoint.semanticUsageComplete;
          delete checkpoint.defectLineCount;
          delete checkpoint.defectLineNumbers;
          seeded
            .prepare("UPDATE source_files SET codex_state_json = ? WHERE generation_key = ?")
            .run(JSON.stringify(checkpoint), state.generationKey);
          seeded.prepare("UPDATE provider_state SET complete = 0 WHERE source = ?").run(source);
        }
      } finally {
        seeded.close();
      }

      let capturedReads = 0;
      const healed = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(2);
      expect(healed.mutated).toBe(true);
      for (const [source, sourceStat, version] of [
        ["claude", claudeStat, CLAUDE_USAGE_PARSER_VERSION],
        ["codex", codexStat, CODEX_USAGE_PARSER_VERSION],
      ] as const) {
        const checkpoint = store.getLatestSourceFile(
          source,
          String(sourceStat.dev),
          String(sourceStat.ino),
        )!;
        expect(JSON.parse(checkpoint.codexStateJson!)).toEqual(
          expect.objectContaining({
            usageParserVersion: version,
            usageComplete: true,
            defectLineCount: 1,
          }),
        );
      }

      capturedReads = 0;
      const noop = await ingestOnce(store, {
        afterCapturedRead: () => {
          capturedReads += 1;
        },
        metadataFastPathSupported: () => true,
      });
      expect(capturedReads).toBe(0);
      expect(noop.mutated).toBe(false);
    });
  });

  test("expired-missing pending rows coexist with defects and only active generations count", async () => {
    await withFixture(async ({ projectsDir, store, storePath }) => {
      const projectDir = join(projectsDir, "-home-user-ssd-demo");
      await mkdir(projectDir, { recursive: true });
      const primaryPath = join(projectDir, "active-defect.jsonl");
      const aliasPath = join(projectDir, "active-defect-alias.jsonl");
      const valid = defectClaudeLine("2026-07-16T14:00:00.000Z", 10);
      await writeFile(primaryPath, `${valid}\n{"torn"\n${valid}\n`);
      await link(primaryPath, aliasPath);
      const db = new Database(storePath);
      try {
        db.prepare(
          "INSERT INTO legacy_reconciliation_pending (path, source, reason) VALUES (?, 'claude', 'missing-source')",
        ).run(join(projectsDir, "gone.jsonl"));
      } finally {
        db.close();
      }

      await ingestOnce(store);
      expect(store.getProviderStates().find((state) => state.source === "claude")).toEqual(
        expect.objectContaining({ complete: true, defectLines: 1 }),
      );
      expect(
        store.buildAuthoritativeTokenSnapshot(Date.parse("2026-07-16T15:00:00.000Z")).providers
          .claude,
      ).toEqual(expect.objectContaining({ discoveryComplete: true, defectLines: 1 }));

      await rm(primaryPath);
      await rm(aliasPath);
      await writeFile(primaryPath, `${valid}\n`);
      await ingestOnce(store);
      expect(
        store.getProviderStates().find((state) => state.source === "claude")?.defectLines,
      ).toBe(0);
    });
  });
});

describe("aggregateEvents throughput", () => {
  test("byHarness splits claude vs codex usage", () => {
    const events = [
      mkEvent({
        ts: 1_000,
        source: "claude",
        model: "claude-sonnet-4-6",
        sessionId: "s-claude",
        inputTokens: 100,
        outputTokens: 50,
      }),
      mkEvent({
        ts: 61_000,
        source: "claude",
        model: "claude-sonnet-4-6",
        sessionId: "s-claude",
        inputTokens: 100,
        outputTokens: 50,
      }),
      mkEvent({
        ts: 1_000,
        source: "codex",
        model: "gpt-5.5-cyber-preview",
        sessionId: "s-codex",
        inputTokens: 200,
        outputTokens: 0,
      }),
    ];
    const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
    expect(agg.byHarness.length).toBe(2);
    const claude = agg.byHarness.find((h) => h.harness === "claude");
    const codex = agg.byHarness.find((h) => h.harness === "codex");
    expect(claude?.totalTokens).toBe(300);
    expect(claude?.messages).toBe(2);
    expect(codex?.totalTokens).toBe(200);
    expect(codex?.messages).toBe(1);
    // sorted by totalTokens desc
    expect(agg.byHarness[0]?.harness).toBe("claude");
  });

  test("ModelBucket and Totals expose a period-average tokensPerSecond", () => {
    const events = [
      mkEvent({ ts: 0, model: "claude-sonnet-4-6", inputTokens: 1000, outputTokens: 0 }),
      mkEvent({ ts: 10_000, model: "claude-sonnet-4-6", inputTokens: 1000, outputTokens: 0 }),
    ];
    const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
    const mb = agg.byModel[0]!;
    expect(mb.observedSpanSec).toBeCloseTo(10);
    expect(mb.tokensPerSecond).toBeCloseTo(2000 / 10);
    expect(agg.totals.observedSpanSec).toBeCloseTo(10);
    expect(agg.totals.tokensPerSecond).toBeCloseTo(2000 / 10);
  });

  test("a single event yields tokensPerSecond === null, not 0", () => {
    const events = [mkEvent({ ts: 5_000, inputTokens: 500, outputTokens: 0 })];
    const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
    expect(agg.byModel[0]?.observedSpanSec).toBe(0);
    expect(agg.byModel[0]?.tokensPerSecond).toBeNull();
    expect(agg.byHarness[0]?.tokensPerSecond).toBeNull();
    expect(agg.totals.observedSpanSec).toBe(0);
    expect(agg.totals.tokensPerSecond).toBeNull();
  });

  test("an empty event set yields tokensPerSecond === null", () => {
    const agg = aggregateEvents([], { files: 0, scannedAt: Date.now() });
    expect(agg.totals.observedSpanSec).toBe(0);
    expect(agg.totals.tokensPerSecond).toBeNull();
  });

  test("unsorted input still computes the correct span via Math.min/Math.max", () => {
    // Deliberately out of ts order — aggregateEvents must not rely on
    // events[0]/events[length-1] for span, only independently-tracked min/max.
    const events = [
      mkEvent({ ts: 50_000, model: "claude-sonnet-4-6", inputTokens: 100, outputTokens: 0 }),
      mkEvent({ ts: 0, model: "claude-sonnet-4-6", inputTokens: 100, outputTokens: 0 }),
      mkEvent({ ts: 20_000, model: "claude-sonnet-4-6", inputTokens: 100, outputTokens: 0 }),
    ];
    const agg = aggregateEvents(events, { files: 1, scannedAt: Date.now() });
    expect(agg.byModel[0]?.observedSpanSec).toBeCloseTo(50);
    expect(agg.totals.observedSpanSec).toBeCloseTo(50);
    expect(agg.totals.tokensPerSecond).toBeCloseTo(300 / 50);
  });
});

function closedTextStream(text = ""): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      if (text) controller.enqueue(new TextEncoder().encode(text));
      controller.close();
    },
  });
}

function controlledAggregationSubprocess(options?: {
  exitOnSigterm?: boolean;
  exitOnSigkill?: boolean;
  onSignal?(signal: number | string | undefined): void;
}) {
  const signals: Array<number | string | undefined> = [];
  let resolveExit!: (code: number) => void;
  let stdoutController!: ReadableStreamDefaultController<Uint8Array>;
  let stderrController!: ReadableStreamDefaultController<Uint8Array>;
  let exited = false;
  const exit = new Promise<number>((resolve) => {
    resolveExit = (code) => {
      if (exited) return;
      exited = true;
      stdoutController.close();
      stderrController.close();
      resolve(code);
    };
  });
  const subprocess: UsageAggregationTestSubprocess = {
    stdin: { write: (data) => data.length, end: () => undefined },
    stdout: new ReadableStream({ start: (controller) => (stdoutController = controller) }),
    stderr: new ReadableStream({ start: (controller) => (stderrController = controller) }),
    exited: exit,
    kill(signal) {
      signals.push(signal);
      options?.onSignal?.(signal);
      if (signal === "SIGTERM" && options?.exitOnSigterm) resolveExit(0);
      if (signal === "SIGKILL" && options?.exitOnSigkill) resolveExit(137);
    },
  };
  return {
    subprocess,
    signals,
    failStdout(error: Error): void {
      stdoutController.error(error);
    },
  };
}

function completedAggregationControl(
  exitCode: number,
  stderr = "",
): UsageAggregationTestControlSubprocess {
  return {
    stderr: closedTextStream(stderr),
    exited: Promise.resolve(exitCode),
    kill: () => undefined,
  };
}

describe("aggregation worker cleanup", () => {
  test("rechecks the exact transient unit after the systemd-run wrapper exits", async () => {
    const cleanupOrder: string[] = [];
    const wrapper = controlledAggregationSubprocess({
      exitOnSigterm: true,
      onSignal: (signal) => cleanupOrder.push(`wrapper:${String(signal)}`),
    });
    const commands: string[][] = [];
    let stopCalls = 0;
    try {
      _setUsageAggregationSpawnForTest(() => wrapper.subprocess);
      _setUsageAggregationControlSpawnForTest((command) => {
        commands.push(command);
        cleanupOrder.push(`unit:${command[2]}`);
        if (command[2] === "stop") {
          stopCalls++;
          return stopCalls === 1
            ? completedAggregationControl(5, "Unit not loaded")
            : completedAggregationControl(0);
        }
        return completedAggregationControl(0);
      });
      const active = aggregateUsageOffThread("/tmp/tokens.sqlite", {});
      const observedError = active.catch((error: unknown) => error);

      await expect(shutdownUsageAggregationWorker()).resolves.toBeUndefined();

      const stops = commands.filter((command) => command[2] === "stop");
      expect(stops).toHaveLength(2);
      expect(stops[0]?.at(-1)).toBe(stops[1]?.at(-1));
      expect(commands.some((command) => command[2] === "kill")).toBe(false);
      expect(wrapper.signals).toEqual(["SIGTERM"]);
      expect(cleanupOrder).toEqual(["unit:stop", "wrapper:SIGTERM", "unit:stop"]);
      expect((await observedError) as Error).toHaveProperty(
        "message",
        "aggregation worker shut down",
      );
    } finally {
      await _resetUsageAggregationWorkerForTest();
    }
  });

  test("kills the exact transient unit when systemctl stop fails non-absent", async () => {
    const cleanupOrder: string[] = [];
    const wrapper = controlledAggregationSubprocess({
      exitOnSigterm: true,
      onSignal: (signal) => cleanupOrder.push(`wrapper:${String(signal)}`),
    });
    const commands: string[][] = [];
    try {
      _setUsageAggregationSpawnForTest(() => wrapper.subprocess);
      _setUsageAggregationControlSpawnForTest((command) => {
        commands.push(command);
        cleanupOrder.push(`unit:${command[2]}`);
        return command[2] === "stop"
          ? completedAggregationControl(5, "Access denied")
          : completedAggregationControl(0);
      });
      const active = aggregateUsageOffThread("/tmp/tokens.sqlite", {});
      const observedError = active.catch((error: unknown) => error);

      await expect(shutdownUsageAggregationWorker()).resolves.toBeUndefined();

      const stops = commands.filter((command) => command[2] === "stop");
      const kills = commands.filter((command) => command[2] === "kill");
      expect(stops).toHaveLength(2);
      expect(kills).toHaveLength(2);
      expect(commands.every((command) => command.at(-1) === commands[0]?.at(-1))).toBe(true);
      for (const kill of kills) {
        expect(kill.slice(2, 5)).toEqual(["kill", "--kill-whom=all", "--signal=KILL"]);
      }
      expect(cleanupOrder).toEqual([
        "unit:stop",
        "unit:kill",
        "wrapper:SIGTERM",
        "unit:stop",
        "unit:kill",
      ]);
      expect((await observedError) as Error).toHaveProperty(
        "message",
        "aggregation worker shut down",
      );
    } finally {
      await _resetUsageAggregationWorkerForTest();
    }
  });

  test("escalates a TERM-resistant direct child to KILL within the cleanup deadline", async () => {
    const previousTimeout = process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
    process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS = "100";
    const child = controlledAggregationSubprocess({ exitOnSigkill: true });
    try {
      _setUsageAggregationLauncherForTest("direct");
      _setUsageAggregationSpawnForTest(() => child.subprocess);
      const active = aggregateUsageOffThread("/tmp/tokens.sqlite", {});
      const observedError = active.catch((error: unknown) => error);

      await expect(shutdownUsageAggregationWorker()).resolves.toBeUndefined();
      expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      expect((await observedError) as Error).toHaveProperty(
        "message",
        "aggregation worker shut down",
      );
    } finally {
      if (previousTimeout === undefined) delete process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
      else process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS = previousTimeout;
      await _resetUsageAggregationWorkerForTest();
    }
  });

  test("propagates cleanup timeout and poisons subsequent launches", async () => {
    const previousTimeout = process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
    process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS = "100";
    const child = controlledAggregationSubprocess();
    try {
      _setUsageAggregationLauncherForTest("direct");
      _setUsageAggregationSpawnForTest(() => child.subprocess);
      const active = aggregateUsageOffThread("/tmp/tokens.sqlite", {});
      const observedError = active.catch((error: unknown) => error);

      await expect(shutdownUsageAggregationWorker()).rejects.toThrow(
        "aggregation worker forced stop exceeded its cleanup deadline",
      );
      expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      expect((await observedError) as Error).toHaveProperty(
        "message",
        "aggregation worker shut down",
      );
      await expect(aggregateUsageOffThread("/tmp/tokens.sqlite", {})).rejects.toThrow(
        "aggregation worker cleanup failed; relaunch blocked",
      );
    } finally {
      if (previousTimeout === undefined) delete process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
      else process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS = previousTimeout;
      await _resetUsageAggregationWorkerForTest();
    }
  });

  test("shutdown reuses failure-owned cleanup and preserves its failure poison", async () => {
    const previousTimeout = process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
    process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS = "100";
    let observeFirstSignal!: () => void;
    const firstSignal = new Promise<void>((resolve) => {
      observeFirstSignal = resolve;
    });
    const child = controlledAggregationSubprocess({ onSignal: observeFirstSignal });
    try {
      _setUsageAggregationLauncherForTest("direct");
      _setUsageAggregationSpawnForTest(() => child.subprocess);
      const active = aggregateUsageOffThread("/tmp/tokens.sqlite", {});
      const observedError = active.catch((error: unknown) => error);

      child.failStdout(new Error("injected response read failure"));
      await firstSignal;
      await expect(shutdownUsageAggregationWorker()).rejects.toThrow(
        "aggregation worker forced stop exceeded its cleanup deadline",
      );

      expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
      expect((await observedError) as Error).toHaveProperty(
        "message",
        "injected response read failure; aggregation worker forced stop exceeded its cleanup deadline",
      );
      await expect(aggregateUsageOffThread("/tmp/tokens.sqlite", {})).rejects.toThrow(
        "aggregation worker cleanup failed; relaunch blocked: aggregation worker forced stop exceeded its cleanup deadline",
      );
    } finally {
      if (previousTimeout === undefined) delete process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS;
      else process.env.TOKENS_USAGE_WORKER_STOP_TIMEOUT_MS = previousTimeout;
      await _resetUsageAggregationWorkerForTest();
    }
  });
});

describe("2026-10-07 repricing: haiku-5-5 / fable-5-1 / opus-5-5", () => {
  const u = (i: number, o: number, cr: number, c5 = 0, c1 = 0) => ({
    inputTokens: i,
    outputTokens: o,
    cacheReadTokens: cr,
    cacheWrite5mTokens: c5,
    cacheWrite1hTokens: c1,
  });

  test("haiku-5-5 matches the haiku-5 key, not haiku-4 or fallback", () => {
    const p = priceForWithSource("claude-haiku-5-5");
    expect(p.source).toBe("table");
    expect(p.price.input).toBe(0.1);
    expect(p.price.output).toBe(0.5);
    expect(p.price.cacheRead).toBe(0.01);
    expect(priceFor("claude-haiku-5-5-20261001").input).toBe(0.1);
    expect(priceFor("claude-haiku-4-5-20251001").input).toBe(1);
  });

  test("haiku-5-5 <=100K prompt uses the low card", () => {
    // prompt = 50K + 50K = 100K exactly -> low tier
    const c = costFor("claude-haiku-5-5", u(50_000, 1_000_000, 50_000));
    expect(c).toBeCloseTo((50_000 * 0.1 + 1_000_000 * 0.5 + 50_000 * 0.01) / 1e6, 9);
  });

  test("haiku-5-5 >100K prompt (input+cacheRead+cacheWrite) uses the long card per message", () => {
    const c = costFor("claude-haiku-5-5", u(10_000, 1_000_000, 90_001, 1_000, 1_000));
    expect(c).toBeCloseTo(
      (10_000 * 0.5 + 1_000_000 * 2.5 + 90_001 * 0.05 + 1_000 * 0.625 + 1_000 * 1) / 1e6,
      9,
    );
  });

  test("haiku-4-5 never uses the haiku-5 long card", () => {
    const c = costFor("claude-haiku-4-5-20251001", u(200_000, 0, 0));
    expect(c).toBeCloseTo(0.2, 9);
  });

  test("fable-5-1 cache read is $0.25, fable-5 stays $1.00 (no prefix shadowing)", () => {
    expect(priceFor("claude-fable-5-1").cacheRead).toBe(0.25);
    expect(priceFor("claude-fable-5-1-20261001").cacheRead).toBe(0.25);
    expect(priceFor("claude-fable-5").cacheRead).toBe(1);
    expect(priceFor("claude-fable-5-20260101").cacheRead).toBe(1);
    expect(costFor("claude-fable-5-1", u(0, 0, 1_000_000))).toBeCloseTo(0.25, 9);
    expect(costFor("claude-fable-5", u(0, 0, 1_000_000))).toBeCloseTo(1, 9);
  });

  test("opus-5-5 resolves to $4/$20 with cache read $0.20", () => {
    const p = priceForWithSource("claude-opus-5-5");
    expect(p.source).toBe("table");
    expect([p.price.input, p.price.output, p.price.cacheRead]).toEqual([4, 20, 0.2]);
    expect(priceFor("claude-opus-4-7").input).toBe(15);
  });

  test("opus-5 is $5/$25; 5.5 wins by longer key; dated opus-5 id gets the Opus 5 card", () => {
    for (const m of ["claude-opus-5", "claude-opus-5-20260101"]) {
      const p = priceForWithSource(m);
      expect(p.source).toBe("table");
      expect([p.price.input, p.price.output, p.price.cacheRead]).toEqual([5, 25, 0.5]);
      expect([p.price.cacheWrite5m, p.price.cacheWrite1h]).toEqual([6.25, 10]);
    }
    for (const m of ["claude-opus-5-5", "claude-opus-5-5-20261001"]) {
      const p = priceFor(m);
      expect([p.input, p.output, p.cacheRead, p.cacheWrite5m, p.cacheWrite1h]).toEqual([
        4, 20, 0.2, 5, 8,
      ]);
    }
  });

  test("sonnet-5-5 is $2/$10 cache read $0.20", () => {
    const p = priceFor("claude-sonnet-5-5");
    expect([p.input, p.output, p.cacheRead]).toEqual([2, 10, 0.2]);
  });
});

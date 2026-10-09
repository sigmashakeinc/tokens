import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { _resetScannerStateForTest, ingestOnce } from "../src/scanner.ts";
import {
  type CapturedFileCommit,
  CLAUDE_USAGE_PARSER_VERSION,
  CODEX_USAGE_PARSER_VERSION,
  type StoredEvent,
  TokenStore,
} from "../src/store.ts";
import { issueTokenSnapshotHeartbeatTarget } from "../src/token-snapshot-heartbeat.ts";
import {
  buildAuthoritativeTokenSnapshot,
  dailyTokenWindow,
  isAuthoritativeTokenSnapshot,
} from "../src/token-usage-contract.ts";

const UPDATED_AT = Date.parse("2026-07-15T19:00:00.000Z");

function event(source: "claude" | "codex", overrides: Partial<StoredEvent> = {}): StoredEvent {
  const codex = source === "codex";
  return {
    ts: UPDATED_AT - 1_000,
    date: "2026-07-15",
    model: codex ? " gpt-5-codex " : " claude-sonnet-4-6 ",
    project: "snapshot-cache",
    projectSlug: "-snapshot-cache",
    sessionId: `${source}-session`,
    inputTokens: codex ? 5 : 10,
    outputTokens: codex ? 6 : 4,
    cacheReadTokens: codex ? 1 : 3,
    cacheWrite5mTokens: codex ? 0 : 2,
    cacheWrite1hTokens: 0,
    cost: codex ? 0.2 : 0.1,
    filePath: `/fixtures/${source}.jsonl`,
    lineOffset: codex ? 2 : 1,
    source,
    tokenClasses: {
      input: codex ? 5 : 10,
      cachedRead: codex ? 1 : 3,
      cacheWrite: codex ? null : 2,
      visibleOutput: codex ? 2 : 4,
      reasoningOutput: codex ? 4 : null,
      complete: false,
    },
    ...overrides,
  };
}

function sourceCommit(options: {
  generationKey: string;
  mtimeMs: number;
  usageComplete: boolean;
  eventTs?: number;
  source?: "claude" | "codex";
  checkpoint?: Record<string, unknown>;
}): CapturedFileCommit {
  const path = `/fixtures/${options.generationKey}.jsonl`;
  const source = options.source ?? "claude";
  return {
    state: {
      generationKey: options.generationKey,
      source,
      device: "fixture-device",
      inode: options.generationKey,
      representativePath: path,
      capturedSize: 1,
      mtimeMs: options.mtimeMs,
      mtimeNs: null,
      ctimeNs: null,
      lastByteOffset: 1,
      lastLineNumber: 1,
      committedPrefixSha256: "0".repeat(64),
      codexStateJson: JSON.stringify(
        options.checkpoint ?? {
          usageParserVersion: CLAUDE_USAGE_PARSER_VERSION,
          semanticUsageComplete: options.usageComplete,
          defectLineCount: 0,
          defectLineNumbers: [],
          usageComplete: options.usageComplete,
        },
      ),
    },
    aliases: [path],
    events:
      options.eventTs === undefined
        ? []
        : [
            {
              event: event(source, {
                ts: options.eventTs,
                filePath: path,
                sessionId: options.generationKey,
              }),
              canonicalKey: `fixture:${options.generationKey}`,
              rawRecordSha256: "1".repeat(64),
              immutableIdentity: null,
              logicalMessageKey: null,
              byteStart: 0,
              byteEnd: 1,
              lineNumber: 0,
            },
          ],
  };
}

function completeCodexCheckpoint(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    sessionId: "codex-session",
    model: "gpt-5-codex",
    cwd: "/fixtures",
    project: "fixtures",
    projectSlug: "-fixtures",
    totals: null,
    sessionTimestampMs: null,
    usageParserVersion: CODEX_USAGE_PARSER_VERSION,
    threadSourceRoleRepaired: true,
    codexLineageRepaired: true,
    depthOneDirectLineageRepaired: true,
    codexLineageShapeRepaired: true,
    codexDeployedCheckpointRepaired: true,
    lineageDepth: null,
    rootSessionId: null,
    lineageReplayStage: null,
    childSessionId: null,
    forkParentId: null,
    phase: "none",
    boundaryByte: null,
    boundaryLine: null,
    firstLiveRowPending: false,
    rollingDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    tokenCountRows: 0,
    billedUsageRows: 0,
    inheritedSuppressedRows: 0,
    ambiguousQuarantinedRows: 0,
    zeroNoopRows: 0,
    unrecoverableResetRows: 0,
    invalidUsageRows: 0,
    semanticUsageComplete: true,
    defectLineCount: 0,
    defectLineNumbers: [],
    usageComplete: true,
    ...overrides,
  };
}

function structurallyComplete(store: TokenStore, aliases: CapturedFileCommit[] = []): void {
  store.reconcileDiscovery(
    aliases.flatMap((batch) =>
      batch.aliases.map((path) => ({
        path,
        generationKey: batch.state.generationKey,
        source: batch.state.source,
      })),
    ),
    [
      { source: "claude", available: true, complete: true },
      { source: "codex", available: true, complete: true },
    ],
  );
}

test("semantic tail defects do not overwrite structural provider health", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-structural-scan-"));
  const projectsDir = join(root, "projects");
  const codexSessionsDir = join(root, "codex-sessions");
  const projectDir = join(projectsDir, "-fixture");
  await mkdir(projectDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });
  await writeFile(join(projectDir, "incomplete.jsonl"), '{"torn"');
  const previousClaude = process.env.CLAUDE_PROJECTS_DIR;
  const previousCodex = process.env.CODEX_SESSIONS_DIR;
  process.env.CLAUDE_PROJECTS_DIR = projectsDir;
  process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
  _resetScannerStateForTest();
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    await ingestOnce(store);
    expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
      true,
    );
    expect(
      store.buildAuthoritativeTokenSnapshot(Date.now()).providers.claude.discoveryComplete,
    ).toBe(false);
  } finally {
    store.close();
    _resetScannerStateForTest();
    if (previousClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
    else process.env.CLAUDE_PROJECTS_DIR = previousClaude;
    if (previousCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
    else process.env.CODEX_SESSIONS_DIR = previousCodex;
    await rm(root, { recursive: true, force: true });
  }
});

test("Codex daily completeness accepts only a fully valid current checkpoint", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-codex-checkpoint-"));
  const missingVersion = completeCodexCheckpoint();
  delete missingVersion.usageParserVersion;
  const fixtures: Array<{
    name: string;
    checkpoint: Record<string, unknown>;
    complete: boolean;
  }> = [
    { name: "valid-current", checkpoint: completeCodexCheckpoint(), complete: true },
    { name: "missing-version", checkpoint: missingVersion, complete: false },
    {
      name: "legacy-version",
      checkpoint: completeCodexCheckpoint({ usageParserVersion: CODEX_USAGE_PARSER_VERSION - 1 }),
      complete: false,
    },
    {
      name: "future-version",
      checkpoint: completeCodexCheckpoint({ usageParserVersion: CODEX_USAGE_PARSER_VERSION + 1 }),
      complete: false,
    },
    {
      name: "counter-conservation",
      checkpoint: completeCodexCheckpoint({ tokenCountRows: 1 }),
      complete: false,
    },
    {
      name: "quarantined-row",
      checkpoint: completeCodexCheckpoint({
        tokenCountRows: 1,
        ambiguousQuarantinedRows: 1,
      }),
      complete: false,
    },
    {
      name: "unrecoverable-reset-row",
      checkpoint: completeCodexCheckpoint({
        tokenCountRows: 1,
        unrecoverableResetRows: 1,
      }),
      complete: false,
    },
    {
      name: "invalid-usage-row",
      checkpoint: completeCodexCheckpoint({
        tokenCountRows: 1,
        invalidUsageRows: 1,
      }),
      complete: false,
    },
    {
      name: "lineage-identity",
      checkpoint: completeCodexCheckpoint({ phase: "inherited" }),
      complete: false,
    },
    {
      name: "invalid-totals",
      checkpoint: completeCodexCheckpoint({
        totals: {
          inputTokens: 1,
          cachedInputTokens: 2,
          outputTokens: 0,
          reasoningOutputTokens: null,
        },
      }),
      complete: false,
    },
    {
      name: "timestamp-out-of-range",
      checkpoint: completeCodexCheckpoint({ sessionTimestampMs: Number.MAX_SAFE_INTEGER }),
      complete: false,
    },
    {
      name: "dishonest-current",
      checkpoint: completeCodexCheckpoint({ semanticUsageComplete: false }),
      complete: false,
    },
  ];
  try {
    for (const fixture of fixtures) {
      const store = new TokenStore(join(root, `${fixture.name}.sqlite`));
      try {
        const batch = sourceCommit({
          generationKey: fixture.name,
          mtimeMs: UPDATED_AT,
          usageComplete: fixture.checkpoint.usageComplete === true,
          source: "codex",
          checkpoint: fixture.checkpoint,
        });
        store.commitCapturedFile(batch);
        structurallyComplete(store, [batch]);
        expect(
          store.getProviderStates().find((state) => state.source === "codex")?.complete,
          fixture.name,
        ).toBe(true);
        expect(
          store.buildAuthoritativeTokenSnapshot(UPDATED_AT).providers.codex.discoveryComplete,
          fixture.name,
        ).toBe(fixture.complete);
      } finally {
        store.close();
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("daily completeness ignores historical defects, rejects today's tail, and ages at LA midnight", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-window-completeness-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    const window = dailyTokenWindow(UPDATED_AT);
    const historical = sourceCommit({
      generationKey: "historical-incomplete",
      mtimeMs: window.startMs - 1,
      usageComplete: false,
    });
    store.commitCapturedFile(historical);
    structurallyComplete(store, [historical]);
    expect(
      store.buildAuthoritativeTokenSnapshot(UPDATED_AT).providers.claude.discoveryComplete,
    ).toBe(true);

    const currentTail = sourceCommit({
      generationKey: "current-incomplete",
      mtimeMs: window.startMs,
      usageComplete: false,
    });
    store.commitCapturedFile(currentTail);
    structurallyComplete(store, [historical, currentTail]);
    expect(
      store.buildAuthoritativeTokenSnapshot(UPDATED_AT).providers.claude.discoveryComplete,
    ).toBe(false);

    const nextWindowAt = window.endMs + 1;
    expect(
      store.buildAuthoritativeTokenSnapshot(nextWindowAt).providers.claude.discoveryComplete,
    ).toBe(true);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("current occurrences keep old and inactive incomplete generations fail-closed", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-occurrence-completeness-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    const window = dailyTokenWindow(UPDATED_AT);
    const incomplete = sourceCommit({
      generationKey: "inactive-current-occurrence",
      mtimeMs: window.startMs - 1,
      usageComplete: false,
      eventTs: UPDATED_AT - 1_000,
    });
    store.commitCapturedFile(incomplete);
    structurallyComplete(store);

    const snapshot = store.buildAuthoritativeTokenSnapshot(UPDATED_AT);
    expect(snapshot.providers.claude.discoveryComplete).toBe(false);
    expect(snapshot.providers.claude.rawTotals.messages).toBe(1);
    expect(snapshot.providers.claude.classes.input).toEqual({ tokens: null, complete: false });
    expect(snapshot.classes.input).toEqual({ tokens: null, complete: false });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("legacy pending and structural scan failures remain independently fail-closed", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-fail-closed-"));
  const databasePath = join(root, "tokens.sqlite");
  const store = new TokenStore(databasePath);
  try {
    structurallyComplete(store);
    const db = new Database(databasePath);
    try {
      db.prepare(
        "INSERT INTO legacy_reconciliation_pending (path, source, reason) VALUES ('/fixtures/unreadable.jsonl', 'claude', 'unreadable')",
      ).run();
    } finally {
      db.close();
    }
    expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
      true,
    );
    expect(
      store.buildAuthoritativeTokenSnapshot(UPDATED_AT).providers.claude.discoveryComplete,
    ).toBe(false);

    store.reconcileDiscovery([], [{ source: "claude", available: true, complete: false }]);
    const dbAfterFailure = new Database(databasePath);
    try {
      dbAfterFailure
        .prepare(
          "UPDATE legacy_reconciliation_pending SET reason = 'expired-missing' WHERE source = 'claude'",
        )
        .run();
    } finally {
      dbAfterFailure.close();
    }
    expect(
      store.buildAuthoritativeTokenSnapshot(UPDATED_AT).providers.claude.discoveryComplete,
    ).toBe(false);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("semantic sibling defects do not block structurally safe ENOENT expiry", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-legacy-expiry-"));
  const databasePath = join(root, "tokens.sqlite");
  const store = new TokenStore(databasePath);
  try {
    const window = dailyTokenWindow(UPDATED_AT);
    const incomplete = sourceCommit({
      generationKey: "current-semantic-defect",
      mtimeMs: window.startMs,
      usageComplete: false,
    });
    store.commitCapturedFile(incomplete);
    const missingPath = "/fixtures/historical-missing.jsonl";
    const db = new Database(databasePath);
    try {
      db.prepare(
        "INSERT INTO legacy_reconciliation_pending (path, source, reason) VALUES (?, 'claude', 'unreadable')",
      ).run(missingPath);
    } finally {
      db.close();
    }

    const reconciled = store.reconcileDiscovery(
      incomplete.aliases.map((path) => ({
        path,
        generationKey: incomplete.state.generationKey,
        source: incomplete.state.source,
      })),
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
      [{ path: missingPath, source: "claude", reason: "unreadable", observed: "ENOENT" }],
    );
    expect(reconciled.expiredMissingBySource.claude).toBe(1);
    expect(store.getProviderStates().find((state) => state.source === "claude")?.complete).toBe(
      true,
    );
    expect(
      store.buildAuthoritativeTokenSnapshot(UPDATED_AT).providers.claude.discoveryComplete,
    ).toBe(false);

    const readonly = new Database(databasePath, { readonly: true });
    try {
      expect(
        readonly
          .query("SELECT reason FROM legacy_reconciliation_pending WHERE path = ?")
          .get(missingPath),
      ).toEqual({ reason: "expired-missing" });
    } finally {
      readonly.close();
    }
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("daily authority is SQL-rolled, validated, and durable without event materialization", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-cache-"));
  const databasePath = join(root, "tokens.sqlite");
  let store: TokenStore | null = new TokenStore(databasePath);
  try {
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
    );
    store.insertEvents([event("claude"), event("codex")]);
    const eventScan = spyOn(store, "selectEventsInWindow").mockImplementation(() => {
      throw new Error("daily authority materialized event rows");
    });
    try {
      const snapshot = store.refreshAuthoritativeTokenSnapshot(UPDATED_AT);

      expect(eventScan).not.toHaveBeenCalled();
      expect(isAuthoritativeTokenSnapshot(snapshot)).toBe(true);
      expect(snapshot.revision).toBe(store.getRevision());
      expect(snapshot.window.windowKey).toBe("2026-07-15");
      expect(snapshot.calls).toBe(2);
      expect(snapshot.rawTotals).toMatchObject({
        inputTokens: 15,
        rawInputTokens: 21,
        outputTokens: 10,
        cacheReadTokens: 4,
        cacheWrite5mTokens: 2,
        cacheWrite1hTokens: 0,
        messages: 2,
      });
      expect(snapshot.totalCost).toBeCloseTo(0.3);
      expect(snapshot.classes.input).toEqual({ tokens: 15, complete: true });
      expect(snapshot.classes.visibleOutput).toEqual({ tokens: 6, complete: true });
      expect(snapshot.classes.cacheWrite).toEqual({ tokens: null, complete: false });
      expect(snapshot.classes.reasoningOutput).toEqual({ tokens: null, complete: false });
      expect(snapshot.providers.claude.models.map((model) => model.model)).toEqual([
        "claude-sonnet-4-6",
      ]);
      expect(snapshot.providers.codex.models.map((model) => model.model)).toEqual(["gpt-5-codex"]);
      expect(store.readCachedTokenSnapshot()).toEqual(snapshot);
    } finally {
      eventScan.mockRestore();
    }

    store.close();
    store = null;
    const reopened = new TokenStore(databasePath, { readonly: true });
    try {
      const cached = reopened.readCachedTokenSnapshot();
      expect(cached?.revision).toBe(reopened.getRevision());
      expect(cached?.calls).toBe(2);
    } finally {
      reopened.close();
    }
  } finally {
    store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("daily authority excludes the half-open window end", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-window-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
    );
    const window = store.buildAuthoritativeTokenSnapshot(UPDATED_AT).window;
    store.insertEvents([
      event("claude", { ts: window.startMs }),
      event("codex", { ts: window.endMs }),
    ]);

    const snapshot = store.buildAuthoritativeTokenSnapshot(UPDATED_AT);
    expect(snapshot.calls).toBe(1);
    expect(snapshot.providers.claude.rawTotals.messages).toBe(1);
    expect(snapshot.providers.codex.rawTotals.messages).toBe(0);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("authority heartbeat is revision-pinned and changes no semantic totals", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-heartbeat-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    store.insertEvents([event("claude")]);
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: true, complete: true },
      ],
    );
    const revision = store.getRevision();
    const snapshot = store.refreshAuthoritativeTokenSnapshot(UPDATED_AT, revision);
    const touched = store.touchCachedTokenSnapshot(
      UPDATED_AT + 1_000,
      revision,
      snapshot.window.windowKey,
      snapshot.updatedAt,
    );
    expect(touched.updatedAt).toBe(UPDATED_AT + 1_000);
    expect({ ...touched, updatedAt: snapshot.updatedAt }).toEqual(snapshot);

    const replacement = { ...touched, updatedAt: UPDATED_AT + 1_500 };
    store.writeCachedTokenSnapshot(replacement);
    expect(() =>
      store.touchCachedTokenSnapshot(
        UPDATED_AT + 2_000,
        revision,
        snapshot.window.windowKey,
        touched.updatedAt,
      ),
    ).toThrow("authoritative token snapshot heartbeat target is stale");
    expect(store.readCachedTokenSnapshot()).toEqual(replacement);

    const advanced = store.touchCachedTokenSnapshot(
      UPDATED_AT + 2_000,
      revision,
      snapshot.window.windowKey,
      replacement.updatedAt,
    );
    expect(advanced.updatedAt).toBe(UPDATED_AT + 2_000);

    // A mid-scan flush bumps the live store revision without publishing a
    // new snapshot (src/scanner.ts commitScan) — the heartbeat must survive
    // that, since the CAS is against the cached snapshot's own revision, not
    // the live store revision.
    store.insertEvents([event("codex")]);
    expect(issueTokenSnapshotHeartbeatTarget(store, UPDATED_AT + 2_000)).not.toBeNull();
    const survivedRevisionBump = store.touchCachedTokenSnapshot(
      UPDATED_AT + 2_500,
      revision,
      snapshot.window.windowKey,
      advanced.updatedAt,
    );
    expect(survivedRevisionBump.updatedAt).toBe(UPDATED_AT + 2_500);
    expect({ ...survivedRevisionBump, updatedAt: advanced.updatedAt }).toEqual(advanced);
    expect(store.readCachedTokenSnapshot()).toEqual(survivedRevisionBump);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("rejects unsafe authority timestamps and fractional complete classes", () => {
  const snapshot = buildAuthoritativeTokenSnapshot([event("codex")], {
    window: dailyTokenWindow(UPDATED_AT),
    revision: 1,
    updatedAt: UPDATED_AT,
    providerStates: [
      { source: "claude", available: false, complete: false },
      { source: "codex", available: true, complete: true },
    ],
  });

  const fractional = structuredClone(snapshot);
  const model = fractional.providers.codex.models[0]!;
  model.classes.visibleOutput = { tokens: 1.5, complete: true };
  model.classes.reasoningOutput = { tokens: 4.5, complete: true };
  fractional.providers.codex.classes.visibleOutput = { tokens: 1.5, complete: true };
  fractional.providers.codex.classes.reasoningOutput = { tokens: 4.5, complete: true };
  expect(isAuthoritativeTokenSnapshot(fractional)).toBe(false);

  const fractionalTime = structuredClone(snapshot);
  fractionalTime.updatedAt = UPDATED_AT + 0.5;
  expect(isAuthoritativeTokenSnapshot(fractionalTime)).toBe(false);
  const negativeTime = structuredClone(snapshot);
  negativeTime.updatedAt = -1;
  expect(isAuthoritativeTokenSnapshot(negativeTime)).toBe(false);
});

test("rejects fractional token classes before event persistence", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-class-validation-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    const fractional = event("codex", {
      tokenClasses: {
        input: 5,
        cachedRead: 1,
        cacheWrite: null,
        visibleOutput: 1.5,
        reasoningOutput: 4.5,
        complete: false,
      },
    });
    expect(() => store.insertEvents([fractional])).toThrow(
      "stored event token class visibleOutput must be a non-negative safe integer or null",
    );
    const negative = event("claude", {
      lineOffset: 3,
      tokenClasses: {
        input: 10,
        cachedRead: -1,
        cacheWrite: 2,
        visibleOutput: 4,
        reasoningOutput: null,
        complete: false,
      },
    });
    expect(() => store.insertEvents([negative])).toThrow(
      "stored event token class cachedRead must be a non-negative safe integer or null",
    );
    expect(store.stats().events).toBe(0);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("SQL and shared builders use the same model normalization, order, and class gaps", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-snapshot-parity-"));
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    store.reconcileDiscovery(
      [],
      [
        { source: "claude", available: true, complete: true },
        { source: "codex", available: false, complete: false },
      ],
    );
    const events = [
      event("claude", {
        model: "\tAlpha\u00a0",
        lineOffset: 10,
        cost: 100_000_000,
      }),
      event("claude", { model: "Alpha", lineOffset: 11, cost: 0.00001 }),
      event("claude", {
        model: "alpha",
        lineOffset: 12,
        cost: 0.2,
        tokenClasses: {
          input: null,
          cachedRead: 3,
          cacheWrite: 2,
          visibleOutput: 4,
          reasoningOutput: null,
          complete: false,
        },
      }),
    ];
    store.insertEvents(events);

    const sql = store.buildAuthoritativeTokenSnapshot(UPDATED_AT);
    const shared = buildAuthoritativeTokenSnapshot(events, {
      window: dailyTokenWindow(UPDATED_AT),
      revision: store.getRevision(),
      updatedAt: UPDATED_AT,
      providerStates: store.getProviderStates(),
    });

    expect(sql.providers.claude.models.map((model) => model.model)).toEqual(["Alpha", "alpha"]);
    expect(sql.providers.claude.models.map((model) => model.model)).toEqual(
      shared.providers.claude.models.map((model) => model.model),
    );
    expect(sql.rawTotals).toMatchObject({
      inputTokens: shared.rawTotals.inputTokens,
      rawInputTokens: shared.rawTotals.rawInputTokens,
      outputTokens: shared.rawTotals.outputTokens,
      cacheReadTokens: shared.rawTotals.cacheReadTokens,
      cacheWrite5mTokens: shared.rawTotals.cacheWrite5mTokens,
      cacheWrite1hTokens: shared.rawTotals.cacheWrite1hTokens,
      messages: shared.rawTotals.messages,
    });
    expect(Math.abs(sql.totalCost - shared.totalCost)).toBeLessThan(0.001);
    expect(sql.providers.claude.classes.input).toEqual({ tokens: null, complete: false });
    expect(sql.providers.claude.classes).toEqual(shared.providers.claude.classes);
    expect(isAuthoritativeTokenSnapshot(sql)).toBe(true);
    expect(isAuthoritativeTokenSnapshot(shared)).toBe(true);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

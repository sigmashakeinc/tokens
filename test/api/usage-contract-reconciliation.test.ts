// A4 — non-vacuous ground-truth reconciliation for the GET /api/usage
// byModel/unrecognized/health contract.
//
// This test deliberately does NOT stand up an in-process fixture store —
// that would test the test. Instead it:
//   1. Writes a raw Claude-Code-shaped JSONL fixture into the REAL
//      ~/.claude/projects tree the live sigmashake-tokens daemon already
//      polls every TOKENS_POLL_MS (5s), under a uniquely-tagged synthetic
//      project cwd so it can never collide with real session data.
//   2. Polls the LIVE daemon over HTTP (127.0.0.1:7913) until it has
//      ingested those lines.
//   3. Recomputes the expected totals from the raw JSONL bytes with an
//      INDEPENDENT parser + classifier that imports NOTHING from src/ —
//      in particular, it does not import normalizeModelId.
//   4. Asserts a hard-coded non-zero total (not `expect(a).toBe(b)` at
//      0 === 0), the conservation invariant against the full LIVE dataset,
//      and that a "gpt-5-opus-review" sibling id is never routed onto the
//      Claude side.
//
// KNOWN, UNAVOIDABLE-VIA-HTTP SIDE EFFECT: this adds ~10.5k synthetic
// tokens (~$0.05) to the daemon's SQLite history under a project dir
// tagged "fixture-a4-<runId>". The test deletes its own JSONL fixture file
// after asserting against it (see the `finally` block below and the
// `afterAll` backstop) — but that deletion does NOT retroactively remove
// the events already ingested from it. That is intentional, documented
// behavior of this service, not a bug the test can "fix" from the HTTP
// surface: /api/usage's aggregation reads unconditionally from the
// `events` table with no join against `source_aliases.active`
// (src/store.ts `aggregateUsage`), and `reconcileDiscovery` only flips a
// vanished file's alias to inactive bookkeeping — it never deletes rows
// (src/store.ts `reconcileDiscovery`). Per CLAUDE.md: "even if a session
// log gets rotated or deleted, the events stay in the SQLite store" — cost
// history is deliberately permanent, and there is deliberately no HTTP
// delete API. So this test's fixture ids DO persist in `unrecognized[]`
// after the file is gone; asserting otherwise would be asserting a false
// claim about the store. What the test CAN and DOES genuinely assert
// end-to-end, over HTTP, after removing the fixture file: (1) the daemon's
// next ingest tick still reaches "ready" health with no ingest errors
// caused by the now-missing file (exercises reconcileDiscovery's
// alias-deactivation branch for this exact path without crashing/
// degrading ingest), and (2) the full-dataset conservation invariant
// (sum(byModel)+sum(unrecognized) === totals.totalTokens) still holds
// afterward. Actual removal of the residue from `unrecognized[]` requires
// an out-of-band maintenance step with the daemon stopped:
// `tools/purge-tokens-fixture-residue.ts --pattern /fixture-a4- --confirm`.

import { afterAll, describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

const BASE_URL = (process.env.TOKENS_BASE_URL ?? "http://127.0.0.1:7913").replace(/\/+$/, "");
const LIVE = process.env.SIGMASHAKE_LIVE_TESTS === "1";

// --- independent JSONL row model (deliberately duplicated, not imported) ---

interface FixtureRow {
  model: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
}

function fixtureLine(row: FixtureRow, cwd: string, sessionId: string, ts: string): string {
  return JSON.stringify({
    type: "assistant",
    uuid: randomUUID(),
    requestId: randomUUID(),
    sessionId,
    cwd,
    timestamp: ts,
    message: {
      role: "assistant",
      id: randomUUID(),
      model: row.model,
      usage: {
        input_tokens: row.inputTokens,
        output_tokens: row.outputTokens,
        cache_read_input_tokens: row.cacheReadTokens,
        cache_creation: {
          ephemeral_5m_input_tokens: row.cacheWrite5mTokens,
          ephemeral_1h_input_tokens: row.cacheWrite1hTokens,
        },
      },
    },
  });
}

// Deliberately independent classifier: different regex shapes, no shared
// table with src/model-identity.ts, and no import from src/ at all. Only
// needs to be correct for the fixture rows below, but is written using the
// same structural (vendor-prefix, not includes()) discipline the real
// implementation is required to use.
function independentClassify(model: string): {
  harness: "claude" | "codex" | "unknown";
  recognized: boolean;
} {
  const bedrockStripped = model.replace(/^[a-z0-9-]+\.anthropic\./, "").replace(/-v\d+:\d+$/, "");
  if (/^claude-/.test(bedrockStripped)) {
    const known = [
      /^claude-opus-5(-\d{8})?$/,
      /^claude-sonnet-5(-\d{8})?$/,
      /^claude-haiku-4-5(-\d{8})?$/,
      /^claude-haiku-5(-\d{1,2})?(-\d{8})?$/,
    ];
    return { harness: "claude", recognized: known.some((r) => r.test(bedrockStripped)) };
  }
  if (/^gpt-/.test(model)) {
    const known = [/^gpt-5\.3-codex-spark$/];
    return { harness: "codex", recognized: known.some((r) => r.test(model)) };
  }
  return { harness: "unknown", recognized: false };
}

/** Independently re-parses raw fixture JSONL bytes off disk — does not
 *  reuse the in-memory FixtureRow objects used to write the file. */
function independentRecount(rawLines: string[]): {
  totalTokens: number;
  messages: number;
  byRecognizedModel: Map<string, { totalTokens: number; messages: number }>;
  byUnrecognizedModel: Map<string, { totalTokens: number; messages: number }>;
} {
  const byRecognizedModel = new Map<string, { totalTokens: number; messages: number }>();
  const byUnrecognizedModel = new Map<string, { totalTokens: number; messages: number }>();
  let totalTokens = 0;
  let messages = 0;
  for (const line of rawLines) {
    if (!line.trim()) continue;
    const parsed = JSON.parse(line) as {
      message: { model: string; usage: Record<string, unknown> };
    };
    const u = parsed.message.usage;
    const creation = (u.cache_creation ?? {}) as Record<string, number>;
    const total =
      Number(u.input_tokens ?? 0) +
      Number(u.output_tokens ?? 0) +
      Number(u.cache_read_input_tokens ?? 0) +
      Number(creation.ephemeral_5m_input_tokens ?? 0) +
      Number(creation.ephemeral_1h_input_tokens ?? 0);
    totalTokens += total;
    messages += 1;
    const model = parsed.message.model;
    const { recognized } = independentClassify(model);
    const bucket = recognized ? byRecognizedModel : byUnrecognizedModel;
    const existing = bucket.get(model) ?? { totalTokens: 0, messages: 0 };
    existing.totalTokens += total;
    existing.messages += 1;
    bucket.set(model, existing);
  }
  return { totalTokens, messages, byRecognizedModel, byUnrecognizedModel };
}

async function waitForCondition(
  check: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return true;
    if (Date.now() > deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

const runId = randomUUID().slice(0, 8);
// Deliberately avoids a "sigmashake" path segment: /api/usage's `projects`
// filter matches the store's short display "project" name, which for any
// cwd containing "sigmashake-<x>" collapses to "sigmashake-<x>" (see
// CLAUDE.md "Project slug -> cwd is lossy") — that would alias onto real
// sigmashake-tokens session data instead of staying uniquely scoped to
// this fixture. A plain, sigmashake-free cwd keeps the short name as
// exactly the trailing two path segments.
const fixtureCwd = `/tmp/tokensfixture-a4-${runId}`;
// Mirrors the documented (and trivial) cwd -> project short-name transform
// (public, already-documented encoding behavior — not the model-identity
// logic under test): slug = leading "-", "/" -> "-"; short name = last two
// "-"-split segments unless a "sigmashake-<x>" pair is present.
function projectShortNameForTest(cwd: string): string {
  const slug = `-${cwd.replace(/^\/+/, "").replace(/\//g, "-")}`;
  const parts = slug.replace(/^-/, "").split("-");
  for (let i = parts.length - 2; i >= 0; i--) {
    if (parts[i] === "sigmashake" && parts[i + 1]) return `sigmashake-${parts[i + 1]}`;
  }
  return parts.slice(-2).join("-") || slug;
}
const projectShortName = projectShortNameForTest(fixtureCwd);
const projectDirName = `fixture-a4-${runId}`;
const sessionId = randomUUID();
const fixtureDir = join(homedir(), ".claude", "projects", projectDirName);
const fixtureFile = join(fixtureDir, `${sessionId}.jsonl`);

const rows: FixtureRow[] = [
  {
    model: "claude-opus-5",
    inputTokens: 1000,
    outputTokens: 200,
    cacheReadTokens: 300,
    cacheWrite5mTokens: 50,
    cacheWrite1hTokens: 10,
  },
  {
    model: "claude-sonnet-5",
    inputTokens: 2000,
    outputTokens: 400,
    cacheReadTokens: 600,
    cacheWrite5mTokens: 100,
    cacheWrite1hTokens: 20,
  },
  {
    model: "claude-haiku-4-5-20251001",
    inputTokens: 500,
    outputTokens: 100,
    cacheReadTokens: 150,
    cacheWrite5mTokens: 25,
    cacheWrite1hTokens: 5,
  },
  {
    model: "us.anthropic.claude-opus-5-20250917-v1:0",
    inputTokens: 1500,
    outputTokens: 300,
    cacheReadTokens: 450,
    cacheWrite5mTokens: 75,
    cacheWrite1hTokens: 15,
  },
  {
    model: "gpt-5.3-codex-spark",
    inputTokens: 800,
    outputTokens: 150,
    cacheReadTokens: 200,
    cacheWrite5mTokens: 40,
    cacheWrite1hTokens: 0,
  },
  // Deliberately unmapped model id — must be RETAINED in unrecognized[],
  // tokens still counted in totals, never dropped.
  {
    model: "zeta-unmapped-9000",
    inputTokens: 999,
    outputTokens: 111,
    cacheReadTokens: 222,
    cacheWrite5mTokens: 33,
    cacheWrite1hTokens: 6,
  },
  // Prefix-matching correctness case: contains the literal substring
  // "opus" but is a codex/gpt-vendor id with no recognized gpt-5 suffix
  // grammar. Must never be classified as Claude.
  {
    model: "gpt-5-opus-review",
    inputTokens: 111,
    outputTokens: 22,
    cacheReadTokens: 33,
    cacheWrite5mTokens: 4,
    cacheWrite1hTokens: 1,
  },
];

const HARD_CODED_EXPECTED_TOTAL_TOKENS = 10532;
const HARD_CODED_EXPECTED_RECOGNIZED_TOTAL = 8990;
const HARD_CODED_EXPECTED_UNRECOGNIZED_TOTAL = 1542;

interface UsageModelBucket {
  model: string;
  displayName: string;
  family: string;
  harness: string;
  recognized: boolean;
  totalTokens: number;
  messages: number;
}

interface UsageUnrecognizedBucket {
  rawModel: string;
  totalTokens: number;
  messages: number;
}

interface UsageResponse {
  totals: { totalTokens: number; messages: number };
  byModel: UsageModelBucket[];
  unrecognized: UsageUnrecognizedBucket[];
  health: {
    state: string;
    consecutiveFailures: number;
    lastIngestAt: number;
    staleSec: number;
  };
}

interface FullUsageWithHealth {
  totals: { totalTokens: number };
  byModel: Array<{ totalTokens: number }>;
  unrecognized: Array<{ totalTokens: number }>;
  health: { state: string; lastIngestAt: number };
}

const maybeDescribe = LIVE ? describe : describe.skip;

maybeDescribe("GET /api/usage — live-daemon byModel/unrecognized/health contract (A4)", () => {
  afterAll(async () => {
    await rm(fixtureDir, { recursive: true, force: true });
  });

  test("ingests a raw JSONL fixture tree and reconciles against an independent recount", async () => {
    try {
      await runFixtureIngestAndReconciliationAssertions();
    } finally {
      // Self-cleaning: remove the fixture FILES immediately, even on
      // assertion failure, so a failing run doesn't leave the JSONL tree
      // behind for longer than this test's own lifetime. (This does not,
      // and per the comment at the top of this file cannot, remove the
      // already-ingested EVENT ROWS — see `tools/purge-tokens-fixture-
      // residue.ts` for that.) `afterAll` below is a suite-level backstop
      // in case this `finally` itself never runs (e.g. a hard process kill).
      await rm(fixtureDir, { recursive: true, force: true });
    }
  }, 90_000);

  async function runFixtureIngestAndReconciliationAssertions(): Promise<void> {
    await mkdir(fixtureDir, { recursive: true });
    const baseTs = Date.parse("2026-07-31T12:00:00.000Z");
    const rawLines = rows.map((row, i) =>
      fixtureLine(row, fixtureCwd, sessionId, new Date(baseTs + i * 1000).toISOString()),
    );
    await writeFile(fixtureFile, `${rawLines.join("\n")}\n`, "utf8");

    // 1. Independent recount from the raw bytes on disk (re-read, not the
    // in-memory `rows`/`rawLines` closures) — imports nothing from src/.
    const diskLines = (await readFile(fixtureFile, "utf8")).split("\n");
    const expected = independentRecount(diskLines);
    expect(expected.totalTokens).toBe(HARD_CODED_EXPECTED_TOTAL_TOKENS);
    let recognizedSum = 0;
    for (const v of expected.byRecognizedModel.values()) recognizedSum += v.totalTokens;
    let unrecognizedSum = 0;
    for (const v of expected.byUnrecognizedModel.values()) unrecognizedSum += v.totalTokens;
    expect(recognizedSum).toBe(HARD_CODED_EXPECTED_RECOGNIZED_TOTAL);
    expect(unrecognizedSum).toBe(HARD_CODED_EXPECTED_UNRECOGNIZED_TOTAL);
    expect(recognizedSum + unrecognizedSum).toBe(HARD_CODED_EXPECTED_TOTAL_TOKENS);

    // 2. Query the LIVE daemon over HTTP, scoped to our uniquely-tagged
    // synthetic project, until it has picked up the fixture (background
    // ingest tick runs every TOKENS_POLL_MS, default 5s).
    const scopedUrl = `${BASE_URL}/api/usage?projects=${encodeURIComponent(projectShortName)}`;
    let last: UsageResponse | null = null;
    const ready = await waitForCondition(
      async () => {
        const res = await fetch(scopedUrl);
        if (!res.ok) return false;
        last = (await res.json()) as UsageResponse;
        return last?.totals?.messages === rows.length;
      },
      45_000,
      2_000,
    );
    expect(ready).toBe(true);
    if (!last) throw new Error("live daemon never returned a scoped /api/usage response");
    const usage: UsageResponse = last;

    // 3. Hard-coded non-zero assertion against the LIVE scoped response —
    // not a bare expect(live).toBe(expected), which would pass vacuously
    // at 0 === 0.
    expect(usage.totals.totalTokens).toBe(HARD_CODED_EXPECTED_TOTAL_TOKENS);
    expect(usage.totals.messages).toBe(rows.length);

    // 4. Conservation invariant on the scoped fixture slice, cross-checked
    // against the independent recount (not just internal self-consistency).
    const liveByModelSum = usage.byModel.reduce((sum, m) => sum + m.totalTokens, 0);
    const liveUnrecognizedSum = usage.unrecognized.reduce((sum, m) => sum + m.totalTokens, 0);
    expect(liveByModelSum + liveUnrecognizedSum).toBe(usage.totals.totalTokens);
    expect(liveByModelSum).toBe(HARD_CODED_EXPECTED_RECOGNIZED_TOTAL);
    expect(liveUnrecognizedSum).toBe(HARD_CODED_EXPECTED_UNRECOGNIZED_TOTAL);

    // 5. Prefix-matching correctness: "gpt-5-opus-review" must be routed to
    // unrecognized[] (never absorbed into a Claude byModel bucket, and
    // never silently merged into the unrelated "gpt-5" recognized family).
    const opusReviewInByModel = usage.byModel.some((m) => m.model === "gpt-5-opus-review");
    expect(opusReviewInByModel).toBe(false);
    const opusReviewUnrecognized = usage.unrecognized.find(
      (m) => m.rawModel === "gpt-5-opus-review",
    );
    expect(opusReviewUnrecognized?.totalTokens).toBe(171);
    // And it must not have inflated the bare "gpt-5" family bucket, which
    // this fixture never emits — proves no substring/prefix bleed.
    const bareGpt5 = usage.byModel.find((m) => m.family === "gpt-5");
    expect(bareGpt5).toBeUndefined();

    // 6. Every byModel entry claims recognized:true and carries the new
    // contract fields; every unrecognized entry keeps its raw id.
    for (const m of usage.byModel) {
      if (!rows.some((r) => r.model === m.model)) continue;
      expect(m.recognized).toBe(true);
      expect(typeof m.family).toBe("string");
      expect(typeof m.displayName).toBe("string");
      expect(["claude", "codex"]).toContain(m.harness);
    }
    for (const raw of ["zeta-unmapped-9000", "gpt-5-opus-review"]) {
      const entry = usage.unrecognized.find((m) => m.rawModel === raw);
      expect(entry).toBeDefined();
    }

    // 7. health block is present and well-typed.
    expect(["ready", "degraded"]).toContain(usage.health.state);
    expect(typeof usage.health.consecutiveFailures).toBe("number");
    expect(typeof usage.health.lastIngestAt).toBe("number");
    expect(typeof usage.health.staleSec).toBe("number");

    // 8. Genuine, bounded end-to-end coverage of reconcileDiscovery's
    // alias-deactivation path for THIS fixture, without asserting the false
    // claim that ingested events disappear (see the file-header comment for
    // why that's architecturally impossible via the HTTP-only surface this
    // test is required to use). Delete the fixture file, then force and
    // await a fresh ingest tick that observes it gone, and assert the
    // daemon: (a) does not degrade/error because a previously-tracked file
    // vanished mid-stream, and (b) still satisfies the full-dataset
    // conservation invariant afterward. `lastIngestAt` strictly advancing
    // past `preDeleteHealth.lastIngestAt` proves a real post-deletion scan
    // ran (not a stale/cached response), which is what actually exercises
    // the alias-deactivation branch in `reconcileDiscovery` for this path.
    const preDeleteLastIngestAt = usage.health.lastIngestAt;
    await rm(fixtureDir, { recursive: true, force: true });

    let fullBody: FullUsageWithHealth | null = null;
    const postDeleteScanned = await waitForCondition(
      async () => {
        const res = await fetch(`${BASE_URL}/api/usage`);
        if (!res.ok) return false;
        fullBody = (await res.json()) as FullUsageWithHealth;
        return (
          fullBody !== null &&
          fullBody.health.state === "ready" &&
          fullBody.health.lastIngestAt > preDeleteLastIngestAt
        );
      },
      20_000,
      2_000,
    );
    expect(postDeleteScanned).toBe(true);
    if (!fullBody)
      throw new Error("live daemon never returned a post-deletion /api/usage response");

    const fullByModelSum = fullBody.byModel.reduce((sum, m) => sum + m.totalTokens, 0);
    const fullUnrecognizedSum = fullBody.unrecognized.reduce((sum, m) => sum + m.totalTokens, 0);
    expect(fullByModelSum + fullUnrecognizedSum).toBe(fullBody.totals.totalTokens);
  }

  test("conservation invariant holds against the FULL live dataset, not just the fixture slice", async () => {
    const res = await fetch(`${BASE_URL}/api/usage`);
    expect(res.ok).toBe(true);
    const body = (await res.json()) as {
      totals: { totalTokens: number };
      byModel: Array<{ totalTokens: number }>;
      unrecognized: Array<{ totalTokens: number }>;
    };
    const byModelSum = body.byModel.reduce((sum, m) => sum + m.totalTokens, 0);
    const unrecognizedSum = body.unrecognized.reduce((sum, m) => sum + m.totalTokens, 0);
    expect(byModelSum + unrecognizedSum).toBe(body.totals.totalTokens);
    expect(body.totals.totalTokens).toBeGreaterThan(0);
  }, 30_000);
});

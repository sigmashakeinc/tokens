import { describe, expect, test } from "bun:test";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readSavings } from "../src/savings.ts";

// Helper: creates a temp .sigmashake dir, writes lines to token-savings.jsonl,
// sets SIGMASHAKE_HOME, runs fn, then cleans up.
async function withLedger(lines: string[], fn: () => Promise<void>): Promise<void> {
  const dir = join(tmpdir(), `savings-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  const prev = process.env.SIGMASHAKE_HOME;
  process.env.SIGMASHAKE_HOME = dir;
  try {
    await writeFile(join(dir, "token-savings.jsonl"), lines.join("\n") + "\n");
    await fn();
  } finally {
    if (prev === undefined) delete process.env.SIGMASHAKE_HOME;
    else process.env.SIGMASHAKE_HOME = prev;
    await rm(dir, { recursive: true, force: true });
  }
}

// Helper: sets SIGMASHAKE_HOME to an empty dir (no ledger file).
async function withEmptyHome(fn: () => Promise<void>): Promise<void> {
  const dir = join(tmpdir(), `savings-test-empty-${Date.now()}`);
  await mkdir(dir, { recursive: true });
  const prev = process.env.SIGMASHAKE_HOME;
  process.env.SIGMASHAKE_HOME = dir;
  try {
    await fn();
  } finally {
    if (prev === undefined) delete process.env.SIGMASHAKE_HOME;
    else process.env.SIGMASHAKE_HOME = prev;
    await rm(dir, { recursive: true, force: true });
  }
}

describe("readSavings", () => {
  test("missing file returns zeros and present=false", async () => {
    await withEmptyHome(async () => {
      const result = await readSavings();
      expect(result.present).toBe(false);
      expect(result.combined.count).toBe(0);
      expect(result.combined.origBytes).toBe(0);
      expect(result.combined.compBytes).toBe(0);
      expect(result.combined.savedBytes).toBe(0);
      expect(result.combined.savedPct).toBe(0);
      expect(result.combined.savedTokensEst).toBe(0);
      expect(result.byTool.stk.count).toBe(0);
      expect(result.byTool.headroom.count).toBe(0);
    });
  });

  test("per-tool and combined aggregation", async () => {
    const lines = [
      JSON.stringify({ tool: "stk", ts: 1000, orig_bytes: 1000, comp_bytes: 400 }),
      JSON.stringify({ tool: "stk", ts: 1001, orig_bytes: 2000, comp_bytes: 800 }),
      JSON.stringify({ tool: "headroom", ts: 1002, orig_bytes: 500, comp_bytes: 200 }),
    ];
    await withLedger(lines, async () => {
      const result = await readSavings();
      expect(result.present).toBe(true);

      // STK: 2 events, 3000 orig, 1200 comp
      expect(result.byTool.stk.count).toBe(2);
      expect(result.byTool.stk.origBytes).toBe(3000);
      expect(result.byTool.stk.compBytes).toBe(1200);
      expect(result.byTool.stk.savedBytes).toBe(1800);
      expect(result.byTool.stk.savedPct).toBeCloseTo(60);
      expect(result.byTool.stk.savedTokensEst).toBe(Math.round(1800 / 4));

      // headroom: 1 event, 500 orig, 200 comp
      expect(result.byTool.headroom.count).toBe(1);
      expect(result.byTool.headroom.origBytes).toBe(500);
      expect(result.byTool.headroom.compBytes).toBe(200);
      expect(result.byTool.headroom.savedBytes).toBe(300);
      expect(result.byTool.headroom.savedPct).toBeCloseTo(60);
      expect(result.byTool.headroom.savedTokensEst).toBe(Math.round(300 / 4));

      // combined: 3 events, 3500 orig, 1400 comp
      expect(result.combined.count).toBe(3);
      expect(result.combined.origBytes).toBe(3500);
      expect(result.combined.compBytes).toBe(1400);
      expect(result.combined.savedBytes).toBe(2100);
      expect(result.combined.savedPct).toBeCloseTo(60);
      expect(result.combined.savedTokensEst).toBe(Math.round(2100 / 4));
    });
  });

  test("malformed lines are skipped — invalid JSON, blank lines, missing fields", async () => {
    const lines = [
      JSON.stringify({ tool: "stk", ts: 1000, orig_bytes: 500, comp_bytes: 200 }), // valid
      "not json at all", // bad JSON
      "", // blank
      '{"tool":"stk","ts":1001}', // missing orig_bytes/comp_bytes
      '{"tool":"headroom","orig_bytes":"string","comp_bytes":50}', // wrong type for orig_bytes
      JSON.stringify({ tool: "headroom", ts: 1002, orig_bytes: 100, comp_bytes: 50 }), // valid
    ];
    await withLedger(lines, async () => {
      const result = await readSavings();
      // Only 2 valid lines should be counted
      expect(result.combined.count).toBe(2);
      expect(result.byTool.stk.count).toBe(1);
      expect(result.byTool.headroom.count).toBe(1);
      expect(result.byTool.stk.savedBytes).toBe(300);
      expect(result.byTool.headroom.savedBytes).toBe(50);
    });
  });

  test("negative saved_bytes are summed honestly", async () => {
    // tiny input where comp > orig (e.g. overhead bytes added)
    const lines = [
      JSON.stringify({ tool: "stk", ts: 1000, orig_bytes: 10, comp_bytes: 15 }), // -5 saved
      JSON.stringify({ tool: "stk", ts: 1001, orig_bytes: 1000, comp_bytes: 400 }), // +600 saved
    ];
    await withLedger(lines, async () => {
      const result = await readSavings();
      // savedBytes = (10 - 15) + (1000 - 400) = -5 + 600 = 595
      expect(result.byTool.stk.savedBytes).toBe(595);
      expect(result.combined.savedBytes).toBe(595);
      // savedTokensEst = Math.round(595 / 4) = 149
      expect(result.byTool.stk.savedTokensEst).toBe(Math.round(595 / 4));
    });
  });

  test("unknown tool is bucketed under combined only, not under stk or headroom", async () => {
    const lines = [
      JSON.stringify({ tool: "unknown-future-tool", ts: 1000, orig_bytes: 200, comp_bytes: 100 }),
      JSON.stringify({ tool: "stk", ts: 1001, orig_bytes: 300, comp_bytes: 120 }),
    ];
    await withLedger(lines, async () => {
      const result = await readSavings();
      // combined sees both
      expect(result.combined.count).toBe(2);
      expect(result.combined.origBytes).toBe(500);
      // stk sees only its own entry
      expect(result.byTool.stk.count).toBe(1);
      expect(result.byTool.stk.origBytes).toBe(300);
      // headroom sees nothing
      expect(result.byTool.headroom.count).toBe(0);
    });
  });

  test("savedPct is 0 when origBytes is 0", async () => {
    // Edge case: all entries have zero orig_bytes
    const lines = [JSON.stringify({ tool: "stk", ts: 1000, orig_bytes: 0, comp_bytes: 0 })];
    await withLedger(lines, async () => {
      const result = await readSavings();
      expect(result.combined.savedPct).toBe(0);
      expect(result.byTool.stk.savedPct).toBe(0);
    });
  });

  test("ledgerPath reflects the SIGMASHAKE_HOME env variable", async () => {
    await withEmptyHome(async () => {
      const result = await readSavings();
      expect(result.ledgerPath).toContain("token-savings.jsonl");
      expect(result.ledgerPath).toContain(process.env.SIGMASHAKE_HOME as string);
    });
  });
});

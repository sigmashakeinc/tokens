// Reads the shared token-savings ledger written by the STK and headroom CLI
// reducers. This is a completely separate data path from the SQLite events
// store — it never touches tokens.sqlite, the scanner, or the store.
//
// Ledger path: ${SIGMASHAKE_HOME:-~/.sigmashake}/token-savings.jsonl
// Each line: {"tool":"stk"|"headroom","ts":<unix_seconds>,"orig_bytes":<int>,"comp_bytes":<int>}
//
// Called on every /api/savings request; file is small so read+aggregate
// per-request is fine — no poll loop, no sqlite ingestion, no new SSE channel.

import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { dataHome } from "./paths.ts";

export interface SavingsAgg {
  /** Number of reduction events recorded for this bucket. */
  count: number;
  /** Sum of original byte lengths before reduction. */
  origBytes: number;
  /** Sum of compressed/reduced byte lengths actually emitted. */
  compBytes: number;
  /** origBytes - compBytes. May be negative for tiny inputs; summed honestly. */
  savedBytes: number;
  /** savedBytes / origBytes * 100. 0 when origBytes === 0. */
  savedPct: number;
  /** Rough token estimate: Math.round(savedBytes / 4). */
  savedTokensEst: number;
}

/** Dynamic per-tool map. stk and headroom are always present (zero-valued
 *  when no ledger entries exist) so existing callers accessing .stk / .headroom
 *  continue to work. Additional tools (graphify, oracle, …) appear as extra keys. */
export type ByToolMap = Record<string, SavingsAgg> & { stk: SavingsAgg; headroom: SavingsAgg };

export interface SavingsResult {
  byTool: ByToolMap;
  combined: SavingsAgg;
  ledgerPath: string;
  /** false when the ledger file does not exist yet (all values are zero). */
  present: boolean;
}

function zeroed(): SavingsAgg {
  return { count: 0, origBytes: 0, compBytes: 0, savedBytes: 0, savedPct: 0, savedTokensEst: 0 };
}

/** Computes the derived fields in-place from accumulated origBytes/compBytes. */
function finalize(agg: SavingsAgg): SavingsAgg {
  agg.savedBytes = agg.origBytes - agg.compBytes;
  agg.savedPct = agg.origBytes > 0 ? (agg.savedBytes / agg.origBytes) * 100 : 0;
  agg.savedTokensEst = Math.round(agg.savedBytes / 4);
  return agg;
}

/** Builds the typed ByToolMap, guaranteeing stk and headroom are always present.
 *  Callers must have already called finalize() on all existing map entries. */
function buildByTool(map: Map<string, SavingsAgg>): ByToolMap {
  if (!map.has("stk")) map.set("stk", finalize(zeroed()));
  if (!map.has("headroom")) map.set("headroom", finalize(zeroed()));
  return Object.fromEntries(map) as ByToolMap;
}

export async function readSavings(): Promise<SavingsResult> {
  const home = dataHome();
  const ledgerPath = join(home, "token-savings.jsonl");

  const byTool = new Map<string, SavingsAgg>();
  const combined = zeroed();

  let text: string;
  try {
    text = await readFile(ledgerPath, "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    // A genuinely absent ledger is a complete zero state. Permission, I/O,
    // and wrong-file-type errors reject so callers cannot report false zeros.
    return {
      byTool: buildByTool(byTool),
      combined: finalize(combined),
      ledgerPath,
      present: false,
    };
  }

  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line) continue;

    let rec: unknown;
    try {
      rec = JSON.parse(line);
    } catch {
      continue; // skip malformed / partial lines
    }

    if (typeof rec !== "object" || rec === null) continue;
    const r = rec as Record<string, unknown>;

    const origBytes = typeof r.orig_bytes === "number" ? r.orig_bytes : null;
    const compBytes = typeof r.comp_bytes === "number" ? r.comp_bytes : null;
    // Both fields must be numeric — skip lines that lack them.
    if (origBytes === null || compBytes === null) continue;

    // Combined always accumulates.
    combined.count++;
    combined.origBytes += origBytes;
    combined.compBytes += compBytes;

    // Dynamic per-tool bucket — any non-empty tool string gets its own entry.
    const tool = typeof r.tool === "string" ? r.tool : "";
    if (tool) {
      let bucket = byTool.get(tool);
      if (!bucket) {
        bucket = zeroed();
        byTool.set(tool, bucket);
      }
      bucket.count++;
      bucket.origBytes += origBytes;
      bucket.compBytes += compBytes;
    }
  }

  // Finalize all accumulated per-tool buckets.
  for (const agg of byTool.values()) finalize(agg);

  return {
    byTool: buildByTool(byTool),
    combined: finalize(combined),
    ledgerPath,
    present: true,
  };
}

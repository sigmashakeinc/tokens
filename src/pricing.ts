// Anthropic and OpenAI API pricing in USD per 1M tokens.
// Anthropic: cache-read = 10% of input; 5m ephemeral cache-write = 125%; 1h ephemeral = 200%.
// GPT-5.6 family: explicit per-class prices including cache-write at 1.25x input (entries below);
// long-context >272K is per-class and request-scoped (input classes 2x, output 1.5x).
// Match by substring (longest-prefix wins) so dated model IDs still resolve; the gpt-5.6 family
// additionally uses strict snapshot-shaped suffix matching (no unrestricted substring).

export interface ModelPrice {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite5m: number;
  cacheWrite1h: number;
}

const TABLE: Record<string, ModelPrice> = {
  // source: developers.openai.com/api/docs/pricing, retrieved 2026-07-10T01:05Z.
  // Standard tier only; Flex, Batch, and Priority tiers are out of scope.
  // OpenAI cache-write events map to cacheWrite5m (the only write class this
  // schema carries); the documented rate is 1.25x input. No 1h distinction.
  "gpt-5.6-terra": {
    input: 2.5,
    output: 15,
    cacheRead: 0.25,
    cacheWrite5m: 3.125,
    cacheWrite1h: 3.125,
  },
  "gpt-5.6-luna": {
    input: 1,
    output: 6,
    cacheRead: 0.1,
    cacheWrite5m: 1.25,
    cacheWrite1h: 1.25,
  },
  "gpt-5.6-sol": {
    input: 5,
    output: 30,
    cacheRead: 0.5,
    cacheWrite5m: 6.25,
    cacheWrite1h: 6.25,
  },
  "gpt-5.5-cyber-preview": {
    input: 5,
    output: 30,
    cacheRead: 0.5,
    cacheWrite5m: 5,
    cacheWrite1h: 5,
  },
  "gpt-5.5-cyber": {
    input: 5,
    output: 30,
    cacheRead: 0.5,
    cacheWrite5m: 5,
    cacheWrite1h: 5,
  },
  "gpt-5.5": {
    input: 5,
    output: 30,
    cacheRead: 0.5,
    cacheWrite5m: 5,
    cacheWrite1h: 5,
  },
  "gpt-5.4-mini": {
    input: 0.75,
    output: 4.5,
    cacheRead: 0.075,
    cacheWrite5m: 0.75,
    cacheWrite1h: 0.75,
  },
  "gpt-5.4": {
    input: 2.5,
    output: 15,
    cacheRead: 0.25,
    cacheWrite5m: 2.5,
    cacheWrite1h: 2.5,
  },
  "gpt-5.3-codex": {
    input: 1.75,
    output: 14,
    cacheRead: 0.175,
    cacheWrite5m: 1.75,
    cacheWrite1h: 1.75,
  },
  "gpt-5.2-codex": {
    input: 1.75,
    output: 14,
    cacheRead: 0.175,
    cacheWrite5m: 1.75,
    cacheWrite1h: 1.75,
  },
  "gpt-5.1-codex": {
    input: 1.75,
    output: 14,
    cacheRead: 0.175,
    cacheWrite5m: 1.75,
    cacheWrite1h: 1.75,
  },
  "gpt-5-codex": {
    input: 1.75,
    output: 14,
    cacheRead: 0.175,
    cacheWrite5m: 1.75,
    cacheWrite1h: 1.75,
  },
  "gpt-5": {
    input: 2.5,
    output: 15,
    cacheRead: 0.25,
    cacheWrite5m: 2.5,
    cacheWrite1h: 2.5,
  },
  // source: platform.claude.com/docs/en/about-claude/pricing, fetched 2026-07-01.
  // Fable 5 — the new Mythos-class tier, priced above Opus 4.8 ($5/$25).
  // Flat rate (not time-tiered, unlike Sonnet 5 below).
  "claude-fable-5": {
    input: 10,
    output: 50,
    cacheRead: 1,
    cacheWrite5m: 12.5,
    cacheWrite1h: 20,
  },
  // source: Anthropic bundled claude-api skill (Claude Code 2.1.293) models.md,
  // 2026-10-07. Fable 5.1 cache read is 0.025x input ($0.25), NOT Fable 5's $1.00.
  // Write multipliers (1.25x / 2x) are ASSUMED identical to Fable 5 (not separately published).
  // Longer key wins, so this is never shadowed by "claude-fable-5".
  "claude-fable-5-1": {
    input: 10,
    output: 50,
    cacheRead: 0.25,
    cacheWrite5m: 12.5,
    cacheWrite1h: 20,
  },
  // Opus 5.5: $4/$20, cache read 0.05x, writes 1.25x / 2x. Longer key beats "claude-opus-5".
  "claude-opus-5-5": {
    input: 4,
    output: 20,
    cacheRead: 0.2,
    cacheWrite5m: 5,
    cacheWrite1h: 8,
  },
  // Opus 5 (predecessor of 5.5): $5/$25 per the claude-api model table. Docs give no
  // explicit cache-read price; ASSUMED 0.1x input ($0.50), writes 1.25x / 2x.
  // Before 2026-10-07 this id had no key and fell to FALLBACK ($3/$15).
  "claude-opus-5": {
    input: 5,
    output: 25,
    cacheRead: 0.5,
    cacheWrite5m: 6.25,
    cacheWrite1h: 10,
  },
  "claude-opus-4": {
    input: 15,
    output: 75,
    cacheRead: 1.5,
    cacheWrite5m: 18.75,
    cacheWrite1h: 30,
  },
  "claude-opus-3": {
    input: 15,
    output: 75,
    cacheRead: 1.5,
    cacheWrite5m: 18.75,
    cacheWrite1h: 30,
  },
  // source: platform.claude.com/docs/en/about-claude/pricing, fetched 2026-07-01.
  // Sonnet 5 INTRODUCTORY pricing, in effect through 2026-08-31. Standard
  // pricing takes over 2026-09-01 and rises to input:3/output:15/
  // cacheRead:0.3/cacheWrite5m:3.75/cacheWrite1h:6 (identical to the
  // claude-sonnet-4 entry below) — bump this entry on/after that date.
  "claude-sonnet-5": {
    input: 2,
    output: 10,
    cacheRead: 0.2,
    cacheWrite5m: 2.5,
    cacheWrite1h: 4,
  },
  "claude-sonnet-4": {
    input: 3,
    output: 15,
    cacheRead: 0.3,
    cacheWrite5m: 3.75,
    cacheWrite1h: 6,
  },
  "claude-3-7-sonnet": {
    input: 3,
    output: 15,
    cacheRead: 0.3,
    cacheWrite5m: 3.75,
    cacheWrite1h: 6,
  },
  "claude-3-5-sonnet": {
    input: 3,
    output: 15,
    cacheRead: 0.3,
    cacheWrite5m: 3.75,
    cacheWrite1h: 6,
  },
  "claude-haiku-4": {
    input: 1,
    output: 5,
    cacheRead: 0.1,
    cacheWrite5m: 1.25,
    cacheWrite1h: 2,
  },
  // Haiku 5 (claude-haiku-5-5): this entry is the <=100K-prompt card. The >100K
  // card is HAIKU_5_LONG, applied per message inside costFor() (which receives
  // the usage). priceFor()/priceForWithSource() are model-id-only, so they (and
  // the /api/usage `pricing` map) always report the <=100K card.
  "claude-haiku-5": {
    input: 0.1,
    output: 0.5,
    cacheRead: 0.01,
    cacheWrite5m: 0.125,
    cacheWrite1h: 0.2,
  },
  "claude-3-5-haiku": {
    input: 0.8,
    output: 4,
    cacheRead: 0.08,
    cacheWrite5m: 1,
    cacheWrite1h: 1.6,
  },
  "claude-3-haiku": {
    input: 0.25,
    output: 1.25,
    cacheRead: 0.03,
    cacheWrite5m: 0.3,
    cacheWrite1h: 0.5,
  },
};

/** Haiku 5 card for prompts >100K tokens (input + cache read + cache creation). */
const HAIKU_5_LONG: ModelPrice = {
  input: 0.5,
  output: 2.5,
  cacheRead: 0.05,
  cacheWrite5m: 0.625,
  cacheWrite1h: 1,
};
const HAIKU_5_LONG_PROMPT_TOKENS = 100_000;

const FALLBACK: ModelPrice = {
  input: 3,
  output: 15,
  cacheRead: 0.3,
  cacheWrite5m: 3.75,
  cacheWrite1h: 6,
};

/** "table" = matched an explicit TABLE entry. "fallback" = unknown model,
 *  silently priced at the Sonnet-tier FALLBACK rate. "unavailable" = a known
 *  vendor without a published USD row (Grok); cost stays 0. */
export type PriceSource = "table" | "fallback" | "unavailable";

export interface PricedModel {
  price: ModelPrice;
  source: PriceSource;
}

function tableKeyMatches(model: string, key: string): boolean {
  if (!model.startsWith(key)) return false;
  if (model === key) return true;
  const suffix = model.slice(key.length);
  if (!suffix.startsWith("-")) return false;

  // GPT-5.6 snapshots have a deliberately narrow suffix grammar. This keeps
  // gpt-5.6-solar and other sibling IDs from being priced as gpt-5.6-sol.
  if (key.startsWith("gpt-5.6-")) {
    return suffix === "-preview" || /^-\d{4}-\d{2}-\d{2}$/.test(suffix);
  }
  return true;
}

/** Provenance-visible price lookup. priceFor() below is a thin wrapper
 *  kept for existing call sites that only need the price, not its source. */
const UNAVAILABLE_PRICE: ModelPrice = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite5m: 0,
  cacheWrite1h: 0,
};

export function priceForWithSource(model: string): PricedModel {
  if (!model) return { price: FALLBACK, source: "fallback" };
  const m = model.toLowerCase();
  if (/^grok(-|$)/.test(m)) return { price: UNAVAILABLE_PRICE, source: "unavailable" };
  const keys = Object.keys(TABLE).sort((a, b) => b.length - a.length);
  for (const k of keys) {
    if (tableKeyMatches(m, k)) return { price: TABLE[k]!, source: "table" };
  }
  return { price: FALLBACK, source: "fallback" };
}

export function priceFor(model: string): ModelPrice {
  return priceForWithSource(model).price;
}

export interface UsageRow {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
}

export type LongContextOutcome = "base-price" | "multiplied" | "adjustment-required";

export interface RequestCost {
  cost: number;
  longContext: LongContextOutcome;
}

/**
 * Request-scoped GPT-5.6 cost outcome. Above 272K billable context, input,
 * cache-read, and cache-write are 2x while output is 1.5x. Contested capture:
 * pricing-page captures disagreed (an archived grep's 10/1/12.5/60 row is the
 * Priority tier), corroborated via batch-tier halving; re-verify at next
 * operator-attended fetch. Existing ingestion does not persist a per-request
 * context length, so callers without it receive an explicit adjustment-required
 * outcome rather than an unqualified base-price result.
 */
export function costForRequest(
  model: string,
  u: UsageRow,
  billableContextTokens?: number,
): RequestCost {
  const cost = costFor(model, u);
  const isGpt56 =
    priceForWithSource(model).source === "table" && model.toLowerCase().startsWith("gpt-5.6-");
  if (!isGpt56) return { cost, longContext: "base-price" };
  if (billableContextTokens === undefined) return { cost, longContext: "adjustment-required" };
  if (billableContextTokens <= 272_000) return { cost, longContext: "base-price" };

  const p = priceFor(model);
  const longContextCost =
    (u.inputTokens * p.input * 2 +
      u.cacheReadTokens * p.cacheRead * 2 +
      u.cacheWrite5mTokens * p.cacheWrite5m * 2 +
      u.cacheWrite1hTokens * p.cacheWrite1h * 2 +
      u.outputTokens * p.output * 1.5) /
    1_000_000;
  return { cost: longContextCost, longContext: "multiplied" };
}

export function costFor(model: string, u: UsageRow): number {
  const priced = priceForWithSource(model);
  let p = priced.price;
  if (priced.price === TABLE["claude-haiku-5"]) {
    const prompt =
      u.inputTokens + u.cacheReadTokens + u.cacheWrite5mTokens + u.cacheWrite1hTokens;
    if (prompt > HAIKU_5_LONG_PROMPT_TOKENS) p = HAIKU_5_LONG;
  }
  return (
    (u.inputTokens * p.input +
      u.outputTokens * p.output +
      u.cacheReadTokens * p.cacheRead +
      u.cacheWrite5mTokens * p.cacheWrite5m +
      u.cacheWrite1hTokens * p.cacheWrite1h) /
    1_000_000
  );
}

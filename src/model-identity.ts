// Canonical model-id normalization shared by the /api/usage byModel /
// unrecognized[] contract. Pure, side-effect-free, no store/network access.
//
// Matches on VENDOR/PREFIX STRUCTURE, never on unrestricted substring
// (`includes()`). A substring match on a bare "opus" or "gpt-5" would
// misroute a hypothetical id like "gpt-5-opus-review" onto Claude pricing,
// or let an unrelated sibling snapshot silently absorb a stale family's
// price. Every table entry below requires an exact vendor-prefixed key
// followed by either nothing, a dated suffix (-YYYYMMDD / -YYYY-MM-DD), or
// a Bedrock inference-profile version suffix (-v<major>:<minor>).

export type Harness = "claude" | "codex" | "grok" | "unknown";

export interface NormalizedModelIdentity {
  family: string;
  displayName: string;
  harness: Harness;
  recognized: boolean;
}

interface FamilyDef {
  /** Exact vendor-prefixed key the raw (lowercased, prefix-stripped) id must
   *  start with. Longer keys are tried first so e.g. "claude-haiku-4-5"
   *  wins over the shorter "claude-haiku-4". */
  key: string;
  family: string;
  displayName: string;
}

// source: platform.claude.com/docs/en/about-claude/pricing +
// developers.openai.com/api/docs/pricing, cross-checked against src/pricing.ts.
const CLAUDE_FAMILIES: FamilyDef[] = [
  { key: "claude-fable-5", family: "fable-5", displayName: "Fable 5" },
  { key: "claude-opus-5", family: "opus-5", displayName: "Opus 5" },
  { key: "claude-opus-4", family: "opus-4", displayName: "Opus 4" },
  { key: "claude-opus-3", family: "opus-3", displayName: "Opus 3" },
  { key: "claude-sonnet-5", family: "sonnet-5", displayName: "Sonnet 5" },
  { key: "claude-sonnet-4", family: "sonnet-4", displayName: "Sonnet 4" },
  { key: "claude-3-7-sonnet", family: "sonnet-3-7", displayName: "Sonnet 3.7" },
  { key: "claude-3-5-sonnet", family: "sonnet-3-5", displayName: "Sonnet 3.5" },
  { key: "claude-haiku-5", family: "haiku-5", displayName: "Haiku 5" },
  { key: "claude-haiku-4-5", family: "haiku-4-5", displayName: "Haiku 4.5" },
  { key: "claude-haiku-4", family: "haiku-4", displayName: "Haiku 4" },
  { key: "claude-3-5-haiku", family: "haiku-3-5", displayName: "Haiku 3.5" },
  { key: "claude-3-haiku", family: "haiku-3", displayName: "Haiku 3" },
];

const CODEX_FAMILIES: FamilyDef[] = [
  { key: "gpt-5.6-terra", family: "gpt-5.6-terra", displayName: "GPT-5.6 Terra" },
  { key: "gpt-5.6-luna", family: "gpt-5.6-luna", displayName: "GPT-5.6 Luna" },
  { key: "gpt-5.6-sol", family: "gpt-5.6-sol", displayName: "GPT-5.6 Sol" },
  {
    key: "gpt-5.5-cyber-preview",
    family: "gpt-5.5-cyber-preview",
    displayName: "GPT-5.5 Cyber (Preview)",
  },
  { key: "gpt-5.5-cyber", family: "gpt-5.5-cyber", displayName: "GPT-5.5 Cyber" },
  { key: "gpt-5.5", family: "gpt-5.5", displayName: "GPT-5.5" },
  { key: "gpt-5.4-mini", family: "gpt-5.4-mini", displayName: "GPT-5.4 Mini" },
  { key: "gpt-5.4", family: "gpt-5.4", displayName: "GPT-5.4" },
  {
    key: "gpt-5.3-codex-spark",
    family: "gpt-5.3-codex-spark",
    displayName: "GPT-5.3 Codex Spark",
  },
  { key: "gpt-5.3-codex", family: "gpt-5.3-codex", displayName: "GPT-5.3 Codex" },
  { key: "gpt-5.3", family: "gpt-5.3", displayName: "GPT-5.3" },
  {
    key: "gpt-5.1-codex-max",
    family: "gpt-5.1-codex-max",
    displayName: "GPT-5.1 Codex Max",
  },
  { key: "gpt-5.2-codex", family: "gpt-5.2-codex", displayName: "GPT-5.2 Codex" },
  { key: "gpt-5.2", family: "gpt-5.2", displayName: "GPT-5.2" },
  { key: "gpt-5.1-codex", family: "gpt-5.1-codex", displayName: "GPT-5.1 Codex" },
  { key: "gpt-5.1", family: "gpt-5.1", displayName: "GPT-5.1" },
  { key: "gpt-5-codex", family: "gpt-5-codex", displayName: "GPT-5 Codex" },
  { key: "gpt-5", family: "gpt-5", displayName: "GPT-5" },
];

const GROK_FAMILIES: FamilyDef[] = [
  { key: "grok-4.6", family: "grok-4.6", displayName: "Grok 4.6" },
  { key: "grok-4.5", family: "grok-4.5", displayName: "Grok 4.5" },
  { key: "grok-4", family: "grok-4", displayName: "Grok 4" },
  { key: "grok-3", family: "grok-3", displayName: "Grok 3" },
  { key: "grok-2", family: "grok-2", displayName: "Grok 2" },
  { key: "grok", family: "grok", displayName: "Grok" },
];

const CLAUDE_FAMILIES_BY_LENGTH = [...CLAUDE_FAMILIES].sort((a, b) => b.key.length - a.key.length);
const CODEX_FAMILIES_BY_LENGTH = [...CODEX_FAMILIES].sort((a, b) => b.key.length - a.key.length);
const GROK_FAMILIES_BY_LENGTH = [...GROK_FAMILIES].sort((a, b) => b.key.length - a.key.length);

/** A recognized suffix after an exact key match: nothing, a 1-2 digit patch
 *  number (e.g. "claude-opus-4-8" for the Opus 4.8 patch release), a bare
 *  8-digit or dashed ISO date, a patch number followed by a dated snapshot,
 *  a Bedrock inference-profile version tag, or "-preview". Anything else
 *  (e.g. "-opus-review", "-max") is a DIFFERENT model, not this family. */
function suffixIsRecognized(suffix: string): boolean {
  if (suffix === "") return true;
  if (/^-\d{1,2}$/.test(suffix)) return true;
  if (/^-\d{8}$/.test(suffix)) return true;
  if (/^-\d{4}-\d{2}-\d{2}$/.test(suffix)) return true;
  if (/^-\d{1,2}-\d{8}$/.test(suffix)) return true;
  if (/^-v\d+:\d+$/.test(suffix)) return true;
  if (suffix === "-preview") return true;
  return false;
}

function matchFamily(id: string, table: FamilyDef[]): FamilyDef | null {
  for (const def of table) {
    if (!id.startsWith(def.key)) continue;
    const suffix = id.slice(def.key.length);
    if (suffixIsRecognized(suffix)) return def;
  }
  return null;
}

/** Strips a trailing bracketed context-window annotation, e.g.
 *  "claude-fable-5[1m]" -> "claude-fable-5". */
function stripBracketSuffix(id: string): string {
  return id.replace(/\[[^\]]*\]$/, "");
}

/** Strips a Bedrock/region inference-profile prefix, e.g.
 *  "us.anthropic.claude-opus-5-..." -> "claude-opus-5-...". Region tokens are
 *  lowercase letters/digits/hyphens only (never "claude"/"gpt" themselves),
 *  so this cannot eat into the model id proper. */
function stripBedrockPrefix(id: string): string {
  const match = /^[a-z0-9-]+\.anthropic\.(.+)$/.exec(id);
  return match ? match[1]! : id;
}

/** Strips a trailing Bedrock inference-profile version tag, e.g.
 *  "claude-opus-5-20250917-v1:0" -> "claude-opus-5-20250917", so the date
 *  suffix underneath can still match the plain dated-suffix grammar. */
function stripBedrockVersionSuffix(id: string): string {
  return id.replace(/-v\d+:\d+$/, "");
}

/** Vendor/harness is decided on a WHOLE leading path segment (up to the
 *  first "-"), never on a substring occurring anywhere in the id. This is
 *  what keeps "gpt-5-opus-review" on the codex/OpenAI side even though it
 *  contains the literal text "opus". */
function harnessFor(id: string): Harness {
  if (/^claude(-|$)/.test(id)) return "claude";
  if (/^(gpt|o1|o3|chatgpt)(-|$)/.test(id)) return "codex";
  if (/^grok(-|$)/.test(id)) return "grok";
  return "unknown";
}

const UNRECOGNIZED: NormalizedModelIdentity = {
  family: "unknown",
  displayName: "",
  harness: "unknown",
  recognized: false,
};

/**
 * Canonical model-id normalizer for the /api/usage byModel / unrecognized[]
 * contract. Never throws; an id this cannot resolve comes back with
 * `recognized: false` and `harness` reflecting whatever vendor prefix (if
 * any) was structurally detected, so callers can still route the raw
 * tokens without misclassifying them onto the wrong vendor.
 */
export function normalizeModelId(raw: string | null | undefined): NormalizedModelIdentity {
  if (!raw) return { ...UNRECOGNIZED, displayName: raw || "unknown" };
  const trimmed = raw.trim();
  if (!trimmed) return { ...UNRECOGNIZED, displayName: raw };

  const id = stripBedrockVersionSuffix(
    stripBedrockPrefix(stripBracketSuffix(trimmed)).toLowerCase(),
  );
  const harness = harnessFor(id);

  if (harness === "claude") {
    const def = matchFamily(id, CLAUDE_FAMILIES_BY_LENGTH);
    if (def) return { family: def.family, displayName: def.displayName, harness, recognized: true };
  } else if (harness === "codex") {
    const def = matchFamily(id, CODEX_FAMILIES_BY_LENGTH);
    if (def) return { family: def.family, displayName: def.displayName, harness, recognized: true };
  } else if (harness === "grok") {
    const def = matchFamily(id, GROK_FAMILIES_BY_LENGTH);
    if (def) return { family: def.family, displayName: def.displayName, harness, recognized: true };
    return { family: id, displayName: raw, harness, recognized: true };
  }

  return { family: "unknown", displayName: raw, harness, recognized: false };
}

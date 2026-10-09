export const CODEX_USAGE_PARSER_VERSION = 4;

export type CodexCheckpointMigrationReason =
  | "none"
  | "baseline-repair"
  | "legacy-complete-replay"
  | "thread-source-role-repair"
  | "lineage-repair"
  | "depth-one-direct-repair"
  | "lineage-shape-repair"
  | "deployed-legacy-depth-one-repair"
  | "deployed-depth-two-without-ancestors-repair"
  | "deployed-depth-one-ready-repair"
  | "t3-honesty-surface-migration"
  | "fail-closed";

export interface CodexCheckpointMigrationDecision {
  readRequired: boolean;
  replacementAuthorized: boolean;
  reason: CodexCheckpointMigrationReason;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function nonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function honestySurfaceShape(state: Record<string, unknown>): "absent" | "valid" | "malformed" {
  const fields = ["semanticUsageComplete", "defectLineCount", "defectLineNumbers"];
  const fieldCount = fields.filter((field) => Object.hasOwn(state, field)).length;
  if (fieldCount === 0) return "absent";
  const count = state.defectLineCount;
  const lines = state.defectLineNumbers;
  return fieldCount === fields.length &&
    typeof state.semanticUsageComplete === "boolean" &&
    nonNegativeInteger(count) &&
    Array.isArray(lines) &&
    lines.length === Math.min(count, 8) &&
    lines.every(
      (line, index) =>
        nonNegativeInteger(line) && (index === 0 || line > (lines[index - 1] as number)),
    )
    ? "valid"
    : "malformed";
}

function validTotals(value: unknown): boolean {
  if (value === null) return true;
  const totals = record(value);
  if (!totals) return false;
  const input = totals.inputTokens;
  const cached = totals.cachedInputTokens;
  const output = totals.outputTokens;
  const reasoning = totals.reasoningOutputTokens;
  return (
    nonNegativeInteger(input) &&
    nonNegativeInteger(cached) &&
    nonNegativeInteger(output) &&
    (reasoning === null || nonNegativeInteger(reasoning)) &&
    cached <= input &&
    (reasoning === null || reasoning <= output)
  );
}

/** Strict authority guard for checkpoints emitted by the current parser.
 * Migration classification accepts several legacy shapes so they can be
 * replayed; daily completeness must accept only a fully self-consistent v4
 * checkpoint that needs no migration or interpretation by an older schema. */
export function isCompleteCurrentCodexCheckpoint(value: unknown): boolean {
  const state = record(value);
  if (
    !state ||
    state.usageParserVersion !== CODEX_USAGE_PARSER_VERSION ||
    state.usageComplete !== true ||
    state.semanticUsageComplete !== true ||
    honestySurfaceShape(state) !== "valid" ||
    typeof state.sessionId !== "string" ||
    typeof state.model !== "string" ||
    typeof state.cwd !== "string" ||
    typeof state.project !== "string" ||
    typeof state.projectSlug !== "string" ||
    (state.sessionTimestampMs !== null &&
      (!nonNegativeInteger(state.sessionTimestampMs) ||
        Number.isNaN(new Date(state.sessionTimestampMs).getTime()))) ||
    !validTotals(state.totals)
  ) {
    return false;
  }

  const dispositionFields = [
    "billedUsageRows",
    "inheritedSuppressedRows",
    "ambiguousQuarantinedRows",
    "zeroNoopRows",
    "unrecoverableResetRows",
    "invalidUsageRows",
  ] as const;
  if (
    !nonNegativeInteger(state.tokenCountRows) ||
    !dispositionFields.every((field) => nonNegativeInteger(state[field])) ||
    state.tokenCountRows !==
      dispositionFields.reduce((sum, field) => sum + (state[field] as number), 0) ||
    state.ambiguousQuarantinedRows !== 0 ||
    state.unrecoverableResetRows !== 0 ||
    state.invalidUsageRows !== 0 ||
    (typeof state.childSessionId !== "string" && state.childSessionId !== null) ||
    (typeof state.forkParentId !== "string" && state.forkParentId !== null) ||
    !["none", "inherited", "live"].includes(state.phase as string) ||
    typeof state.rollingDigest !== "string" ||
    !/^[0-9a-f]{64}$/.test(state.rollingDigest) ||
    typeof state.firstLiveRowPending !== "boolean"
  ) {
    return false;
  }

  const repairMarkers = [
    "threadSourceRoleRepaired",
    "codexLineageRepaired",
    "depthOneDirectLineageRepaired",
    "codexLineageShapeRepaired",
    "codexDeployedCheckpointRepaired",
  ] as const;
  if (
    !repairMarkers.every((field) => {
      const marker = state[field];
      return marker === undefined || marker === null || typeof marker === "boolean";
    }) ||
    (state.lineageDepth !== undefined &&
      state.lineageDepth !== null &&
      state.lineageDepth !== 1 &&
      state.lineageDepth !== 2) ||
    (state.rootSessionId !== undefined &&
      state.rootSessionId !== null &&
      typeof state.rootSessionId !== "string") ||
    (state.lineageReplayStage !== undefined &&
      state.lineageReplayStage !== null &&
      !["await-parent", "await-root", "ready"].includes(state.lineageReplayStage as string)) ||
    (state.boundaryByte !== null && !nonNegativeInteger(state.boundaryByte)) ||
    (state.boundaryLine !== null && !nonNegativeInteger(state.boundaryLine))
  ) {
    return false;
  }

  if (state.childSessionId !== null && state.sessionId !== state.childSessionId) return false;
  if (state.phase === "inherited" || state.phase === "live") {
    if (
      state.childSessionId === null ||
      state.forkParentId === null ||
      state.childSessionId === state.forkParentId
    ) {
      return false;
    }
  } else if (state.forkParentId !== null) {
    return false;
  }
  if (state.phase === "live") {
    if (!nonNegativeInteger(state.boundaryByte) || !nonNegativeInteger(state.boundaryLine)) {
      return false;
    }
  } else if (state.boundaryByte !== null || state.boundaryLine !== null) {
    return false;
  }
  return state.firstLiveRowPending !== true || state.phase === "live";
}

function hasAllAmbiguousRows(state: Record<string, unknown>): boolean {
  const rows = state.tokenCountRows;
  return (
    state.phase === "ambiguous" &&
    state.usageComplete === false &&
    nonNegativeInteger(rows) &&
    rows > 0 &&
    state.billedUsageRows === 0 &&
    state.inheritedSuppressedRows === 0 &&
    state.ambiguousQuarantinedRows === rows &&
    state.zeroNoopRows === 0 &&
    state.unrecoverableResetRows === 0 &&
    state.invalidUsageRows === 0
  );
}

function lifecycleReason(state: Record<string, unknown>): CodexCheckpointMigrationReason | null {
  if (state.usageComplete !== false) return null;
  const rows = state.tokenCountRows;
  const basicAmbiguity =
    state.phase === "ambiguous" &&
    nonNegativeInteger(rows) &&
    rows > 0 &&
    state.billedUsageRows === 0 &&
    state.ambiguousQuarantinedRows === rows;

  if (!Object.hasOwn(state, "threadSourceRoleRepaired") && basicAmbiguity) {
    return "thread-source-role-repair";
  }
  if (
    state.threadSourceRoleRepaired === true &&
    !Object.hasOwn(state, "codexLineageRepaired") &&
    hasAllAmbiguousRows(state)
  ) {
    return "lineage-repair";
  }
  if (
    state.threadSourceRoleRepaired === true &&
    state.codexLineageRepaired === true &&
    !Object.hasOwn(state, "depthOneDirectLineageRepaired") &&
    hasAllAmbiguousRows(state) &&
    state.lineageDepth === 1 &&
    state.lineageReplayStage === "await-root" &&
    typeof state.childSessionId === "string" &&
    typeof state.rootSessionId === "string" &&
    state.sessionId === state.childSessionId &&
    state.forkParentId === state.rootSessionId &&
    state.childSessionId !== state.rootSessionId
  ) {
    return "depth-one-direct-repair";
  }
  if (
    state.threadSourceRoleRepaired === true &&
    state.codexLineageRepaired === true &&
    !Object.hasOwn(state, "codexLineageShapeRepaired") &&
    hasAllAmbiguousRows(state)
  ) {
    const unclassifiedDepthOne =
      state.depthOneDirectLineageRepaired === true &&
      state.childSessionId === null &&
      state.forkParentId === null &&
      state.lineageDepth === null &&
      state.rootSessionId === null &&
      state.lineageReplayStage === null;
    const strandedDepthTwo =
      !Object.hasOwn(state, "depthOneDirectLineageRepaired") &&
      state.lineageDepth === 2 &&
      state.lineageReplayStage === "await-parent" &&
      typeof state.childSessionId === "string" &&
      typeof state.rootSessionId === "string" &&
      typeof state.forkParentId === "string" &&
      state.sessionId === state.childSessionId &&
      state.childSessionId !== state.forkParentId &&
      state.childSessionId !== state.rootSessionId &&
      state.forkParentId !== state.rootSessionId;
    if (unclassifiedDepthOne || strandedDepthTwo) return "lineage-shape-repair";
  }
  return deployedReason(state);
}

const DEPLOYED_LEGACY_KEYS = [
  "cwd",
  "firstLiveRowPending",
  "model",
  "phase",
  "project",
  "projectSlug",
  "sessionId",
  "sessionTimestampMs",
  "totals",
  "usageComplete",
  "usageParserVersion",
] as const;

function deployedReason(state: Record<string, unknown>): CodexCheckpointMigrationReason | null {
  if (Object.hasOwn(state, "codexDeployedCheckpointRepaired")) return null;
  const keys = Object.keys(state).sort();
  const totals = record(state.totals);
  const totalKeys = totals ? Object.keys(totals).sort() : [];
  if (
    keys.length === DEPLOYED_LEGACY_KEYS.length &&
    keys.every((key, index) => key === DEPLOYED_LEGACY_KEYS[index]) &&
    typeof state.sessionId === "string" &&
    typeof state.model === "string" &&
    typeof state.cwd === "string" &&
    typeof state.project === "string" &&
    typeof state.projectSlug === "string" &&
    nonNegativeInteger(state.sessionTimestampMs) &&
    totalKeys.join(",") === "cachedInputTokens,inputTokens,outputTokens,reasoningOutputTokens" &&
    state.totals !== null &&
    validTotals(state.totals) &&
    state.phase === "ambiguous" &&
    state.usageComplete === false &&
    state.firstLiveRowPending === false
  ) {
    return "deployed-legacy-depth-one-repair";
  }
  if (!hasAllAmbiguousRows(state)) return null;
  const exactIdentity =
    typeof state.childSessionId === "string" &&
    typeof state.rootSessionId === "string" &&
    typeof state.forkParentId === "string" &&
    state.sessionId === state.childSessionId &&
    state.childSessionId !== state.forkParentId &&
    state.childSessionId !== state.rootSessionId;
  if (
    state.threadSourceRoleRepaired === true &&
    state.codexLineageRepaired === true &&
    state.depthOneDirectLineageRepaired === true &&
    state.codexLineageShapeRepaired === true &&
    state.lineageDepth === 2 &&
    state.lineageReplayStage === "await-parent" &&
    exactIdentity &&
    state.forkParentId !== state.rootSessionId
  ) {
    return "deployed-depth-two-without-ancestors-repair";
  }
  if (
    state.threadSourceRoleRepaired === true &&
    state.codexLineageRepaired === true &&
    state.depthOneDirectLineageRepaired === true &&
    !Object.hasOwn(state, "codexLineageShapeRepaired") &&
    state.lineageDepth === 1 &&
    state.lineageReplayStage === "ready" &&
    exactIdentity &&
    state.forkParentId === state.rootSessionId
  ) {
    return "deployed-depth-one-ready-repair";
  }
  return null;
}

function decision(
  reason: CodexCheckpointMigrationReason,
  generationHasOccurrences: boolean,
): CodexCheckpointMigrationDecision {
  const readRequired = reason !== "none" && reason !== "fail-closed";
  return {
    readRequired,
    replacementAuthorized: readRequired && generationHasOccurrences,
    reason,
  };
}

/**
 * Classifies full-file Codex checkpoint migrations. Lifecycle repairs are
 * recognized from their marker/counter shape; they deliberately do not depend
 * on a parser-version predecessor derived from the current version.
 */
export function classifyCodexCheckpointMigration(
  checkpointJson: string | null,
  generationHasOccurrences: boolean,
): CodexCheckpointMigrationDecision {
  let state: Record<string, unknown> | null = null;
  if (checkpointJson) {
    try {
      state = record(JSON.parse(checkpointJson) as unknown);
    } catch {
      // Malformed persisted state remains fail-closed when historical rows exist.
    }
  }
  if (!state) {
    return decision(generationHasOccurrences ? "fail-closed" : "baseline-repair", false);
  }
  const version = state.usageParserVersion;
  if (!nonNegativeInteger(version) || version < 1) {
    return decision(generationHasOccurrences ? "fail-closed" : "baseline-repair", false);
  }
  if (version > CODEX_USAGE_PARSER_VERSION) return decision("fail-closed", false);
  if (typeof state.usageComplete !== "boolean") {
    return decision(generationHasOccurrences ? "fail-closed" : "baseline-repair", false);
  }
  const honestyShape = honestySurfaceShape(state);
  if (honestyShape === "malformed") {
    return decision(generationHasOccurrences ? "fail-closed" : "baseline-repair", false);
  }

  const lifecycle = lifecycleReason(state);
  if (lifecycle && (version < CODEX_USAGE_PARSER_VERSION || honestyShape === "valid")) {
    return decision(lifecycle, generationHasOccurrences);
  }

  if (version === CODEX_USAGE_PARSER_VERSION) {
    return honestyShape === "valid"
      ? decision("none", false)
      : decision(generationHasOccurrences ? "fail-closed" : "baseline-repair", false);
  }
  if (state.usageComplete === true) {
    return state.totals !== null && validTotals(state.totals)
      ? decision("legacy-complete-replay", generationHasOccurrences)
      : decision(generationHasOccurrences ? "fail-closed" : "baseline-repair", false);
  }
  if (!generationHasOccurrences && state.totals === null) {
    return decision("baseline-repair", false);
  }

  if (honestyShape !== "absent") return decision("fail-closed", false);

  if (state.totals !== null && validTotals(state.totals)) {
    return decision("t3-honesty-surface-migration", generationHasOccurrences);
  }
  return decision("fail-closed", false);
}

export function isDeployedDepthTwoMigration(reason: CodexCheckpointMigrationReason): boolean {
  return reason === "deployed-depth-two-without-ancestors-repair";
}

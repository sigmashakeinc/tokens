/** Pure, dependency-free token snapshot contract shared by the local tokens
 * authority and its consumers. Billing persistence and provider parsing stay
 * in their owning packages. */

export type TokenSource = "claude" | "codex" | "grok";

export const TOKEN_SOURCES: readonly TokenSource[] = ["claude", "codex", "grok"];

export interface NormalizedTokenClasses {
  input: number | null;
  cachedRead: number | null;
  cacheWrite: number | null;
  visibleOutput: number | null;
  reasoningOutput: number | null;
  complete: boolean;
}

export interface TokenUsageEvent {
  source: TokenSource;
  /** Optional at ingestion boundaries. Snapshots retain unattributed events
   * under the explicit `unknown` model bucket so provider totals still
   * compose exactly. */
  model?: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  tokenClasses?: NormalizedTokenClasses;
}

export interface TokenProviderState {
  source: TokenSource;
  available: boolean;
  complete: boolean;
  /** Permanent syntactic JSONL defects across distinct active generations. */
  defectLines?: number;
}

export type ClaudeRecordIdentityKind = "uuid" | "response" | "request" | "unidentified";

export interface ClaudeRecordIdentity {
  kind: ClaudeRecordIdentityKind;
  canonicalIdentity: string | null;
  logicalMessageKey: string | null;
  immutableIdentity: string | null;
  immutablePayload: string | null;
  collapseRevisions: boolean;
}

export interface ClaudeRecordIdentityContext {
  sessionId?: string;
}

export interface ClaudeAuthorityCandidate {
  timestampMs: number;
  rawRecordSha256: string;
}

/** Total order for collapsed Claude revisions. The earliest provider
 * timestamp wins; the raw record digest makes equal timestamps deterministic
 * regardless of discovery/path order. */
export function compareClaudeAuthorityCandidates(
  left: ClaudeAuthorityCandidate,
  right: ClaudeAuthorityCandidate,
): number {
  if (left.timestampMs !== right.timestampMs) return left.timestampMs < right.timestampMs ? -1 : 1;
  if (left.rawRecordSha256 === right.rawRecordSha256) return 0;
  return left.rawRecordSha256 < right.rawRecordSha256 ? -1 : 1;
}

function nonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const valueRecord = record(value);
  if (!valueRecord) return "{}";
  return `{${Object.keys(valueRecord)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(valueRecord[key])}`)
    .join(",")}}`;
}

/** Shared Claude response identity. Mutable usage/timestamps/correlation and
 * file coordinates are intentionally excluded from the stable identity. */
export function claudeProviderRecordIdentity(
  value: unknown,
  _context: ClaudeRecordIdentityContext = {},
): ClaudeRecordIdentity {
  const event = record(value);
  const message = record(event?.message);
  const payload = record(event?.payload);
  const uuid = nonEmptyString(event?.uuid) ?? nonEmptyString(event?.event_uuid);
  const responseId =
    nonEmptyString(message?.id) ??
    nonEmptyString(event?.responseId) ??
    nonEmptyString(event?.response_id) ??
    nonEmptyString(event?.messageId) ??
    nonEmptyString(payload?.response_id) ??
    nonEmptyString(payload?.message_id);
  const requestId =
    nonEmptyString(event?.requestId) ??
    nonEmptyString(event?.request_id) ??
    nonEmptyString(payload?.request_id);
  let kind: "uuid" | "response" | "request" | null = null;
  let id: string | null = null;
  if (uuid) {
    kind = "uuid";
    id = uuid;
  } else if (responseId) {
    kind = "response";
    id = responseId;
  } else if (requestId) {
    kind = "request";
    id = requestId;
  }
  if (!kind || !id) {
    return {
      kind: "unidentified",
      canonicalIdentity: null,
      logicalMessageKey: null,
      immutableIdentity: null,
      immutablePayload: null,
      collapseRevisions: false,
    };
  }
  // Provider IDs identify one logical response independently of mutable
  // classification fields. A corrected model/role must replace the same
  // authority row instead of creating another billable event.
  const canonicalIdentity = `claude:${kind}:${id}`;
  const immutablePayload =
    kind === "uuid"
      ? stableJson({
          type: event?.type ?? null,
          content: message?.content ?? payload?.content ?? null,
        })
      : null;
  return {
    kind,
    canonicalIdentity,
    logicalMessageKey: canonicalIdentity,
    immutableIdentity: kind === "uuid" ? canonicalIdentity : null,
    immutablePayload,
    collapseRevisions: true,
  };
}

export const HUD_TIME_ZONE = "America/Los_Angeles";

export interface DailyTokenWindow {
  kind: "daily";
  timeZone: string;
  windowKey: string;
  startMs: number;
  endMs: number;
  start: string;
  end: string;
}

export const ROLLING_24H_MS = 24 * 60 * 60 * 1000;

export interface Rolling24hTokenWindow {
  kind: "rolling24h";
  timeZone: string;
  windowKey: string;
  startMs: number;
  endMs: number;
  start: string;
  end: string;
}

export type TokenWindow = DailyTokenWindow | Rolling24hTokenWindow;

export interface SnapshotTokenClass {
  tokens: number | null;
  complete: boolean;
}

export interface SnapshotTokenClasses {
  input: SnapshotTokenClass;
  cachedRead: SnapshotTokenClass;
  cacheWrite: SnapshotTokenClass;
  visibleOutput: SnapshotTokenClass;
  reasoningOutput: SnapshotTokenClass;
  complete: boolean;
}

export interface SnapshotRawTotals {
  inputTokens: number;
  /** Display-only raw prompt total; never a pricing input. */
  rawInputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  messages: number;
}

export interface SnapshotProvider {
  source: TokenSource;
  available: boolean;
  discoveryComplete: boolean;
  defectLines: number;
  rawTotals: SnapshotRawTotals;
  classes: SnapshotTokenClasses;
  models: SnapshotModel[];
}

export interface SnapshotModel {
  model: string;
  rawTotals: SnapshotRawTotals;
  classes: SnapshotTokenClasses;
}

export interface AuthoritativeTokenSnapshot {
  source: "sigmashake-tokens";
  window: TokenWindow;
  revision: number;
  updatedAt: number;
  /** Compatibility aliases for existing HUD consumers. */
  totalCost: number;
  calls: number;
  rawTotals: SnapshotRawTotals;
  classes: SnapshotTokenClasses;
  hud: {
    in: SnapshotTokenClass;
    out: SnapshotTokenClass;
  };
  providers: Record<TokenSource, SnapshotProvider>;
}

interface ZonedParts {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
}

function zonedParts(atMs: number, timeZone: string): ZonedParts {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date(atMs));
  const values = new Map(parts.map((part) => [part.type, Number(part.value)]));
  return {
    year: values.get("year")!,
    month: values.get("month")!,
    day: values.get("day")!,
    hour: values.get("hour")!,
    minute: values.get("minute")!,
    second: values.get("second")!,
  };
}

function zonedMidnightMs(year: number, month: number, day: number, timeZone: string): number {
  const desired = Date.UTC(year, month - 1, day);
  let guess = desired;
  for (let attempt = 0; attempt < 4; attempt++) {
    const actual = zonedParts(guess, timeZone);
    const represented = Date.UTC(
      actual.year,
      actual.month - 1,
      actual.day,
      actual.hour,
      actual.minute,
      actual.second,
    );
    const adjustment = desired - represented;
    guess += adjustment;
    if (adjustment === 0) break;
  }
  return guess;
}

function pad2(value: number): string {
  return String(value).padStart(2, "0");
}

/** Canonical model-bucket normalization shared by event and SQL producers. */
export function normalizeTokenModel(model: string | undefined): string {
  return model?.trim() || "unknown";
}

/** Locale-independent UTF-16 order keeps floating composition deterministic. */
export function compareTokenModelNames(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

/** IANA-zone half-open daily window; calendar advancement makes DST days
 * correctly 23 or 25 hours. */
export function dailyTokenWindow(atMs: number, timeZone: string = HUD_TIME_ZONE): DailyTokenWindow {
  const local = zonedParts(atMs, timeZone);
  const windowKey = `${local.year}-${pad2(local.month)}-${pad2(local.day)}`;
  const startMs = zonedMidnightMs(local.year, local.month, local.day, timeZone);
  const next = new Date(Date.UTC(local.year, local.month - 1, local.day + 1));
  const endMs = zonedMidnightMs(
    next.getUTCFullYear(),
    next.getUTCMonth() + 1,
    next.getUTCDate(),
    timeZone,
  );
  return {
    kind: "daily",
    timeZone,
    windowKey,
    startMs,
    endMs,
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString(),
  };
}

/** Half-open wall-clock [now-24h, now] window for the HUD burn snapshot. */
export function rolling24hTokenWindow(
  atMs: number,
  timeZone: string = HUD_TIME_ZONE,
): Rolling24hTokenWindow {
  const endMs = atMs;
  const startMs = atMs - ROLLING_24H_MS;
  return {
    kind: "rolling24h",
    timeZone,
    windowKey: `rolling24h:${startMs}:${endMs}`,
    startMs,
    endMs,
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString(),
  };
}

function rawTotals(events: TokenUsageEvent[]): SnapshotRawTotals {
  const totals: SnapshotRawTotals = {
    inputTokens: 0,
    rawInputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
    messages: 0,
  };
  for (const event of events) {
    totals.inputTokens += event.inputTokens;
    totals.rawInputTokens +=
      event.inputTokens +
      event.cacheReadTokens +
      event.cacheWrite5mTokens +
      event.cacheWrite1hTokens;
    totals.outputTokens += event.outputTokens;
    totals.cacheReadTokens += event.cacheReadTokens;
    totals.cacheWrite5mTokens += event.cacheWrite5mTokens;
    totals.cacheWrite1hTokens += event.cacheWrite1hTokens;
    totals.cost += event.cost;
    totals.messages += 1;
  }
  return totals;
}

function composeRawTotals(parts: SnapshotRawTotals[]): SnapshotRawTotals {
  return parts.reduce<SnapshotRawTotals>(
    (totals, part) => ({
      inputTokens: totals.inputTokens + part.inputTokens,
      rawInputTokens: totals.rawInputTokens + part.rawInputTokens,
      outputTokens: totals.outputTokens + part.outputTokens,
      cacheReadTokens: totals.cacheReadTokens + part.cacheReadTokens,
      cacheWrite5mTokens: totals.cacheWrite5mTokens + part.cacheWrite5mTokens,
      cacheWrite1hTokens: totals.cacheWrite1hTokens + part.cacheWrite1hTokens,
      cost: totals.cost + part.cost,
      messages: totals.messages + part.messages,
    }),
    {
      inputTokens: 0,
      rawInputTokens: 0,
      outputTokens: 0,
      cacheReadTokens: 0,
      cacheWrite5mTokens: 0,
      cacheWrite1hTokens: 0,
      cost: 0,
      messages: 0,
    },
  );
}

const CLASS_KEYS = [
  "input",
  "cachedRead",
  "cacheWrite",
  "visibleOutput",
  "reasoningOutput",
] as const;

type ClassKey = (typeof CLASS_KEYS)[number];

function unavailableClasses(): SnapshotTokenClasses {
  return {
    input: { tokens: null, complete: false },
    cachedRead: { tokens: null, complete: false },
    cacheWrite: { tokens: null, complete: false },
    visibleOutput: { tokens: null, complete: false },
    reasoningOutput: { tokens: null, complete: false },
    complete: false,
  };
}

export function composeProviderTokenClasses(
  source: TokenSource,
  events: TokenUsageEvent[],
  state: TokenProviderState | undefined,
): SnapshotTokenClasses {
  if (!state?.available || !state.complete) return unavailableClasses();
  const values: Record<ClassKey, number | null> = {
    input: 0,
    cachedRead: 0,
    cacheWrite: source === "claude" ? 0 : null,
    visibleOutput: 0,
    reasoningOutput: source === "claude" ? null : 0,
  };
  for (const event of events) {
    for (const key of CLASS_KEYS) {
      if (values[key] === null) continue;
      const value = event.tokenClasses?.[key] ?? null;
      values[key] = value === null ? null : values[key]! + value;
    }
  }
  const result: SnapshotTokenClasses = {
    input: { tokens: values.input, complete: values.input !== null },
    cachedRead: { tokens: values.cachedRead, complete: values.cachedRead !== null },
    cacheWrite: { tokens: values.cacheWrite, complete: values.cacheWrite !== null },
    visibleOutput: { tokens: values.visibleOutput, complete: values.visibleOutput !== null },
    reasoningOutput: { tokens: values.reasoningOutput, complete: values.reasoningOutput !== null },
    complete: false,
  };
  result.complete = CLASS_KEYS.every((key) => result[key].complete);
  return result;
}

export function composeSnapshotTokenClasses(providers: SnapshotProvider[]): SnapshotTokenClasses {
  const result = unavailableClasses();
  for (const key of CLASS_KEYS) {
    const complete = providers.every(
      (provider) =>
        provider.available && provider.discoveryComplete && provider.classes[key].complete,
    );
    result[key] = {
      tokens: complete
        ? providers.reduce((sum, provider) => sum + provider.classes[key].tokens!, 0)
        : null,
      complete,
    };
  }
  result.complete = CLASS_KEYS.every((key) => result[key].complete);
  return result;
}

/** Compose HUD classes from available providers only. An absent Grok tree
 * must not null out Claude/Codex; a Grok-only window must still compose. */
export function snapshotClassProviders(
  providers: Record<TokenSource, SnapshotProvider>,
): SnapshotProvider[] {
  const ordered = TOKEN_SOURCES.map((source) => providers[source]);
  const available = ordered.filter((provider) => provider.available);
  return available.length > 0 ? available : ordered;
}

export function buildAuthoritativeTokenSnapshot(
  events: TokenUsageEvent[],
  meta: {
    window: TokenWindow;
    revision: number;
    updatedAt: number;
    providerStates: TokenProviderState[];
  },
): AuthoritativeTokenSnapshot {
  const states = new Map(meta.providerStates.map((state) => [state.source, state]));
  const providerFor = (source: TokenSource): SnapshotProvider => {
    const state = states.get(source);
    const available = state?.available ?? false;
    const discoveryComplete = available && (state?.complete ?? false);
    const defectLines = state?.defectLines ?? 0;
    const sourceEvents = events.filter((event) => event.source === source);
    const byModel = new Map<string, TokenUsageEvent[]>();
    for (const event of sourceEvents) {
      const model = normalizeTokenModel(event.model);
      const group = byModel.get(model) ?? [];
      group.push(event);
      byModel.set(model, group);
    }
    const models = [...byModel.entries()]
      .sort(([left], [right]) => compareTokenModelNames(left, right))
      .map(([model, modelEvents]) => ({
        model,
        rawTotals: rawTotals(modelEvents),
        classes: composeProviderTokenClasses(source, modelEvents, state),
      }));
    return {
      source,
      available,
      discoveryComplete,
      defectLines,
      // Compose through the same ordered model buckets validated by consumers.
      // Floating-point costs are otherwise order-dependent on large mixed-model
      // histories and can make a builder-produced snapshot fail its own guard.
      rawTotals: composeRawTotals(models.map((model) => model.rawTotals)),
      classes: composeProviderTokenClasses(source, sourceEvents, state),
      models,
    };
  };
  const claude = providerFor("claude");
  const codex = providerFor("codex");
  const grok = providerFor("grok");
  const providers = { claude, codex, grok };
  const classes = composeSnapshotTokenClasses(snapshotClassProviders(providers));
  const totals = composeRawTotals([claude.rawTotals, codex.rawTotals, grok.rawTotals]);
  return {
    source: "sigmashake-tokens",
    window: meta.window,
    revision: meta.revision,
    updatedAt: meta.updatedAt,
    totalCost: totals.cost,
    calls: totals.messages,
    rawTotals: totals,
    classes,
    hud: { in: classes.input, out: classes.visibleOutput },
    providers,
  };
}

export function shouldResetTokenSnapshot(
  previous: AuthoritativeTokenSnapshot,
  current: AuthoritativeTokenSnapshot,
): boolean {
  if (previous.window.windowKey !== current.window.windowKey) return true;
  const classReset = (before: SnapshotTokenClasses, after: SnapshotTokenClasses): boolean => {
    for (const key of CLASS_KEYS) {
      const previousClass = before[key];
      const currentClass = after[key];
      if (previousClass.complete !== currentClass.complete) return true;
      if (
        previousClass.complete &&
        currentClass.complete &&
        previousClass.tokens !== null &&
        currentClass.tokens !== null &&
        currentClass.tokens < previousClass.tokens
      ) {
        return true;
      }
    }
    return false;
  };
  const rawKeys = [
    "inputTokens",
    "rawInputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWrite5mTokens",
    "cacheWrite1hTokens",
    "cost",
    "messages",
  ] as const;
  const rawDecreased = (before: SnapshotRawTotals, after: SnapshotRawTotals): boolean =>
    rawKeys.some((key) => {
      if (key === "cost") return after.cost < before.cost && !equalCost(after.cost, before.cost);
      return after[key] < before[key];
    });

  if (classReset(previous.classes, current.classes)) return true;
  if (rawDecreased(previous.rawTotals, current.rawTotals)) return true;
  if (
    (current.totalCost < previous.totalCost && !equalCost(current.totalCost, previous.totalCost)) ||
    current.calls < previous.calls
  ) {
    return true;
  }
  for (const source of TOKEN_SOURCES) {
    const before = previous.providers[source];
    const after = current.providers[source];
    if (
      before.available !== after.available ||
      before.discoveryComplete !== after.discoveryComplete ||
      before.defectLines !== after.defectLines ||
      classReset(before.classes, after.classes) ||
      rawDecreased(before.rawTotals, after.rawTotals)
    ) {
      return true;
    }
    const beforeModels = new Map(before.models.map((model) => [model.model, model]));
    const afterModels = new Map(after.models.map((model) => [model.model, model]));
    if (
      beforeModels.size !== afterModels.size ||
      [...beforeModels.keys()].some((model) => !afterModels.has(model))
    ) {
      return true;
    }
    for (const [model, beforeModel] of beforeModels) {
      const afterModel = afterModels.get(model)!;
      if (
        classReset(beforeModel.classes, afterModel.classes) ||
        rawDecreased(beforeModel.rawTotals, afterModel.rawTotals)
      ) {
        return true;
      }
    }
  }
  return false;
}

export function shouldResetAuthoritativeHud(
  previous: AuthoritativeTokenSnapshot,
  current: AuthoritativeTokenSnapshot,
): boolean {
  return shouldResetTokenSnapshot(previous, current);
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function validClass(value: unknown): value is SnapshotTokenClass {
  const candidate = record(value);
  if (!candidate || typeof candidate.complete !== "boolean") return false;
  if (
    candidate.tokens !== null &&
    (typeof candidate.tokens !== "number" ||
      !Number.isSafeInteger(candidate.tokens) ||
      candidate.tokens < 0)
  ) {
    return false;
  }
  return candidate.complete ? typeof candidate.tokens === "number" : candidate.tokens === null;
}

function validClasses(value: unknown): value is SnapshotTokenClasses {
  const candidate = record(value);
  if (!candidate || typeof candidate.complete !== "boolean") return false;
  if (!CLASS_KEYS.every((key) => validClass(candidate[key]))) return false;
  return (
    candidate.complete === CLASS_KEYS.every((key) => record(candidate[key])?.complete === true)
  );
}

function validRawTotals(value: unknown): value is SnapshotRawTotals {
  const candidate = record(value);
  if (!candidate) return false;
  const countKeys = [
    "inputTokens",
    "rawInputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWrite5mTokens",
    "cacheWrite1hTokens",
    "messages",
  ];
  if (
    !countKeys.every((key) => {
      const field = candidate[key];
      return typeof field === "number" && Number.isSafeInteger(field) && field >= 0;
    })
  ) {
    return false;
  }
  const cost = candidate.cost;
  return typeof cost === "number" && Number.isFinite(cost) && cost >= 0;
}

function validClassesForRawTotals(
  classes: SnapshotTokenClasses,
  raw: SnapshotRawTotals,
  source: TokenSource,
): boolean {
  if (classes.input.complete && classes.input.tokens !== raw.inputTokens) return false;
  if (classes.cachedRead.complete && classes.cachedRead.tokens !== raw.cacheReadTokens) {
    return false;
  }
  if (source === "claude") {
    return (
      !classes.reasoningOutput.complete &&
      classes.reasoningOutput.tokens === null &&
      (!classes.cacheWrite.complete ||
        classes.cacheWrite.tokens === raw.cacheWrite5mTokens + raw.cacheWrite1hTokens) &&
      (!classes.visibleOutput.complete || classes.visibleOutput.tokens === raw.outputTokens)
    );
  }
  return (
    !classes.cacheWrite.complete &&
    classes.cacheWrite.tokens === null &&
    classes.visibleOutput.complete === classes.reasoningOutput.complete &&
    (!classes.visibleOutput.complete ||
      classes.visibleOutput.tokens! + classes.reasoningOutput.tokens! === raw.outputTokens)
  );
}

function validModel(value: unknown, source: TokenSource): value is SnapshotModel {
  const candidate = record(value);
  if (!candidate || typeof candidate.model !== "string") return false;
  if (!candidate.model.trim() || candidate.model !== candidate.model.trim()) return false;
  if (!validRawTotals(candidate.rawTotals) || !validClasses(candidate.classes)) return false;
  const raw = candidate.rawTotals;
  if (
    raw.rawInputTokens !==
    raw.inputTokens + raw.cacheReadTokens + raw.cacheWrite5mTokens + raw.cacheWrite1hTokens
  ) {
    return false;
  }
  const classes = candidate.classes;
  const unavailable =
    !classes.complete &&
    CLASS_KEYS.every((key) => !classes[key].complete && classes[key].tokens === null);
  return unavailable || validClassesForRawTotals(classes, raw, source);
}

function composedModelClasses(models: SnapshotModel[]): SnapshotTokenClasses {
  const result = unavailableClasses();
  for (const key of CLASS_KEYS) {
    const complete = models.length > 0 && models.every((model) => model.classes[key].complete);
    result[key] = {
      tokens: complete ? models.reduce((sum, model) => sum + model.classes[key].tokens!, 0) : null,
      complete,
    };
  }
  result.complete = CLASS_KEYS.every((key) => result[key].complete);
  return result;
}

function modelsComposeProvider(models: SnapshotModel[], provider: SnapshotProvider): boolean {
  const names = new Set<string>();
  for (const model of models) {
    if (names.has(model.model)) return false;
    names.add(model.model);
  }
  const integerKeys = [
    "inputTokens",
    "rawInputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWrite5mTokens",
    "cacheWrite1hTokens",
    "messages",
  ] as const;
  if (
    !integerKeys.every(
      (key) =>
        models.reduce((sum, model) => sum + model.rawTotals[key], 0) === provider.rawTotals[key],
    ) ||
    !equalCost(
      models.reduce((sum, model) => sum + model.rawTotals.cost, 0),
      provider.rawTotals.cost,
    )
  ) {
    return false;
  }
  if (models.length === 0) return provider.rawTotals.messages === 0;
  return sameClasses(composedModelClasses(models), provider.classes);
}

function validProvider(value: unknown, source: TokenSource): value is SnapshotProvider {
  const candidate = record(value);
  const classesValue = candidate?.classes;
  const rawTotalsValue = candidate?.rawTotals;
  const modelsValue = candidate?.models;
  if (
    !candidate ||
    candidate.source !== source ||
    typeof candidate.available !== "boolean" ||
    typeof candidate.discoveryComplete !== "boolean" ||
    typeof candidate.defectLines !== "number" ||
    !Number.isSafeInteger(candidate.defectLines) ||
    candidate.defectLines < 0 ||
    (!candidate.available && candidate.discoveryComplete) ||
    !validRawTotals(rawTotalsValue) ||
    !validClasses(classesValue) ||
    !Array.isArray(modelsValue) ||
    !modelsValue.every((model) => validModel(model, source))
  ) {
    return false;
  }
  if (
    rawTotalsValue.rawInputTokens !==
    rawTotalsValue.inputTokens +
      rawTotalsValue.cacheReadTokens +
      rawTotalsValue.cacheWrite5mTokens +
      rawTotalsValue.cacheWrite1hTokens
  ) {
    return false;
  }
  const classes = classesValue;
  if (candidate.available && candidate.discoveryComplete) {
    return (
      validClassesForRawTotals(classes, rawTotalsValue, source) &&
      modelsComposeProvider(modelsValue, candidate as unknown as SnapshotProvider)
    );
  }
  return (
    !classes.complete &&
    CLASS_KEYS.every((key) => !classes[key].complete && classes[key].tokens === null) &&
    modelsComposeProvider(modelsValue, candidate as unknown as SnapshotProvider)
  );
}

function validRolling24hWindow(value: unknown): value is Rolling24hTokenWindow {
  const candidate = record(value);
  if (
    !candidate ||
    candidate.kind !== "rolling24h" ||
    candidate.timeZone !== HUD_TIME_ZONE ||
    typeof candidate.windowKey !== "string" ||
    typeof candidate.startMs !== "number" ||
    typeof candidate.endMs !== "number" ||
    !Number.isSafeInteger(candidate.startMs) ||
    !Number.isSafeInteger(candidate.endMs) ||
    candidate.endMs - candidate.startMs !== ROLLING_24H_MS ||
    typeof candidate.start !== "string" ||
    typeof candidate.end !== "string"
  ) {
    return false;
  }
  const expected = rolling24hTokenWindow(candidate.endMs, candidate.timeZone);
  return (
    expected.windowKey === candidate.windowKey &&
    expected.startMs === candidate.startMs &&
    expected.endMs === candidate.endMs &&
    expected.start === candidate.start &&
    expected.end === candidate.end
  );
}

function validSnapshotWindow(value: unknown): value is TokenWindow {
  return validDailyWindow(value) || validRolling24hWindow(value);
}

function validDailyWindow(value: unknown): value is DailyTokenWindow {
  const candidate = record(value);
  if (
    !candidate ||
    candidate.kind !== "daily" ||
    candidate.timeZone !== HUD_TIME_ZONE ||
    typeof candidate.windowKey !== "string" ||
    !/^\d{4}-\d{2}-\d{2}$/.test(candidate.windowKey) ||
    typeof candidate.startMs !== "number" ||
    typeof candidate.endMs !== "number" ||
    !Number.isFinite(candidate.startMs) ||
    !Number.isFinite(candidate.endMs) ||
    !Number.isInteger(candidate.startMs) ||
    !Number.isInteger(candidate.endMs) ||
    candidate.startMs >= candidate.endMs ||
    typeof candidate.start !== "string" ||
    typeof candidate.end !== "string"
  ) {
    return false;
  }
  const startDate = new Date(candidate.startMs);
  const endDate = new Date(candidate.endMs);
  if (!Number.isFinite(startDate.getTime()) || !Number.isFinite(endDate.getTime())) return false;
  if (candidate.start !== startDate.toISOString() || candidate.end !== endDate.toISOString()) {
    return false;
  }
  const anchor = Date.parse(`${candidate.windowKey}T12:00:00.000Z`);
  if (!Number.isFinite(anchor)) return false;
  const expected = dailyTokenWindow(anchor);
  return (
    expected.windowKey === candidate.windowKey &&
    expected.startMs === candidate.startMs &&
    expected.endMs === candidate.endMs &&
    dailyTokenWindow(candidate.startMs).windowKey === candidate.windowKey
  );
}

function sameClasses(left: SnapshotTokenClasses, right: SnapshotTokenClasses): boolean {
  return (
    left.complete === right.complete &&
    CLASS_KEYS.every(
      (key) => left[key].tokens === right[key].tokens && left[key].complete === right[key].complete,
    )
  );
}

function equalCost(left: number, right: number): boolean {
  const scale = Math.max(1, Math.abs(left), Math.abs(right));
  return Math.abs(left - right) <= Number.EPSILON * 16 * scale;
}

function sameComposedRawTotals(
  combined: SnapshotRawTotals,
  parts: SnapshotRawTotals[],
): boolean {
  const integerKeys = [
    "inputTokens",
    "rawInputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWrite5mTokens",
    "cacheWrite1hTokens",
    "messages",
  ] as const;
  return (
    integerKeys.every(
      (key) => combined[key] === parts.reduce((sum, part) => sum + part[key], 0),
    ) && equalCost(combined.cost, parts.reduce((sum, part) => sum + part.cost, 0))
  );
}

function exactProviderKeys(providers: Record<string, unknown>): boolean {
  const keys = Object.keys(providers).sort();
  return keys.length === 3 && keys[0] === "claude" && keys[1] === "codex" && keys[2] === "grok";
}

/** Structural and semantic guard for runtime consumers. */
export function isAuthoritativeTokenSnapshot(value: unknown): value is AuthoritativeTokenSnapshot {
  const snapshot = record(value);
  const windowValue = snapshot?.window;
  const rawTotalsValue = snapshot?.rawTotals;
  const classesValue = snapshot?.classes;
  const hud = record(snapshot?.hud);
  const hudIn = hud?.in;
  const hudOut = hud?.out;
  const providers = record(snapshot?.providers);
  const claudeValue = providers?.claude;
  const codexValue = providers?.codex;
  const grokValue = providers?.grok;
  if (
    !snapshot ||
    snapshot.source !== "sigmashake-tokens" ||
    !validSnapshotWindow(windowValue) ||
    typeof snapshot.revision !== "number" ||
    !Number.isSafeInteger(snapshot.revision) ||
    snapshot.revision < 0 ||
    typeof snapshot.updatedAt !== "number" ||
    !Number.isSafeInteger(snapshot.updatedAt) ||
    snapshot.updatedAt < 0 ||
    typeof snapshot.totalCost !== "number" ||
    !Number.isFinite(snapshot.totalCost) ||
    snapshot.totalCost < 0 ||
    typeof snapshot.calls !== "number" ||
    !Number.isSafeInteger(snapshot.calls) ||
    snapshot.calls < 0 ||
    !validRawTotals(rawTotalsValue) ||
    !validClasses(classesValue) ||
    !hud ||
    !validClass(hudIn) ||
    !validClass(hudOut) ||
    !providers ||
    !exactProviderKeys(providers) ||
    !validProvider(claudeValue, "claude") ||
    !validProvider(codexValue, "codex") ||
    !validProvider(grokValue, "grok")
  ) {
    return false;
  }
  const providerRecord = {
    claude: claudeValue,
    codex: codexValue,
    grok: grokValue,
  };
  const composed = composeSnapshotTokenClasses(snapshotClassProviders(providerRecord));
  return (
    equalCost(snapshot.totalCost as number, rawTotalsValue.cost) &&
    snapshot.calls === rawTotalsValue.messages &&
    sameComposedRawTotals(rawTotalsValue, [
      claudeValue.rawTotals,
      codexValue.rawTotals,
      grokValue.rawTotals,
    ]) &&
    sameClasses(classesValue, composed) &&
    hudIn.tokens === composed.input.tokens &&
    hudIn.complete === composed.input.complete &&
    hudOut.tokens === composed.visibleOutput.tokens &&
    hudOut.complete === composed.visibleOutput.complete
  );
}

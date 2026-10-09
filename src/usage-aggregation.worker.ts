import { TokenStore } from "./store.ts";
import {
  USAGE_AGGREGATION_REQUEST_MAX_BYTES,
  USAGE_AGGREGATION_RESPONSE_MAX_BYTES,
  type UsageAggregationWorkerRequest,
  type UsageAggregationWorkerResponse,
} from "./usage-aggregation-worker-protocol.ts";

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function readBoundedInput(): Promise<Uint8Array> {
  const reader = Bun.stdin.stream().getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > USAGE_AGGREGATION_REQUEST_MAX_BYTES) {
        throw new Error(`aggregation request exceeds ${USAGE_AGGREGATION_REQUEST_MAX_BYTES} bytes`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function parseRequest(bytes: Uint8Array): UsageAggregationWorkerRequest {
  const input = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!input.endsWith("\n") || input.indexOf("\n") !== input.length - 1) {
    throw new Error("aggregation worker requires exactly one NDJSON request");
  }
  const parsed: unknown = JSON.parse(input.slice(0, -1));
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("type" in parsed) ||
    parsed.type !== "aggregate" ||
    !("id" in parsed) ||
    !Number.isSafeInteger(parsed.id) ||
    !("databasePath" in parsed) ||
    typeof parsed.databasePath !== "string" ||
    !("options" in parsed) ||
    typeof parsed.options !== "object" ||
    parsed.options === null
  ) {
    throw new Error("aggregation worker received an invalid request");
  }
  return parsed as UsageAggregationWorkerRequest;
}

function encodeResponse(response: UsageAggregationWorkerResponse): string {
  const encoded = `${JSON.stringify(response)}\n`;
  if (new TextEncoder().encode(encoded).byteLength <= USAGE_AGGREGATION_RESPONSE_MAX_BYTES) {
    return encoded;
  }
  return `${JSON.stringify({
    type: "error",
    id: response.id,
    error: `aggregation response exceeds ${USAGE_AGGREGATION_RESPONSE_MAX_BYTES} bytes`,
  } satisfies UsageAggregationWorkerResponse)}\n`;
}

const request = parseRequest(await readBoundedInput());

// These branches are unreachable from HTTP input. They let the regression
// suite prove that real CPU work is isolated and that a dead process is
// replaced, instead of substituting a mock Promise on the main event loop.
if (request.testAction === "crash") throw new Error("injected aggregation worker crash");
if (request.testAction && typeof request.testAction === "object") {
  const delayMs = Math.max(0, Math.min(5_000, Math.trunc(request.testAction.delayMs)));
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
}

let response: UsageAggregationWorkerResponse;
let store: TokenStore | null = null;
try {
  store = new TokenStore(request.databasePath, { readonly: true });
  response = {
    type: "result",
    id: request.id,
    snapshot: store.aggregateUsage(request.options),
  };
} catch (error) {
  response = { type: "error", id: request.id, error: errorText(error) };
} finally {
  store?.close();
}

process.stdout.write(encodeResponse(response));

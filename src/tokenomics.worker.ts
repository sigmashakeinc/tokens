// Off-thread tokenomics worker entry. Spawned as a transient systemd-run unit
// by tokenomics-worker.ts, one process per computation. Runs the same
// CPU-bound JSONL corpus scan (analyzeEfficiency) that used to execute
// synchronously in the HTTP server's event loop — see computeTokenomicsWithStore
// in tokenomics.ts. Opens the store readonly so it never competes with the
// main process's WAL writer.

import { TokenStore } from "./store.ts";
import { computeTokenomicsWithStore } from "./tokenomics.ts";
import {
  TOKENOMICS_WORKER_REQUEST_MAX_BYTES,
  TOKENOMICS_WORKER_RESPONSE_MAX_BYTES,
  type TokenomicsWorkerRequest,
  type TokenomicsWorkerResponse,
} from "./tokenomics-worker-protocol.ts";

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
      if (total > TOKENOMICS_WORKER_REQUEST_MAX_BYTES) {
        throw new Error(
          `tokenomics worker request exceeds ${TOKENOMICS_WORKER_REQUEST_MAX_BYTES} bytes`,
        );
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

function parseRequest(bytes: Uint8Array): TokenomicsWorkerRequest {
  const input = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!input.endsWith("\n") || input.indexOf("\n") !== input.length - 1) {
    throw new Error("tokenomics worker requires exactly one NDJSON request");
  }
  const parsed: unknown = JSON.parse(input.slice(0, -1));
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("type" in parsed) ||
    parsed.type !== "tokenomics" ||
    !("id" in parsed) ||
    !Number.isSafeInteger(parsed.id) ||
    !("databasePath" in parsed) ||
    typeof parsed.databasePath !== "string"
  ) {
    throw new Error("tokenomics worker received an invalid request");
  }
  return parsed as TokenomicsWorkerRequest;
}

function encodeResponse(response: TokenomicsWorkerResponse): string {
  const encoded = `${JSON.stringify(response)}\n`;
  if (new TextEncoder().encode(encoded).byteLength <= TOKENOMICS_WORKER_RESPONSE_MAX_BYTES) {
    return encoded;
  }
  return `${JSON.stringify({
    type: "error",
    id: response.id,
    error: `tokenomics worker response exceeds ${TOKENOMICS_WORKER_RESPONSE_MAX_BYTES} bytes`,
  } satisfies TokenomicsWorkerResponse)}\n`;
}

const request = parseRequest(await readBoundedInput());

// Unreachable from HTTP input — lets the regression suite prove real CPU work
// is isolated in this process and a dead worker is replaced, instead of
// substituting a mock Promise on the main event loop.
if (request.testAction === "crash") throw new Error("injected tokenomics worker crash");
if (request.testAction && typeof request.testAction === "object") {
  const delayMs = Math.max(0, Math.min(5_000, Math.trunc(request.testAction.delayMs)));
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
}

let response: TokenomicsWorkerResponse;
let store: TokenStore | null = null;
try {
  store = new TokenStore(request.databasePath, { readonly: true });
  const result = await computeTokenomicsWithStore(store);
  response = { type: "result", id: request.id, result };
} catch (error) {
  response = { type: "error", id: request.id, error: errorText(error) };
} finally {
  store?.close();
}

process.stdout.write(encodeResponse(response));

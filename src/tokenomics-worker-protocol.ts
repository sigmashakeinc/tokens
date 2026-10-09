import type { TokenomicsResult } from "./tokenomics.ts";

export const TOKENOMICS_WORKER_REQUEST_MAX_BYTES = 16 * 1024;
export const TOKENOMICS_WORKER_RESPONSE_MAX_BYTES = 4 * 1024 * 1024;

export interface TokenomicsWorkerRequest {
  type: "tokenomics";
  id: number;
  databasePath: string;
  /** Test-only fault injection set solely by an underscored launcher hook. */
  testAction?: "crash" | { delayMs: number };
}

export type TokenomicsWorkerResponse =
  | {
      type: "result";
      id: number;
      result: TokenomicsResult;
    }
  | {
      type: "error";
      id: number;
      error: string;
    };

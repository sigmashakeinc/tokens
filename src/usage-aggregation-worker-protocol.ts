import type { UsageAggregateOptions, UsageAggregateSnapshot } from "./store.ts";

export const USAGE_AGGREGATION_REQUEST_MAX_BYTES = 64 * 1024;
export const USAGE_AGGREGATION_RESPONSE_MAX_BYTES = 2 * 1024 * 1024;

export interface UsageAggregationWorkerRequest {
  type: "aggregate";
  id: number;
  databasePath: string;
  options: UsageAggregateOptions;
  /** Test-only fault injection set solely by an underscored launcher hook. */
  testAction?: "crash" | { delayMs: number };
}

export type UsageAggregationWorkerResponse =
  | {
      type: "result";
      id: number;
      snapshot: UsageAggregateSnapshot;
    }
  | {
      type: "error";
      id: number;
      error: string;
    };

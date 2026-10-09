import type { LiveEventDelta } from "./live.ts";
import type { RateSnapshot } from "./rate.ts";

export const USAGE_INGEST_REQUEST_MAX_BYTES = 64 * 1024;
export const USAGE_INGEST_RESPONSE_MAX_BYTES = 2 * 1024 * 1024;
export const USAGE_INGEST_RATE_MAX_BYTES = 128 * 1024;
export const USAGE_INGEST_RECENT_MAX_EVENTS = 10;
export const USAGE_INGEST_RECENT_TEXT_MAX_BYTES = 64 * 1024;

export interface TokenSnapshotHeartbeatTarget {
  revision: number;
  windowKey: string;
  updatedAt: number;
}

export interface UsageIngestWorkerRequest {
  type: "ingest";
  id: number;
  databasePath: string;
  claudeProjectsDir: string;
  codexSessionsDir: string;
  grokLogsDir?: string;
  grokSessionsDir?: string;
  /** Explicit capability issued only from a previously-ready parent cycle. */
  heartbeatTarget?: TokenSnapshotHeartbeatTarget;
  /** Test-only fault injection set solely through an underscored launcher hook. */
  testAction?:
    | "crash"
    | { delayMs: number; heartbeatIntervalMs?: number; failAfterDelay?: boolean };
}

export interface UsageIngestWorkerResult {
  files: number;
  mutated: boolean;
  revision: number;
  /** True when this run published a missing, changed-revision, or new-day HUD cache. */
  snapshotRefreshed: boolean;
  /** Exact fixed-window output computed and persisted inside the bounded child. */
  rate: RateSnapshot;
  update: LiveEventDelta;
}

export type UsageIngestWorkerResponse =
  | {
      type: "result";
      id: number;
      result: UsageIngestWorkerResult;
    }
  | {
      type: "error";
      id: number;
      error: string;
    };

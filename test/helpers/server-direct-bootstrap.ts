// Process-level server lifecycle fixture. Production never selects this
// launcher; tests must opt in through the explicit underscored seam.
const EARLY_SIGTERM_MAX_HOLD_MS = 2_000;
let earlySigtermRequested = false;
let earlySigtermDeadline: ReturnType<typeof setTimeout> | null = null;
const captureEarlySigterm = (): void => {
  if (earlySigtermRequested) return;
  earlySigtermRequested = true;
  earlySigtermDeadline = setTimeout(() => {
    process.off("SIGTERM", captureEarlySigterm);
    process.kill(process.pid, "SIGTERM");
  }, EARLY_SIGTERM_MAX_HOLD_MS);
  earlySigtermDeadline.unref?.();
};
process.on("SIGTERM", captureEarlySigterm);

try {
  const ingestWorker = await import("../../src/usage-ingest-worker.ts");
  ingestWorker._setUsageIngestLauncherForTest("direct");
  const server = await import("../../src/server.ts");
  const lifecycle = server.startTokensServer();
  process.off("SIGTERM", captureEarlySigterm);
  if (earlySigtermDeadline !== null) clearTimeout(earlySigtermDeadline);
  if (earlySigtermRequested) await lifecycle.shutdown();
} catch (error) {
  process.off("SIGTERM", captureEarlySigterm);
  if (earlySigtermDeadline !== null) clearTimeout(earlySigtermDeadline);
  throw error;
}

// Keep non-handler values outside the Worker entry module: workerd validates exports.
export const MUTATING_ROUTES: ReadonlySet<string> = new Set([
	"/api/seed",
	"/api/enrich-bios",
	"/api/process-ny-oca",
	"/api/upload",
	"/api/upload-raw",
]);

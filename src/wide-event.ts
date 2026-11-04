// ── Wide-event logging (native observability rollout, 2026-11-04) ──
//
// One canonical JSON event per request, emitted to Workers Logs via
// console.log — the loggingsucks.com "wide event" pattern, implemented
// with Cloudflare-native tooling only (no external observability
// product): every field a future query needs (route, status, outcome,
// duration, colo, request id) rides on a single structured line instead
// of being scattered across free-text logs. Per-function detail comes
// from the platform: Workers Traces ([observability.traces] in
// wrangler.toml) records spans for the runtime, fetch, and bindings
// automatically.
//
// Privacy posture: pathname only — no query string (tokens can ride in
// it), no cookies, no body. The request id is the Cloudflare ray id,
// which joins this event to the platform trace and to Cloudflare Issues.
//
// This entry is a plain fetch handler (no Hono), so instead of
// middleware the default export wraps the handler and calls
// emitWideEvent once per request from a finally-equivalent path.

export function emitWideEvent(
	request: Request,
	status: number,
	start: number,
	errorName?: string,
): void {
	const cf = (request as { cf?: { colo?: string; country?: string } }).cf;
	console.log(
		JSON.stringify({
			event: "http_request",
			ts: new Date().toISOString(),
			request_id: request.headers.get("cf-ray") ?? undefined,
			method: request.method,
			path: new URL(request.url).pathname,
			status,
			outcome:
				errorName || status >= 500
					? "error"
					: status >= 400
						? "client_error"
						: "ok",
			duration_ms: Date.now() - start,
			colo: cf?.colo,
			country: cf?.country,
			user_agent: request.headers.get("user-agent")?.slice(0, 120),
			...(errorName ? { error: errorName } : {}),
		}),
	);
}

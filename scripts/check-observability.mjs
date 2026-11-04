// Observability lint rule (native observability rollout, 2026-11-04).
//
// This Worker must never ship without the platform observability that
// feeds Workers Logs, Workers Traces, and Cloudflare Issues — and the
// wide-event wrap must stay wired into the app entry. Run via
// `npm run lint:observability`; it runs first in the `check` script so a
// regression fails the normal gate.
import { readFileSync } from "node:fs";

const failures = [];

const wrangler = readFileSync(
	new URL("../wrangler.toml", import.meta.url),
	"utf8",
);
for (const section of ["observability.logs", "observability.traces"]) {
	const block = new RegExp(`\\[${section}\\]([^[]*)`).exec(wrangler)?.[1] ?? "";
	if (!/enabled\s*=\s*true/.test(block)) {
		failures.push(`wrangler.toml: [${section}] must exist with enabled = true`);
	}
}

const entry = readFileSync(new URL("../src/index.ts", import.meta.url), "utf8");
if (!entry.includes("emitWideEvent") || !entry.includes("wide-event")) {
	failures.push("src/index.ts: wide-event wrap is not wired into the entry");
}

if (failures.length > 0) {
	console.error(failures.join("\n"));
	process.exit(1);
}
console.log(
	"observability check passed: logs + traces enabled, wideEvent wired",
);

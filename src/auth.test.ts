// Auth gate for the write surface.  Run: npm test   (node --test, Node >= 23)
//
// Until 2026-09-03 three MUTATING routes were reachable by anyone:
//   /api/seed             re-scrapes a city and overwrites its R2 object
//   /api/enrich-bios      writes R2
//   /api/process-ny-oca   writes R2
// Only /api/upload and /api/upload-raw checked the bearer token.
//
// These tests drive the REAL exported fetch() so they exercise the actual
// dispatch wiring, not the helper in isolation — a helper can be perfect while
// the route that needs it never calls it, which is precisely the bug that
// shipped. Every gate below is paired with a negative control proving it goes
// RED on a known-bad input.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import worker, { MUTATING_ROUTES } from "./index.ts";

const SECRET = "test-upload-secret";

// Minimal R2 stub. A DENIED request never reaches it — that is the whole point,
// the gate runs before any handler — but an ALLOWED one does, and without this
// the handler throws on `env.DATA.get` and the test cannot read a status.
const emptyR2 = {
	get: async () => null,
	put: async () => undefined,
	list: async () => ({ objects: [], truncated: false }),
	delete: async () => undefined,
};
const env = { UPLOAD_SECRET: SECRET, DATA: emptyR2 } as unknown as Parameters<
	typeof worker.fetch
>[1];

/**
 * Did the request get PAST the auth gate?
 *
 * The gate always returns a Response (401) and never throws, so a throw from
 * deeper in a handler is itself proof that auth passed — we deliberately do not
 * stub these handlers into full working order, since their behaviour is not
 * what this file tests.
 */
async function passedGate(path: string, token?: string): Promise<boolean> {
	try {
		const res = await worker.fetch(req(path, token), env);
		return res.status !== 401;
	} catch {
		return true; // threw inside the handler => it was reached => auth passed
	}
}

const req = (path: string, token?: string) =>
	new Request(`https://example.test${path}`, {
		headers: token ? { Authorization: `Bearer ${token}` } : {},
	});

const WRITE_PATHS = [
	"/api/seed",
	"/api/enrich-bios",
	"/api/process-ny-oca",
	"/api/upload",
	"/api/upload-raw",
];

// Read-only routes. This is a public-records site: public reads are the
// product, so a regression that starts demanding auth here is also a bug.
const READ_PATHS = [
	"/api/city",
	"/api/judge",
	"/api/cities",
	"/api/judges",
	"/api/worst",
	"/api/status",
	"/judges",
];

for (const p of WRITE_PATHS) {
	test(`${p} rejects a request with NO token`, async () => {
		const res = await worker.fetch(req(p), env);
		assert.equal(res.status, 401, `${p} must 401 without a bearer token`);
	});

	test(`${p} rejects a WRONG token`, async () => {
		const res = await worker.fetch(req(p, "not-the-secret"), env);
		assert.equal(res.status, 401);
	});

	// Negative control for the two tests above: with the CORRECT token the gate
	// must let the request through. If this fails, the 401s prove nothing —
	// they could be a 401 for some unrelated reason.
	test(`${p} lets the CORRECT token past the gate`, async () => {
		assert.ok(
			await passedGate(p, SECRET),
			`${p} rejected a valid token — the 401 tests above are then vacuous`,
		);
	});
}

test("write routes fail CLOSED when UPLOAD_SECRET is unset", async () => {
	const noSecret = {} as unknown as Parameters<typeof worker.fetch>[1];
	for (const p of WRITE_PATHS) {
		// Even an empty bearer must not match an empty/undefined secret.
		const res = await worker.fetch(req(p, ""), noSecret);
		assert.equal(res.status, 401, `${p} fell OPEN with no secret configured`);
	}
});

test("read-only routes stay public (no token required)", async () => {
	for (const p of READ_PATHS) {
		assert.ok(await passedGate(p), `${p} must not require auth`);
	}
});

// ── Structural gate ────────────────────────────────────────────────────────
// The tests above pin today's five routes. This one fails on TOMORROW's: any
// new route whose handler writes R2 but which nobody added to MUTATING_ROUTES.
// A behavioural test can only cover paths someone remembered to list.

/** Extract a function body by brace-matching. Throws rather than returning ""
 *  — an extractor that returns empty makes every `includes` check silently
 *  false, converting one broken helper into a suite-wide false negative. */
function bodyOf(decl: string, src: string): string {
	const start = src.indexOf(decl);
	if (start === -1) throw new Error(`declaration not found: ${decl}`);
	let depth = 0;
	let seen = false;
	for (let i = start; i < src.length; i++) {
		if (src[i] === "{") {
			depth++;
			seen = true;
		} else if (src[i] === "}" && seen && --depth === 0) {
			return src.slice(start, i + 1);
		}
	}
	throw new Error(`unbalanced braces after: ${decl}`);
}

test("every dispatched route whose handler writes R2 is in MUTATING_ROUTES", () => {
	const src = readFileSync(new URL("./index.ts", import.meta.url), "utf8");
	const dispatch = bodyOf("async fetch(request: Request, env: Env)", src);

	// path -> handler name, straight from the dispatcher.
	const routes = [
		...dispatch.matchAll(/p === "([^"]+)"[^\n]*?return (\w+)\(/g),
	];
	assert.ok(
		routes.length >= 10,
		`only parsed ${routes.length} routes — the dispatch regex has drifted, so this test is vacuous`,
	);

	const unguarded: string[] = [];
	for (const [, path, handler] of routes) {
		let body: string;
		try {
			body = bodyOf(`async function ${handler}(`, src);
		} catch {
			continue; // handler defined elsewhere; nothing to assert
		}
		// A direct R2 write, or a call to seedCity() which performs one.
		const writes = /\.put\(|seedCity\(/.test(body);
		if (writes && !MUTATING_ROUTES.has(path))
			unguarded.push(`${path} -> ${handler}`);
	}

	assert.deepEqual(
		unguarded,
		[],
		`route(s) write stored data but are NOT in MUTATING_ROUTES, so they are publicly writable:\n  ${unguarded.join("\n  ")}`,
	);
});

test("negative control: the structural gate detects a missing route", () => {
	// Same logic as above, but with a MUTATING_ROUTES that has had /api/seed
	// removed. It MUST flag it — otherwise the test above passes for free.
	const src = readFileSync(new URL("./index.ts", import.meta.url), "utf8");
	const dispatch = bodyOf("async fetch(request: Request, env: Env)", src);
	const holed = new Set([...MUTATING_ROUTES].filter((r) => r !== "/api/seed"));

	const unguarded: string[] = [];
	for (const [, path, handler] of dispatch.matchAll(
		/p === "([^"]+)"[^\n]*?return (\w+)\(/g,
	)) {
		let body: string;
		try {
			body = bodyOf(`async function ${handler}(`, src);
		} catch {
			continue;
		}
		if (/\.put\(|seedCity\(/.test(body) && !holed.has(path))
			unguarded.push(path);
	}

	assert.ok(
		unguarded.includes("/api/seed"),
		"removing /api/seed from MUTATING_ROUTES did NOT trip the structural gate — the gate is vacuous",
	);
});

test("timing-safe compare is used, not a plain !== on the secret", () => {
	const src = readFileSync(new URL("./index.ts", import.meta.url), "utf8");
	assert.equal(
		/token !== env\.UPLOAD_SECRET/.test(src),
		false,
		"a short-circuiting !== compare on the bearer token leaks it through response timing",
	);
	assert.ok(
		src.includes("function timingSafeEqual("),
		"timingSafeEqual() is missing",
	);
});

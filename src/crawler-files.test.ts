// Crawler + disclosure files.  Run: npm test
//
// Until 2026-09-04 the dispatcher had no case for any of these, so all three
// fell through to `htmlResponse(HTML)`. Measured against production that day:
//
//   /robots.txt    200  text/html;charset=utf-8   (87 KB of web page)
//   /sitemap.xml   200  text/html;charset=utf-8
//   /security.txt  200  text/html;charset=utf-8   (should redirect)
//
// None of that is an error anyone sees — a 200 is a 200 — which is exactly why
// it survived. Every assertion below therefore checks the CONTENT TYPE too, not
// just the status: `text/html` here is the regression.

import assert from "node:assert/strict";
import { test } from "node:test";
import worker from "./index.ts";

/** Read-only routes; the R2 binding is never touched, so it may stay absent. */
const env = {} as Parameters<typeof worker.fetch>[1];

const get = (path: string, host = "https://judge-search.example") =>
	worker.fetch(new Request(`${host}${path}`), env);

test("/robots.txt is plain text, not the homepage", async () => {
	const res = await get("/robots.txt");
	assert.equal(res.status, 200);
	assert.match(
		res.headers.get("content-type") ?? "",
		/^text\/plain/,
		"robots.txt served as HTML — it fell through to the catch-all again",
	);
	const body = await res.text();
	assert.match(body, /^User-agent: \*/m);
	assert.match(body, /^Disallow: \/api\//m);
});

test("robots.txt advertises a sitemap that actually exists", async () => {
	// A Sitemap: line pointing at a 404 is worse than none — it tells the crawler
	// to keep asking. So follow the URL it publishes rather than assuming.
	const body = await (await get("/robots.txt")).text();
	const m = body.match(/^Sitemap: (\S+)$/m);
	assert.ok(m, "robots.txt publishes no Sitemap: line");
	const res = await get(new URL(m[1]).pathname);
	assert.equal(res.status, 200, `${m[1]} is not reachable`);
	assert.match(res.headers.get("content-type") ?? "", /xml/);
});

test("/sitemap.xml is XML listing both crawlable pages", async () => {
	const res = await get("/sitemap.xml");
	assert.equal(res.status, 200);
	assert.match(
		res.headers.get("content-type") ?? "",
		/xml/,
		"sitemap served as HTML — Search Console would reject it",
	);
	const body = await res.text();
	assert.match(body, /^<\?xml version="1\.0"/);
	for (const loc of [
		"https://judge-search.example/",
		"https://judge-search.example/judges",
	]) {
		assert.ok(body.includes(`<loc>${loc}</loc>`), `sitemap is missing ${loc}`);
	}
	assert.match(body, /<lastmod>\d{4}-\d{2}-\d{2}<\/lastmod>/);
});

test("the sitemap names the host it was fetched from", async () => {
	// A sitemap that names a different origin than the one serving it is ignored.
	// This Worker has answered on more than one hostname, so the origin must come
	// from the request, never from a constant.
	const body = await (
		await get("/sitemap.xml", "https://other.example")
	).text();
	assert.ok(body.includes("<loc>https://other.example/</loc>"));
	assert.ok(
		!body.includes("judge-search.example"),
		"sitemap hardcodes a host instead of using the request origin",
	);
});

test("/security.txt redirects to the well-known path (RFC 9116)", async () => {
	const res = await get("/security.txt");
	assert.equal(
		res.status,
		301,
		"/security.txt returned a body instead of redirecting — a second copy of a disclosure policy is a copy that goes stale",
	);
	assert.equal(
		res.headers.get("location"),
		"https://judge-search.example/.well-known/security.txt",
	);
});

test("negative control: an unknown path still returns the homepage", async () => {
	// Proves the three cases above are matched by their OWN routes and not by
	// some blanket change to the catch-all. If this ever returns 404, the tests
	// above stop proving anything about routing order.
	const res = await get("/definitely-not-a-route");
	assert.equal(res.status, 200);
	assert.match(res.headers.get("content-type") ?? "", /text\/html/);
});

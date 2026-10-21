import assert from "node:assert/strict";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

const runtime = new Miniflare(
	convertV4MiniflareOptions({
		cf: false,
		modules: true,
		scriptPath: "dist/index.js",
		compatibilityDate: "2025-04-01",
		r2Buckets: ["DATA"],
		bindings: { UPLOAD_SECRET: "fixture-only-runtime-secret" },
		outboundService() {
			throw new Error("Runtime check forbids external requests.");
		},
	}),
);
try {
	const request = (path, options) =>
		runtime.dispatchFetch(`http://localhost${path}`, options);
	const home = await request("/");
	assert.equal(home.status, 200, await home.clone().text());
	assert.match(await home.text(), /<html/i);
	assert.ok(home.headers.get("Content-Security-Policy"));
	const robots = await request("/robots.txt");
	assert.match(robots.headers.get("Content-Type"), /text\/plain/);
	assert.match(await robots.text(), /User-agent:/);
	const sitemap = await request("/sitemap.xml");
	assert.match(sitemap.headers.get("Content-Type"), /xml/);
	assert.match(await sitemap.text(), /http:\/\/localhost\/judges/);
	for (const path of [
		"/api/seed",
		"/api/enrich-bios",
		"/api/process-ny-oca",
		"/api/upload",
		"/api/upload-raw",
	]) {
		assert.equal((await request(path)).status, 401);
	}
	const fixture = {
		city: "Synthetic fixture",
		state: "",
		judges: [],
		source: "test-only",
		total_cases: 0,
		total_fta: 0,
		total_rearrests: 0,
		total_revocations: 0,
	};
	const uploaded = await request("/api/upload?slug=fixture", {
		method: "POST",
		headers: {
			Authorization: "Bearer fixture-only-runtime-secret",
			"Content-Type": "application/json",
		},
		body: JSON.stringify(fixture),
	});
	assert.equal(uploaded.status, 200, await uploaded.clone().text());
	const stored = await runtime.getR2Bucket("DATA");
	assert.ok(await stored.get("courts/fixture.json"));
	const read = await request("/api/city?slug=fixture");
	assert.equal(read.status, 200);
	assert.equal((await read.json()).city, fixture.city);
	console.log(
		"Built workerd startup, public content, security headers, auth and ephemeral R2 roundtrip passed.",
	);
} finally {
	await runtime.dispose();
}

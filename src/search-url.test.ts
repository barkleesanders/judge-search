// URL-addressable name search: /?q=<name>   Run: npm test
//
// Until 2026-09-04 the search box was invisible to the URL. GET /?q=colfax and
// GET / returned byte-identical 84,672-byte responses (measured), the client
// read only all/city/id/key/n/slug, and a search could not be linked to.
//
// These tests run against the RENDERED client script (the exported HTML
// constant, template escapes already resolved) — the artifact that actually
// ships — and evaluate the real functions rather than asserting on their text
// where behaviour is what matters.

import assert from "node:assert/strict";
import { test } from "node:test";
import { HTML } from "./index.ts";
import { bodyOf, clientScript } from "./source-probe.ts";

const JS = clientScript(HTML);

// ── syncUrl: what the address bar ends up saying ───────────────────────────

/** syncUrl with `history` and `location` injected, so no DOM is needed. */
function makeSyncUrl(href: string): {
	sync: (v: string) => void;
	urls: string[];
} {
	const urls: string[] = [];
	const factory = new Function(
		"history",
		"location",
		`${bodyOf("function syncUrl(v){", JS)}\nreturn syncUrl;`,
	) as (h: unknown, l: unknown) => (v: string) => void;
	const sync = factory(
		{ replaceState: (_s: unknown, _t: unknown, u: string) => urls.push(u) },
		{ href },
	);
	return { sync, urls };
}

test("typing writes ?q= into the URL", () => {
	const { sync, urls } = makeSyncUrl("https://x.test/");
	sync("colfax");
	assert.deepEqual(urls, ["/?q=colfax"]);
});

test("clearing the box removes ?q= rather than leaving ?q=", () => {
	const { sync, urls } = makeSyncUrl("https://x.test/?q=colfax");
	sync("");
	assert.deepEqual(urls, ["/"]);
});

test("searching drops a stale ?judge= deep link", () => {
	// Otherwise a reload of the shared URL would focus a judge the visitor had
	// already navigated away from, while the box showed a different search.
	const { sync, urls } = makeSyncUrl(
		"https://x.test/?judge=abc&city=miami#sources",
	);
	sync("colfax");
	assert.deepEqual(urls, ["/?q=colfax#sources"]);
});

test("unrelated query params and the hash survive", () => {
	const { sync, urls } = makeSyncUrl("https://x.test/?utm_source=x&q=old#m");
	sync("linda colfax");
	assert.deepEqual(urls, ["/?utm_source=x&q=linda+colfax#m"]);
});

test("a query needing encoding round-trips", () => {
	const { sync, urls } = makeSyncUrl("https://x.test/");
	sync("o'brien & smith");
	assert.equal(urls.length, 1);
	assert.equal(
		new URL(urls[0], "https://x.test").searchParams.get("q"),
		"o'brien & smith",
	);
});

// ── runSearch: the single writer of search state ───────────────────────────

type RunHarness = {
	run: (v: string, updateUrl?: boolean) => void;
	synced: string[];
	rendered: string[];
	box: { value: string };
};

function makeRunSearch(): RunHarness {
	const synced: string[] = [];
	const rendered: string[] = [];
	const box = { value: "" };
	const factory = new Function(
		"$",
		"syncUrl",
		"closeSearch",
		"jStatus",
		"_index",
		"renderHits",
		"searchJudges",
		"openSearch",
		"loadIndex",
		"esc",
		`${bodyOf("function runSearch(v,updateUrl){", JS)}\nreturn runSearch;`,
	) as (...deps: unknown[]) => (v: string, updateUrl?: boolean) => void;
	const run = factory(
		(id: string) => (id === "jq" ? box : null),
		(v: string) => synced.push(v),
		() => {},
		() => {},
		[{ name: "Judge Linda Colfax" }], // _index loaded => synchronous branch
		(_all: unknown, q: string) => rendered.push(q),
		() => [],
		() => {},
		() => Promise.resolve([]),
		(s: string) => s,
	);
	return { run, synced, rendered, box };
}

test("typing syncs the URL; restoring a link does NOT rewrite it", () => {
	const h = makeRunSearch();
	h.run("colfax", true); // input handler
	h.run("colfax", false); // on-load, URL is already the source
	// 0 would mean syncUrl is never called at all; 2 would mean the
	// updateUrl===false guard is gone and a shared link rewrites its own bar.
	assert.deepEqual(h.synced, ["colfax"]);
	assert.deepEqual(h.rendered, ["colfax", "colfax"]);
});

test("restoring ?q= populates the search box", () => {
	const h = makeRunSearch();
	h.run("colfax", false);
	assert.equal(
		h.box.value,
		"colfax",
		"a shared ?q= link left the box empty, so the visitor cannot see or edit the query that produced the results",
	);
});

test("an omitted updateUrl still syncs (default is not silently off)", () => {
	const h = makeRunSearch();
	h.run("colfax");
	assert.deepEqual(h.synced, ["colfax"]);
});

test("a whitespace-only query clears instead of searching", () => {
	const h = makeRunSearch();
	h.run("   ", true);
	assert.deepEqual(h.rendered, []);
	assert.deepEqual(h.synced, ["   "]);
});

// ── Structural gates: catch TOMORROW's entry point ─────────────────────────

test("every renderHits(searchJudges(...)) call sits inside runSearch", () => {
	// A future input path that renders hits directly would work perfectly and
	// silently stop updating the URL — no error, no failing behavioural test.
	const all = [...JS.matchAll(/renderHits\(searchJudges\(/g)].map(
		(m) => m.index as number,
	);
	assert.ok(
		all.length >= 1,
		"found no renderHits(searchJudges( call at all — the search was rewritten and this gate is vacuous",
	);
	const body = bodyOf("function runSearch(v,updateUrl){", JS);
	const start = JS.indexOf(body);
	assert.ok(start >= 0);
	const outside = all.filter((i) => i < start || i >= start + body.length);
	assert.deepEqual(
		outside,
		[],
		"a search entry point renders hits without going through runSearch(), so it will not sync the URL",
	);
});

test("negative control: that gate detects a call placed outside runSearch", () => {
	const holed = `${JS}\nfunction sneaky(){renderHits(searchJudges('x'),'x');}`;
	const all = [...holed.matchAll(/renderHits\(searchJudges\(/g)].map(
		(m) => m.index as number,
	);
	const body = bodyOf("function runSearch(v,updateUrl){", holed);
	const start = holed.indexOf(body);
	const outside = all.filter((i) => i < start || i >= start + body.length);
	assert.equal(
		outside.length,
		1,
		"adding a renderHits call outside runSearch did NOT trip the gate — it is vacuous",
	);
});

test("on load, ?judge= is resolved before ?q=", () => {
	// Both indices must be real: indexOf returns -1 when absent, and -1 < n is
	// true, so an ordering assertion passes exactly when the thing it orders
	// has been deleted.
	const judge = JS.indexOf("sp.get('judge')");
	const query = JS.indexOf("sp.get('q')");
	assert.ok(judge >= 0, "the on-load handler no longer reads ?judge=");
	assert.ok(query >= 0, "the on-load handler does not read ?q=");
	assert.ok(
		judge < query,
		"?q= is handled before ?judge=, so a link naming one specific judge loses to a query that may match several",
	);
	assert.ok(
		JS.includes("runSearch(q,false)"),
		"the on-load ?q= path does not call runSearch(q,false)",
	);
});

test("picking a result leaves the canonical judge deep link in the URL", () => {
	const pick = bodyOf("function pickHit(i){", JS);
	assert.ok(
		pick.includes("history.replaceState"),
		"pickHit does not update the URL, so a link copied after choosing a judge reopens the wrong page",
	);
	// Must be the SAME scheme judgeHref() emits on /judges, not a second one.
	assert.ok(
		pick.includes(
			"'/?judge='+encodeURIComponent(j.id)+'&city='+encodeURIComponent(j.slug)",
		),
		"pickHit writes a deep link that is not judgeHref's /?judge=<id>&city=<slug> scheme",
	);
});

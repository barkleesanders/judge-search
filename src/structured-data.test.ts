// schema.org JSON-LD.  Run: npm test
//
// The point of these tests is NOT that the markup exists — a block of JSON is
// trivially easy to ship and impossible to be wrong about syntactically. The
// point is that each CLAIM is one the site can back:
//
//   - the SearchAction's urlTemplate must name a param the client actually
//     reads. Until 2026-09-04 it did not: GET /?q=colfax and GET / returned
//     byte-identical responses, so this node would have advertised a
//     capability the site did not have. That is the failure worth testing for,
//     and no JSON-schema validator can see it.
//   - Organization.sameAs must be absent or genuinely populated, never a
//     plausible-looking guess. A fabricated profile URL has a valid shape and
//     passes every structural check there is.

import assert from "node:assert/strict";
import { test } from "node:test";
import { HTML } from "./index.ts";
import { clientScript } from "./source-probe.ts";

/** The parsed ld+json graph, or a loud failure. */
function graph(): Record<string, unknown>[] {
	const m = HTML.match(
		/<script type="application\/ld\+json">([\s\S]*?)<\/script>/,
	);
	assert.ok(m, "the page carries no ld+json block");
	const parsed = JSON.parse(m[1]) as { "@graph"?: Record<string, unknown>[] };
	const nodes = parsed["@graph"];
	assert.ok(
		Array.isArray(nodes) && nodes.length > 0,
		"ld+json parsed but its @graph is empty — every assertion below would be vacuous",
	);
	return nodes;
}

const node = (type: string): Record<string, unknown> => {
	const found = graph().find((n) => n["@type"] === type);
	assert.ok(found, `no ${type} node in the ld+json graph`);
	return found;
};

test("the ld+json block is valid JSON", () => {
	// A malformed block is silently ignored by every consumer — no error, no
	// warning, just an absent entity. Parsing it here is the only signal.
	assert.ok(graph().length >= 2);
});

test("the SearchAction points at a param the client actually reads", () => {
	const site = node("WebSite");
	const action = site.potentialAction as {
		"@type": string;
		target: { urlTemplate: string };
		"query-input": string;
	};
	assert.equal(action["@type"], "SearchAction");
	assert.equal(action["query-input"], "required name=search_term_string");

	const tpl = action.target.urlTemplate;
	const m = tpl.match(/[?&](\w+)=\{search_term_string\}/);
	assert.ok(
		m,
		`urlTemplate does not bind search_term_string to a param: ${tpl}`,
	);
	const param = m[1];

	// THE load-bearing assertion. Read the shipped client script and confirm it
	// reads this exact param on load. If someone renames ?q= the markup keeps
	// validating while the sitelinks searchbox silently sends users nowhere.
	const js = clientScript(HTML);
	assert.ok(
		js.includes(`sp.get('${param}')`),
		`the SearchAction advertises ?${param}= but the client never reads it — the markup would promise a search the site cannot perform`,
	);
	assert.ok(
		js.includes("runSearch("),
		"the client reads the param but no longer runs a search with it",
	);
});

test("the SearchAction target is a real URL on this site", () => {
	const site = node("WebSite");
	const tpl = (site.potentialAction as { target: { urlTemplate: string } })
		.target.urlTemplate;
	// Substituting a term must yield a URL that parses and stays same-origin.
	const filled = tpl.replace("{search_term_string}", "colfax");
	const u = new URL(filled);
	assert.equal(u.origin, new URL(String(site.url)).origin);
	assert.equal(u.searchParams.get("q"), "colfax");
});

test("Organization.sameAs is either absent or genuinely populated", () => {
	// An EMPTY sameAs array is worse than none: it looks answered. And a guessed
	// profile URL has a perfectly valid shape, so nothing downstream can catch
	// it — this is the only place the distinction can be enforced.
	const org = node("Organization");
	if (!("sameAs" in org)) return; // honest absence — nothing to verify
	const same = org.sameAs;
	assert.ok(
		Array.isArray(same) && same.length > 0,
		"Organization declares sameAs but it is empty — drop the key instead",
	);
	for (const u of same) {
		assert.doesNotThrow(
			() => new URL(String(u)),
			`sameAs entry is not a URL: ${u}`,
		);
	}
});

test("the graph's internal @id references all resolve", () => {
	// publisher points at the Organization by @id. A typo there yields a
	// dangling reference that consumers drop silently, unlinking the two nodes.
	const nodes = graph();
	const ids = new Set(nodes.map((n) => n["@id"]).filter(Boolean));
	const site = node("WebSite");
	const pub = site.publisher as { "@id"?: string } | undefined;
	assert.ok(pub?.["@id"], "WebSite has no publisher reference");
	assert.ok(
		ids.has(pub["@id"]),
		`publisher points at ${pub["@id"]}, which is not a node in this graph`,
	);
});

test("the homepage declares a canonical URL", () => {
	// / is reachable as /, /?q=..., /?judge=... and (today) every unmatched
	// path. Without a canonical each of those is a separate indexable URL.
	const head = HTML.slice(0, HTML.indexOf("</head>"));
	assert.match(
		head,
		/<link rel="canonical" href="https:\/\/judge-search\.barkleesanders\.workers\.dev\/">/,
	);
});

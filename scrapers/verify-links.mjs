#!/usr/bin/env node
/**
 * Link verifier for JudgeSearch
 * Checks every external URL used in the site returns 200.
 * Run before deploy: node scrapers/verify-links.mjs
 */

const LINKS = [
	// Nav links (courtwatch.us/methodology removed — now using in-page #method)
	{ url: "https://free.law/about/", label: "Nav: Free Law Project" },

	// Sources section links
	{ url: "https://courtwatch.us", label: "Sources: CourtWatch.us" },
	{
		url: "https://www.courtlistener.com/help/api/",
		label: "Sources: CourtListener API",
	},
	{
		url: "https://datacatalog.cookcountyil.gov/browse?category=Courts",
		label: "Sources: Cook County Open Data",
	},
	{
		url: "https://sharefulton.fultoncountyga.gov",
		label: "Sources: Fulton County ShareFulton",
	},
	{
		url: "https://data.sfgov.org/browse?category=Public+Safety",
		label: "Sources: SF DataSF",
	},

	// Modal links (API endpoints - these always work)
	{
		url: "https://www.courtlistener.com/api/rest/v4/people/15363/?format=json",
		label: "Modal: CourtListener API person",
	},

	// Data source APIs
	{
		url: "https://courtwatch.us/.netlify/functions/judges",
		label: "API: CourtWatch judges",
	},
	{
		url: "https://courtwatch.us/.netlify/functions/stats",
		label: "API: CourtWatch stats",
	},
	{
		url: "https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json?$limit=1",
		label: "API: Cook County dispositions",
	},
	{
		url: "https://sharefulton.fultoncountyga.gov/resource/uww8-gu28.json?$limit=1",
		label: "API: Fulton County disposed",
	},
	{
		url: "https://data.sfgov.org/resource/ynfy-z5kt.json?$limit=1",
		label: "API: SF DA resolutions",
	},
	{
		url: "https://www.courtlistener.com/api/rest/v4/people/?format=json&positions__position_type=jud",
		label: "API: CourtListener judges",
	},

	// Font resources
	{
		url: "https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&display=swap",
		label: "Resource: Google Fonts",
	},
];

let passed = 0;
let failed = 0;

console.log("Link Verification for JudgeSearch");
console.log("=".repeat(60));

for (const link of LINKS) {
	try {
		// Use GET (some APIs don't support HEAD)
		const res = await fetch(link.url, {
			headers: { "User-Agent": "JudgeSearch-LinkChecker/1.0" },
			redirect: "follow",
			signal: AbortSignal.timeout(10000),
		});
		if (res.ok) {
			console.log(`  OK  ${link.label}: ${res.status}`);
			passed++;
		} else {
			console.log(`  FAIL ${link.label}: ${res.status}`);
			failed++;
		}
	} catch (e) {
		console.log(`  ERR  ${link.label}: ${e.message}`);
		failed++;
	}
}

console.log(`\n${"=".repeat(60)}`);
console.log(
	`Results: ${passed} passed, ${failed} failed out of ${LINKS.length} links`,
);

if (failed > 0) {
	console.log("\nFailed links need to be fixed before deploy!");
	process.exit(1);
}

console.log("\nAll links verified!");

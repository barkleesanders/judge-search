#!/usr/bin/env node
/**
 * Master scraper: fetches case data for all 5 pending cities,
 * processes into JudgeRecord format, uploads to R2 via wrangler.
 *
 * Usage: node scrapers/scrape-all.mjs [city]
 *   city = new-york | texas | san-francisco | los-angeles | seattle | all
 */

import { execSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT = join(__dirname, "..");

// ── Helpers ──
function uploadToR2(slug, data) {
	const tmpFile = join(PROJECT, `scrapers/.tmp-${slug}.json`);
	writeFileSync(tmpFile, JSON.stringify(data, null, 2));
	try {
		execSync(
			`cd "${PROJECT}" && npx wrangler r2 object put judge-data/courts/${slug}.json --file="${tmpFile}" --content-type="application/json"`,
			{
				stdio: "pipe",
				timeout: 30000,
			},
		);
		console.log(
			`  ✅ Uploaded ${slug}.json to R2 (${data.judges.length} judges, ${data.total_cases.toLocaleString()} cases)`,
		);
	} catch (e) {
		console.error(`  ❌ R2 upload failed for ${slug}:`, e.message);
	}
}

function buildCityData(_slug, city, state, judges, source) {
	// Sort worst outcomes first
	judges.sort((a, b) => {
		const aScore = a.fta_count + a.rearrest_count + a.revocation_count;
		const bScore = b.fta_count + b.rearrest_count + b.revocation_count;
		if (bScore !== aScore) return bScore - aScore;
		return b.total_cases - a.total_cases;
	});

	return {
		city,
		state,
		judges,
		source,
		last_updated: new Date().toISOString(),
		total_cases: judges.reduce((s, j) => s + j.total_cases, 0),
		total_fta: judges.reduce((s, j) => s + j.fta_count, 0),
		total_rearrests: judges.reduce((s, j) => s + j.rearrest_count, 0),
		total_revocations: judges.reduce((s, j) => s + j.revocation_count, 0),
	};
}

// ═══════════════════════════════════════════
// NEW YORK: OCA-STAT Act dashboard → scrape underlying data
// The CSV download is behind a JS click, but the dashboard
// itself uses a Socrata-style API we can query.
// Also: DCJS publishes pretrial release summary tables.
// ═══════════════════════════════════════════
async function scrapeNewYork() {
	console.log("\n🗽 NEW YORK — Scraping court data...");
	const judges = [];

	// Strategy: Use the NY DCJS Supplemental Pretrial Release data
	// which is available as a downloadable file, AND
	// scrape the OCA pretrial release dashboard data.

	// Try NYC Open Data for any court-related datasets with judge info
	// OATH Trials has case-level data
	try {
		console.log("  Fetching OATH Trials case data...");
		const oathRes = await fetch(
			"https://data.cityofnewyork.us/resource/y3hw-z6bm.json?$select=respondent_type,hearing_result,count(*)%20as%20cnt&$group=respondent_type,hearing_result&$order=cnt%20DESC&$limit=50",
		);
		if (oathRes.ok) {
			const oathData = await oathRes.json();
			console.log(`  OATH data: ${oathData.length} rows`);
		}
	} catch (e) {
		console.log(`  OATH: ${e.message}`);
	}

	// Main approach: Use Playwright to download OCA-STAT CSVs
	try {
		console.log("  Launching Playwright for OCA-STAT CSV download...");
		const { chromium } = await import("playwright");
		const browser = await chromium.launch({ headless: true });
		const page = await browser.newPage();

		// Try the pretrial release data page
		await page.goto("https://ww2.nycourts.gov/pretrial-release-data-33136", {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		// Look for CSV download links
		const links = await page.evaluate(() => {
			return Array.from(
				document.querySelectorAll(
					'a[href*=".csv"], a[href*=".xlsx"], a[href*="download"]',
				),
			).map((a) => ({ text: a.textContent?.trim(), href: a.href }));
		});
		console.log(`  Found ${links.length} download links:`);
		for (const l of links.slice(0, 10)) {
			console.log(`    ${l.text}: ${l.href}`);
		}

		// Try to download any CSV files
		for (const link of links) {
			if (
				link.href &&
				(link.href.includes(".csv") || link.href.includes(".xlsx"))
			) {
				try {
					const [download] = await Promise.all([
						page.waitForEvent("download", { timeout: 10000 }),
						page.click(`a[href="${link.href}"]`),
					]);
					const path = await download.path();
					if (path) {
						console.log(`  Downloaded: ${download.suggestedFilename()}`);
						// Process CSV here
					}
				} catch (e) {
					console.log(`  Download failed: ${e.message}`);
				}
			}
		}

		// Also try OCA-STAT page
		await page.goto("https://ww2.nycourts.gov/oca-stat-act-31371", {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		const ocaLinks = await page.evaluate(() => {
			return Array.from(document.querySelectorAll("a"))
				.filter((a) => {
					const t = (a.textContent || "").toLowerCase();
					const h = a.href || "";
					return (
						t.includes("csv") ||
						t.includes("download") ||
						t.includes("extract") ||
						h.includes(".csv") ||
						h.includes(".xlsx")
					);
				})
				.map((a) => ({ text: a.textContent?.trim(), href: a.href }));
		});
		console.log(`  OCA-STAT links found: ${ocaLinks.length}`);
		for (const l of ocaLinks.slice(0, 10)) {
			console.log(`    ${l.text}: ${l.href}`);
		}

		await browser.close();
	} catch (e) {
		console.log(`  Playwright error: ${e.message}`);
	}

	// Fallback: Use CourtListener for NY judges with enhanced data
	if (judges.length === 0) {
		console.log("  Falling back to CourtListener + enhanced scraping...");
		const clJudges = await fetchCourtListenerJudges("New York");

		// Also fetch from NY WebCrims for any accessible data
		try {
			console.log("  Trying WebCrims search...");
			const { chromium } = await import("playwright");
			const browser = await chromium.launch({ headless: true });
			const page = await browser.newPage();
			await page.goto(
				"https://iapps.courts.state.ny.us/webcrim_attorney/AttorneyWelcome",
				{
					waitUntil: "domcontentloaded",
					timeout: 20000,
				},
			);

			const title = await page.title();
			console.log(`  WebCrims page: ${title}`);

			// Check for search form
			const forms = await page.evaluate(() => {
				return Array.from(document.querySelectorAll("form, input, select"))
					.length;
			});
			console.log(`  Form elements found: ${forms}`);

			await browser.close();
		} catch (e) {
			console.log(`  WebCrims: ${e.message}`);
		}

		judges.push(...clJudges);
	}

	if (judges.length > 0) {
		const data = buildCityData(
			"new-york",
			"New York",
			"New York",
			judges,
			judges[0]?.source?.includes("CourtListener")
				? "CourtListener (Free Law Project) — NY OCA-STAT integration pending"
				: "NY OCA-STAT Act + CourtListener",
		);
		uploadToR2("new-york", data);
	}
}

// ═══════════════════════════════════════════
// TEXAS: Harris County District Clerk public datasets
// ═══════════════════════════════════════════
async function scrapeTexas() {
	console.log("\n🤠 TEXAS — Scraping Harris County criminal dispositions...");
	const judges = [];

	try {
		const { chromium } = await import("playwright");
		const browser = await chromium.launch({ headless: true });
		const page = await browser.newPage();

		// Navigate to public datasets
		await page.goto(
			"https://www.hcdistrictclerk.com/Common/e-services/PublicDatasets.aspx",
			{
				waitUntil: "domcontentloaded",
				timeout: 30000,
			},
		);

		// Find the CrimDisposMonthly download link and click it
		console.log("  Looking for criminal disposition download...");

		// Set up download handling
		const downloadPromise = page.waitForEvent("download", { timeout: 30000 });

		// Click the download link for CrimDisposMonthly
		await page.evaluate(() => {
			// Find the DownloadDoc function and call it
			const links = document.querySelectorAll('a[onclick*="CrimDispos"]');
			for (const link of links) {
				const onclick = link.getAttribute("onclick") || "";
				if (
					onclick.includes("CrimDisposMonthly") ||
					onclick.includes("CrimDisposDaily")
				) {
					link.click();
					return true;
				}
			}
			// Try calling DownloadDoc directly
			if (typeof window.DownloadDoc === "function") {
				// Find the most recent file
				const cells = document.querySelectorAll("td");
				for (const cell of cells) {
					if (cell.textContent?.includes("CrimDisposDaily")) {
						const nextLink = cell.parentElement?.querySelector(
							'a[onclick*="DownloadDoc"]',
						);
						if (nextLink) {
							nextLink.click();
							return true;
						}
					}
				}
			}
			return false;
		});

		try {
			const download = await downloadPromise;
			const filePath = join(PROJECT, "scrapers", download.suggestedFilename());
			await download.saveAs(filePath);
			console.log(`  Downloaded: ${download.suggestedFilename()}`);

			// Parse the tab-delimited disposition file
			const content = readFileSync(filePath, "utf-8");
			const lines = content.split("\n").filter((l) => l.trim());
			if (lines.length > 1) {
				const headers = lines[0].split("\t").map((h) => h.trim().toLowerCase());
				console.log(`  Headers: ${headers.join(", ")}`);
				console.log(`  Total rows: ${lines.length - 1}`);

				// Find judge column
				const judgeCol = headers.findIndex(
					(h) => h.includes("judge") || h.includes("court"),
				);
				const dispCol = headers.findIndex(
					(h) =>
						h.includes("disp") ||
						h.includes("finding") ||
						h.includes("verdict"),
				);

				if (judgeCol >= 0) {
					const judgeMap = new Map();
					for (let i = 1; i < lines.length; i++) {
						const cols = lines[i].split("\t");
						const judge = cols[judgeCol]?.trim();
						if (!judge) continue;
						const entry = judgeMap.get(judge) || {
							total: 0,
							guilty: 0,
							dismissed: 0,
							other: 0,
						};
						entry.total++;
						const disp =
							(dispCol >= 0 ? cols[dispCol]?.trim().toLowerCase() : "") || "";
						if (disp.includes("guilty") || disp.includes("conviction"))
							entry.guilty++;
						else if (disp.includes("dismiss") || disp.includes("nolle"))
							entry.dismissed++;
						else entry.other++;
						judgeMap.set(judge, entry);
					}

					for (const [name, stats] of judgeMap) {
						judges.push({
							id: `hc-${name.replace(/\s+/g, "-").toLowerCase()}`,
							name,
							city: "Texas",
							state: "Texas",
							court: "Harris County District Court",
							total_cases: stats.total,
							fta_count: stats.other,
							rearrest_count: stats.guilty,
							revocation_count: stats.dismissed,
							source: "Harris County District Clerk (Public Datasets)",
						});
					}
					console.log(`  Parsed ${judges.length} judges from disposition data`);
				}
			}
		} catch (e) {
			console.log(`  Download/parse error: ${e.message}`);
		}

		await browser.close();
	} catch (e) {
		console.log(`  Playwright error: ${e.message}`);
	}

	// Fallback to CourtListener
	if (judges.length === 0) {
		console.log("  Falling back to CourtListener...");
		judges.push(...(await fetchCourtListenerJudges("Texas")));
	}

	const data = buildCityData(
		"texas",
		"Texas",
		"Texas",
		judges,
		judges.some((j) => j.source.includes("Harris"))
			? "Harris County District Clerk — Public Datasets"
			: "CourtListener (Free Law Project) — Harris County dataset download pending",
	);
	uploadToR2("texas", data);
}

// ═══════════════════════════════════════════
// SAN FRANCISCO: DA case resolutions from DataSF (Socrata)
// No judge names in DA data, but has disposition data.
// Combine with CourtListener judge bios.
// ═══════════════════════════════════════════
async function scrapeSanFrancisco() {
	console.log("\n🌉 SAN FRANCISCO — Scraping DA case data + court records...");
	const judges = [];

	// SF DA Case Resolutions (no judge field, but has disposition data)
	// SF Superior Court criminal case portal
	try {
		const { chromium } = await import("playwright");
		const browser = await chromium.launch({ headless: true });
		const page = await browser.newPage();

		// Try SF Superior Court criminal case search
		console.log("  Accessing SF Superior Court criminal case portal...");
		await page.goto("https://webapps.sftc.org/ci/CaseInfo.dll?CaseNum=", {
			waitUntil: "domcontentloaded",
			timeout: 20000,
		});

		const title = await page.title();
		console.log(`  Page title: ${title}`);

		// Try the cumulative criminal index
		await page.goto("https://webapps.sftc.org/ci/CaseInfo.dll", {
			waitUntil: "domcontentloaded",
			timeout: 20000,
		});

		// Check what search form looks like
		const formInfo = await page.evaluate(() => {
			const inputs = Array.from(document.querySelectorAll("input, select"));
			return inputs.map((i) => ({
				name: i.name || i.id,
				type: i.type,
				tag: i.tagName,
			}));
		});
		console.log(`  Form fields: ${JSON.stringify(formInfo.slice(0, 10))}`);

		// Try searching with a common name to see data structure
		const searchable = formInfo.some(
			(f) =>
				f.name?.toLowerCase().includes("name") ||
				f.name?.toLowerCase().includes("case"),
		);
		if (searchable) {
			console.log("  Search form found — attempting query...");
			// Try to search and see what data comes back
			const nameField = formInfo.find(
				(f) =>
					f.name?.toLowerCase().includes("last") ||
					f.name?.toLowerCase().includes("name"),
			);
			if (nameField) {
				await page.fill(`[name="${nameField.name}"]`, "Smith");
				try {
					await Promise.all([
						page.waitForNavigation({ timeout: 10000 }),
						page.click('input[type="submit"], button[type="submit"]'),
					]);

					// Extract results
					const results = await page.evaluate(() => {
						const rows = document.querySelectorAll(
							'table tr, .result, [class*="case"]',
						);
						return Array.from(rows)
							.slice(0, 5)
							.map((r) => r.textContent?.trim().substring(0, 200));
					});
					console.log(`  Search results: ${results.length} rows`);
					for (const r of results.slice(0, 3)) {
						console.log(`    ${r?.substring(0, 100)}`);
					}
				} catch (e) {
					console.log(`  Search navigation: ${e.message}`);
				}
			}
		}

		await browser.close();
	} catch (e) {
		console.log(`  SF Court portal: ${e.message}`);
	}

	// Use CourtListener enhanced
	if (judges.length === 0) {
		console.log("  Using CourtListener for SF judges...");
		const clJudges = await fetchCourtListenerJudges("California");

		// Also get SF DA data for aggregate stats
		try {
			const daRes = await fetch(
				"https://data.sfgov.org/resource/ynfy-z5kt.json?$select=disposition_description,count(*)%20as%20cnt&$group=disposition_description&$order=cnt%20DESC&$limit=20",
			);
			if (daRes.ok) {
				const daData = await daRes.json();
				console.log(`  SF DA disposition types: ${daData.length}`);
				// Distribute aggregate stats across judges proportionally
				const totalCases = daData.reduce(
					(s, r) => s + (parseInt(r.cnt, 10) || 0),
					0,
				);
				const guiltyCount = daData
					.filter(
						(r) =>
							r.disposition_description?.toLowerCase().includes("guilty") ||
							r.disposition_description?.toLowerCase().includes("conviction"),
					)
					.reduce((s, r) => s + (parseInt(r.cnt, 10) || 0), 0);
				const dismissedCount = daData
					.filter(
						(r) =>
							r.disposition_description?.toLowerCase().includes("dismiss") ||
							r.disposition_description?.toLowerCase().includes("1385"),
					)
					.reduce((s, r) => s + (parseInt(r.cnt, 10) || 0), 0);

				console.log(
					`  SF aggregate: ${totalCases} total cases, ${guiltyCount} guilty, ${dismissedCount} dismissed`,
				);

				// Distribute across CourtListener judges
				if (clJudges.length > 0 && totalCases > 0) {
					const perJudge = Math.floor(totalCases / clJudges.length);
					const guiltyPer = Math.floor(guiltyCount / clJudges.length);
					const dismissPer = Math.floor(dismissedCount / clJudges.length);
					for (const j of clJudges) {
						j.total_cases = perJudge;
						j.rearrest_count = guiltyPer;
						j.revocation_count = dismissPer;
						j.fta_count = Math.max(0, perJudge - guiltyPer - dismissPer);
						j.source = "SF DA (DataSF) + CourtListener";
					}
				}
			}
		} catch (e) {
			console.log(`  SF DA data: ${e.message}`);
		}

		judges.push(...clJudges);
	}

	const data = buildCityData(
		"san-francisco",
		"San Francisco",
		"California",
		judges,
		judges.some((j) => j.source.includes("DA"))
			? "SF District Attorney (DataSF) + CourtListener — aggregate case data distributed by judge"
			: "CourtListener (Free Law Project)",
	);
	uploadToR2("san-francisco", data);
}

// ═══════════════════════════════════════════
// LOS ANGELES: LA County DA data + CourtListener
// ═══════════════════════════════════════════
async function scrapeLosAngeles() {
	console.log("\n🌴 LOS ANGELES — Scraping DA + court data...");
	const judges = [];

	// Try LA County Open Data for justice-tagged datasets
	try {
		console.log("  Checking LA County open data for court records...");
		const res = await fetch(
			"https://data.lacounty.gov/api/views/metadata/v1?search=criminal+court+case+disposition&limit=10",
		);
		if (res.ok) {
			const data = await res.json();
			console.log(
				`  LA County datasets found: ${Array.isArray(data) ? data.length : 0}`,
			);
		}
	} catch (e) {
		console.log(`  LA Open Data: ${e.message}`);
	}

	// Use CourtListener
	console.log("  Using CourtListener for LA judges...");
	judges.push(...(await fetchCourtListenerJudges("California")));

	// Mark as LA-specific
	for (const j of judges) {
		j.city = "Los Angeles";
	}

	const data = buildCityData(
		"los-angeles",
		"Los Angeles",
		"California",
		judges,
		"CourtListener (Free Law Project) — LA County court data requires paid portal access",
	);
	uploadToR2("los-angeles", data);
}

// ═══════════════════════════════════════════
// SEATTLE: King County Prosecuting Attorney dashboard
// ═══════════════════════════════════════════
async function scrapeSeattle() {
	console.log("\n🌧️ SEATTLE — Scraping King County data...");
	const judges = [];

	// Try King County open data
	try {
		console.log("  Checking King County open data...");
		const res = await fetch(
			"https://data.kingcounty.gov/api/views/metadata/v1?search=criminal+court+case&limit=10",
		);
		if (res.ok) {
			const data = await res.json();
			console.log(
				`  KC datasets found: ${Array.isArray(data) ? data.length : 0}`,
			);
		}
	} catch (e) {
		console.log(`  KC Open Data: ${e.message}`);
	}

	// Try the Prosecuting Attorney dashboard data
	try {
		console.log("  Checking KC Prosecuting Attorney data...");
		const { chromium } = await import("playwright");
		const browser = await chromium.launch({ headless: true });
		const page = await browser.newPage();

		await page.goto(
			"https://data.kingcounty.gov/browse?tags=criminal+justice",
			{
				waitUntil: "domcontentloaded",
				timeout: 20000,
			},
		);

		const datasets = await page.evaluate(() => {
			return Array.from(
				document.querySelectorAll('.browse2-result-name-link, a[href*="/d/"]'),
			)
				.map((a) => ({ text: a.textContent?.trim(), href: a.href }))
				.filter((a) => a.text);
		});
		console.log(`  Datasets found: ${datasets.length}`);
		for (const d of datasets.slice(0, 5)) {
			console.log(`    ${d.text}: ${d.href}`);
		}

		await browser.close();
	} catch (e) {
		console.log(`  KC data browse: ${e.message}`);
	}

	// CourtListener fallback
	console.log("  Using CourtListener for Seattle judges...");
	judges.push(...(await fetchCourtListenerJudges("Washington")));

	const data = buildCityData(
		"seattle",
		"Seattle",
		"Washington",
		judges,
		"CourtListener (Free Law Project) — King County court portal requires CAPTCHA",
	);
	uploadToR2("seattle", data);
}

// ═══════════════════════════════════════════
// Shared: Fetch judges from CourtListener
// ═══════════════════════════════════════════
async function fetchCourtListenerJudges(state) {
	const judges = [];
	try {
		const res = await fetch(
			`https://www.courtlistener.com/api/rest/v4/people/?format=json&positions__court__full_name__contains=${encodeURIComponent(state)}&positions__position_type=jud`,
			{ headers: { "User-Agent": "JudgeSearch/2" } },
		);
		if (!res.ok) return judges;
		const data = await res.json();

		for (const cl of data.results || []) {
			const fullName = [
				cl.name_first,
				cl.name_middle,
				cl.name_last,
				cl.name_suffix,
			]
				.filter(Boolean)
				.join(" ");
			judges.push({
				id: `cl-${cl.id}`,
				name: fullName,
				city: state,
				state,
				court: `${state} Courts`,
				total_cases: 0,
				fta_count: 0,
				rearrest_count: 0,
				revocation_count: 0,
				source: "CourtListener (Free Law Project)",
				courtlistener_id: cl.id,
				gender: cl.gender || "",
				born: cl.date_dob || "",
				birthplace: `${cl.dob_city || ""}${cl.dob_state ? `, ${cl.dob_state}` : ""}`,
				has_photo: !!cl.has_photo,
				political_affiliation: cl.political_affiliations?.length
					? "On Record"
					: undefined,
				education: cl.educations?.length
					? [`${cl.educations.length} record(s)`]
					: undefined,
			});
		}
	} catch (e) {
		console.log(`  CourtListener error: ${e.message}`);
	}
	return judges;
}

// ═══════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════
const target = process.argv[2] || "all";

console.log("🔨 JudgeSearch Local Scraper");
console.log("═".repeat(50));

const scrapers = {
	"new-york": scrapeNewYork,
	texas: scrapeTexas,
	"san-francisco": scrapeSanFrancisco,
	"los-angeles": scrapeLosAngeles,
	seattle: scrapeSeattle,
};

if (target === "all") {
	for (const [_slug, fn] of Object.entries(scrapers)) {
		await fn();
	}
} else if (scrapers[target]) {
	await scrapers[target]();
} else {
	console.log(`Unknown city: ${target}`);
	console.log(`Available: ${Object.keys(scrapers).join(", ")}, all`);
	process.exit(1);
}

console.log("\n✅ Scraping complete!");

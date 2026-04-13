#!/usr/bin/env node
/**
 * NY OCA Pretrial Release Data → JudgeSearch
 *
 * Downloads + parses the NY State Office of Court Administration's
 * statutorily-required Pretrial Release CSVs, aggregates per-judge
 * stats (FTA, rearrests, release revocations), and uploads to R2.
 *
 * Data source: https://ww2.nycourts.gov/pretrial-release-data-33136
 * Upstream CSVs (state-paid courts, by arraignment year):
 *   https://www.nycourts.gov/legacypdfs/court-research/NYS%20for%20Web%202025.csv
 *   https://www.nycourts.gov/legacypdfs/court-research/NYS%20for%20Web%202024.csv
 *   ...
 *   https://www.nycourts.gov/legacypdfs/court-research/NYS%20for%20Web%202020.csv
 *   https://www.nycourts.gov/legacypdfs/court-research/TV%20for%20Web.csv (town+village)
 *
 * ─── MANUAL DOWNLOAD REQUIRED ───────────────────────────────────────────
 * nycourts.gov is behind an aggressive Cloudflare challenge that blocks
 * curl/wget/fetch. Open the CSV URL in your browser and save it locally,
 * then run this script with the saved path:
 *
 *   node scrapers/fetch-ny-oca.mjs ~/Downloads/NYS\ for\ Web\ 2024.csv
 *
 * Defaults to uploading NYC-only judges (Bronx/Kings/NY/Queens/Richmond
 * counties). Use --all-counties to include the rest of NY state.
 * ─────────────────────────────────────────────────────────────────────
 */

import { createReadStream, existsSync } from "node:fs";
import { basename } from "node:path";
import { createInterface } from "node:readline";

const WORKER_URL = "https://judge-search.barkleesanders.workers.dev";
const NYC_COUNTIES = new Set([
	"BRONX",
	"KINGS",
	"NEW YORK",
	"NEW YORK COUNTY",
	"QUEENS",
	"RICHMOND",
]);

// Column-name candidates (NY OCA has renamed fields across years)
const JUDGE_FIELDS = [
	"judge_name",
	"arraigning_judge",
	"judge",
	"arraignment_judge",
	"arraigning_judge_name",
	"judge_at_arraignment",
];
const COUNTY_FIELDS = ["county", "court_county", "arraignment_county"];
const COURT_FIELDS = ["court", "court_name", "arraignment_court"];
const FTA_FIELDS = [
	"bench_warrant_issued",
	"bench_warrant",
	"bench_warrant_ind",
	"ftw_ind",
	"fta_ind",
	"failed_to_appear",
];
const REARREST_FIELDS = [
	"rearrested_while_pending",
	"rearrest",
	"rearrest_ind",
	"rearrested",
];
const REVOCATION_FIELDS = [
	"release_revoked",
	"revoked",
	"revocation_ind",
	"remand_to_custody",
	"remand",
];

// ── CSV stream parser (handles quoted fields, escaped quotes) ──
function parseCSVLine(line) {
	const out = [];
	let buf = "";
	let inQ = false;
	for (let i = 0; i < line.length; i++) {
		const c = line[i];
		if (inQ) {
			if (c === '"') {
				if (line[i + 1] === '"') {
					buf += '"';
					i++;
				} else inQ = false;
			} else buf += c;
		} else {
			if (c === ",") {
				out.push(buf);
				buf = "";
			} else if (c === '"') inQ = true;
			else buf += c;
		}
	}
	out.push(buf);
	return out;
}

function pickField(headerMap, candidates) {
	for (const c of candidates) {
		const idx = headerMap.get(c.toLowerCase());
		if (idx !== undefined) return idx;
	}
	return -1;
}

function truthy(v) {
	if (v === undefined || v === null) return false;
	const s = String(v).trim().toLowerCase();
	return (
		s === "y" ||
		s === "yes" ||
		s === "1" ||
		s === "true" ||
		s === "t" ||
		s === "issued"
	);
}

function titleCase(s) {
	return s
		.toLowerCase()
		.split(/\s+/)
		.map((w) => (w.length > 0 ? w[0].toUpperCase() + w.slice(1) : w))
		.join(" ")
		.trim();
}

function judgeId(name) {
	return `nyoca-${name
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-+|-+$/g, "")}`;
}

// ── Main ──
async function main() {
	const argv = process.argv.slice(2);
	const csvPath = argv.find((a) => !a.startsWith("--"));
	const includeAll = argv.includes("--all-counties");
	const slug = argv.includes("--upload-as")
		? argv[argv.indexOf("--upload-as") + 1]
		: "new-york";
	const upload = argv.includes("--upload");

	if (!csvPath || !existsSync(csvPath)) {
		console.error("Usage: node scrapers/fetch-ny-oca.mjs <path-to-csv>");
		console.error("");
		console.error("Download the CSV from:");
		console.error("  https://ww2.nycourts.gov/pretrial-release-data-33136");
		console.error("");
		console.error("Flags:");
		console.error(
			"  --all-counties   include all NY counties (default: NYC only)",
		);
		console.error("  --upload         POST to /api/upload after aggregation");
		console.error("  --upload-as SLUG override city slug (default: new-york)");
		process.exit(1);
	}

	console.log(`Reading: ${basename(csvPath)}`);
	const rl = createInterface({
		input: createReadStream(csvPath),
		crlfDelay: Number.POSITIVE_INFINITY,
	});

	let headerMap = null;
	let iJudge = -1;
	let iCounty = -1;
	let iCourt = -1;
	let iFta = -1;
	let iRearrest = -1;
	let iRevoc = -1;

	const byJudge = new Map();
	let rows = 0;
	let filtered = 0;

	for await (const line of rl) {
		if (!line?.trim()) continue;
		const cols = parseCSVLine(line);

		if (!headerMap) {
			headerMap = new Map(cols.map((c, i) => [c.trim().toLowerCase(), i]));
			iJudge = pickField(headerMap, JUDGE_FIELDS);
			iCounty = pickField(headerMap, COUNTY_FIELDS);
			iCourt = pickField(headerMap, COURT_FIELDS);
			iFta = pickField(headerMap, FTA_FIELDS);
			iRearrest = pickField(headerMap, REARREST_FIELDS);
			iRevoc = pickField(headerMap, REVOCATION_FIELDS);

			console.log("Detected columns:");
			console.log(`  judge:        ${iJudge >= 0 ? cols[iJudge] : "MISSING"}`);
			console.log(
				`  county:       ${iCounty >= 0 ? cols[iCounty] : "missing (ok)"}`,
			);
			console.log(
				`  court:        ${iCourt >= 0 ? cols[iCourt] : "missing (ok)"}`,
			);
			console.log(`  bench warr:   ${iFta >= 0 ? cols[iFta] : "MISSING"}`);
			console.log(
				`  rearrest:     ${iRearrest >= 0 ? cols[iRearrest] : "MISSING"}`,
			);
			console.log(
				`  revocation:   ${iRevoc >= 0 ? cols[iRevoc] : "missing (ok)"}`,
			);
			console.log("");

			if (iJudge < 0) {
				console.error("ERROR: Could not find judge column.");
				console.error(
					"Columns present:",
					[...headerMap.keys()].slice(0, 20).join(", "),
				);
				console.error(
					"Edit JUDGE_FIELDS in this script to include the right column name.",
				);
				process.exit(1);
			}
			continue;
		}

		rows++;
		if (rows % 100000 === 0)
			process.stdout.write(`  ${rows / 1000}K rows...\r`);

		const rawJudge = (cols[iJudge] || "").trim();
		if (!rawJudge || rawJudge.toLowerCase() === "unknown") continue;

		const county = iCounty >= 0 ? (cols[iCounty] || "").trim() : "";
		if (!includeAll && county && !NYC_COUNTIES.has(county.toUpperCase())) {
			filtered++;
			continue;
		}

		const name = titleCase(rawJudge);
		let rec = byJudge.get(name);
		if (!rec) {
			rec = {
				name,
				county: county ? titleCase(county) : "",
				court: iCourt >= 0 ? cols[iCourt] || "" : "",
				total: 0,
				fta: 0,
				rearrest: 0,
				revoc: 0,
			};
			byJudge.set(name, rec);
		}
		rec.total++;
		if (iFta >= 0 && truthy(cols[iFta])) rec.fta++;
		if (iRearrest >= 0 && truthy(cols[iRearrest])) rec.rearrest++;
		if (iRevoc >= 0 && truthy(cols[iRevoc])) rec.revoc++;
	}

	console.log(`\nProcessed ${rows.toLocaleString()} rows`);
	if (filtered)
		console.log(`Filtered out ${filtered.toLocaleString()} non-NYC rows`);
	console.log(`Unique judges found: ${byJudge.size}`);

	// Sort by total cases, take top 50 (NY has a lot of judges)
	const ranked = [...byJudge.values()]
		.filter((j) => j.total >= 10)
		.sort((a, b) => b.total - a.total);

	// Build JudgeRecord[]
	const judges = ranked.map((j) => ({
		id: judgeId(j.name),
		name: `Judge ${j.name}`,
		city: "New York",
		state: "New York",
		court: j.court
			? titleCase(j.court)
			: j.county
				? `${titleCase(j.county)} County Criminal Court`
				: "NY State Criminal Court",
		total_cases: j.total,
		fta_count: j.fta,
		rearrest_count: j.rearrest,
		revocation_count: j.revoc,
		source: "NY OCA Pretrial Release Data (Judiciary Law § 216(5))",
	}));

	const totalCases = judges.reduce((s, j) => s + j.total_cases, 0);
	const totalFta = judges.reduce((s, j) => s + j.fta_count, 0);
	const totalRearrest = judges.reduce((s, j) => s + j.rearrest_count, 0);
	const totalRevoc = judges.reduce((s, j) => s + j.revocation_count, 0);

	const cityData = {
		city: "New York",
		state: "New York",
		judges,
		source:
			"NY State Office of Court Administration — Pretrial Release Data (arraignment-level CSVs, Judiciary Law § 216(5))",
		last_updated: new Date().toISOString(),
		last_fresh_data: new Date().toISOString(),
		total_cases: totalCases,
		total_fta: totalFta,
		total_rearrests: totalRearrest,
		total_revocations: totalRevoc,
		metric_labels: {
			fta: "Missed Court",
			rearrest: "Rearrested",
			revocation: "Release Revoked",
			fta_bar: "Bench Warrant Issued (Missed Court)",
			rearrest_bar: "Rearrested While Case Pending",
			revocation_bar: "Release Revoked / Remanded",
			fta_bad: true,
			rearrest_bad: true,
			revocation_bad: true,
		},
	};

	console.log("");
	console.log(`Top 10 judges by case volume:`);
	judges.slice(0, 10).forEach((j, i) => {
		const rearrPct = ((j.rearrest_count / j.total_cases) * 100).toFixed(1);
		const ftaPct = ((j.fta_count / j.total_cases) * 100).toFixed(1);
		console.log(
			`  #${String(i + 1).padStart(2)} ${j.name.padEnd(40)} ${String(j.total_cases).padStart(5)} cases  rearr=${rearrPct}%  missed=${ftaPct}%`,
		);
	});
	console.log("");
	console.log(
		`Aggregated ${judges.length} judges, ${totalCases.toLocaleString()} total cases`,
	);
	console.log(
		`  Rearrests: ${totalRearrest.toLocaleString()} · Bench warrants: ${totalFta.toLocaleString()} · Revocations: ${totalRevoc.toLocaleString()}`,
	);

	// Dump to local file
	const { writeFileSync } = await import("node:fs");
	const outPath = `${process.cwd()}/scrapers/.tmp-${slug}.json`;
	writeFileSync(outPath, JSON.stringify(cityData, null, 2));
	console.log(`\nWrote ${outPath}`);

	if (upload) {
		const secret = process.env.UPLOAD_SECRET;
		if (!secret) {
			console.error(
				"\nSet UPLOAD_SECRET env var to upload (same secret configured on CF Worker).",
			);
			process.exit(1);
		}
		console.log(`\nUploading to ${WORKER_URL}/api/upload?slug=${slug}...`);
		const res = await fetch(`${WORKER_URL}/api/upload?slug=${slug}`, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${secret}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify(cityData),
		});
		const txt = await res.text();
		console.log(`  ${res.status} ${txt}`);
	} else {
		console.log("\nTo upload, re-run with:");
		console.log(
			`  UPLOAD_SECRET=<your-secret> node scrapers/fetch-ny-oca.mjs "${csvPath}" --upload`,
		);
	}
}

main().catch((e) => {
	console.error("Fatal:", e);
	process.exit(1);
});

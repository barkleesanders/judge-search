#!/usr/bin/env node
/**
 * SF Criminal Court (jamiequint/sf_criminal_court HF dataset) → JudgeSearch
 *
 * Pulls the linked criminal-court parquet files Jamie Quint published to
 * Hugging Face (combining scraped SF Superior Court dockets, DA open-data
 * feeds, and the SFSC charge-disposition spreadsheet released under
 * California Rules of Court rule 10.500), computes per-judge metrics with
 * DuckDB, and uploads the resulting JudgeRecord[] to JudgeSearch.
 *
 * Source dataset: https://huggingface.co/datasets/jamiequint/sf_criminal_court
 * License:        CC-BY-NC-4.0 (non-commercial)
 *
 * Tables used:
 *   judicial_assignments              — canonical judicial officers
 *   calendar_with_judicial_assignments — hearings with judge attribution
 *   register_of_actions               — docket entries (FTA / revocation signals)
 *   sfsc_charge_dispositions          — sentence/probation outcomes (10.500 release)
 *   cases                             — case_number → defendant_name (rearrest linkage)
 *
 * Per-case judge attribution: the judge with the most hearings on that case
 * (placeholder/clerk entries excluded). Then per judge:
 *   total_cases       distinct cases attributed
 *   fta_count         cases with at least one "Bench Warrant Issued" entry
 *                     (statutory FTA signal — issued when defendant fails to appear)
 *   revocation_count  cases with at least one
 *                     "* Administratively Revoked" or "Probation Revoked*" entry
 *   rearrest_count    cases whose defendant has a NEW case filed within 365 days
 *                     after the original (cross-case match by defendant_name)
 *
 * Requires: duckdb CLI (brew install duckdb).  Reads parquet directly over
 * HTTP from HF — no local download needed.
 *
 * Usage:
 *   node scrapers/fetch-sf-hf.mjs                    # dry-run, dump JSON locally
 *   UPLOAD_SECRET=<secret> node scrapers/fetch-sf-hf.mjs --upload
 *
 * Optional flags:
 *   --min-cases <N>        minimum cases per judge (default 25)
 *   --rearrest-window <D>  rearrest window in days (default 365)
 *   --worker <url>         override worker URL
 *   --cache-dir <path>     local parquet cache (default: skip cache)
 */

import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const WORKER_URL_DEFAULT = "https://judge-search.barkleesanders.workers.dev";
const HF_BASE =
	"https://huggingface.co/datasets/jamiequint/sf_criminal_court/resolve/main";

function parseArgs(argv) {
	const args = {
		upload: false,
		minCases: 25,
		rearrestWindow: 365,
		worker: WORKER_URL_DEFAULT,
		cacheDir: null,
	};
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === "--upload") args.upload = true;
		else if (a === "--min-cases") args.minCases = Number(argv[++i]);
		else if (a === "--rearrest-window") args.rearrestWindow = Number(argv[++i]);
		else if (a === "--worker") args.worker = argv[++i];
		else if (a === "--cache-dir") args.cacheDir = argv[++i];
	}
	return args;
}

function checkDuckdb() {
	try {
		const v = execFileSync("duckdb", ["--version"], { encoding: "utf8" });
		console.log(`Using duckdb ${v.trim().split("\n")[0]}`);
	} catch {
		console.error(
			"ERROR: duckdb CLI not found. Install with: brew install duckdb",
		);
		process.exit(1);
	}
}

function tableSource(name, cacheDir) {
	if (!cacheDir) return `'${HF_BASE}/${name}.parquet'`;
	const local = join(cacheDir, `${name}.parquet`);
	if (!existsSync(local)) {
		console.log(`  fetching ${name}.parquet → ${local}`);
		execFileSync("curl", ["-sSL", "-o", local, `${HF_BASE}/${name}.parquet`], {
			stdio: "inherit",
		});
	}
	return `'${local}'`;
}

function buildSQL(opts) {
	const ja = tableSource("judicial_assignments", opts.cacheDir);
	const cal = tableSource("calendar_with_judicial_assignments", opts.cacheDir);
	const roa = tableSource("register_of_actions", opts.cacheDir);
	const cases = tableSource("cases", opts.cacheDir);
	return `
INSTALL httpfs; LOAD httpfs;
SET enable_progress_bar = false;

WITH hearings AS (
	SELECT
		case_number,
		assigned_judge_canonical_name AS judge,
		COUNT(*) AS hearing_count
	FROM read_parquet(${cal})
	WHERE assigned_judge_canonical_name IS NOT NULL
		AND assigned_judge_canonical_name <> ''
		AND COALESCE(assigned_judge_is_placeholder, 0) = 0
	GROUP BY case_number, assigned_judge_canonical_name
),
ranked AS (
	SELECT case_number, judge, hearing_count,
		ROW_NUMBER() OVER (
			PARTITION BY case_number
			ORDER BY hearing_count DESC, judge
		) AS rn
	FROM hearings
),
case_judge AS (
	SELECT case_number, judge FROM ranked WHERE rn = 1
),
case_fta AS (
	SELECT DISTINCT case_number
	FROM read_parquet(${roa})
	WHERE proceeding = 'Bench Warrant Issued'
),
case_revoc AS (
	SELECT DISTINCT case_number
	FROM read_parquet(${roa})
	WHERE proceeding LIKE '%Administratively Revoked%'
		OR proceeding LIKE 'Probation Revoked%'
),
defendants AS (
	SELECT case_number, defendant_name,
		TRY_CAST(filed_date AS DATE) AS fd
	FROM read_parquet(${cases})
	WHERE defendant_name IS NOT NULL AND defendant_name <> ''
),
case_rearrest AS (
	SELECT DISTINCT a.case_number
	FROM defendants a JOIN defendants b
		ON a.defendant_name = b.defendant_name
	WHERE a.case_number <> b.case_number
		AND b.fd > a.fd
		AND b.fd <= a.fd + INTERVAL ${opts.rearrestWindow} DAY
),
judge_courts AS (
	SELECT judge_canonical_name AS judge,
		ARG_MAX(department_label, COALESCE(effective_start, '')) AS court_label
	FROM read_parquet(${ja})
	WHERE judge_canonical_name IS NOT NULL
		AND COALESCE(is_placeholder_judge, 0) = 0
	GROUP BY judge_canonical_name
)
SELECT
	cj.judge AS name,
	COALESCE(jc.court_label, 'San Francisco Superior Court') AS court,
	COUNT(DISTINCT cj.case_number) AS total_cases,
	COUNT(DISTINCT f.case_number) AS fta_count,
	COUNT(DISTINCT r.case_number) AS revocation_count,
	COUNT(DISTINCT ra.case_number) AS rearrest_count
FROM case_judge cj
LEFT JOIN case_fta f USING (case_number)
LEFT JOIN case_revoc r USING (case_number)
LEFT JOIN case_rearrest ra USING (case_number)
LEFT JOIN judge_courts jc ON cj.judge = jc.judge
GROUP BY cj.judge, jc.court_label
HAVING COUNT(DISTINCT cj.case_number) >= ${opts.minCases}
ORDER BY total_cases DESC;
`;
}

function runDuckdb(sql) {
	console.log("Running DuckDB query (this can take 30–90s on first run)…");
	const t0 = Date.now();
	const out = execFileSync("duckdb", ["-json", ":memory:"], {
		input: sql,
		encoding: "utf8",
		maxBuffer: 256 * 1024 * 1024,
	});
	console.log(`  done in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
	const trimmed = out.trim();
	if (!trimmed) return [];
	return JSON.parse(trimmed);
}

function judgeId(name) {
	return `sf-${name
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-|-$/g, "")}`;
}

async function main() {
	const opts = parseArgs(process.argv.slice(2));
	if (opts.cacheDir && !existsSync(opts.cacheDir)) {
		mkdirSync(opts.cacheDir, { recursive: true });
	}
	checkDuckdb();

	const rows = runDuckdb(buildSQL(opts));
	console.log(`\nGot ${rows.length} judges (≥${opts.minCases} cases)\n`);
	if (!rows.length) {
		console.error("No judges returned. Check schema or filters.");
		process.exit(1);
	}

	const judges = rows.map((r) => ({
		id: judgeId(r.name),
		name: r.name.replace(/^Judge\s+/i, "").trim(),
		city: "San Francisco",
		state: "California",
		court: r.court || "San Francisco Superior Court",
		total_cases: Number(r.total_cases) || 0,
		fta_count: Number(r.fta_count) || 0,
		rearrest_count: Number(r.rearrest_count) || 0,
		revocation_count: Number(r.revocation_count) || 0,
		source:
			"jamiequint/sf_criminal_court (HF) — SF Superior Court docket scrape + 10.500 release · CC-BY-NC-4.0",
	}));

	// Restore "Judge " prefix for display consistency with NY/etc.
	for (const j of judges) j.name = `Judge ${j.name}`;

	const total_cases = judges.reduce((s, j) => s + j.total_cases, 0);
	const total_fta = judges.reduce((s, j) => s + j.fta_count, 0);
	const total_rearrests = judges.reduce((s, j) => s + j.rearrest_count, 0);
	const total_revocations = judges.reduce((s, j) => s + j.revocation_count, 0);

	const cityData = {
		city: "San Francisco",
		state: "California",
		judges,
		source:
			"Hugging Face: jamiequint/sf_criminal_court — SF Superior Court docket scrape, DA open-data feeds, and SFSC charge-disposition spreadsheet released under California Rules of Court rule 10.500 (CC-BY-NC-4.0)",
		last_updated: new Date().toISOString(),
		last_fresh_data: new Date().toISOString(),
		total_cases,
		total_fta,
		total_rearrests,
		total_revocations,
		metric_labels: {
			fta: "Missed Court",
			rearrest: "Rearrested",
			revocation: "Probation Revoked",
			fta_bar: "Bench Warrant Issued (Missed Court)",
			rearrest_bar: `New Case Filed Within ${opts.rearrestWindow} Days (Rearrest Proxy)`,
			revocation_bar: "Probation / PRCS / Parole Revoked",
			fta_bad: true,
			rearrest_bad: true,
			revocation_bad: true,
		},
	};

	console.log("Top 10 judges by case volume:");
	for (let i = 0; i < Math.min(10, judges.length); i++) {
		const j = judges[i];
		const r = ((j.rearrest_count / j.total_cases) * 100).toFixed(1);
		const f = ((j.fta_count / j.total_cases) * 100).toFixed(1);
		const v = ((j.revocation_count / j.total_cases) * 100).toFixed(1);
		console.log(
			`  #${String(i + 1).padStart(2)} ${j.name.padEnd(40)} ${String(j.total_cases).padStart(5)} cases  fta=${f}%  rearr=${r}%  revoc=${v}%`,
		);
	}
	console.log("");
	console.log(
		`Aggregated ${judges.length} judges, ${total_cases.toLocaleString()} cases — fta=${total_fta.toLocaleString()} rearr=${total_rearrests.toLocaleString()} revoc=${total_revocations.toLocaleString()}`,
	);

	const outPath = `${process.cwd()}/scrapers/.tmp-san-francisco.json`;
	writeFileSync(outPath, JSON.stringify(cityData, null, 2));
	console.log(`\nWrote ${outPath}`);

	if (opts.upload) {
		const secret = process.env.UPLOAD_SECRET;
		if (!secret) {
			console.error("\nSet UPLOAD_SECRET env var to upload.");
			process.exit(1);
		}
		console.log(`\nUploading to ${opts.worker}/api/upload?slug=san-francisco…`);
		const res = await fetch(`${opts.worker}/api/upload?slug=san-francisco`, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${secret}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify(cityData),
		});
		const txt = await res.text();
		console.log(`  ${res.status} ${txt}`);
		if (!res.ok) process.exit(1);
	} else {
		console.log("\nTo upload, re-run with:");
		console.log(
			"  UPLOAD_SECRET=<your-secret> node scrapers/fetch-sf-hf.mjs --upload",
		);
	}
}

main().catch((e) => {
	console.error("Fatal:", e);
	process.exit(1);
});

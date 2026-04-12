// ── Types ──
interface Env {
	DATA: R2Bucket;
	UPLOAD_SECRET?: string;
	COURTLISTENER_TOKEN?: string; // Free token from courtlistener.com/register/
}

interface JudgeRecord {
	id: string;
	name: string;
	city: string;
	state: string;
	court: string;
	total_cases: number;
	fta_count: number;
	rearrest_count: number;
	revocation_count: number;
	gender?: string;
	born?: string;
	birthplace?: string;
	appointed_by?: string;
	political_affiliation?: string;
	education?: string[];
	position_type?: string;
	date_start?: string;
	source: string;
	courtlistener_id?: number;
	has_photo?: boolean;
}

interface MetricLabels {
	fta: string; // short label for cards
	rearrest: string;
	revocation: string;
	fta_bar: string; // label for rate-bar row
	rearrest_bar: string;
	revocation_bar: string;
	fta_bad: boolean; // true = higher is worse (shown red)
	rearrest_bad: boolean;
	revocation_bad: boolean;
}

interface CityStats {
	annual_arrests?: number;
	annual_felony_arrests?: number;
	annual_dispositions?: number;
	label?: string; // e.g. "2025 YTD", "last 365 days"
	note?: string; // explanatory note
	source?: string;
}

interface CityData {
	city: string;
	state: string;
	judges: JudgeRecord[];
	source: string;
	last_updated: string;
	last_fresh_data?: string; // last time we got real (non-zero) case data
	is_stale?: boolean; // true if last fresh data is older than 7 days
	total_cases: number;
	total_fta: number;
	total_rearrests: number;
	total_revocations: number;
	metric_labels?: MetricLabels;
	city_stats?: CityStats; // city-wide aggregates when per-judge data unavailable
}

// ── Config ──
const CL = "https://www.courtlistener.com/api/rest/v4";
const CW = "https://courtwatch.us/.netlify/functions";

const CITIES: Record<string, { state: string; searchTerm: string }> = {
	"san-francisco": { state: "California", searchTerm: "California" },
	"new-york": { state: "New York", searchTerm: "New York" },
	chicago: { state: "Illinois", searchTerm: "Illinois" },
	atlanta: { state: "Georgia", searchTerm: "Georgia" },
	miami: { state: "Florida", searchTerm: "Florida" },
	seattle: { state: "Washington", searchTerm: "Washington" },
	texas: { state: "Texas", searchTerm: "Texas" },
	"los-angeles": { state: "California", searchTerm: "California" },
};

// ── Worker entry ──
export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		const p = url.pathname;

		if (p === "/api/city") return handleCity(url, env);
		if (p === "/api/judge") return handleJudge(url, env);
		if (p === "/api/seed") return handleSeed(url, env);
		if (p === "/api/cities") return handleCities(env);
		if (p === "/api/worst") return handleWorst(url, env);
		if (p === "/api/upload" && request.method === "POST")
			return handleUpload(request, url, env);

		// Static assets from R2
		if (p === "/favicon.png" || p === "/favicon.svg" || p === "/og-image.png") {
			const key = `static${p}`;
			const obj = await env.DATA.get(key);
			if (obj) {
				const ct = p.endsWith(".svg") ? "image/svg+xml" : "image/png";
				return new Response(obj.body, {
					headers: {
						"content-type": ct,
						"cache-control": "public, max-age=86400",
					},
				});
			}
		}

		return new Response(HTML, {
			headers: { "content-type": "text/html;charset=utf-8" },
		});
	},

	async scheduled(_event: ScheduledEvent, env: Env): Promise<void> {
		// Daily cron: re-seed all cities
		for (const slug of Object.keys(CITIES)) {
			await seedCity(slug, env);
		}
	},
};

// ── API: Get city data from R2 ──
async function handleCity(url: URL, env: Env): Promise<Response> {
	const slug = url.searchParams.get("slug") || "";
	if (!slug) return json({ error: "slug required" }, 400);

	const obj = await env.DATA.get(`courts/${slug}.json`);
	if (!obj) {
		if (!CITIES[slug])
			return json(
				{ error: `City not found. Try: ${Object.keys(CITIES).join(", ")}` },
				404,
			);
		return json(
			{
				error: `No data yet. Run /api/seed?slug=${slug} to populate.`,
				city: slug,
			},
			404,
		);
	}

	const data = JSON.parse(await obj.text()) as CityData;
	if (data.metric_labels) {
		data.metric_labels = normalizeLabels(data.metric_labels);
	}
	return json(data);
}

// Normalize any stored jargon labels to plain English.
// Applied on read so stale R2 data always renders with plain-English labels.
function normalizeLabels(ml: MetricLabels): MetricLabels {
	const map: Record<string, string> = {
		"FTA Cases": "Missed Court",
		"FTA Count": "Missed Court",
		FTA: "Missed Court",
		"Failure to Appear": "Missed Court",
		"FTA Rate": "Missed Court Rate",
		"Bench Warrants": "Missed Court",
		"Bench Warrant (Missed Court) Rate": "Missed Court Rate",
		"DA Convictions": "Convicted",
		"DA Conviction Rate": "Conviction Rate",
		Convictions: "Convicted",
		"Conviction Rate": "Conviction Rate",
		"Found Guilty": "Convicted",
		Acquittals: "Not Guilty",
		"Acquittal Rate": "Not Guilty Rate",
		"Found Not Guilty": "Not Guilty",
		Cleared: "Not Guilty",
		"Cleared Rate": "Not Guilty Rate",
		Revocations: "Release Revoked",
		"Revocation Rate": "Release Revoked Rate",
		Rearrests: "Rearrested",
		"Rearrest Rate": "Rearrested Rate",
	};
	const fix = (s: string) => map[s] || s;
	return {
		...ml,
		fta: fix(ml.fta),
		rearrest: fix(ml.rearrest),
		revocation: fix(ml.revocation),
		fta_bar: fix(ml.fta_bar),
		rearrest_bar: fix(ml.rearrest_bar),
		revocation_bar: fix(ml.revocation_bar),
	};
}

// ── API: Get judge detail ──
async function handleJudge(url: URL, env: Env): Promise<Response> {
	const id = url.searchParams.get("id");
	const city = url.searchParams.get("city") || "";
	if (!id) return json({ error: "id required" }, 400);

	// Find judge in city data
	const obj = await env.DATA.get(`courts/${city}.json`);
	if (obj) {
		const data = JSON.parse(await obj.text()) as CityData;
		const judge = data.judges.find((j) => j.id === id);
		if (judge) {
			// Enrich with CourtListener if we have the ID
			let clData = null;
			if (judge.courtlistener_id) {
				try {
					const [pRes, posRes] = await Promise.all([
						fetch(`${CL}/people/${judge.courtlistener_id}/?format=json`, {
							headers: { "User-Agent": "JudgeSearch/2" },
						}),
						fetch(
							`${CL}/positions/?format=json&person=${judge.courtlistener_id}&position_type=jud`,
							{ headers: { "User-Agent": "JudgeSearch/2" } },
						),
					]);
					if (pRes.ok) {
						const person = await pRes.json();
						const positions = posRes.ok
							? ((await posRes.json()) as { results?: unknown[] }).results || []
							: [];
						clData = { person, positions };
					}
				} catch (_) {
					/* CourtListener unavailable, still return local data */
				}
			}
			return json({ judge, courtlistener: clData });
		}
	}

	return json({ error: "Judge not found" }, 404);
}

// ── API: List all seeded cities ──
async function handleCities(env: Env): Promise<Response> {
	const list = await env.DATA.list({ prefix: "courts/" });
	const cities = [];
	for (const obj of list.objects) {
		const data = await env.DATA.get(obj.key);
		if (data) {
			const parsed = JSON.parse(await data.text()) as CityData;
			cities.push({
				slug: obj.key.replace("courts/", "").replace(".json", ""),
				city: parsed.city,
				state: parsed.state,
				judges: parsed.judges.length,
				total_cases: parsed.total_cases,
				total_fta: parsed.total_fta,
				total_rearrests: parsed.total_rearrests,
				total_revocations: parsed.total_revocations,
				source: parsed.source,
				last_updated: parsed.last_updated,
			});
		}
	}
	return json(cities);
}

// ── API: Worst judges in America ──
// Aggregates judges across all cities where the data actually measures
// rearrests-while-on-pretrial-release (not convictions or transfers).
// Ranks by a composite danger score: rearrest rate + missed-court rate,
// weighted by case volume. Excludes judges with <20 cases to avoid
// statistically weak rankings (1 rearrest out of 3 cases ≠ 33% danger).
async function handleWorst(url: URL, env: Env): Promise<Response> {
	const n = Math.min(Number(url.searchParams.get("n") || 50), 200);
	const minCases = Number(url.searchParams.get("min_cases") || 20);

	type Ranked = {
		rank: number;
		name: string;
		city: string;
		state: string;
		court: string;
		total_cases: number;
		rearrest_count: number;
		fta_count: number;
		revocation_count: number;
		rearrest_rate: number;
		fta_rate: number;
		revocation_rate: number;
		danger_score: number;
		courtlistener_id?: number;
	};

	const ranked: Ranked[] = [];
	const excluded_cities: Array<{ slug: string; reason: string }> = [];
	let cities_checked = 0;

	for (const slug of Object.keys(CITIES)) {
		const obj = await env.DATA.get(`courts/${slug}.json`);
		if (!obj) continue;
		cities_checked++;
		const d = JSON.parse(await obj.text()) as CityData;
		const ml = d.metric_labels;
		// Only count cities where "rearrest" label actually measures rearrests
		// (not "Convicted" / "Transferred Out" / "Guilty/Deferred" / etc.)
		const measuresRearrest =
			!!ml &&
			(/rearrest/i.test(ml.rearrest || "") ||
				/rearrest/i.test(ml.rearrest_bar || ""));
		if (!measuresRearrest) {
			excluded_cities.push({
				slug,
				reason: ml
					? `"${ml.rearrest}" is not a rearrest metric`
					: "no metric labels",
			});
			continue;
		}
		for (const j of d.judges) {
			if (j.total_cases < minCases) continue;
			const rearrestRate = j.rearrest_count / j.total_cases;
			const ftaRate = j.fta_count / j.total_cases;
			const revocRate =
				ml.revocation_bad !== false ? j.revocation_count / j.total_cases : 0;
			// Danger score: weighted combination — rearrest is worst (new crime),
			// FTA next (court avoidance), revocation last (system caught it).
			// Multiplied by log(cases+1) so high-volume judges can't hide behind
			// small case counts but extreme rates on tiny samples don't dominate.
			const rawRate = rearrestRate * 2 + ftaRate * 1 + revocRate * 0.5;
			const volumeWeight = Math.log10(j.total_cases + 1);
			const danger_score = Number((rawRate * volumeWeight * 100).toFixed(2));
			ranked.push({
				rank: 0,
				name: j.name,
				city: j.city,
				state: j.state,
				court: j.court,
				total_cases: j.total_cases,
				rearrest_count: j.rearrest_count,
				fta_count: j.fta_count,
				revocation_count: j.revocation_count,
				rearrest_rate: Number(rearrestRate.toFixed(4)),
				fta_rate: Number(ftaRate.toFixed(4)),
				revocation_rate: Number(revocRate.toFixed(4)),
				danger_score,
				courtlistener_id: j.courtlistener_id,
			});
		}
	}

	ranked.sort((a, b) => b.danger_score - a.danger_score);
	const top = ranked.slice(0, n).map((r, i) => ({ ...r, rank: i + 1 }));

	return json({
		generated_at: new Date().toISOString(),
		cities_checked,
		cities_with_rearrest_data: cities_checked - excluded_cities.length,
		excluded_cities,
		min_cases: minCases,
		total_qualified_judges: ranked.length,
		returned: top.length,
		judges: top,
		methodology:
			"Danger score = (rearrest_rate × 2 + fta_rate × 1 + revocation_rate × 0.5) × log10(cases+1) × 100. Only includes judges in cities whose data actually measures rearrests-while-on-pretrial-release, and only judges with ≥" +
			minCases +
			" cases. Missing from rankings: cities that report convictions/transfers (not rearrests) and cities without per-judge case data.",
	});
}

// ── API: Seed a city (manual trigger) ──
async function handleSeed(url: URL, env: Env): Promise<Response> {
	const slug = url.searchParams.get("slug") || "all";

	if (slug === "all") {
		const results: Record<string, number> = {};
		for (const s of Object.keys(CITIES)) {
			const data = await seedCity(s, env);
			results[s] = data?.judges.length || 0;
		}
		return json({ seeded: results });
	}

	const data = await seedCity(slug, env);
	if (!data) return json({ error: "Unknown city" }, 404);
	return json({ seeded: slug, judges: data.judges.length });
}

// Cities that have real case-level scrapers. For all others, the scraper only
// returns CourtListener biographical data (0 cases) — we must never let those
// overwrite previously-uploaded real data (SF, Texas).
const LIVE_SCRAPERS = new Set([
	"miami",
	"chicago",
	"atlanta",
	"los-angeles",
	"seattle",
	"new-york",
]);

// ── Seeding logic: dispatches to per-city scrapers ──
// PROTECTIVE MERGE: never overwrite good data with zeros. If the scraper
// returns fewer cases than we already have in R2, keep the existing data
// and just update last_updated (proving the scraper ran).
async function seedCity(slug: string, env: Env): Promise<CityData | null> {
	const conf = CITIES[slug];
	if (!conf) return null;

	// Load existing data first — the floor we refuse to fall below
	const existing = await loadExisting(slug, env);

	const cityName = slug
		.split("-")
		.map((w) => w[0].toUpperCase() + w.slice(1))
		.join(" ");
	let judges: JudgeRecord[] = [];
	let source = "CourtListener (Free Law Project)";
	let metric_labels: MetricLabels | undefined;
	let city_stats: CityStats | undefined;
	let scrapeErr: unknown = null;

	// Dispatch to city-specific scrapers — wrap in try/catch so a failed
	// scraper never wipes R2 data.
	try {
		if (slug === "miami") {
			const r = await scrapeMiami(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
		} else if (slug === "chicago") {
			const r = await scrapeChicago(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
		} else if (slug === "atlanta") {
			const r = await scrapeAtlanta(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
		} else if (slug === "los-angeles") {
			const r = await scrapeLosAngeles(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
			city_stats = r.city_stats;
			await enrichWithDocketCounts(judges, env.COURTLISTENER_TOKEN);
		} else if (slug === "seattle") {
			const r = await scrapeSeattle(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
			city_stats = r.city_stats;
			await enrichWithDocketCounts(judges, env.COURTLISTENER_TOKEN);
		} else if (slug === "new-york") {
			const r = await scrapeNewYork(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
			city_stats = r.city_stats;
			await enrichWithDocketCounts(judges, env.COURTLISTENER_TOKEN);
		} else {
			// No live scraper — skip entirely if we have existing data
			if (existing && existing.total_cases > 0) {
				return existing; // PROTECTIVE: never wipe manually-uploaded data
			}
			judges = await scrapeCourtListener(cityName, conf);
			source =
				"CourtListener (Free Law Project) — Case outcome scrapers in development";
		}
	} catch (e) {
		scrapeErr = e;
		judges = [];
	}

	const newTotalCases = judges.reduce((s, j) => s + j.total_cases, 0);

	// PROTECTIVE MERGE: if existing data has more cases, keep it.
	// Only update freshness flag so users see last-successful-refresh date.
	if (
		existing &&
		existing.total_cases > 0 &&
		(scrapeErr ||
			newTotalCases < existing.total_cases * 0.5 ||
			(LIVE_SCRAPERS.has(slug) && newTotalCases === 0))
	) {
		// New data is worse — keep existing, mark as stale-since-today
		const merged: CityData = {
			...existing,
			last_updated: new Date().toISOString(),
			// Keep the original last_fresh_data; don't update it
			is_stale: isOlderThanDays(existing.last_fresh_data, 7),
		};
		await env.DATA.put(`courts/${slug}.json`, JSON.stringify(merged));
		return merged;
	}

	// Sort: worst outcomes first — only count genuinely bad metrics (fta + rearrest)
	const revBad = metric_labels?.revocation_bad ?? true;
	judges.sort((a, b) => {
		const aScore =
			a.fta_count + a.rearrest_count + (revBad ? a.revocation_count : 0);
		const bScore =
			b.fta_count + b.rearrest_count + (revBad ? b.revocation_count : 0);
		if (bScore !== aScore) return bScore - aScore;
		return b.total_cases - a.total_cases;
	});

	const now = new Date().toISOString();
	const hasFreshData = newTotalCases > 0;
	const cityData: CityData = {
		city: cityName,
		state: conf.state,
		judges,
		source,
		last_updated: now,
		last_fresh_data: hasFreshData
			? now
			: existing?.last_fresh_data || undefined,
		is_stale: hasFreshData
			? false
			: isOlderThanDays(existing?.last_fresh_data, 7),
		total_cases: newTotalCases,
		total_fta: judges.reduce((s, j) => s + j.fta_count, 0),
		total_rearrests: judges.reduce((s, j) => s + j.rearrest_count, 0),
		total_revocations: judges.reduce((s, j) => s + j.revocation_count, 0),
		metric_labels: metric_labels || existing?.metric_labels,
		city_stats: city_stats || existing?.city_stats,
	};

	await env.DATA.put(`courts/${slug}.json`, JSON.stringify(cityData));
	return cityData;
}

async function loadExisting(slug: string, env: Env): Promise<CityData | null> {
	const obj = await env.DATA.get(`courts/${slug}.json`);
	if (!obj) return null;
	try {
		return JSON.parse(await obj.text()) as CityData;
	} catch {
		return null;
	}
}

function isOlderThanDays(iso: string | undefined, days: number): boolean {
	if (!iso) return true;
	const age = Date.now() - new Date(iso).getTime();
	return age > days * 24 * 60 * 60 * 1000;
}

// ── API: Direct upload to R2 (for local scrapers) ──
async function handleUpload(
	request: Request,
	url: URL,
	env: Env,
): Promise<Response> {
	// Require bearer token auth
	const authHeader = request.headers.get("Authorization") || "";
	const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
	if (!env.UPLOAD_SECRET || token !== env.UPLOAD_SECRET) {
		return json({ error: "Unauthorized" }, 401);
	}
	const slug = url.searchParams.get("slug");
	if (!slug) return json({ error: "slug required" }, 400);
	const body = await request.text();
	const parsed = JSON.parse(body) as CityData;
	// If upload contains real case data, record the freshness timestamp so
	// the stale-data flag logic has something to compare against.
	if (parsed.total_cases > 0) {
		parsed.last_fresh_data = parsed.last_fresh_data || new Date().toISOString();
		parsed.is_stale = false;
	}
	parsed.last_updated = parsed.last_updated || new Date().toISOString();
	await env.DATA.put(`courts/${slug}.json`, JSON.stringify(parsed), {
		httpMetadata: { contentType: "application/json" },
	});
	return json({
		uploaded: slug,
		judges: parsed.judges.length,
		cases: parsed.total_cases,
		last_fresh_data: parsed.last_fresh_data,
	});
}

// ═══════════════════════════════════════════
// ── SCRAPERS ──
// ═══════════════════════════════════════════

// ── Miami: CourtWatch.us API (FSS 907.043) ──
async function scrapeMiami(cityName: string): Promise<{
	judges: JudgeRecord[];
	source: string;
	metric_labels?: MetricLabels;
}> {
	const judges: JudgeRecord[] = [];
	try {
		const res = await fetch(`${CW}/judges`);
		if (!res.ok) throw new Error("CourtWatch unavailable");
		const data = (await res.json()) as Array<{
			id: number;
			name: string;
			total_cases: number;
			failure_to_appear_count: number;
			new_arrest_count: number;
			revocation_count: number;
		}>;

		for (const cw of data) {
			judges.push({
				id: `cw-${cw.id}`,
				name: cw.name,
				city: cityName,
				state: "Florida",
				court: "Circuit Court, Orange County, FL",
				total_cases: cw.total_cases,
				fta_count: cw.failure_to_appear_count,
				rearrest_count: cw.new_arrest_count,
				revocation_count: cw.revocation_count,
				source: "CourtWatch.us / FSS 907.043",
			});
		}
	} catch (_) {
		/* fallback handled by caller */
	}
	return {
		judges,
		source: "CourtWatch.us — Florida Citizens Right to Know Act (FSS 907.043)",
		metric_labels: {
			fta: "Missed Court",
			rearrest: "Rearrested",
			revocation: "Release Revoked",
			fta_bar: "Missed Court Date (Didn't Show Up)",
			rearrest_bar: "Rearrested While Awaiting Trial",
			revocation_bar: "Pretrial Release Revoked",
			fta_bad: true,
			rearrest_bad: true,
			revocation_bad: true,
		},
	};
}

// ── Chicago: Cook County Open Data (Socrata API) ──
// Dataset: Dispositions (apwk-dzx8) — 1M+ records with judge, charge, disposition
async function scrapeChicago(cityName: string): Promise<{
	judges: JudgeRecord[];
	source: string;
	metric_labels?: MetricLabels;
}> {
	const judges: JudgeRecord[] = [];
	try {
		// Get per-judge aggregates: total cases, guilty pleas, guilty verdicts, BFW (bench warrants/FTA)
		const queries = await Promise.all([
			// Total dispositions per judge
			fetch(
				"https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json?$select=judge,count(*)%20as%20total&$group=judge&$order=total%20DESC&$limit=50&$where=judge%20IS%20NOT%20NULL",
			),
			// Guilty dispositions per judge (Plea of Guilty + Finding Guilty + Verdict Guilty)
			fetch(
				"https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json?$select=judge,count(*)%20as%20guilty_count&$group=judge&$order=guilty_count%20DESC&$limit=50&$where=judge%20IS%20NOT%20NULL%20AND%20(charge_disposition%3D%27Plea%20Of%20Guilty%27%20OR%20charge_disposition%3D%27Finding%20Guilty%27%20OR%20charge_disposition%3D%27Verdict%20Guilty%27%20OR%20charge_disposition%3D%27Finding%20Guilty%20-%20Lesser%20Included%27)",
			),
			// BFW (Bench Warrant / Failure to appear) per judge
			fetch(
				"https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json?$select=judge,count(*)%20as%20bfw_count&$group=judge&$order=bfw_count%20DESC&$limit=50&$where=judge%20IS%20NOT%20NULL%20AND%20charge_disposition%3D%27BFW%27",
			),
			// Not Guilty per judge
			fetch(
				"https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json?$select=judge,count(*)%20as%20ng_count&$group=judge&$order=ng_count%20DESC&$limit=50&$where=judge%20IS%20NOT%20NULL%20AND%20(charge_disposition%3D%27FNG%27%20OR%20charge_disposition%3D%27Verdict-Not%20Guilty%27%20OR%20charge_disposition%3D%27Finding%20Not%20Not%20Guilty%27)",
			),
		]);

		const [totalData, guiltyData, bfwData, ngData] = await Promise.all(
			queries.map((r) =>
				r.ok
					? (r.json() as Promise<Array<Record<string, string>>>)
					: Promise.resolve([]),
			),
		);

		// Build lookup maps
		const guiltyMap = new Map(
			guiltyData.map((r) => [r.judge, parseInt(r.guilty_count, 10) || 0]),
		);
		const bfwMap = new Map(
			bfwData.map((r) => [r.judge, parseInt(r.bfw_count, 10) || 0]),
		);
		const ngMap = new Map(
			ngData.map((r) => [r.judge, parseInt(r.ng_count, 10) || 0]),
		);

		for (const row of totalData) {
			const name = row.judge;
			if (!name) continue;
			const total = parseInt(row.total, 10) || 0;
			const guilty = guiltyMap.get(name) || 0;
			const bfw = bfwMap.get(name) || 0;
			const ng = ngMap.get(name) || 0;

			judges.push({
				id: `cc-${name.replace(/\s+/g, "-").toLowerCase()}`,
				name,
				city: cityName,
				state: "Illinois",
				court: "Circuit Court of Cook County",
				total_cases: total,
				fta_count: bfw, // BFW = bench warrant / failure to appear
				rearrest_count: guilty, // Using guilty count as conviction metric
				revocation_count: ng, // Using not-guilty count (acquittals)
				source: "Cook County Open Data (Socrata)",
			});
		}
	} catch (_) {
		/* fallback handled */
	}
	return {
		judges,
		source: "Cook County Clerk — Open Data Portal (1M+ disposition records)",
		metric_labels: {
			fta: "Missed Court",
			rearrest: "Convicted",
			revocation: "Not Guilty",
			fta_bar: "Missed Court Rate",
			rearrest_bar: "Conviction Rate",
			revocation_bar: "Not Guilty Rate",
			fta_bad: true,
			rearrest_bad: true,
			revocation_bad: false,
		},
	};
}

// ── Atlanta: Fulton County Open Data (Socrata API) ──
// Datasets: Disposed Cases (uww8-gu28) + Pending by Judge (dg7p-62bk)
async function scrapeAtlanta(cityName: string): Promise<{
	judges: JudgeRecord[];
	source: string;
	metric_labels?: MetricLabels;
}> {
	const judges: JudgeRecord[] = [];
	try {
		const [disposedRes, pendingRes] = await Promise.all([
			// Disposed cases per judge
			fetch(
				"https://sharefulton.fultoncountyga.gov/resource/uww8-gu28.json?$select=judge,count(*)%20as%20total,disposition&$group=judge,disposition&$order=total%20DESC&$limit=200&$where=judge%20IS%20NOT%20NULL",
			),
			// Pending cases per judge (current snapshot)
			fetch(
				"https://sharefulton.fultoncountyga.gov/resource/dg7p-62bk.json?$order=n_pending_t%20DESC&$limit=30",
			),
		]);

		const disposedData = disposedRes.ok
			? ((await disposedRes.json()) as Array<Record<string, string>>)
			: [];
		const pendingData = pendingRes.ok
			? ((await pendingRes.json()) as Array<Record<string, string>>)
			: [];

		// Aggregate disposed data by judge (filter generic names)
		const genericPrefixes = [
			"Family",
			"Non-Complex",
			"Complex",
			"Judge,",
			"Unassigned",
		];
		const judgeMap = new Map<
			string,
			{ total: number; exported: number; noReport: number }
		>();
		for (const row of disposedData) {
			const name = row.judge;
			if (!name || genericPrefixes.some((p) => name.startsWith(p))) continue;
			const count = parseInt(row.total, 10) || 0;
			const entry = judgeMap.get(name) || {
				total: 0,
				exported: 0,
				noReport: 0,
			};
			entry.total += count;
			if (row.disposition?.includes("Export")) entry.exported += count;
			if (row.disposition?.includes("Do Not Report")) entry.noReport += count;
			judgeMap.set(name, entry);
		}

		// Build pending lookup
		const pendingMap = new Map(
			pendingData.map((r) => [r.judge, parseInt(r.n_pending_t, 10) || 0]),
		);

		for (const [name, stats] of judgeMap) {
			const pending = pendingMap.get(name) || 0;

			judges.push({
				id: `fc-${name.replace(/[,.\s]+/g, "-").toLowerCase()}`,
				name: name
					.split(",")
					.reverse()
					.map((s) => s.trim())
					.join(" "),
				city: cityName,
				state: "Georgia",
				court: "Fulton County Superior Court",
				total_cases: stats.total,
				fta_count: pending,
				rearrest_count: stats.exported,
				revocation_count: stats.noReport,
				source: "Fulton County Open Data (ShareFulton)",
			});
		}
	} catch (_) {
		/* fallback */
	}
	return {
		judges,
		source:
			"Fulton County — ShareFulton Open Data Portal (disposed cases + pending)",
		metric_labels: {
			fta: "Still Waiting",
			rearrest: "Transferred Out",
			revocation: "No Outcome Filed",
			fta_bar: "Cases Still Pending (Backlog)",
			rearrest_bar: "Transferred to Another Court",
			revocation_bar: "No Outcome on Record",
			fta_bad: true,
			rearrest_bad: false,
			revocation_bad: false,
		},
	};
}

// ── Los Angeles Superior Court ──
// LA Superior Court and LA County do not publish judge-level case outcome data
// via a public JSON API accessible without authentication or browser automation.
// LASC's media portal is an Angular SPA gated by reCAPTCHA; LA County's data
// portal has no criminal disposition datasets keyed to judge names.
// Strategy: CourtListener v4 bios for LASC judges (v4 required — v3 returns 0).
// Fetch a single count from a Socrata-compatible endpoint
async function socrataCount(url: string): Promise<number> {
	try {
		const res = await fetch(url, {
			headers: { "User-Agent": "JudgeSearch/2" },
		});
		if (!res.ok) return 0;
		const data = (await res.json()) as Array<{ count?: string }>;
		return Number(data[0]?.count || 0);
	} catch {
		return 0;
	}
}

// CourtListener opinion count for a judge via text search. State court
// judges aren't linked via /dockets/ (that's PACER/federal), but their
// names DO appear in state appellate opinions that CL scrapes. This gives
// real per-judge opinion counts for state court judges.
// Free token from https://www.courtlistener.com/register/ required.
async function clOpinionCount(
	judgeName: string,
	token: string | undefined,
): Promise<number> {
	if (!token || !judgeName) return 0;
	try {
		const q = encodeURIComponent(`"${judgeName}"`);
		const res = await fetch(
			`${CL}/search/?q=${q}&type=o&format=json&page_size=1`,
			{
				headers: {
					Authorization: `Token ${token}`,
					"User-Agent": "JudgeSearch/2",
				},
			},
		);
		if (!res.ok) return 0;
		const data = (await res.json()) as { count?: number };
		return Number(data.count || 0);
	} catch {
		return 0;
	}
}

// Enrich judges with real opinion counts from CourtListener search.
// 60/min rate limit — process in chunks of 6 per second.
async function enrichWithDocketCounts(
	judges: JudgeRecord[],
	token: string | undefined,
): Promise<void> {
	if (!token || judges.length === 0) return;
	const chunkSize = 6;
	for (let i = 0; i < judges.length; i += chunkSize) {
		const chunk = judges.slice(i, i + chunkSize);
		const counts = await Promise.all(
			chunk.map((j) => clOpinionCount(j.name, token)),
		);
		for (let k = 0; k < chunk.length; k++) {
			const count = counts[k];
			if (count > 0) {
				chunk[k].total_cases = count;
			}
		}
	}
}

async function scrapeLosAngeles(_cityName: string): Promise<{
	judges: JudgeRecord[];
	source: string;
	metric_labels: MetricLabels;
	city_stats?: CityStats;
}> {
	const judges: JudgeRecord[] = [];
	const metric_labels: MetricLabels = {
		fta: "Missed Court",
		rearrest: "Rearrested",
		revocation: "Release Revoked",
		fta_bar: "Missed Court Date (Didn't Show Up)",
		rearrest_bar: "Rearrested While Awaiting Trial",
		revocation_bar: "Pretrial Release Revoked",
		fta_bad: true,
		rearrest_bad: true,
		revocation_bad: true,
	};

	try {
		// v4 API with icontains — v3 /people/ returns 0 for state-level searches
		const url =
			`${CL}/people/?format=json&positions__court__full_name__icontains=` +
			`Los+Angeles&positions__position_type=jud&page_size=20`;
		const res = await fetch(url, {
			headers: { "User-Agent": "JudgeSearch/2" },
		});
		if (res.ok) {
			const data = (await res.json()) as {
				results?: Array<Record<string, unknown>>;
			};
			for (const cl of data.results || []) {
				const fullName = [
					cl.name_first,
					cl.name_middle,
					cl.name_last,
					cl.name_suffix,
				]
					.filter(Boolean)
					.map(String)
					.join(" ");
				judges.push({
					id: `cl-${cl.id}`,
					name: fullName,
					city: "Los Angeles",
					state: "California",
					court: "Los Angeles Superior Court",
					total_cases: 0,
					fta_count: 0,
					rearrest_count: 0,
					revocation_count: 0,
					source: "CourtListener (Free Law Project)",
					courtlistener_id: Number(cl.id),
					gender: String(cl.gender || ""),
					born: String(cl.date_dob || ""),
					birthplace: `${cl.dob_city || ""}${cl.dob_state ? `, ${cl.dob_state}` : ""}`,
					has_photo: Boolean(cl.has_photo),
					political_affiliation: (cl.political_affiliations as unknown[])
						?.length
						? "On Record"
						: undefined,
					education: (cl.educations as unknown[])?.length
						? [`${(cl.educations as unknown[]).length} record(s)`]
						: undefined,
				});
			}
		}
	} catch (_) {
		/* unavailable */
	}

	// Fetch LA city-wide arrest volume (real public data)
	const laArrests = await socrataCount(
		"https://data.lacity.org/resource/amvf-fr72.json?$select=count(*)",
	);

	return {
		judges,
		source:
			"CourtListener (Free Law Project) — LA Superior Court does not publish judge-level case data via public API",
		metric_labels,
		city_stats: laArrests
			? {
					annual_arrests: laArrests,
					label: "Total arrests on record",
					source: "data.lacity.org Arrests dataset",
					note: "City-wide total — individual judge caseloads not published by LA Superior Court",
				}
			: undefined,
	};
}

// ── Seattle / King County Superior Court ──
// Seattle Municipal Court and King County Superior Court do not publish
// judge-level case outcome data via a public JSON API accessible from a Worker.
// King County's open data portal has jail booking records but no judge name field.
// cos-data.seattle.gov court datasets are non-tabular story pages (not API-accessible).
// Strategy: CourtListener v4 bios for Washington state judges.
async function scrapeSeattle(_cityName: string): Promise<{
	judges: JudgeRecord[];
	source: string;
	metric_labels: MetricLabels;
	city_stats?: CityStats;
}> {
	const judges: JudgeRecord[] = [];
	const metric_labels: MetricLabels = {
		fta: "Missed Court",
		rearrest: "Rearrested",
		revocation: "Release Revoked",
		fta_bar: "Missed Court Date (Didn't Show Up)",
		rearrest_bar: "Rearrested While Awaiting Trial",
		revocation_bar: "Pretrial Release Revoked",
		fta_bad: true,
		rearrest_bad: true,
		revocation_bad: true,
	};

	try {
		const url =
			`${CL}/people/?format=json&positions__court__full_name__icontains=` +
			`Washington&positions__position_type=jud&page_size=20`;
		const res = await fetch(url, {
			headers: { "User-Agent": "JudgeSearch/2" },
		});
		if (res.ok) {
			const data = (await res.json()) as {
				results?: Array<Record<string, unknown>>;
			};
			for (const cl of data.results || []) {
				const fullName = [
					cl.name_first,
					cl.name_middle,
					cl.name_last,
					cl.name_suffix,
				]
					.filter(Boolean)
					.map(String)
					.join(" ");
				judges.push({
					id: `cl-${cl.id}`,
					name: fullName,
					city: "Seattle",
					state: "Washington",
					court: "King County Superior Court",
					total_cases: 0,
					fta_count: 0,
					rearrest_count: 0,
					revocation_count: 0,
					source: "CourtListener (Free Law Project)",
					courtlistener_id: Number(cl.id),
					gender: String(cl.gender || ""),
					born: String(cl.date_dob || ""),
					birthplace: `${cl.dob_city || ""}${cl.dob_state ? `, ${cl.dob_state}` : ""}`,
					has_photo: Boolean(cl.has_photo),
					political_affiliation: (cl.political_affiliations as unknown[])
						?.length
						? "On Record"
						: undefined,
					education: (cl.educations as unknown[])?.length
						? [`${(cl.educations as unknown[]).length} record(s)`]
						: undefined,
				});
			}
		}
	} catch (_) {
		/* unavailable */
	}

	// Fetch King County jail booking volume (real public data)
	const kcBookings = await socrataCount(
		"https://data.kingcounty.gov/resource/j56h-zgnm.json?$select=count(*)",
	);

	return {
		judges,
		source:
			"CourtListener (Free Law Project) — King County Superior Court does not publish judge-level case data via public API",
		metric_labels,
		city_stats: kcBookings
			? {
					annual_arrests: kcBookings,
					label: "Adult jail bookings (12 months)",
					source: "data.kingcounty.gov Adult Jail Booking dataset",
					note: "County-wide total — per-judge caseloads not published by King County Superior Court",
				}
			: undefined,
	};
}

// ── New York City Criminal Court ──
// NYC Criminal Court and NYC Supreme Court are NY State agencies; they do not
// appear on NYC Open Data (data.cityofnewyork.us). The NY State OCA pretrial
// stats portal (ww2.nycourts.gov) is behind Cloudflare JS challenge and returns
// an HTML challenge page to non-browser HTTP clients.
// Strategy: CourtListener v4 bios for New York state judges.
async function scrapeNewYork(_cityName: string): Promise<{
	judges: JudgeRecord[];
	source: string;
	metric_labels: MetricLabels;
	city_stats?: CityStats;
}> {
	const judges: JudgeRecord[] = [];
	const metric_labels: MetricLabels = {
		fta: "Missed Court",
		rearrest: "Rearrested",
		revocation: "Release Revoked",
		fta_bar: "Missed Court Date (Didn't Show Up)",
		rearrest_bar: "Rearrested While Awaiting Trial",
		revocation_bar: "Pretrial Release Revoked",
		fta_bad: true,
		rearrest_bad: true,
		revocation_bad: true,
	};

	try {
		const url =
			`${CL}/people/?format=json&positions__court__full_name__icontains=` +
			`New+York&positions__position_type=jud&page_size=20`;
		const res = await fetch(url, {
			headers: { "User-Agent": "JudgeSearch/2" },
		});
		if (res.ok) {
			const data = (await res.json()) as {
				results?: Array<Record<string, unknown>>;
			};
			for (const cl of data.results || []) {
				const fullName = [
					cl.name_first,
					cl.name_middle,
					cl.name_last,
					cl.name_suffix,
				]
					.filter(Boolean)
					.map(String)
					.join(" ");
				judges.push({
					id: `cl-${cl.id}`,
					name: fullName,
					city: "New York",
					state: "New York",
					court: "New York City Criminal Court",
					total_cases: 0,
					fta_count: 0,
					rearrest_count: 0,
					revocation_count: 0,
					source: "CourtListener (Free Law Project)",
					courtlistener_id: Number(cl.id),
					gender: String(cl.gender || ""),
					born: String(cl.date_dob || ""),
					birthplace: `${cl.dob_city || ""}${cl.dob_state ? `, ${cl.dob_state}` : ""}`,
					has_photo: Boolean(cl.has_photo),
					political_affiliation: (cl.political_affiliations as unknown[])
						?.length
						? "On Record"
						: undefined,
					education: (cl.educations as unknown[])?.length
						? [`${(cl.educations as unknown[]).length} record(s)`]
						: undefined,
				});
			}
		}
	} catch (_) {
		/* unavailable */
	}

	// NYC arrest data is real public open data (NYPD YTD)
	const [nycArrests, nycFelonies] = await Promise.all([
		socrataCount(
			"https://data.cityofnewyork.us/resource/uip8-fykc.json?$select=count(*)",
		),
		socrataCount(
			"https://data.cityofnewyork.us/resource/uip8-fykc.json?$select=count(*)&$where=law_cat_cd='F'",
		),
	]);

	return {
		judges,
		source:
			"CourtListener (Free Law Project) — NYC courts are NY State agencies without public judge-level case data APIs",
		metric_labels,
		city_stats: nycArrests
			? {
					annual_arrests: nycArrests,
					annual_felony_arrests: nycFelonies,
					label: "NYPD arrests year-to-date",
					source: "data.cityofnewyork.us NYPD Arrest Data",
					note: "City-wide total — NY State court system does not publish per-judge case data via public API",
				}
			: undefined,
	};
}

// ── Generic: CourtListener bios for cities without case data APIs ──
async function scrapeCourtListener(
	cityName: string,
	conf: { state: string; searchTerm: string },
): Promise<JudgeRecord[]> {
	const judges: JudgeRecord[] = [];
	try {
		const res = await fetch(
			`${CL}/people/?format=json&positions__court__full_name__contains=${encodeURIComponent(conf.searchTerm)}&positions__position_type=jud`,
			{ headers: { "User-Agent": "JudgeSearch/2" } },
		);
		if (!res.ok) return judges;
		const data = (await res.json()) as {
			results?: Array<Record<string, unknown>>;
		};

		for (const cl of data.results || []) {
			const fullName = [
				cl.name_first,
				cl.name_middle,
				cl.name_last,
				cl.name_suffix,
			]
				.filter(Boolean)
				.map(String)
				.join(" ");

			judges.push({
				id: `cl-${cl.id}`,
				name: fullName,
				city: cityName,
				state: conf.state,
				court: `${conf.state} Courts`,
				total_cases: 0,
				fta_count: 0,
				rearrest_count: 0,
				revocation_count: 0,
				source: "CourtListener (Free Law Project)",
				courtlistener_id: Number(cl.id),
				gender: String(cl.gender || ""),
				born: String(cl.date_dob || ""),
				birthplace: `${cl.dob_city || ""}${cl.dob_state ? `, ${cl.dob_state}` : ""}`,
				has_photo: Boolean(cl.has_photo),
				political_affiliation: (cl.political_affiliations as unknown[])?.length
					? "On Record"
					: undefined,
				education: (cl.educations as unknown[])?.length
					? [`${(cl.educations as unknown[]).length} record(s)`]
					: undefined,
			});
		}
	} catch (_) {
		/* unavailable */
	}
	return judges;
}

// ── Helpers ──
function json(data: unknown, status = 200) {
	return new Response(JSON.stringify(data), {
		status,
		headers: {
			"content-type": "application/json",
			"access-control-allow-origin": "*",
		},
	});
}

// ── HTML ──
const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>JudgeSearch — Know Your Judges. Hold Them Accountable.</title>
<meta name="description" content="Search real court records across 8 U.S. cities. 880,000+ cases. See how often defendants released by each judge missed court, got rearrested, or had their release revoked. All public data.">
<meta name="theme-color" content="#c8a84b">
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon.png">
<link rel="apple-touch-icon" href="/favicon.png">
<!-- Open Graph -->
<meta property="og:site_name" content="JudgeSearch">
<meta property="og:title" content="JudgeSearch — Know Your Judges. Hold Them Accountable.">
<meta property="og:description" content="Search real court records across 8 U.S. cities. 880,000+ cases. See how often defendants released by each judge missed court, got rearrested, or had their release revoked. All public data, explained in plain English.">
<meta property="og:image" content="https://judge-search.barkleesanders.workers.dev/og-image.png">
<meta property="og:image:type" content="image/png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="JudgeSearch — scales of justice on a dark background. Search real court records across 8 U.S. cities, 225+ judges, 881K cases. Sorted by impact.">
<meta property="og:type" content="website">
<meta property="og:url" content="https://judge-search.barkleesanders.workers.dev">
<!-- Twitter Card -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="JudgeSearch — Judicial Accountability">
<meta name="twitter:description" content="Search 880,000+ court records across 8 U.S. cities. See which judges had the most missed court dates, rearrests, and release revocations. Public data, plain English.">
<meta name="twitter:image" content="https://judge-search.barkleesanders.workers.dev/og-image.png">
<meta name="twitter:image:alt" content="JudgeSearch — scales of justice on dark background, showing 8 cities, 225+ judges, 881K cases covered.">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,700;0,800;1,700&family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0a0a0a;--s:#111;--s2:#161616;--s3:#1c1c1c;
  --b:#222;--b2:#333;
  --t:#f0ece4;--t2:#a09a8c;--t3:#6b6560;
  --gold:#c8a84b;--gold2:#e0c96a;--gg:rgba(200,168,75,.12);
  --red:#e84040;--orange:#f0883e;--green:#34d399;
  --serif:'Playfair Display',Georgia,serif;
  --sans:'IBM Plex Sans',-apple-system,system-ui,sans-serif;
  --mono:'IBM Plex Mono','SF Mono',monospace;
  --r:8px;
}
body{font-family:var(--sans);background:var(--bg);color:var(--t);line-height:1.6;min-height:100vh}
a{color:var(--gold);text-decoration:none}a:hover{color:var(--gold2)}
.wrap{max-width:1200px;margin:0 auto;padding:0 24px}

/* NAV */
nav{position:sticky;top:0;z-index:50;background:rgba(10,10,10,.95);backdrop-filter:blur(12px);border-bottom:1px solid var(--b);padding:14px 0}
.ni{max-width:1200px;margin:0 auto;padding:0 24px;display:flex;align-items:center;justify-content:space-between}
.logo{font-family:var(--serif);font-size:1.4rem;font-weight:800}.logo span{color:var(--gold)}
.nl{display:flex;gap:20px}.nl a{font-family:var(--mono);font-size:.8rem;color:var(--t2);text-transform:uppercase;letter-spacing:.05em}

/* HERO */
.hero{text-align:center;padding:60px 24px 40px;border-bottom:1px solid var(--b)}
.hero h1{font-family:var(--serif);font-size:2.8rem;font-weight:800;line-height:1.15;margin-bottom:12px}
.hero h1 em{font-style:italic;color:var(--gold)}
.hero p{color:var(--t2);font-size:1.05rem;max-width:620px;margin:0 auto;line-height:1.7}

/* MAP */
.map-box{border:1px solid var(--b);border-radius:var(--r);overflow:hidden;margin:32px auto;max-width:1000px}
#leaflet-map{width:100%;height:380px;background:var(--s2)}
.map-legend{display:flex;gap:20px;justify-content:center;padding:10px;background:var(--s);font-family:var(--mono);font-size:.75rem;color:var(--t3)}
.ml-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px;vertical-align:middle}
.leaflet-popup-content-wrapper{background:var(--s)!important;color:var(--t)!important;border:1px solid var(--b2)!important;border-radius:var(--r)!important;font-family:var(--sans)!important}
.leaflet-popup-tip{background:var(--s)!important}
.leaflet-control-attribution{background:rgba(10,10,10,.8)!important;color:var(--t3)!important;font-size:.6rem!important}
.leaflet-control-attribution a{color:var(--gold)!important}
.leaflet-control-zoom a{background:var(--s)!important;color:var(--gold)!important;border-color:var(--b)!important}

/* PILLS */
.pills{display:flex;flex-wrap:wrap;gap:8px;justify-content:center;margin:24px auto;max-width:800px}
.pill{padding:8px 18px;background:var(--s);border:1px solid var(--b);border-radius:20px;color:var(--t2);font-family:var(--mono);font-size:.8rem;cursor:pointer;transition:all .2s}
.pill:hover,.pill.on{border-color:var(--gold);color:var(--gold);background:var(--gg)}

/* STATS BAR */
.sbar{display:none;gap:1px;background:var(--b);border:1px solid var(--b);border-radius:var(--r);overflow:hidden;max-width:800px;margin:0 auto 20px}
.sc{flex:1;padding:16px;background:var(--s);text-align:center}
.sc .n{font-family:var(--serif);font-size:1.8rem;font-weight:800;color:var(--gold)}
.sc .n.red{color:var(--red)}.sc .n.ora{color:var(--orange)}.sc .n.grn{color:var(--green)}
.sc .l{font-family:var(--mono);font-size:.65rem;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;margin-top:4px}

/* SOURCE BANNER */
.src-banner{max-width:800px;margin:0 auto 24px;padding:10px 16px;background:var(--s);border:1px solid var(--b);border-left:3px solid var(--gold);border-radius:var(--r);font-size:.85rem;color:var(--t2);display:none}

/* BASEBALL CARDS */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:16px;margin-bottom:32px}
.bcard{background:var(--s);border:1px solid var(--b);border-radius:var(--r);overflow:hidden;transition:border-color .2s}
.bcard:hover{border-color:var(--gold)}
.bcard-head{padding:16px 20px;border-bottom:1px solid var(--b);display:flex;align-items:center;gap:14px}
.bcard-av{width:48px;height:48px;border-radius:50%;background:var(--s2);display:flex;align-items:center;justify-content:center;font-family:var(--serif);font-size:1.1rem;font-weight:700;color:var(--gold);border:2px solid var(--b);flex-shrink:0}
.bcard-name{font-family:var(--serif);font-size:1.05rem;font-weight:700;line-height:1.3}
.bcard-court{font-size:.78rem;color:var(--t2);margin-top:2px}
.bcard-loc{font-family:var(--mono);font-size:.68rem;color:var(--t3);margin-top:1px}

/* Stat row */
.bcard-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--b)}
.bs{padding:10px 6px;background:var(--s2);text-align:center}
.bs .bv{font-family:var(--serif);font-size:1.2rem;font-weight:800}
.bs .bv.w{color:var(--t)}.bs .bv.r{color:var(--red)}.bs .bv.o{color:var(--orange)}.bs .bv.g{color:var(--gold)}
.bs .bl{font-family:var(--mono);font-size:.55rem;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;margin-top:2px}

/* Rate bars */
.bcard-rates{padding:12px 20px 16px}
.rate-row{display:flex;align-items:center;gap:10px;margin-bottom:6px}
.rate-row:last-child{margin-bottom:0}
.rate-label{font-size:.78rem;color:var(--t2);width:100px;flex-shrink:0}
.rate-bar{flex:1;height:8px;background:var(--s3);border-radius:4px;overflow:hidden}
.rate-fill{height:100%;border-radius:4px;transition:width .6s ease}
.rate-pct{font-family:var(--mono);font-size:.78rem;font-weight:600;width:48px;text-align:right;flex-shrink:0}
.rate-vs{font-family:var(--mono);font-size:.62rem;width:70px;text-align:right;flex-shrink:0}
.rate-vs.above{color:var(--red)}.rate-vs.below{color:var(--green)}.rate-vs.avg{color:var(--t3)}

/* No data tag */
.nodata{font-family:var(--mono);font-size:.72rem;color:var(--t3);padding:10px 20px}

/* Remaining judges compact list */
.more-judges{margin-bottom:40px}
.more-toggle{display:block;width:100%;padding:12px;background:var(--s);border:1px solid var(--b);border-radius:var(--r);color:var(--t2);font-family:var(--mono);font-size:.82rem;cursor:pointer;text-align:center;transition:all .2s;margin-bottom:12px}
.more-toggle:hover{border-color:var(--gold);color:var(--gold)}
.jtable{width:100%;border-collapse:collapse;display:none}
.jtable.show{display:table}
.jtable th{font-family:var(--mono);font-size:.65rem;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;padding:8px 10px;text-align:left;border-bottom:1px solid var(--b)}
.jtable td{padding:8px 10px;border-bottom:1px solid var(--b);font-size:.85rem}
.jtable tr:hover{background:var(--s2)}
.jtable .num{font-family:var(--mono);text-align:right}

/* LOADING / EMPTY */
.loading{text-align:center;padding:60px 20px;color:var(--t3)}
.spin{width:32px;height:32px;border:3px solid var(--b);border-top-color:var(--gold);border-radius:50%;animation:sp .7s linear infinite;margin:0 auto 12px}
@keyframes sp{to{transform:rotate(360deg)}}
.empty{text-align:center;padding:80px 24px;color:var(--t3)}
.empty h3{font-family:var(--serif);color:var(--t);margin-bottom:8px}

/* FOOTER */
footer{text-align:center;padding:40px 24px;border-top:1px solid var(--b);color:var(--t3);font-size:.8rem;font-family:var(--mono)}
footer a{color:var(--gold)}

@media(max-width:700px){
  .hero{padding:36px 16px 28px}
  .hero h1{font-size:1.75rem}
  .hero p{font-size:.95rem}
  .wrap{padding:0 12px}
  .ni{padding:0 14px}
  .logo{font-size:1.2rem}
  .nl{gap:12px}.nl a{font-size:.72rem}
  .nl a:nth-child(n+3){display:none}
  #leaflet-map{height:220px}
  .map-legend{flex-wrap:wrap;gap:10px;font-size:.7rem}
  .pills{gap:6px;margin:16px auto}
  .pill{padding:6px 14px;font-size:.75rem}
  .sbar{display:grid!important;grid-template-columns:repeat(2,1fr)}
  .sc{padding:12px 8px}
  .sc .n{font-size:1.4rem}
  .sc .l{font-size:.6rem}
  .cards{grid-template-columns:1fr}
  .bcard-stats{grid-template-columns:repeat(2,1fr)}
  .bs .bv{font-size:1rem}
  .bs .bl{font-size:.52rem}
  .rate-label{width:80px;font-size:.72rem}
  .rate-vs{display:none}
  .bcard-rates{padding:10px 14px 14px}
  .jtable th:nth-child(2),.jtable td:nth-child(2){display:none}
  .wtable .w-fta,.wtable .w-cases{display:none}
  .wtable{font-size:.75rem}
  .wtable th,.wtable td{padding:8px 4px!important}
  .how-grid{grid-template-columns:1fr!important}
  section#method{padding:36px 16px}
  footer{padding:28px 16px;font-size:.75rem}
}
</style>
</head>
<body>
<nav><div class="ni"><div class="logo">Judge<span>Search</span>.us</div><div class="nl"><a href="#map-box">Map</a><a href="#worst50" style="color:var(--red)">Worst 50</a><a href="#method">About</a><a href="#sources">Data Sources</a><a href="https://free.law/about/" target="_blank">Free Law Project</a></div></div></nav>

<section class="hero">
<h1>Know your judges.<br><em>Hold them accountable.</em></h1>
<p>Search real court records across 8 U.S. cities. See how often defendants under each judge <strong>didn't show up to court</strong>, got <strong>arrested again while waiting for trial</strong>, or had their <strong>release conditions taken away</strong>. All public data — no sign-up needed.</p>
</section>

<div class="wrap">
<div class="map-box" id="map-box">
<div id="leaflet-map"></div>
<div class="map-legend">
<span><span class="ml-dot" style="background:var(--gold)"></span> Live case data</span>
<span><span class="ml-dot" style="background:var(--t3)"></span> Bio data only</span>
</div>
</div>

<div class="pills" id="pills"></div>
<div class="sbar" id="sbar"></div>
<div class="src-banner" id="srcban"></div>
</div>

<div class="wrap" id="results">
<div class="empty"><h3>Select a city to view judges</h3><p>Click a city on the map or choose from the buttons above.</p></div>
</div>

<section style="padding:50px 24px;border-top:1px solid var(--b);background:linear-gradient(180deg,#0a0a0a,#140a0a)" id="worst50">
<div style="max-width:1000px;margin:0 auto">
<div style="text-align:center;margin-bottom:8px"><span style="font-family:var(--mono);font-size:.72rem;color:var(--red);letter-spacing:.12em;padding:4px 12px;background:rgba(232,64,64,.08);border:1px solid rgba(232,64,64,.3);border-radius:20px">NATIONAL ACCOUNTABILITY INDEX</span></div>
<h2 style="font-family:var(--serif);font-size:1.8rem;text-align:center;margin:12px 0 8px 0">The 50 Worst Judges in America</h2>
<p style="text-align:center;color:var(--t2);font-size:.95rem;margin-bottom:24px;max-width:720px;margin-left:auto;margin-right:auto">Ranked by <strong style="color:var(--t)">Danger Score</strong> — a composite of rearrest rate, missed-court rate, and release-revocation rate, weighted by case volume. Only includes judges in cities whose public records actually measure rearrests while defendants are out on pretrial release.</p>

<div id="worst50-list">
<div class="empty" style="padding:30px 20px"><div class="spin" style="margin:0 auto 12px"></div><p>Loading national rankings...</p></div>
</div>

<div id="worst50-meta" style="margin-top:18px;font-family:var(--mono);font-size:.72rem;color:var(--t3);text-align:center;line-height:1.6"></div>

<div style="margin-top:24px;padding:16px 20px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r);color:var(--t2);font-size:.82rem;line-height:1.6">
<strong style="color:var(--gold)">How to read this list.</strong> A high danger score means the judge has released defendants who often went on to be arrested again or skip court while their original case was still open. The list will grow as more cities publish per-judge rearrest data. Jurisdictions that only publish convictions or transfers (not rearrests) are excluded from the ranking because their data can't be compared apples-to-apples. See the <a href="#sources" style="color:var(--gold)">Data Sources</a> section for which cities currently qualify.
</div>
</div>
</section>

<section style="padding:50px 24px;border-top:1px solid var(--b);background:var(--s)" id="method">
<div style="max-width:900px;margin:0 auto">
<h2 style="font-family:var(--serif);font-size:1.6rem;text-align:center;margin-bottom:8px">About JudgeSearch</h2>
<p style="text-align:center;color:var(--t2);font-size:.95rem;margin-bottom:36px">Public court records, explained in plain English. No spin, no agenda — just the numbers.</p>
<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:40px" class="how-grid">
<div style="padding:24px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-family:var(--serif);font-size:1.8rem;font-weight:800;color:var(--gold);margin-bottom:6px">01</div>
<h3 style="font-family:var(--serif);font-size:1rem;margin-bottom:8px">Where the data comes from</h3>
<p style="color:var(--t2);font-size:.88rem">Case records pulled directly from county open data portals and state court APIs — the same databases used by reporters and researchers. No scraping. No guessing. Official public records only.</p>
</div>
<div style="padding:24px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-family:var(--serif);font-size:1.8rem;font-weight:800;color:var(--gold);margin-bottom:6px">02</div>
<h3 style="font-family:var(--serif);font-size:1rem;margin-bottom:8px">What the numbers mean</h3>
<p style="color:var(--t2);font-size:.88rem"><strong style="color:var(--t)">Missed court</strong> means the defendant didn't show up to their hearing. <strong style="color:var(--t)">Rearrested</strong> means they were picked up again while still waiting for trial. <strong style="color:var(--t)">Release revoked</strong> means the judge took away their freedom before their case was resolved. These are real outcomes — not predictions.</p>
</div>
<div style="padding:24px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-family:var(--serif);font-size:1.8rem;font-weight:800;color:var(--gold);margin-bottom:6px">03</div>
<h3 style="font-family:var(--serif);font-size:1rem;margin-bottom:8px">How judges are compared</h3>
<p style="color:var(--t2);font-size:.88rem">Each judge's outcomes are compared to every other judge in the same city. A red bar means that judge's rate is higher than average for a bad outcome. Green means lower. Cases are grouped by the judge named on the court record — no sampling, no modeling.</p>
</div>
</div>
<div style="padding:28px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r);margin-bottom:16px">
<h3 style="font-family:var(--serif);font-size:1rem;margin-bottom:10px;color:var(--gold)">What JudgeSearch is not</h3>
<p style="color:var(--t2);font-size:.88rem;line-height:1.7">This tool does not predict outcomes or tell you what a judge will do. It does not account for the types of cases a judge handles (a judge who hears more serious cases may see more missed court dates). It does not measure whether a judge's decisions were legally correct. It shows you the public record — what actually happened in cases that came before each judge. You decide what it means.</p>
</div>
<div style="padding:28px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r)">
<h3 style="font-family:var(--serif);font-size:1rem;margin-bottom:10px;color:var(--gold)">Why this matters</h3>
<p style="color:var(--t2);font-size:.88rem;line-height:1.7">Judges make decisions every day that affect people's lives — bail, detention, sentencing. But most people have no idea who their judge is or what their track record looks like. This site exists to change that. If you're a defendant, a family member, a journalist, or just a citizen who cares about how courts work in your city, this is for you.</p>
</div>
</div>
</section>

<section style="padding:40px 24px;border-top:1px solid var(--b)" id="sources">
<div style="max-width:950px;margin:0 auto">
<h2 style="font-family:var(--serif);font-size:1.4rem;margin-bottom:8px">Data Sources</h2>
<p style="color:var(--t2);font-size:.9rem;margin-bottom:24px">All data is pulled directly from official government open-data portals or non-profit legal databases. Nothing is scraped; nothing is proprietary. You can verify every number yourself.</p>

<div style="display:grid;grid-template-columns:repeat(2,1fr);gap:12px" class="how-grid">

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">MIAMI · FLORIDA</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge pretrial outcomes</strong> — missed court, rearrests, release revocations.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Source: <a href="https://courtwatch.us" target="_blank" style="color:var(--gold)">CourtWatch.us</a> (FSS 907.043 public disclosure law)</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">CHICAGO · COOK COUNTY</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge case dispositions</strong> — bench warrants, convictions, acquittals across 700K+ criminal cases.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Source: <a href="https://datacatalog.cookcountyil.gov/Courts/Sentencing/tg8v-tm6u" target="_blank" style="color:var(--gold)">Cook County Open Data Portal</a> (Socrata API, dataset <code style="font-family:var(--mono)">apwk-dzx8</code>)</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">ATLANTA · FULTON COUNTY</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge case dispositions + pending caseload</strong> — 91K+ cases from Fulton County Superior Court.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Source: <a href="https://sharefulton.fultoncountyga.gov" target="_blank" style="color:var(--gold)">ShareFulton</a> (Socrata API, datasets <code style="font-family:var(--mono)">uww8-gu28</code> + <code style="font-family:var(--mono)">dg7p-62bk</code>)</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">SAN FRANCISCO · SF COUNTY</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge DA case resolutions</strong> — 12K+ case outcomes from the SF District Attorney.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Source: <a href="https://data.sfgov.org/Public-Safety/District-Attorney-Actions-Taken-on-Arrests-Presented/czsm-3ei3" target="_blank" style="color:var(--gold)">DataSF</a> (Socrata API) + <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a> judicial bios</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">HOUSTON · HARRIS COUNTY</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge criminal case outcomes</strong> — ~2K cases from Harris County criminal courts.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Source: <a href="https://jpwebsite.harriscountytx.gov/PublicExtracts/search.jsp" target="_blank" style="color:var(--gold)">Harris County JP Public Data Extract</a> + <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a> judicial bios</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">NEW YORK · NYC</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge opinion counts</strong> (CourtListener text search) + NYPD city-wide arrest totals. NY State courts do not publish judge-keyed dockets publicly.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Sources: <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a> (Free Law Project), <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-/uip8-fykc" target="_blank" style="color:var(--gold)">NYC OpenData</a> (<code style="font-family:var(--mono)">uip8-fykc</code>)</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">LOS ANGELES · LA COUNTY</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge opinion counts</strong> (CourtListener text search) + LA city-wide arrest totals. LA Superior Court does not publish judge-keyed dockets publicly.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Sources: <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a>, <a href="https://data.lacity.org/Public-Safety/Arrest-Data-from-2020-to-Present/amvf-fr72" target="_blank" style="color:var(--gold)">LA OpenData</a> (<code style="font-family:var(--mono)">amvf-fr72</code>)</p>
</div>

<div style="padding:18px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-size:.75rem;color:var(--gold);font-family:var(--mono);letter-spacing:.05em;margin-bottom:6px">SEATTLE · KING COUNTY</div>
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge opinion counts</strong> (CourtListener text search) + King County jail booking totals. WA Superior Court does not publish judge-keyed dockets publicly.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0">Sources: <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a>, <a href="https://data.kingcounty.gov/Equity-Justice/Adult-Jail-Booking-/j56h-zgnm" target="_blank" style="color:var(--gold)">King County OpenData</a> (<code style="font-family:var(--mono)">j56h-zgnm</code>)</p>
</div>

</div>

<div style="margin-top:28px;padding:20px;background:var(--s2);border:1px solid var(--b2);border-radius:var(--r)">
<h3 style="font-family:var(--serif);font-size:1rem;margin-bottom:8px;color:var(--gold)">How the data flows</h3>
<p style="color:var(--t2);font-size:.85rem;line-height:1.6;margin-bottom:8px"><strong style="color:var(--t)">1. Daily refresh</strong> at 06:00 UTC via Cloudflare cron. Each city's scraper pulls directly from its source, aggregates by judge name, and writes to Cloudflare R2 storage.</p>
<p style="color:var(--t2);font-size:.85rem;line-height:1.6;margin-bottom:8px"><strong style="color:var(--t)">2. Protective merge</strong> — if a source feed is down or returns fewer cases than before, the existing data is kept and flagged with <em>"Data may be outdated"</em>. Good data is never overwritten with zeros.</p>
<p style="color:var(--t2);font-size:.85rem;line-height:1.6;margin-bottom:8px"><strong style="color:var(--t)">3. Judge bios</strong> (dates, education, appointer, political affiliation) come from <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a>, a non-profit legal database run by <a href="https://free.law/about/" target="_blank" style="color:var(--gold)">Free Law Project</a>.</p>
<p style="color:var(--t2);font-size:.85rem;line-height:1.6;margin:0"><strong style="color:var(--t)">4. Labels</strong> — all jargon is translated to plain English on read (e.g. "FTA" becomes "Missed Court", "Acquittal" becomes "Not Guilty"). You can click any data source link above to verify the raw numbers yourself.</p>
</div>

<div style="margin-top:16px;padding:16px 20px;background:rgba(232,64,64,.04);border:1px solid var(--b);border-radius:var(--r)">
<p style="color:var(--t2);font-size:.82rem;line-height:1.55;margin:0"><strong style="color:var(--t)">What we don't have.</strong> LA Superior Court, King County WA Superior Court, and the NY State Unified Court System do not publish judge-keyed case dockets via any public API. For those cities we show real CourtListener opinion counts (appellate decisions that cite each judge) plus real city-wide arrest totals from the cities' own open-data portals. Per-trial-court-judge data for these three cities would require paid services like <a href="https://trellis.law" target="_blank" style="color:var(--gold)">Trellis.law</a> or direct FOIA requests to each court clerk.</p>
</div>

</div>
</section>

<footer>
<p>Public court data · Built with <a href="https://free.law/about/" target="_blank">Free Law Project</a> data · <a href="#method">About</a> · <a href="#sources">Sources</a></p>
<p style="margin-top:8px;font-size:.72rem;color:var(--t3)">Not legal advice. All data is public record. &copy; 2025 JudgeSearch</p>
</footer>

<script>
const $=id=>document.getElementById(id);
const CITIES=[
  {slug:'miami',label:'Miami',lat:25.76,lng:-80.19,live:true},
  {slug:'chicago',label:'Chicago',lat:41.88,lng:-87.63,live:true},
  {slug:'atlanta',label:'Atlanta',lat:33.75,lng:-84.39,live:true},
  {slug:'san-francisco',label:'San Francisco',lat:37.77,lng:-122.42,live:true},
  {slug:'texas',label:'Houston, TX',lat:29.76,lng:-95.37,live:true},
  {slug:'new-york',label:'New York',lat:40.71,lng:-74.01,live:false},
  {slug:'los-angeles',label:'Los Angeles',lat:34.05,lng:-118.24,live:false},
  {slug:'seattle',label:'Seattle',lat:47.61,lng:-122.33,live:false},
];

// Pills
$('pills').innerHTML=CITIES.map(c=>
  '<button class="pill'+(c.live?' live':'')+'" data-slug="'+c.slug+'" onclick="loadCity(this)">'+c.label+'</button>'
).join('');

// Leaflet map
let map=L.map('leaflet-map',{scrollWheelZoom:false}).setView([39,-96],4);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{
  attribution:'&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
  subdomains:'abcd',maxZoom:18
}).addTo(map);
CITIES.forEach(c=>{
  const color=c.live?'#c8a84b':'#555';
  const icon=L.divIcon({className:'',html:'<div style="width:14px;height:14px;border-radius:50%;background:'+color+';border:2px solid '+(c.live?'#e0c96a':'#666')+';box-shadow:0 0 8px '+color+'"></div>',iconSize:[14,14],iconAnchor:[7,7]});
  L.marker([c.lat,c.lng],{icon}).addTo(map)
    .bindTooltip(c.label,{direction:'top',offset:[0,-10]})
    .on('click',()=>{
      const pill=document.querySelector('[data-slug="'+c.slug+'"]');
      if(pill)loadCity(pill);
    });
});

function loadCity(el){
  const slug=el.dataset.slug;
  document.querySelectorAll('.pill').forEach(p=>p.classList.remove('on'));
  el.classList.add('on');
  const c=CITIES.find(x=>x.slug===slug);
  if(c)map.flyTo([c.lat,c.lng],7,{duration:1});
  fetchCity(slug);
}

async function fetchCity(slug){
  const area=$('results');
  area.innerHTML='<div class="loading"><div class="spin"></div><p>Loading judges...</p></div>';
  $('sbar').style.display='none';
  $('srcban').style.display='none';
  try{
    const res=await fetch('/api/city?slug='+slug);
    if(!res.ok)throw new Error('Data unavailable');
    render(await res.json());
  }catch(e){
    area.innerHTML='<div class="empty"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
  }
}

// National Accountability Index — 50 worst judges in America
async function fetchWorst50(){
  const area=$('worst50-list');
  const meta=$('worst50-meta');
  try{
    const res=await fetch('/api/worst?n=50');
    if(!res.ok)throw new Error('Rankings unavailable');
    const d=await res.json();
    if(!d.judges||d.judges.length===0){
      area.innerHTML='<div class="empty" style="padding:30px 20px"><h3 style="color:var(--orange)">Not enough data yet</h3><p>Ranking requires cities with per-judge rearrest data. '+d.cities_with_rearrest_data+' of '+d.cities_checked+' cities currently qualify.</p></div>';
      return;
    }
    let h='<div style="overflow-x:auto"><table class="wtable" style="width:100%;border-collapse:collapse;font-family:var(--sans);font-size:.85rem">';
    h+='<thead><tr style="border-bottom:2px solid var(--b2);text-align:left">';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;width:40px">#</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em">JUDGE</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em" class="w-city">CITY</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right" class="w-cases">CASES</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right">REARREST</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right" class="w-fta">MISSED</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right">DANGER</th>';
    h+='</tr></thead><tbody>';
    for(const j of d.judges){
      const rearrPct=(j.rearrest_rate*100).toFixed(1);
      const ftaPct=(j.fta_rate*100).toFixed(1);
      const rankColor=j.rank<=10?'var(--red)':j.rank<=25?'var(--orange)':'var(--gold)';
      h+='<tr style="border-bottom:1px solid var(--b)">';
      h+='<td style="padding:10px 8px;font-family:var(--serif);font-weight:800;font-size:1rem;color:'+rankColor+'">'+j.rank+'</td>';
      h+='<td style="padding:10px 8px"><div style="font-weight:600;color:var(--t)">'+esc(j.name)+'</div><div style="color:var(--t3);font-size:.72rem">'+esc(j.court)+'</div></td>';
      h+='<td style="padding:10px 8px;color:var(--t2)" class="w-city">'+esc(j.city)+', '+esc(j.state)+'</td>';
      h+='<td style="padding:10px 8px;text-align:right;color:var(--t2);font-variant-numeric:tabular-nums" class="w-cases">'+j.total_cases.toLocaleString()+'</td>';
      h+='<td style="padding:10px 8px;text-align:right;color:var(--red);font-weight:600;font-variant-numeric:tabular-nums">'+rearrPct+'%</td>';
      h+='<td style="padding:10px 8px;text-align:right;color:var(--orange);font-variant-numeric:tabular-nums" class="w-fta">'+ftaPct+'%</td>';
      h+='<td style="padding:10px 8px;text-align:right;font-family:var(--serif);font-weight:800;color:'+rankColor+';font-size:1rem">'+j.danger_score.toFixed(1)+'</td>';
      h+='</tr>';
    }
    h+='</tbody></table></div>';
    area.innerHTML=h;
    meta.innerHTML='Showing '+d.returned+' of '+d.total_qualified_judges+' qualified judges · '+d.cities_with_rearrest_data+'/'+d.cities_checked+' cities have rearrest data · Min '+d.min_cases+' cases to qualify · Generated '+new Date(d.generated_at).toLocaleString();
  }catch(e){
    area.innerHTML='<div class="empty" style="padding:30px 20px"><h3 style="color:var(--red)">Error</h3><p>'+esc(e.message)+'</p></div>';
  }
}

// Load rankings on page ready
fetchWorst50();

function render(d){
  // Per-city metric labels with fallbacks
  const lbl=d.metric_labels||{
    fta:'Missed Court',rearrest:'Rearrested',revocation:'Release Revoked',
    fta_bar:"Missed Court Date (Didn't Show Up)",rearrest_bar:'Rearrested While Awaiting Trial',revocation_bar:'Pretrial Release Revoked',
    fta_bad:true,rearrest_bad:true,revocation_bad:true
  };
  const col2=lbl.rearrest_bad?'var(--orange)':'var(--green)';
  const col3=lbl.revocation_bad?'var(--red)':'var(--green)';

  // Stats bar
  const sb=$('sbar');
  sb.innerHTML='<div class="sc"><div class="n">'+d.judges.length+'</div><div class="l">Judges</div></div>'+
    '<div class="sc"><div class="n">'+d.total_cases.toLocaleString()+'</div><div class="l">Total Cases</div></div>'+
    '<div class="sc"><div class="n red">'+d.total_fta.toLocaleString()+'</div><div class="l">'+esc(lbl.fta)+'</div></div>'+
    '<div class="sc"><div class="n" style="color:'+col2+'">'+d.total_rearrests.toLocaleString()+'</div><div class="l">'+esc(lbl.rearrest)+'</div></div>'+
    '<div class="sc"><div class="n" style="color:'+col3+'">'+d.total_revocations.toLocaleString()+'</div><div class="l">'+esc(lbl.revocation)+'</div></div>';
  sb.style.display='flex';

  // Source — show last-successful-fresh-data date; flag if stale
  const bn=$('srcban');
  const freshDate=d.last_fresh_data?new Date(d.last_fresh_data).toLocaleDateString():null;
  const updDate=new Date(d.last_updated).toLocaleDateString();
  if(d.total_cases>0){
    let html='<strong style="color:var(--gold)">'+esc(d.source)+'</strong> &mdash; Last refreshed: '+(freshDate||updDate);
    if(d.is_stale){
      html+=' <span style="color:var(--orange);font-weight:600">(Data may be outdated — source feed temporarily unavailable)</span>';
    }
    bn.innerHTML=html;
  }else if(d.city_stats){
    bn.innerHTML='<strong style="color:var(--gold)">City-wide public data</strong> &mdash; '+esc(d.city_stats.source||'')+' &mdash; Last refreshed: '+updDate;
  }else{
    bn.innerHTML='<strong style="color:var(--orange)">Biographical data only</strong> &mdash; Case outcome scrapers in development for '+esc(d.state);
  }
  bn.style.display='block';

  // City-wide stats panel — shown when per-judge case data isn't available
  // but city-wide arrest/booking data is (LA, Seattle, NY).
  if(d.total_cases===0&&d.city_stats){
    const cs=d.city_stats;
    const stats=[];
    if(cs.annual_arrests){
      stats.push('<div class="sc"><div class="n">'+cs.annual_arrests.toLocaleString()+'</div><div class="l">'+esc(cs.label||'Arrests')+'</div></div>');
    }
    if(cs.annual_felony_arrests){
      stats.push('<div class="sc"><div class="n red">'+cs.annual_felony_arrests.toLocaleString()+'</div><div class="l">Felony Arrests</div></div>');
    }
    stats.push('<div class="sc"><div class="n">'+d.judges.length+'</div><div class="l">Judges on Record</div></div>');
    sb.innerHTML=stats.join('');
    sb.style.display='flex';
  }

  // Calculate city-wide average rates
  const live=d.judges.filter(j=>j.total_cases>0);
  const cityTotalCases=live.reduce((s,j)=>s+j.total_cases,0);
  const cityTotalRearrests=live.reduce((s,j)=>s+j.rearrest_count,0);
  const cityRearrestRate=cityTotalCases?cityTotalRearrests/cityTotalCases:0;
  const cityFtaRate=cityTotalCases?live.reduce((s,j)=>s+j.fta_count,0)/cityTotalCases:0;
  const cityRevocRate=cityTotalCases?live.reduce((s,j)=>s+j.revocation_count,0)/cityTotalCases:0;

  // Does this city's "rearrest" field actually measure rearrests-while-out
  // (vs convictions/transfers)? Only Miami measures true rearrest rate.
  const measuresRearrest=/rearrest/i.test(lbl.rearrest||'')||/rearrest/i.test(lbl.rearrest_bar||'');

  // Top judges as baseball cards (show first 12)
  const topJudges=d.judges.slice(0,12);
  const restJudges=d.judges.slice(12);

  const area=$('results');
  let h='<h2 style="font-family:var(--serif);font-size:1.3rem;margin-bottom:16px">'+d.judges.length+' Judges &mdash; '+esc(d.city)+', '+esc(d.state)+'</h2>';

  // City-wide context note (LA/Seattle/NY — where we have real arrest totals but
  // not per-judge data).
  if(cityTotalCases===0&&d.city_stats&&d.city_stats.note){
    h+='<div style="background:rgba(200,168,75,.06);border:1px solid var(--b2);border-radius:var(--r);padding:16px 20px;margin-bottom:24px;color:var(--t2);font-size:.85rem;line-height:1.5">';
    h+='<strong style="color:var(--gold)">Why city-wide only?</strong> '+esc(d.city_stats.note);
    h+='</div>';
  }

  // City-level detention prevention summary — only shown when rearrest data is real
  if(measuresRearrest&&cityTotalCases>0){
    const pct=(cityRearrestRate*100).toFixed(1);
    h+='<div style="background:linear-gradient(135deg,rgba(232,64,64,.08),rgba(200,168,75,.06));border:1px solid var(--b2);border-radius:var(--r);padding:20px 22px;margin-bottom:24px">';
    h+='<div style="font-family:var(--mono);font-size:.7rem;color:var(--gold);letter-spacing:.08em;margin-bottom:8px">THE DETENTION-vs-RELEASE TRADE-OFF</div>';
    h+='<div style="display:grid;grid-template-columns:auto 1fr;gap:20px;align-items:center" class="prev-grid">';
    h+='<div style="font-family:var(--serif);font-size:3rem;font-weight:800;color:var(--red);line-height:1">'+pct+'%</div>';
    h+='<div style="color:var(--t2);font-size:.9rem;line-height:1.55">of defendants released by these judges were <strong style="color:var(--t)">arrested again</strong> while their original case was still open. That is <strong style="color:var(--t)">'+cityTotalRearrests.toLocaleString()+' new crimes</strong> out of '+cityTotalCases.toLocaleString()+' release decisions — crimes that would not have occurred if those defendants had been detained until trial.</div>';
    h+='</div>';
    h+='<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--b);color:var(--t3);font-size:.75rem;font-style:italic">Numbers reflect what actually happened in the public record. They do not account for wrongful arrests, ability to pay bail, or the cost of detention. Judges make release decisions one case at a time based on individual circumstances.</div>';
    h+='</div>';
  }
  h+='<div class="cards">';

  for(const j of topJudges){
    const initials=(j.name||'').split(' ').map(w=>(w[0]||'')).join('').slice(0,2);
    const has=j.total_cases>0;

    // Per-judge rates
    const rearrRate=has?j.rearrest_count/j.total_cases:0;
    const ftaRate=has?j.fta_count/j.total_cases:0;
    const revocRate=has?j.revocation_count/j.total_cases:0;

    // Compare to city average (positive = above avg for that metric)
    const rearrVs=has&&cityRearrestRate>0?((rearrRate-cityRearrestRate)/cityRearrestRate*100):0;
    const ftaVs=has&&cityFtaRate>0?((ftaRate-cityFtaRate)/cityFtaRate*100):0;
    const revocVs=has&&cityRevocRate>0?((revocRate-cityRevocRate)/cityRevocRate*100):0;

    // For bad metrics, above-avg = red warning; for good metrics, above-avg = green
    const rearrAbove=lbl.rearrest_bad?rearrVs>15:rearrVs<-15;
    const ftaAbove=ftaVs>15;
    const revocAbove=lbl.revocation_bad?revocVs>15:revocVs<-15;

    h+='<div class="bcard">';
    // Header
    h+='<div class="bcard-head"><div class="bcard-av">'+esc(initials)+'</div><div style="flex:1;min-width:0">';
    h+='<div class="bcard-name">'+esc(j.name)+'</div>';
    h+='<div class="bcard-court">'+esc(j.court)+'</div>';
    h+='<div class="bcard-loc">'+esc(j.city)+', '+esc(j.state)+'</div>';
    h+='</div>';
    // Prominent rearrest % for cities with real rearrest data
    if(has&&measuresRearrest){
      const pct=(rearrRate*100).toFixed(1);
      const color=rearrRate>0.15?'var(--red)':rearrRate>0.08?'var(--orange)':'var(--green)';
      h+='<div style="text-align:right;padding-left:10px;border-left:1px solid var(--b);min-width:78px">';
      h+='<div style="font-family:var(--serif);font-size:1.6rem;font-weight:800;color:'+color+';line-height:1">'+pct+'%</div>';
      h+='<div style="font-size:.6rem;color:var(--t3);font-family:var(--mono);letter-spacing:.05em;margin-top:2px">REARRESTED</div>';
      h+='</div>';
    }
    h+='</div>';

    // Stats row
    h+='<div class="bcard-stats">';
    h+='<div class="bs"><div class="bv w">'+j.total_cases.toLocaleString()+'</div><div class="bl">Cases</div></div>';
    h+='<div class="bs"><div class="bv r">'+j.fta_count.toLocaleString()+'</div><div class="bl">'+esc(lbl.fta)+'</div></div>';
    h+='<div class="bs"><div class="bv" style="color:'+col2+'">'+j.rearrest_count.toLocaleString()+'</div><div class="bl">'+esc(lbl.rearrest)+'</div></div>';
    h+='<div class="bs"><div class="bv" style="color:'+col3+'">'+j.revocation_count.toLocaleString()+'</div><div class="bl">'+esc(lbl.revocation)+'</div></div>';
    h+='</div>';

    // Rate bars (only if has data)
    if(has){
      h+='<div class="bcard-rates">';
      h+=rateBar(lbl.fta_bar,ftaRate,ftaVs,ftaAbove,'var(--red)');
      h+=rateBar(lbl.rearrest_bar,rearrRate,rearrVs,rearrAbove,col2);
      h+=rateBar(lbl.revocation_bar,revocRate,revocVs,revocAbove,col3);
      h+='</div>';
    }else{
      h+='<div class="nodata">Case outcome data pending</div>';
    }
    h+='</div>';
  }
  h+='</div>';

  // Remaining judges as compact table
  if(restJudges.length>0){
    h+='<div class="more-judges">';
    h+='<button class="more-toggle" onclick="document.querySelector(\\'.jtable\\').classList.toggle(\\'.show\\');this.textContent=this.textContent.includes(\\'Show\\')?\\'\u25B2 Hide\\':(\\'\\u25BC Show '+restJudges.length+' More Judges\\')">\\u25BC Show '+restJudges.length+' More Judges</button>';
    h+='<table class="jtable">';
    h+='<thead><tr><th>Name</th><th>Court</th><th class="num">Cases</th><th class="num">'+esc(lbl.fta)+'</th><th class="num">'+esc(lbl.rearrest)+'</th><th class="num">'+esc(lbl.revocation)+'</th><th class="num">Rate %</th></tr></thead>';
    h+='<tbody>';
    for(const j of restJudges){
      const cr=j.total_cases>0?Math.round(j.rearrest_count/j.total_cases*100):0;
      h+='<tr><td>'+esc(j.name)+'</td><td style="color:var(--t2);font-size:.8rem">'+esc(j.court)+'</td>';
      h+='<td class="num">'+j.total_cases.toLocaleString()+'</td>';
      h+='<td class="num" style="color:var(--red)">'+j.fta_count.toLocaleString()+'</td>';
      h+='<td class="num" style="color:var(--orange)">'+j.rearrest_count.toLocaleString()+'</td>';
      h+='<td class="num" style="color:var(--green)">'+j.revocation_count.toLocaleString()+'</td>';
      h+='<td class="num" style="color:var(--orange)">'+cr+'%</td></tr>';
    }
    h+='</tbody></table></div>';
  }

  area.innerHTML=h;

  // Fix the toggle button
  const tog=document.querySelector('.more-toggle');
  if(tog){
    tog.onclick=function(){
      const tbl=document.querySelector('.jtable');
      if(tbl){
        tbl.classList.toggle('show');
        this.textContent=tbl.classList.contains('show')?'\\u25B2 Hide remaining judges':'\\u25BC Show '+restJudges.length+' more judges';
      }
    };
  }
}

function rateBar(label,rate,vsAvg,isAbove,color){
  const pct=Math.round(rate*100);
  const barW=Math.min(100,pct*2); // Scale: 50% fills the bar
  // isAbove: caller already determined if this is an above-avg-bad situation
  const vsClass=isAbove?'above':Math.abs(vsAvg)<5?'avg':'below';
  const vsText=Math.abs(vsAvg)<5?'avg':vsAvg>0?'+'+Math.round(vsAvg)+'%':Math.round(vsAvg)+'%';
  return '<div class="rate-row">'
    +'<span class="rate-label">'+esc(label)+'</span>'
    +'<div class="rate-bar"><div class="rate-fill" style="width:'+barW+'%;background:'+color+'"></div></div>'
    +'<span class="rate-pct" style="color:'+color+'">'+pct+'%</span>'
    +'<span class="rate-vs '+vsClass+'">'+vsText+' vs avg</span>'
    +'</div>';
}

function esc(s){return s?String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'):''}
</script>
</body>
</html>`;

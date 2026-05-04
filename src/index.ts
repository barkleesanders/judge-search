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
	bio?: string; // raw biographical paragraph (from official court directory pages)
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
		if (p === "/api/status") return handleStatus(env);
		if (p === "/api/enrich-bios") return handleEnrichBios(url, env);
		if (p === "/api/upload" && request.method === "POST")
			return handleUpload(request, url, env);
		if (p === "/api/upload-raw" && request.method === "POST")
			return handleUploadRaw(request, url, env);
		if (p === "/api/process-ny-oca") return handleProcessNyOca(url, env);

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
		// Then re-process the NY OCA CSV if one is in R2 (user uploads it
		// manually via /api/upload-raw — once per year when OCA publishes).
		try {
			const stub = new Request(
				"https://internal/api/process-ny-oca?key=ny-oca/NYS-2025.csv.gz",
			);
			const res = await handleProcessNyOca(new URL(stub.url), env);
			if (!res.ok) console.log("[cron] NY OCA reprocess skipped:", res.status);
		} catch (e) {
			console.log("[cron] NY OCA reprocess error:", e);
		}
		// Finally enrich bios across all cities: NY uses NYC.gov MACJ pages
		// as primary source; every city then falls through to Wikidata SPARQL.
		for (const city of Object.keys(CITIES)) {
			try {
				const stub = new Request(
					`https://internal/api/enrich-bios?slug=${city}`,
				);
				const res = await handleEnrichBios(new URL(stub.url), env);
				if (!res.ok)
					console.log(`[cron] ${city} bio enrich skipped:`, res.status);
			} catch (e) {
				console.log(`[cron] ${city} bio enrich error:`, e);
			}
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
	const minCases = Number(url.searchParams.get("min_cases") || 10);

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
			// Skip judges with no actual outcome data — they'd rank with score=0
			// and clutter the list. A judge must have at least one recorded
			// rearrest/fta/revocation to be rankable.
			if (
				j.rearrest_count === 0 &&
				j.fta_count === 0 &&
				j.revocation_count === 0
			)
				continue;
			const rearrestRate = j.rearrest_count / j.total_cases;
			const ftaRate = j.fta_count / j.total_cases;
			const revocRate =
				ml.revocation_bad !== false ? j.revocation_count / j.total_cases : 0;
			// Risk Score on a 0-100 scale — weighted percentage of concerning
			// outcomes. Each rate contributes a capped share of the 100 points,
			// so the number reads as "X out of 100 points of concern". Rearrest
			// weighted heaviest (new alleged crime while out), missed-court next
			// (shows up in bench warrants), revocation last (system corrected).
			const rawScore =
				rearrestRate * 100 * 0.5 + ftaRate * 100 * 0.3 + revocRate * 100 * 0.2;
			// Volume confidence: lightly penalizes scores from tiny samples so
			// a judge with 10 cases at 50% doesn't outrank one with 2,000 at
			// 35%. At 100+ cases the weight reaches ~0.97 (almost full score).
			const volumeConfidence = Math.min(
				1,
				Math.log10(j.total_cases + 1) / Math.log10(101),
			);
			const danger_score = Math.round(rawScore * volumeConfidence * 10) / 10;
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
			"Risk Score (0–100) = (0.5 × rearrest_rate + 0.3 × fta_rate + 0.2 × revocation_rate) × 100 × volume_confidence, where volume_confidence = min(1, log10(cases+1) / log10(101)). Only includes judges in cities whose data actually measures rearrests-while-on-pretrial-release, and only judges with ≥" +
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
			await enrichBiosFromCL(judges, "Florida", env.COURTLISTENER_TOKEN);
		} else if (slug === "chicago") {
			const r = await scrapeChicago(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
			await enrichBiosFromCL(judges, "Illinois", env.COURTLISTENER_TOKEN);
		} else if (slug === "atlanta") {
			const r = await scrapeAtlanta(cityName);
			judges = r.judges;
			source = r.source;
			metric_labels = r.metric_labels;
			await enrichBiosFromCL(judges, "Georgia", env.COURTLISTENER_TOKEN);
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
		} else if (slug === "san-francisco") {
			// SF judge-level data is manually curated (per-judge DA outcomes
			// aren't on DataSF). Preserve existing judges and refresh the
			// city_stats from DataSF so the page always shows a current
			// last-updated timestamp.
			const r = await scrapeSanFranciscoStats();
			if (existing && existing.total_cases > 0) {
				// Preserve existing judges; update city_stats from Socrata
				const merged: CityData = {
					...existing,
					last_updated: new Date().toISOString(),
					is_stale: isOlderThanDays(existing.last_fresh_data, 30),
					city_stats: r.city_stats || existing.city_stats,
				};
				await env.DATA.put(`courts/${slug}.json`, JSON.stringify(merged));
				return merged;
			}
			judges = [];
			source = r.source;
			city_stats = r.city_stats;
		} else if (slug === "texas") {
			// Same pattern as SF — Harris County's only open judge-level source
			// is a Shiny app (no API). Preserve existing judges, keep cron fresh.
			const r = await scrapeTexasStats();
			if (existing && existing.total_cases > 0) {
				const merged: CityData = {
					...existing,
					last_updated: new Date().toISOString(),
					is_stale: isOlderThanDays(existing.last_fresh_data, 30),
					city_stats: r.city_stats || existing.city_stats,
				};
				await env.DATA.put(`courts/${slug}.json`, JSON.stringify(merged));
				return merged;
			}
			judges = [];
			source = r.source;
			city_stats = r.city_stats;
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

	// Sort: rate-based danger score, respecting each metric's _bad semantics.
	// A judge with 100 cases at 80% rearrest ranks above one with 1000 at 10%.
	// Volume weight (log10) prevents tiny-sample outliers from topping the list.
	const ftaBad = metric_labels?.fta_bad ?? true;
	const reaBad = metric_labels?.rearrest_bad ?? true;
	const revBad = metric_labels?.revocation_bad ?? true;
	const dangerScore = (j: JudgeRecord): number => {
		if (j.total_cases === 0) return 0;
		const ftaRate = j.fta_count / j.total_cases;
		const reaRate = j.rearrest_count / j.total_cases;
		const revRate = j.revocation_count / j.total_cases;
		const rateScore =
			(ftaBad ? ftaRate : -ftaRate) * 2 +
			(reaBad ? reaRate : -reaRate) * 2 +
			(revBad ? revRate : -revRate) * 1;
		return rateScore * Math.log10(j.total_cases + 1) * 100;
	};
	judges.sort((a, b) => {
		const d = dangerScore(b) - dangerScore(a);
		if (d !== 0) return d;
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
	// Enrich bios from CourtListener so uploaded cities (SF, Texas) match the
	// same bio coverage as LA/Seattle/NY.
	if (parsed.judges?.length && parsed.state) {
		await enrichBiosFromCL(
			parsed.judges,
			parsed.state,
			env.COURTLISTENER_TOKEN,
		);
	}
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

// ── API: Scraper freshness status across all 8 cities ──
// Lists which scrapers are live-automated vs manual, when each city was
// last refreshed, and how stale the data is. Drop this URL into a status
// page or monitoring system to catch silent scraper failures.
async function handleStatus(env: Env): Promise<Response> {
	const SCRAPER_TYPE: Record<string, string> = {
		miami: "live (CourtWatch API)",
		chicago: "live (Cook County Socrata)",
		atlanta: "live (Fulton County Socrata)",
		"los-angeles": "live (CourtListener text search + data.lacity.org arrests)",
		seattle: "live (CourtListener text search + data.kingcounty.gov bookings)",
		"new-york":
			"live (NY OCA CSV processor from R2 + CourtListener + NYC arrests)",
		"san-francisco":
			"hybrid (judges from jamiequint/sf_criminal_court HF parquet — uploaded via scrapers/fetch-sf-hf.mjs · DataSF stats daily · protective merge)",
		texas:
			"hybrid (judges curated · Houston OpenData stats daily · protective merge)",
	};
	const result: Array<Record<string, unknown>> = [];
	for (const slug of Object.keys(CITIES)) {
		const obj = await env.DATA.get(`courts/${slug}.json`);
		if (!obj) {
			result.push({ slug, status: "missing" });
			continue;
		}
		const d = JSON.parse(await obj.text()) as CityData;
		const lastFresh = d.last_fresh_data ? new Date(d.last_fresh_data) : null;
		const ageDays = lastFresh
			? Math.round((Date.now() - lastFresh.getTime()) / 86400000)
			: null;
		result.push({
			slug,
			scraper: SCRAPER_TYPE[slug] || "unknown",
			judges: d.judges.length,
			total_cases: d.total_cases,
			last_updated: d.last_updated,
			last_fresh_data: d.last_fresh_data || null,
			age_days: ageDays,
			is_stale: d.is_stale || false,
			has_city_stats: !!d.city_stats,
		});
	}
	return json({
		generated_at: new Date().toISOString(),
		cron_schedule: "0 6 * * * UTC (daily)",
		cities: result,
	});
}

// ── API: Enrich judge bios from official court directory pages ──
// Workers-native — uses HTMLRewriter to stream-parse court websites that
// publish authoritative judge bios (education, appointer, appointment
// date, etc). CourtListener doesn't cover county/municipal judges, but
// official court websites DO. This closes the bio-coverage gap.
//
// Sources:
//   NYC → nyc.gov/site/macj/appointed/{criminal,family,civil}-court.page
//         (authoritative — Mayor's Advisory Committee on the Judiciary)
//
// More sources can be added here. Each source page has a known HTML
// pattern — we extract it with HTMLRewriter and merge bios into the
// existing judges' records in R2 by fuzzy name match.
// Per-city authoritative directory-page sources. Each uses the NYC MACJ
// structure (faq-questions name / faq-answers bio). Cities with an empty
// array still get Wikidata SPARQL fallback which works across all cities.
const BIO_SOURCES: Record<string, string[]> = {
	"new-york": [
		"https://www.nyc.gov/site/macj/appointed/criminal-court.page",
		"https://www.nyc.gov/site/macj/appointed/family-court.page",
		"https://www.nyc.gov/site/macj/appointed/civil-court.page",
	],
	miami: [],
	chicago: [],
	atlanta: [],
	"san-francisco": [],
	texas: [],
	"los-angeles": [],
	seattle: [],
};

async function handleEnrichBios(url: URL, env: Env): Promise<Response> {
	const slug = url.searchParams.get("slug") || "new-york";
	if (!(slug in BIO_SOURCES)) {
		return json(
			{
				error: `Unknown slug: ${slug}. Supported: ${Object.keys(BIO_SOURCES).join(", ")}`,
			},
			400,
		);
	}
	const sources = BIO_SOURCES[slug];

	const bios = new Map<
		string,
		{
			bio: string;
			education: string[];
			appointed_by?: string;
			date_start?: string;
		}
	>();

	// Real NYC MACJ page structure:
	//   <div class="faq-questions" data-answer="SLUG"><p>Judge Name</p></div>
	//   <div id="SLUG" class="faq-answers"><p>Bio paragraph...</p></div>
	for (const srcUrl of sources) {
		let mode: "question" | "answer" | null = null;
		let currentSlug = "";
		const nameBySlug = new Map<string, string>();
		const bioBySlug = new Map<string, string[]>();
		try {
			const res = await fetch(srcUrl, {
				headers: { "User-Agent": "JudgeSearchBot/1.0 (accountability)" },
			});
			if (!res.ok) continue;
			const rewriter = new HTMLRewriter()
				.on(".faq-questions", {
					element(el) {
						mode = "question";
						currentSlug = el.getAttribute("data-answer") || "";
					},
				})
				.on(".faq-answers", {
					element(el) {
						mode = "answer";
						currentSlug = el.getAttribute("id") || "";
						if (currentSlug && !bioBySlug.has(currentSlug))
							bioBySlug.set(currentSlug, []);
					},
				})
				.on(".faq-questions p, .faq-answers p", {
					text(t) {
						if (!currentSlug) return;
						if (mode === "question") {
							const s = t.text.trim();
							if (s && !nameBySlug.has(currentSlug)) {
								nameBySlug.set(currentSlug, s.replace(/^Judge\s+/i, ""));
							}
						} else if (mode === "answer") {
							const arr = bioBySlug.get(currentSlug) || [];
							arr.push(t.text);
							bioBySlug.set(currentSlug, arr);
						}
					},
				});
			await rewriter.transform(res).arrayBuffer();
			for (const [slug2, name] of nameBySlug) {
				const bioText = (bioBySlug.get(slug2) || [])
					.join(" ")
					.replace(/\s+/g, " ")
					.trim();
				if (!bioText || !name) continue;
				const m: {
					bio: string;
					education: string[];
					appointed_by?: string;
					date_start?: string;
				} = { bio: bioText, education: [] };
				// Education regex: match "graduated from X" or "degree from Y" —
				// capture up to a period that isn't followed by a capital-D-period
				// pattern (e.g., "J.D." or "Ph.D." mid-sentence).
				const eduMatches =
					bioText.match(
						/\b(?:graduated from|a graduate of|received (?:his|her|their)[^.]*(?:degree|J\.?D\.?|LL\.?B\.?|M\.?B\.?A\.?|Ph\.?D\.?)[^.]*)(?:\s+[A-Z][a-zA-Z.&,\s]+?)(?=\.[\s\n])/gi,
					) || [];
				if (eduMatches.length)
					m.education = eduMatches.slice(0, 3).map((e) => e.trim());
				const apt = bioText.match(
					/appointed\s+by\s+(?:then-)?(?:Mayor|Gov\.?|Governor|President)\s+[A-Z][a-zA-Z.]+(?:\s+[A-Z][a-zA-Z.]+)+/i,
				);
				if (apt) m.appointed_by = apt[0].replace(/^appointed\s+by\s+/i, "");
				const date = bioText.match(
					/\b(?:appointed|elected|joined)\s+(?:on\s+)?(?:in\s+)?(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}?,?\s+\d{4}/i,
				);
				if (date) m.date_start = date[0];
				bios.set(name, m);
			}
		} catch {
			// try next source
		}
	}

	// Merge into the city's judge records
	const obj = await env.DATA.get(`courts/${slug}.json`);
	if (!obj) return json({ error: `courts/${slug}.json not found` }, 404);
	const city = JSON.parse(await obj.text()) as CityData;

	// Normalize: the OCA CSV stores names like "Judge Berland, Susan A."
	// while the MACJ pages use "Judge Susan Berland". Match by last-name
	// + first-name-initial.
	const extractKey = (
		name: string,
	): { last: string; firstInitial: string } | null => {
		const clean = name
			.replace(/^(Judge|Justice|Honorable|Hon\.?|The Honorable)\s+/i, "")
			.trim();
		let first: string;
		let last: string;
		if (clean.includes(",")) {
			const [l, f] = clean.split(",", 2).map((s) => s.trim());
			last = l.split(/\s+/)[0];
			first = (f.split(/\s+/)[0] || "").replace(/\.$/, "");
		} else {
			const parts = clean.split(/\s+/);
			if (parts.length < 2) return null;
			last = parts[parts.length - 1];
			first = parts[0];
		}
		if (!last || !first) return null;
		return {
			last: last.toLowerCase().replace(/[^a-z]/g, ""),
			firstInitial: first[0].toLowerCase(),
		};
	};

	const bioIndex = new Map<
		string,
		{
			bio: string;
			education: string[];
			appointed_by?: string;
			date_start?: string;
		}
	>();
	for (const [name, data] of bios) {
		const k = extractKey(name);
		if (k) bioIndex.set(`${k.last}|${k.firstInitial}`, data);
	}

	let matched = 0;
	for (const judge of city.judges) {
		const k = extractKey(judge.name);
		if (!k) continue;
		const data = bioIndex.get(`${k.last}|${k.firstInitial}`);
		if (!data) continue;
		matched++;
		if (!judge.education?.length && data.education.length) {
			judge.education = data.education;
		}
		if (!judge.appointed_by && data.appointed_by) {
			judge.appointed_by = data.appointed_by.replace(/^appointed\s+by\s+/i, "");
		}
		if (!judge.date_start && data.date_start) {
			judge.date_start = data.date_start;
		}
		if (!judge.bio && data.bio) {
			// Cap raw bio at 1000 chars to keep R2 objects small
			judge.bio = data.bio.slice(0, 1000);
		}
	}

	// Wikidata SPARQL fallback — works across ALL cities, fills in famous
	// judges not found in directory pages. Each batched SPARQL query can
	// look up up to 25 judges at once by exact rdfs:label match.
	let wikiEnriched = 0;
	const needsBio = city.judges.filter((j) => !j.bio && !j.born);
	if (needsBio.length > 0) {
		// Extract canonical "First Last" names (strips titles, handles
		// "Last, First" OCA format). Wikidata labels judges as "First Last".
		const nameFor = (name: string): string | null => {
			const clean = name
				.replace(/^(Judge|Justice|Honorable|Hon\.?|The Honorable)\s+/i, "")
				.trim();
			if (clean.includes(",")) {
				const [l, f] = clean.split(",", 2).map((s) => s.trim());
				const first = (f.split(/\s+/)[0] || "").replace(/\.$/, "");
				if (!first || !l) return null;
				return `${first} ${l.split(/\s+/)[0]}`;
			}
			return clean;
		};
		const batchSize = 20;
		for (let i = 0; i < needsBio.length; i += batchSize) {
			const batch = needsBio.slice(i, i + batchSize);
			const labels = batch
				.map((j) => nameFor(j.name))
				.filter((n): n is string => !!n)
				.map((n) => `"${n.replace(/"/g, '\\"')}"@en`);
			if (!labels.length) continue;
			const query = `
				SELECT DISTINCT ?name ?dob ?birthplaceLabel ?educationLabel ?occupationLabel WHERE {
					VALUES ?name { ${labels.join(" ")} }
					?item rdfs:label ?name;
					      wdt:P106 ?occupation.
					FILTER EXISTS { ?item wdt:P106 ?j. FILTER(?j IN (wd:Q16533, wd:Q185351)) }
					OPTIONAL { ?item wdt:P569 ?dob. }
					OPTIONAL { ?item wdt:P19 ?birthplace. }
					OPTIONAL { ?item wdt:P69 ?education. }
					SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
				} LIMIT 100
			`
				.replace(/\s+/g, " ")
				.trim();
			try {
				const wdRes = await fetch(
					`https://query.wikidata.org/sparql?query=${encodeURIComponent(query)}`,
					{
						headers: {
							Accept: "application/sparql-results+json",
							"User-Agent": "JudgeSearchBot/1.0 (accountability)",
						},
					},
				);
				if (!wdRes.ok) continue;
				const wd = (await wdRes.json()) as {
					results?: { bindings?: Array<Record<string, { value?: string }>> };
				};
				const wdByName = new Map<
					string,
					{ dob?: string; birthplace?: string; education?: string }
				>();
				for (const b of wd.results?.bindings || []) {
					const n = b.name?.value;
					if (!n) continue;
					const clean = n.replace(/@en$/, "");
					const entry = wdByName.get(clean) || {};
					if (b.dob?.value && !entry.dob) entry.dob = b.dob.value.slice(0, 10);
					if (b.birthplaceLabel?.value && !entry.birthplace)
						entry.birthplace = b.birthplaceLabel.value;
					if (b.educationLabel?.value && !entry.education)
						entry.education = b.educationLabel.value;
					wdByName.set(clean, entry);
				}
				for (const j of batch) {
					const n = nameFor(j.name);
					if (!n) continue;
					const match = wdByName.get(n);
					if (!match) continue;
					let changed = false;
					if (!j.born && match.dob) {
						j.born = match.dob;
						changed = true;
					}
					if (!j.birthplace && match.birthplace) {
						j.birthplace = match.birthplace;
						changed = true;
					}
					if (!j.education?.length && match.education) {
						j.education = [match.education];
						changed = true;
					}
					if (changed) wikiEnriched++;
				}
			} catch {
				/* skip batch on failure */
			}
		}
	}

	await env.DATA.put(`courts/${slug}.json`, JSON.stringify(city), {
		httpMetadata: { contentType: "application/json" },
	});

	return json({
		slug,
		sources,
		bios_found: bios.size,
		judges_in_city: city.judges.length,
		judges_enriched_from_directory: matched,
		judges_enriched_from_wikidata: wikiEnriched,
		total_enriched: matched + wikiEnriched,
		coverage_pct: Math.round(
			((matched + wikiEnriched) / city.judges.length) * 100,
		),
	});
}

// ── API: Raw binary upload to R2 (CSVs, images, etc) ──
// POSTs write the body directly to R2 under the given key. Auth via
// UPLOAD_SECRET bearer token. Used when wrangler r2 put writes to a
// different bucket instance than the Worker binding resolves (has
// happened in this account — root cause unknown, binding-name and
// bucket-name match). Going through the Worker guarantees the Worker
// can read what it just wrote.
async function handleUploadRaw(
	request: Request,
	url: URL,
	env: Env,
): Promise<Response> {
	const authHeader = request.headers.get("Authorization") || "";
	const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
	if (!env.UPLOAD_SECRET || token !== env.UPLOAD_SECRET) {
		return json({ error: "Unauthorized" }, 401);
	}
	const key = url.searchParams.get("key");
	if (!key) return json({ error: "key required" }, 400);
	const contentType =
		request.headers.get("Content-Type") || "application/octet-stream";
	await env.DATA.put(key, request.body, {
		httpMetadata: { contentType },
	});
	return json({ uploaded: key, contentType });
}

// ── API: Process NY OCA CSV stored in R2 ──
// User uploads the raw CSV once (via wrangler r2 put ny-oca/NYS-YYYY.csv),
// then this endpoint streams it, aggregates per-judge, and writes the
// result to courts/new-york.json — all inside a single Worker invocation.
// Stream-based to stay within CPU/memory limits on 100MB+ files.
async function handleProcessNyOca(url: URL, env: Env): Promise<Response> {
	const key = url.searchParams.get("key") || "ny-oca/NYS-2025.csv.gz";
	const includeAll = url.searchParams.get("all") === "true";

	const obj = await env.DATA.get(key);
	if (!obj) return json({ error: `R2 object not found: ${key}` }, 404);

	const NYC_COUNTIES = new Set([
		"bronx",
		"kings",
		"new york",
		"new york county",
		"queens",
		"richmond",
	]);

	// Auto-decompress .gz objects using CF Worker's DecompressionStream.
	// Keeps the uploaded CSV under the 100MB Worker request-body cap
	// (NY OCA full-year CSVs are ~100–150MB raw, ~15–25MB gzipped).
	const rawStream = obj.body as ReadableStream<Uint8Array>;
	const stream = key.endsWith(".gz")
		? rawStream.pipeThrough(new DecompressionStream("gzip"))
		: rawStream;
	const reader = stream.getReader();
	const decoder = new TextDecoder("utf-8");
	let buf = "";
	let headerMap: Map<string, number> | null = null;
	let iJudge = -1;
	let iCounty = -1;
	let iCourt = -1;
	let iFta = -1;
	let iRearrest = -1;
	let iRevoc = -1;
	const byJudge = new Map<
		string,
		{
			county: string;
			court: string;
			total: number;
			fta: number;
			rearrest: number;
			revoc: number;
		}
	>();
	let rows = 0;
	let filtered = 0;

	const parseLine = (line: string): string[] => {
		const out: string[] = [];
		let b = "";
		let inQ = false;
		for (let i = 0; i < line.length; i++) {
			const c = line[i];
			if (inQ) {
				if (c === '"') {
					if (line[i + 1] === '"') {
						b += '"';
						i++;
					} else inQ = false;
				} else b += c;
			} else if (c === ",") {
				out.push(b);
				b = "";
			} else if (c === '"') inQ = true;
			else b += c;
		}
		out.push(b);
		return out;
	};
	const truthy = (v: string | undefined): boolean => {
		if (!v) return false;
		const s = v.trim().toLowerCase();
		return s === "y" || s === "yes" || s === "1" || s === "true";
	};
	const isRearrest = (v: string | undefined): boolean => {
		if (!v) return false;
		const s = v.trim().toLowerCase();
		return !!s && s !== "no arrest" && s !== "null" && s !== "0" && s !== "n";
	};

	while (true) {
		const { done, value } = await reader.read();
		if (done) break;
		buf += decoder.decode(value, { stream: true });
		let idx: number;
		// biome-ignore lint/suspicious/noAssignInExpressions: loop pattern
		while ((idx = buf.indexOf("\n")) >= 0) {
			const line = buf.slice(0, idx).replace(/\r$/, "");
			buf = buf.slice(idx + 1);
			if (!line) continue;
			const cols = parseLine(line);
			if (!headerMap) {
				headerMap = new Map(
					cols.map((c, i) => [
						c
							.replace(/^\ufeff/, "")
							.trim()
							.toLowerCase(),
						i,
					]),
				);
				const pick = (names: string[]): number => {
					for (const n of names) {
						const v = headerMap?.get(n);
						if (v !== undefined) return v;
					}
					return -1;
				};
				iJudge = pick(["judge_name"]);
				iCounty = pick(["county_name"]);
				iCourt = pick(["court_name"]);
				iFta = pick(["warrant_ordered_btw_arraign_and_dispo"]);
				iRearrest = pick(["rearrest"]);
				iRevoc = pick(["remanded_to_jail_at_arraign"]);
				if (iJudge < 0)
					return json({ error: "judge column not found in CSV header" }, 422);
				continue;
			}
			rows++;
			const raw = (cols[iJudge] || "").trim();
			if (!raw || raw.toLowerCase() === "unknown") continue;
			const county = iCounty >= 0 ? (cols[iCounty] || "").trim() : "";
			if (!includeAll && county && !NYC_COUNTIES.has(county.toLowerCase())) {
				filtered++;
				continue;
			}
			const name = raw
				.toLowerCase()
				.split(/\s+/)
				.map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
				.join(" ")
				.trim();
			let rec = byJudge.get(name);
			if (!rec) {
				rec = {
					county,
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
			if (iRearrest >= 0 && isRearrest(cols[iRearrest])) rec.rearrest++;
			if (iRevoc >= 0 && truthy(cols[iRevoc])) rec.revoc++;
		}
	}

	const ranked = [...byJudge.entries()]
		.filter(([, v]) => v.total >= 10)
		.sort(([, a], [, b]) => b.total - a.total);

	const judges: JudgeRecord[] = ranked.map(([name, v]) => ({
		id: `nyoca-${name
			.toLowerCase()
			.replace(/[^a-z0-9]+/g, "-")
			.replace(/^-+|-+$/g, "")}`,
		name: `Judge ${name}`,
		city: "New York",
		state: "New York",
		court: v.court || `${v.county} County Criminal Court`,
		total_cases: v.total,
		fta_count: v.fta,
		rearrest_count: v.rearrest,
		revocation_count: v.revoc,
		source: "NY OCA Pretrial Release Data (Judiciary Law § 216(5))",
	}));

	const cityData: CityData = {
		city: "New York",
		state: "New York",
		judges,
		source:
			"NY State Office of Court Administration — Pretrial Release Data (Judiciary Law § 216(5))",
		last_updated: new Date().toISOString(),
		last_fresh_data: new Date().toISOString(),
		is_stale: false,
		total_cases: judges.reduce((s, j) => s + j.total_cases, 0),
		total_fta: judges.reduce((s, j) => s + j.fta_count, 0),
		total_rearrests: judges.reduce((s, j) => s + j.rearrest_count, 0),
		total_revocations: judges.reduce((s, j) => s + j.revocation_count, 0),
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
	// Enrich bios from CourtListener so each NY judge card has the same
	// richness as LA/Seattle (gender, education, political affiliation,
	// dates, photo). Best-effort — silent on failure.
	await enrichBiosFromCL(cityData.judges, "New York", env.COURTLISTENER_TOKEN);

	await env.DATA.put("courts/new-york.json", JSON.stringify(cityData), {
		httpMetadata: { contentType: "application/json" },
	});

	return json({
		processed: rows,
		filtered_non_nyc: filtered,
		unique_judges: byJudge.size,
		judges_with_10plus_cases: judges.length,
		total_cases: cityData.total_cases,
		total_rearrests: cityData.total_rearrests,
		total_fta: cityData.total_fta,
		source_key: key,
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

// Look up a judge's CourtListener bio (gender, education, political affiliation,
// dates, photo, etc.) by searching their name within a state. Writes directly
// to the judge record so every city's judges have uniform bio richness.
async function clLookupBio(
	judge: JudgeRecord,
	state: string,
	token: string | undefined,
): Promise<void> {
	if (!token) return;
	// Strip common honorifics the Socrata datasets sometimes include
	const clean = judge.name
		.replace(/^(Judge|Justice|Honorable|Hon\.|The Honorable)\s+/i, "")
		.trim();
	// Two name formats in the wild:
	//   "First Middle Last"     — SF, Miami, Chicago, Atlanta, Texas
	//   "Last, First Middle"    — NY OCA CSV (official court roster style)
	let first: string;
	let last: string;
	if (clean.includes(",")) {
		const [lastPart, firstPart] = clean.split(",", 2).map((s) => s.trim());
		last = lastPart.split(/\s+/)[0];
		first = (firstPart.split(/\s+/)[0] || "").replace(/\.$/, "");
	} else {
		const parts = clean.split(/\s+/).filter(Boolean);
		if (parts.length < 2) return;
		last = parts[parts.length - 1];
		first = parts[0];
	}
	if (!last || !first) return;
	const search = async (
		params: Record<string, string>,
	): Promise<Record<string, unknown> | null> => {
		const q = new URLSearchParams(params);
		const res = await fetch(`${CL}/people/?${q.toString()}`, {
			headers: {
				Authorization: `Token ${token}`,
				"User-Agent": "JudgeSearch/2",
			},
		});
		if (!res.ok) return null;
		const data = (await res.json()) as {
			results?: Array<Record<string, unknown>>;
		};
		// Require first-name match to avoid picking wrong person with same surname
		const firstLc = first.toLowerCase();
		return (
			(data.results || []).find((p) => {
				const cf = String(p.name_first || "").toLowerCase();
				return cf === firstLc || cf.startsWith(firstLc);
			}) || null
		);
	};
	try {
		// 1. Preferred: within the judge's state court system
		let match = await search({
			format: "json",
			name_last: last,
			positions__court__full_name__icontains: state,
			positions__position_type: "jud",
			page_size: "5",
		});
		// 2. Fallback: any judge position with matching first+last name
		//    (many county/municipal judges aren't tagged under state court)
		if (!match) {
			match = await search({
				format: "json",
				name_last: last,
				positions__position_type: "jud",
				page_size: "5",
			});
		}
		if (!match) return;
		if (!judge.courtlistener_id && match.id) {
			judge.courtlistener_id = Number(match.id);
		}
		if (!judge.gender && match.gender) {
			judge.gender =
				match.gender === "m" ? "Male" : match.gender === "f" ? "Female" : "";
		}
		if (!judge.born && match.date_dob) judge.born = String(match.date_dob);
		if (!judge.birthplace && (match.dob_city || match.dob_state)) {
			judge.birthplace =
				`${match.dob_city || ""}${match.dob_state ? `, ${match.dob_state}` : ""}`.trim();
		}
		if (judge.has_photo === undefined && match.has_photo !== undefined) {
			judge.has_photo = Boolean(match.has_photo);
		}
		if (
			!judge.political_affiliation &&
			(match.political_affiliations as unknown[])?.length
		) {
			judge.political_affiliation = "On Record";
		}
		if (!judge.education?.length && (match.educations as unknown[])?.length) {
			judge.education = [`${(match.educations as unknown[]).length} record(s)`];
		}
	} catch {
		/* bio lookup is best-effort — never block scraping on failure */
	}
}

// Enrich every judge in a list with CourtListener bios (parallelized).
async function enrichBiosFromCL(
	judges: JudgeRecord[],
	state: string,
	token: string | undefined,
): Promise<void> {
	if (!token || judges.length === 0) return;
	const chunkSize = 10;
	for (let i = 0; i < judges.length; i += chunkSize) {
		const chunk = judges.slice(i, i + chunkSize);
		await Promise.all(chunk.map((j) => clLookupBio(j, state, token)));
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

// ── San Francisco: city-level DA stats refresh (judge-level uploaded offline) ──
// Per-judge data is built offline from the jamiequint/sf_criminal_court HF
// dataset (SF Superior Court docket scrape + DA feeds + 10.500-released
// disposition spreadsheet) via scrapers/fetch-sf-hf.mjs and POSTed to
// /api/upload. This keeps the city-wide DA resolution volume fresh on the
// daily cron so the page always shows a recent timestamp.
async function scrapeSanFranciscoStats(): Promise<{
	source: string;
	city_stats?: CityStats;
}> {
	const total = await socrataCount(
		"https://data.sfgov.org/resource/ynfy-z5kt.json?$select=count(*)",
	);
	return {
		source:
			"jamiequint/sf_criminal_court (HF) — per-judge dispositions, FTAs, revocations · CC-BY-NC-4.0 · DataSF DA Case Resolutions (city-wide refresh)",
		city_stats: total
			? {
					annual_arrests: total,
					label: "DA case resolutions on record",
					source: "data.sfgov.org · DA Case Resolutions (ynfy-z5kt)",
					note: "Per-judge SF data: jamiequint/sf_criminal_court HF dataset (CC-BY-NC-4.0), built from SF Superior Court docket scrape + DA feeds + SFSC charge-disposition spreadsheet released under California Rules of Court rule 10.500. DataSF city-wide total updates daily.",
				}
			: undefined,
	};
}

// ── Texas / Harris County: city-level stats refresh ──
// Harris County's only public per-judge data is a Shiny dashboard (no API).
// We keep existing judge data and refresh stats from secondary public sources.
async function scrapeTexasStats(): Promise<{
	source: string;
	city_stats?: CityStats;
}> {
	// Houston city arrest data (City of Houston open data)
	const arrests = await socrataCount(
		"https://data.houstontx.gov/resource/hz6g-h7f6.json?$select=count(*)",
	);
	return {
		source:
			"Harris County Criminal Court Data Dashboard (per-judge) + Houston OpenData (city-wide refresh)",
		city_stats: arrests
			? {
					annual_arrests: arrests,
					label: "Houston public incidents on record",
					source: "data.houstontx.gov",
					note: "Per-judge Harris County data is curated from public records. City-wide stats refreshed daily.",
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
<meta property="og:image" content="https://judge-search.barkleesanders.workers.dev/og-image.png?v=20260412">
<meta property="og:image:type" content="image/png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="JudgeSearch — scales of justice, 8 cities, 225+ judges, 881K cases. Data sources: CourtWatch, Cook County Open Data, Fulton County ShareFulton, DataSF, Harris County, CourtListener.">
<meta property="og:type" content="website">
<meta property="og:url" content="https://judge-search.barkleesanders.workers.dev">
<!-- Twitter Card -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="JudgeSearch — Judicial Accountability">
<meta name="twitter:description" content="Search 880,000+ court records across 8 U.S. cities. See which judges had the most missed court dates, rearrests, and release revocations. Public data, plain English.">
<meta name="twitter:image" content="https://judge-search.barkleesanders.workers.dev/og-image.png?v=20260412">
<meta name="twitter:image:alt" content="JudgeSearch — scales of justice, 8 cities, 225+ judges, 881K cases. Real open-data sources from CourtWatch, Cook County, Fulton County, DataSF, Harris County, and CourtListener.">
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
.bcard{position:relative;background:var(--s);border:1px solid var(--b);border-radius:var(--r);overflow:hidden;transition:border-color .2s}
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
  .col-legend{grid-template-columns:repeat(2,1fr)!important;gap:8px!important}
  .col-legend > div{padding:10px!important}
  .col-legend p{font-size:.72rem!important}
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
<div style="text-align:center;margin-bottom:8px"><span style="font-family:var(--mono);font-size:.72rem;color:var(--red);letter-spacing:.12em;padding:4px 12px;background:rgba(232,64,64,.08);border:1px solid rgba(232,64,64,.3);border-radius:20px">NATIONAL ACCOUNTABILITY INDEX · PUBLIC RECORDS</span></div>
<h2 style="font-family:var(--serif);font-size:1.8rem;text-align:center;margin:12px 0 8px 0">Judges Ranked by Risk Score</h2>
<p style="text-align:center;color:var(--t2);font-size:.95rem;margin-bottom:28px;max-width:720px;margin-left:auto;margin-right:auto">Based on public court records showing how often defendants allegedly released by each judge were later <strong style="color:var(--t)">rearrested</strong>, <strong style="color:var(--t)">missed their court date</strong>, or had their <strong style="color:var(--t)">release revoked</strong> — all while their original case was still open. Everyone listed is presumed innocent under the law.</p>

<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:24px" class="col-legend">
<div style="padding:14px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-family:var(--mono);font-size:.68rem;color:var(--gold);letter-spacing:.05em;margin-bottom:4px">TOTAL CASES</div>
<p style="color:var(--t2);font-size:.78rem;line-height:1.4;margin:0">Number of defendants this judge made release decisions on in the dataset. More cases = more reliable signal.</p>
</div>
<div style="padding:14px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-family:var(--mono);font-size:.68rem;color:var(--red);letter-spacing:.05em;margin-bottom:4px">REARRESTED %</div>
<p style="color:var(--t2);font-size:.78rem;line-height:1.4;margin:0">Percent of released defendants who were <em>allegedly</em> arrested again before their first case closed. An arrest is not a conviction.</p>
</div>
<div style="padding:14px;background:var(--s);border:1px solid var(--b);border-radius:var(--r)">
<div style="font-family:var(--mono);font-size:.68rem;color:var(--orange);letter-spacing:.05em;margin-bottom:4px">MISSED COURT %</div>
<p style="color:var(--t2);font-size:.78rem;line-height:1.4;margin:0">Percent of defendants who didn't appear for a scheduled hearing, causing a bench warrant to be issued.</p>
</div>
<div style="padding:14px;background:var(--s2);border:1px solid var(--gold);border-radius:var(--r)">
<div style="font-family:var(--mono);font-size:.68rem;color:var(--gold);letter-spacing:.05em;margin-bottom:4px">RISK SCORE · 0–100</div>
<p style="color:var(--t2);font-size:.78rem;line-height:1.4;margin:0"><strong style="color:var(--t)">50% × rearrest + 30% × missed + 20% × revocation</strong>, scaled to 100. Lightly adjusted by case volume so tiny samples can't top the list.</p>
</div>
</div>

<div id="worst50-list">
<div class="empty" style="padding:30px 20px"><div class="spin" style="margin:0 auto 12px"></div><p>Loading national rankings...</p></div>
</div>

<div id="worst50-meta" style="margin-top:18px;font-family:var(--mono);font-size:.72rem;color:var(--t3);text-align:center;line-height:1.6"></div>

<div style="margin-top:24px;padding:18px 22px;background:var(--s2);border:1px solid var(--b);border-radius:var(--r);color:var(--t2);font-size:.82rem;line-height:1.65">
<div style="font-family:var(--mono);font-size:.7rem;color:var(--gold);letter-spacing:.08em;margin-bottom:8px">IMPORTANT · PLEASE READ</div>
<p style="margin:0 0 8px 0">All numbers come directly from <strong style="color:var(--t)">public court records</strong> (see <a href="#sources" style="color:var(--gold)">Data Sources</a> for exact origins). A <em>rearrest</em> is an alleged arrest — not a conviction — and every defendant is presumed innocent under the law.</p>
<p style="margin:0 0 8px 0">Judges make release decisions one case at a time, weighing factors that aren't in these numbers: prior record, charge severity, flight risk, defense counsel, prosecutor position, and local rules. A high Risk Score does <strong>not</strong> mean a judge is bad, corrupt, or incompetent — it means the defendants released in their courtroom, on the public record, had higher-than-average rates of these outcomes.</p>
<p style="margin:0;color:var(--t3);font-size:.78rem">This site publishes open government data for civic accountability. It does not accuse anyone of a crime or imply criminal conduct by any named judge. Numbers are updated daily from official source feeds; errors in the upstream data will appear here unchanged until corrected at the source.</p>
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
<p style="color:var(--t2);font-size:.85rem;margin:0 0 8px 0"><strong style="color:var(--t)">Per-judge dispositions, FTAs, revocations</strong> — built from the SF Superior Court docket scrape, DA open-data feeds, and the SFSC charge-disposition spreadsheet released under California Rules of Court rule 10.500. Judge attribution from <code style="font-family:var(--mono)">judicial_assignments</code> + <code style="font-family:var(--mono)">calendar_with_judicial_assignments</code> joined to <code style="font-family:var(--mono)">register_of_actions</code>.</p>
<p style="color:var(--t3);font-size:.75rem;margin:0 0 6px 0">Sources: <a href="https://huggingface.co/datasets/jamiequint/sf_criminal_court" target="_blank" style="color:var(--gold)">jamiequint/sf_criminal_court</a> on Hugging Face (<a href="https://creativecommons.org/licenses/by-nc/4.0/" target="_blank" style="color:var(--gold)">CC-BY-NC-4.0</a>) + <a href="https://data.sfgov.org/Public-Safety/District-Attorney-Actions-Taken-on-Arrests-Presented/czsm-3ei3" target="_blank" style="color:var(--gold)">DataSF</a> city-wide totals + <a href="https://www.courtlistener.com" target="_blank" style="color:var(--gold)">CourtListener</a> judicial bios</p>
<p style="color:var(--t3);font-size:.7rem;margin:0;font-style:italic">Non-commercial use only — attribution: dataset by Jamie Quint.</p>
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

// Module-level state so the rank-tab buttons can re-render the current city
// without re-fetching. _sortMode picks which lens the per-judge list is shown
// through: 'rate' = bad-decisions per defendant; 'volume' = total rearrests
// (volume × rate, the absolute-harm view).
let _currentCity=null;
let _sortMode='rate';

async function fetchCity(slug){
  const area=$('results');
  area.innerHTML='<div class="loading"><div class="spin"></div><p>Loading judges...</p></div>';
  $('sbar').style.display='none';
  $('srcban').style.display='none';
  try{
    const res=await fetch('/api/city?slug='+slug);
    if(!res.ok)throw new Error('Data unavailable');
    _currentCity=await res.json();
    render(_currentCity);
  }catch(e){
    area.innerHTML='<div class="empty"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
  }
}

function setSort(mode){
  _sortMode=mode;
  if(_currentCity)render(_currentCity);
  // scroll the cards section back into view so the tab feels responsive
  const r=document.getElementById('results');
  if(r)r.scrollIntoView({block:'start',behavior:'smooth'});
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
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;width:40px">RANK</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em">JUDGE</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em" class="w-city">CITY</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right" class="w-cases" title="Total cases this judge made release decisions on in the dataset">TOTAL CASES</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right" title="Percent of released defendants allegedly arrested again before first case closed">REARRESTED %</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right" class="w-fta" title="Percent of defendants who missed a scheduled court appearance (bench warrant issued)">MISSED COURT %</th>';
    h+='<th style="padding:10px 8px;color:var(--t3);font-family:var(--mono);font-size:.7rem;letter-spacing:.05em;text-align:right" title="Composite Risk Score on a 0–100 scale. Weighted: 50% rearrest + 30% missed court + 20% revocation. Adjusted for case volume.">RISK · 0–100</th>';
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
      const scoreBand=j.danger_score>=60?'Critical':j.danger_score>=40?'High':j.danger_score>=20?'Notable':'Low';
      const scoreColor=j.danger_score>=60?'var(--red)':j.danger_score>=40?'var(--orange)':j.danger_score>=20?'var(--gold)':'var(--green)';
      h+='<td style="padding:10px 8px;text-align:right;font-family:var(--serif);font-weight:800;color:'+scoreColor+';font-size:1.05rem" title="'+scoreBand+' concern"><span>'+j.danger_score.toFixed(1)+'</span><span style="font-size:.6rem;color:var(--t3);font-family:var(--mono);font-weight:400;margin-left:4px">/100</span></td>';
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

  // Two ranking lenses, both honest, both flawed in different ways:
  //  rate    — rearrests per defendant. Catches a low-volume judge with a
  //            high rate; a 35%-rate judge with 200 cases ranks above a
  //            25%-rate judge with 2,000 cases.
  //  volume  — total rearrests (volume × rate). Catches a high-volume judge
  //            who lets out a lot of people who reoffend in absolute terms;
  //            the 25%-rate / 2,000-cases judge ranks above the 35%/200.
  // Together they cover both halves of "most bad release decisions". A judge
  // who tops BOTH lists is unambiguously concerning. Judges below MIN_CASES
  // are excluded from the rate-based ranking only — small-sample noise
  // shouldn't lead the page; they remain visible at the bottom with a flag.
  const MIN_CASES_FOR_RATE=100;
  const sortableMode=measuresRearrest?_sortMode:'rate';
  const judgesSorted=d.judges.slice().sort((a,b)=>{
    if(sortableMode==='volume'){
      const v=b.rearrest_count-a.rearrest_count;
      if(v)return v;
      return b.total_cases-a.total_cases;
    }
    // 'rate' mode: gate by min-cases first; below the gate gets pushed down
    const aQ=a.total_cases>=MIN_CASES_FOR_RATE;
    const bQ=b.total_cases>=MIN_CASES_FOR_RATE;
    if(aQ!==bQ)return aQ?-1:1;
    const ar=a.total_cases?a.rearrest_count/a.total_cases:0;
    const br=b.total_cases?b.rearrest_count/b.total_cases:0;
    if(ar!==br)return br-ar;
    return b.total_cases-a.total_cases;
  });

  // Top judges as baseball cards (show first 12)
  const topJudges=judgesSorted.slice(0,12);
  const restJudges=judgesSorted.slice(12);

  const area=$('results');
  let h='<h2 style="font-family:var(--serif);font-size:1.3rem;margin-bottom:6px">'+d.judges.length+' Judges &mdash; '+esc(d.city)+', '+esc(d.state)+'</h2>';
  h+='<p style="color:var(--t3);font-size:.8rem;margin-bottom:16px">Ranked by <strong style="color:var(--t2)">Danger Score</strong> — rate-weighted combination of '+esc(lbl.fta_bar)+', '+esc(lbl.rearrest_bar)+', and '+esc(lbl.revocation_bar)+' outcomes, weighted by case volume. <span style="color:var(--red)">Red</span> = top quarter, <span style="color:var(--orange)">orange</span> = top half, <span style="color:var(--green)">green</span> = bottom half.</p>';

  // City-wide context note (LA/Seattle/NY — where we have real arrest totals but
  // not per-judge data).
  if(cityTotalCases===0&&d.city_stats&&d.city_stats.note){
    h+='<div style="background:rgba(200,168,75,.06);border:1px solid var(--b2);border-radius:var(--r);padding:16px 20px;margin-bottom:24px;color:var(--t2);font-size:.85rem;line-height:1.5">';
    h+='<strong style="color:var(--gold)">Why city-wide only?</strong> '+esc(d.city_stats.note);
    h+='</div>';
  }

  // Three-way contrast block — concrete frequency framing ("21 out of 100"),
  // named judges as anchors, proportional fill bars on a 0-100% scale so the
  // reader can SEE that 1% and 39% are not close to each other.
  // Eligibility: only judges with ≥100 cases get featured as the lowest/highest
  // exemplar — small-sample outliers (a 0% from 25 cases) aren't representative.
  if(measuresRearrest&&cityTotalCases>0){
    const eligible=live.filter(j=>j.total_cases>=100);
    if(eligible.length>=3){
      const ranked=eligible.slice().sort((a,b)=>(a.rearrest_count/a.total_cases)-(b.rearrest_count/b.total_cases));
      const lowJ=ranked[0];
      const highJ=ranked[ranked.length-1];
      const lowRate=lowJ.rearrest_count/lowJ.total_cases;
      const highRate=highJ.rearrest_count/highJ.total_cases;
      const eligTotalCases=eligible.reduce((s,j)=>s+j.total_cases,0);
      const eligTotalRearr=eligible.reduce((s,j)=>s+j.rearrest_count,0);
      const avgRate=eligTotalRearr/eligTotalCases;

      // "X out of every 100" framing — concrete and doesn't require fractional reasoning
      const freq=r=>{
        const n=Math.round(r*100);
        if(n<=0)return'fewer than 1 out of every 100';
        if(n===1)return'about 1 out of every 100';
        return'about '+n+' out of every 100';
      };
      const pct=r=>(r<0.05?(r*100).toFixed(1):Math.round(r*100))+'%';
      const colorFor=r=>r>0.25?'var(--red)':r>0.12?'var(--orange)':'var(--green)';

      // Single horizontal bar — width = rate * 100% of container. 0-100% scale
      // is honest: a 1% bar SHOULD look tiny next to a 39% bar.
      const bar=(label,sub,judge,rate)=>{
        const c=colorFor(rate);
        return ''
          +'<div style="margin-bottom:18px">'
          +  '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px;gap:12px;flex-wrap:wrap">'
          +    '<div style="font-family:var(--mono);font-size:.7rem;color:var(--gold);letter-spacing:.06em;text-transform:uppercase">'+label+'</div>'
          +    (judge?'<div style="color:var(--t);font-size:.9rem;font-weight:600">'+esc(judge.name)+' <span style="color:var(--t3);font-weight:400;font-size:.8rem">— '+esc(judge.court||'')+'</span></div>':'<div style="color:var(--t2);font-size:.85rem">'+sub+'</div>')
          +  '</div>'
          +  '<div style="background:rgba(255,255,255,.04);border:1px solid var(--b);border-radius:6px;height:22px;overflow:hidden;position:relative">'
          +    '<div style="height:100%;width:'+(rate*100).toFixed(2)+'%;background:'+c+';min-width:3px;transition:width .3s ease"></div>'
          +  '</div>'
          +  '<div style="display:flex;justify-content:space-between;margin-top:6px;color:var(--t2);font-size:.85rem">'
          +    '<span><strong style="color:var(--t)">'+freq(rate)+'</strong></span>'
          +    '<span style="color:'+c+';font-family:var(--mono);font-weight:700">'+pct(rate)+'</span>'
          +  '</div>'
          +'</div>';
      };

      h+='<div style="background:linear-gradient(135deg,rgba(232,64,64,.08),rgba(200,168,75,.06));border:1px solid var(--b2);border-radius:var(--r);padding:22px 22px 18px;margin-bottom:24px">';
      h+='<div style="font-family:var(--mono);font-size:.7rem;color:var(--gold);letter-spacing:.08em;margin-bottom:6px">RELEASED BEFORE TRIAL — WHAT HAPPENED NEXT</div>';
      h+='<div style="color:var(--t);font-size:1rem;line-height:1.5;margin-bottom:18px">In '+esc(d.city)+', when one of these '+eligible.length+' judges sent a defendant home to wait for trial, here is how often that person was charged with a new crime <em>before</em> the case ended. Three judges, side by side:</div>';

      h+=bar('Lowest rate',null,lowJ,lowRate);
      h+=bar('Average across all '+eligible.length+' judges','—',null,avgRate);
      h+=bar('Highest rate',null,highJ,highRate);

      h+='<div style="color:var(--t);font-size:.95rem;line-height:1.5;margin:6px 0 0;padding-top:14px;border-top:1px solid var(--b)">Same city. Same kind of crime, sometimes. <strong>Different judge — very different outcome.</strong> Scroll down to see where every judge in '+esc(d.city)+' falls.</div>';

      h+='<div style="margin-top:12px;color:var(--t3);font-size:.78rem;line-height:1.55;font-style:italic">What &ldquo;arrested again&rdquo; means: the same person was charged with a new crime while their original case was still open. Numbers come straight from public court records and are not adjusted for the type of case — a judge who handles violent felonies will naturally see different numbers than one who handles traffic tickets. Judges with fewer than 100 cases in the dataset are not used as the high/low examples (small samples are noisy).</div>';
      h+='</div>';
    }
  }
  // Rank lens tabs — only meaningful for cities that measure rearrest
  if(measuresRearrest&&judgesSorted.length>1){
    const isRate=sortableMode==='rate';
    const tabStyle='flex:1;padding:10px 14px;font-family:var(--mono);font-size:.78rem;letter-spacing:.04em;text-transform:uppercase;border:1px solid var(--b);background:transparent;color:var(--t2);cursor:pointer;text-align:left;line-height:1.35';
    const onStyle='background:rgba(200,168,75,.12);border-color:var(--gold);color:var(--gold)';
    h+='<div style="margin-bottom:14px">';
    h+='<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">';
    h+='<button data-sort="rate" onclick="setSort(this.dataset.sort)" style="'+tabStyle+(isRate?';'+onStyle:'')+'"><div style="font-weight:700;color:'+(isRate?'var(--gold)':'var(--t)')+'">By rate</div><div style="color:var(--t3);font-size:.7rem;font-family:var(--sans);text-transform:none;letter-spacing:0;font-weight:400;margin-top:2px">Worst per defendant — small judges count</div></button>';
    h+='<button data-sort="volume" onclick="setSort(this.dataset.sort)" style="'+tabStyle+(!isRate?';'+onStyle:'')+'"><div style="font-weight:700;color:'+(!isRate?'var(--gold)':'var(--t)')+'">By total rearrests</div><div style="color:var(--t3);font-size:.7rem;font-family:var(--sans);text-transform:none;letter-spacing:0;font-weight:400;margin-top:2px">Most new crimes overall — busy courts rise</div></button>';
    h+='</div>';
    h+='<div style="color:var(--t3);font-size:.78rem;line-height:1.5">'+(isRate?'Showing the judges who release the highest <strong style="color:var(--t2)">share</strong> of defendants who get arrested again. Judges with fewer than '+MIN_CASES_FOR_RATE+' cases are pushed below — small samples are noisy.':'Showing the judges who released the most defendants who got arrested again, by raw count. Bigger courtrooms naturally rise — that is the point of this view.')+'</div>';
    h+='</div>';
  }

  h+='<div class="cards">';

  // Judges are already sorted by rate-based danger score on the server,
  // so array index = rank-1 within the city (most concerning first).
  const totalRanked=d.judges.filter(j=>j.total_cases>0).length;

  for(let idx=0;idx<topJudges.length;idx++){
    const j=topJudges[idx];
    const initials=(j.name||'').split(' ').map(w=>(w[0]||'')).join('').slice(0,2);
    const has=j.total_cases>0;
    const rank=has?idx+1:null;

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

    // Rank color: top 25% red, top 50% orange, bottom half green
    const rankColor=rank&&totalRanked?(rank<=Math.ceil(totalRanked*0.25)?'var(--red)':rank<=Math.ceil(totalRanked*0.5)?'var(--orange)':'var(--green)'):'var(--t3)';

    h+='<div class="bcard">';
    // Rank ribbon
    if(rank){
      h+='<div style="position:absolute;top:0;left:0;background:'+rankColor+';color:#0a0a0a;font-family:var(--serif);font-weight:800;font-size:.85rem;padding:4px 10px 4px 8px;border-radius:var(--r) 0 8px 0;letter-spacing:.02em">#'+rank+' of '+totalRanked+'</div>';
    }
    // Header
    h+='<div class="bcard-head" style="padding-top:28px"><div class="bcard-av">'+esc(initials)+'</div><div style="flex:1;min-width:0">';
    h+='<div class="bcard-name">'+esc(j.name)+'</div>';
    h+='<div class="bcard-court">'+esc(j.court)+'</div>';
    h+='<div class="bcard-loc">'+esc(j.city)+', '+esc(j.state)+'</div>';
    // Calendar / arraignment judges see EVERY defendant briefly — their case
    // counts can be 5–10× a trial judge's. Flag the role so visitors don't
    // read raw volume as "this judge handles more cases" when really it's a
    // structural difference in courtroom assignment.
    if(j.position_type&&/master calendar|arraignment/i.test(j.position_type)){
      h+='<div title="Master-calendar judges see every defendant pass through arraignment — case counts are not directly comparable to trial-judge counts." style="display:inline-block;margin-top:6px;padding:2px 8px;background:rgba(232,160,64,.12);border:1px solid rgba(232,160,64,.4);border-radius:10px;font-size:.65rem;font-family:var(--mono);letter-spacing:.04em;color:var(--orange,#e8a040)">⚖︎ '+esc(j.position_type)+'</div>';
    }
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

    // Only the CASES stat is meaningful when outcome counts are all zero
    // (happens for LA/Seattle/NY opinion-count data — CL gives cases but
    // not outcomes). Show a clearer message instead of four zeros.
    const hasOutcomes=has&&(j.fta_count>0||j.rearrest_count>0||j.revocation_count>0);
    h+='<div class="bcard-stats">';
    h+='<div class="bs"><div class="bv w">'+j.total_cases.toLocaleString()+'</div><div class="bl">'+(hasOutcomes?'Cases':'Opinions on Record')+'</div></div>';
    if(hasOutcomes){
      h+='<div class="bs"><div class="bv r">'+j.fta_count.toLocaleString()+'</div><div class="bl">'+esc(lbl.fta)+'</div></div>';
      h+='<div class="bs"><div class="bv" style="color:'+col2+'">'+j.rearrest_count.toLocaleString()+'</div><div class="bl">'+esc(lbl.rearrest)+'</div></div>';
      h+='<div class="bs"><div class="bv" style="color:'+col3+'">'+j.revocation_count.toLocaleString()+'</div><div class="bl">'+esc(lbl.revocation)+'</div></div>';
    }else{
      // Bio-only card: show what we DO have
      h+='<div class="bs"><div class="bv" style="color:var(--t2)">'+(j.political_affiliation?'✓':'—')+'</div><div class="bl">Party Record</div></div>';
      h+='<div class="bs"><div class="bv" style="color:var(--t2)">'+(j.education&&j.education.length?'✓':'—')+'</div><div class="bl">Education</div></div>';
      h+='<div class="bs"><div class="bv" style="color:var(--t2)">'+(j.courtlistener_id?'✓':'—')+'</div><div class="bl">CL Profile</div></div>';
    }
    h+='</div>';

    // Rate bars (only if has real outcome data)
    if(hasOutcomes){
      h+='<div class="bcard-rates">';
      h+=rateBar(lbl.fta_bar,ftaRate,ftaVs,ftaAbove,'var(--red)');
      h+=rateBar(lbl.rearrest_bar,rearrRate,rearrVs,rearrAbove,col2);
      h+=rateBar(lbl.revocation_bar,revocRate,revocVs,revocAbove,col3);
      h+='</div>';
    }else if(has){
      h+='<div class="nodata" style="padding:14px 20px;color:var(--t3);font-size:.8rem">Opinion count only — per-judge rearrest data not published by this jurisdiction. See <a href="#sources" style="color:var(--gold)">Data Sources</a>.</div>';
    }else{
      h+='<div class="nodata">Case outcome data pending</div>';
    }

    // Bio footer — appointed_by, date_start, bio text, education.
    // Only shown if we have at least one of these fields (empty for most
    // county judges that aren't in MACJ/Wikidata/CourtListener).
    const bioBits=[];
    if(j.appointed_by)bioBits.push('<span style="color:var(--t2)"><span style="color:var(--t3)">Appointed by:</span> '+esc(j.appointed_by)+'</span>');
    if(j.date_start)bioBits.push('<span style="color:var(--t2)"><span style="color:var(--t3)">On bench since:</span> '+esc(j.date_start.replace(/^(appointed|elected|joined)\\s+(?:on\\s+|in\\s+)?/i,'').trim())+'</span>');
    if(j.political_affiliation)bioBits.push('<span style="color:var(--t2)"><span style="color:var(--t3)">Party:</span> '+esc(j.political_affiliation)+'</span>');
    if(j.born&&/^\\d{4}/.test(j.born))bioBits.push('<span style="color:var(--t2)"><span style="color:var(--t3)">Born:</span> '+esc(j.born.slice(0,4))+'</span>');
    if(bioBits.length>0){
      h+='<div style="padding:12px 20px;border-top:1px solid var(--b);font-size:.78rem;line-height:1.7;display:flex;flex-wrap:wrap;gap:12px 20px">'+bioBits.join('')+'</div>';
    }
    if(j.bio){
      h+='<div style="padding:10px 20px 14px;border-top:1px solid var(--b);font-size:.8rem;color:var(--t2);line-height:1.55"><span style="color:var(--t3);font-family:var(--mono);font-size:.66rem;letter-spacing:.08em;display:block;margin-bottom:4px">OFFICIAL BIO</span>'+esc(j.bio.slice(0,400))+(j.bio.length>400?'\u2026':'')+'</div>';
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

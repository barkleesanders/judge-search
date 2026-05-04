# JudgeSearch

**Know your judges. Hold them accountable.**

Live: https://judge-search.barkleesanders.workers.dev

Public court records across 8 U.S. cities — searchable by judge, sorted by outcome impact, explained in plain English. All data pulled from official government open-data portals and non-profit legal databases. No scraping, no paywalls, no sign-up.

## What it shows

For each judge, where the data is available:
- Total cases handled
- Rearrested rate (defendants arrested again while out on pretrial release)
- Missed-court rate (failure to appear)
- Conviction / not-guilty / release-revoked rates
- Biographical data: appointer, political affiliation, education, dates on the bench

## Coverage

| City | Data | Source |
|---|---|---|
| Miami, FL | Per-judge pretrial outcomes | [CourtWatch.us](https://courtwatch.us) (FSS 907.043) |
| Chicago / Cook County, IL | 700K+ case dispositions per judge | [Cook County Open Data](https://datacatalog.cookcountyil.gov) (Socrata `apwk-dzx8`) |
| Atlanta / Fulton County, GA | 91K+ case dispositions per judge | [ShareFulton](https://sharefulton.fultoncountyga.gov) (Socrata `uww8-gu28`) |
| San Francisco, CA | Per-judge dispositions, FTAs, revocations from SF Superior Court dockets + DA feeds + 10.500 release | [jamiequint/sf_criminal_court](https://huggingface.co/datasets/jamiequint/sf_criminal_court) (HF, CC-BY-NC-4.0) + [DataSF](https://data.sfgov.org) (city-wide refresh) |
| Houston / Harris County, TX | Criminal case outcomes per judge | [Harris County JP Public Data Extract](https://jpwebsite.harriscountytx.gov/PublicExtracts/search.jsp) |
| New York, NY | Per-judge opinion counts + NYPD arrests | [CourtListener](https://www.courtlistener.com) + [NYC OpenData](https://data.cityofnewyork.us) `uip8-fykc` |
| Los Angeles, CA | Per-judge opinion counts + LAPD arrests | [CourtListener](https://www.courtlistener.com) + [LA OpenData](https://data.lacity.org) `amvf-fr72` |
| Seattle / King County, WA | Per-judge opinion counts + jail bookings | [CourtListener](https://www.courtlistener.com) + [King County OpenData](https://data.kingcounty.gov) `j56h-zgnm` |

## Architecture

Single Cloudflare Worker (`src/index.ts`). Everything lives in one file:
- Scrapers (one per city, fetch + normalize to `JudgeRecord[]`)
- Daily cron (`0 6 * * *` UTC) calls all scrapers and writes to R2
- `/api/city?slug=X` reads from R2 (with label normalization on the fly)
- `/api/seed?slug=X` triggers a scraper manually
- `/api/upload` accepts pre-built JSON via authenticated POST
- Inline HTML/CSS/JS rendered from the same file (no frontend framework)

Storage: Cloudflare R2 bucket `judge-data`, one JSON per city at `courts/{slug}.json`.

### Protective merge

`seedCity()` refuses to overwrite R2 data with worse data. If a scraper returns 0 cases where R2 previously had thousands, the existing data is kept and flagged as stale. This prevents the daily cron from wiping good data when an upstream source is down.

### Per-judge enrichment

For cities without a Socrata-compatible case API (LA/Seattle/NY), the worker queries [CourtListener's](https://www.courtlistener.com) opinion search by judge name. Free API, rate-limited to 60/min. Set via `COURTLISTENER_TOKEN` worker secret.

## Development

```bash
npm install
npx wrangler dev             # local dev
npx wrangler deploy          # ship to production
```

### Required secrets

```bash
npx wrangler secret put UPLOAD_SECRET        # for /api/upload POST auth
npx wrangler secret put COURTLISTENER_TOKEN  # for per-judge opinion enrichment (optional)
```

Get a free CourtListener token at https://www.courtlistener.com/register/.

### Offline SF pipeline (jamiequint/sf_criminal_court HF dataset)

Real per-judge SF data — dispositions, FTA (bench-warrant) signals, and
probation/PRCS/parole revocations — built from Jamie Quint's published
parquet files (combining a scraped SF Superior Court docket, DA open-data
feeds, and the SFSC charge-disposition spreadsheet released under
California Rules of Court rule 10.500).

License: **CC-BY-NC-4.0** (non-commercial). Attribution: Jamie Quint.

Requires `duckdb` CLI (`brew install duckdb`). Reads parquet directly
over HTTPS from Hugging Face — no manual download needed.

```bash
# Dry-run: queries HF, computes per-judge metrics, writes
# scrapers/.tmp-san-francisco.json so you can sanity-check before upload.
node scrapers/fetch-sf-hf.mjs

# Upload to the Worker (uses /api/upload bearer auth).
UPLOAD_SECRET=<your-secret> node scrapers/fetch-sf-hf.mjs --upload
```

Flags: `--min-cases <N>` (default 25), `--rearrest-window <D>` (default 365
days), `--cache-dir <path>` (cache parquet locally for repeat runs),
`--worker <url>` (override worker URL).

Per-case judge attribution = the judge with the most hearings on that case
(placeholder/clerk entries excluded). Rearrest is computed by cross-case
defendant-name match within the rearrest window.

Calendar / arraignment judges (those with `Master Calendar` or `Arraignment`
in their assignment designation) are auto-tagged via `position_type` so the
UI can flag them. These judges see every defendant at intake — their case
counts are 5–10× a trial judge's by structural design, not by output.

#### Weekly auto-refresh on Mac mini (launchd + Telegram alerts)

A launchd job runs the scraper weekly and pings Telegram if it fails or if
the case count drops significantly (silent-failure protection — the worker's
protective merge would otherwise mask a broken upstream).

One-time setup:
```bash
# 1. Populate credentials
mkdir -p ~/.config/judgesearch
cp scrapers/credentials-template.txt ~/.config/judgesearch/credentials.env
chmod 600 ~/.config/judgesearch/credentials.env
$EDITOR ~/.config/judgesearch/credentials.env   # fill in UPLOAD_SECRET + Telegram

# 2. Load the launchd job (Sundays 06:30 local)
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.barklee.judgesearch-sf-watcher.plist

# 3. (Optional) trigger immediately to verify
launchctl kickstart -k gui/$(id -u)/com.barklee.judgesearch-sf-watcher
tail -f ~/Library/Logs/judgesearch-sf-watcher.out.log
```

Per the LLM-vs-Shell rule this is launchd, not openclaw cron — the job is
pure shell, so paying for agent tokens would be waste. Telegram alerting
follows the existing `~/tools/cookies/lib/notify.sh` pattern.

### Offline NY OCA pipeline (real per-judge pretrial data)

NY State publishes statutorily-required per-judge pretrial release CSVs
(Judiciary Law § 216(5)) with fields for bench warrants (missed court),
rearrests while pending, and remands. Files are ~200-500MB per year and
live behind a Cloudflare challenge, so download manually:

```bash
# 1. Open in browser + save (CF challenge needs JS)
open "https://ww2.nycourts.gov/pretrial-release-data-33136"
# Download NYS for Web 2024.csv (or latest year) to ~/Downloads/

# 2. Aggregate + upload
UPLOAD_SECRET=<your-secret> \
  node scrapers/fetch-ny-oca.mjs \
  "~/Downloads/NYS for Web 2024.csv" \
  --upload
```

Flags: `--all-counties` (include non-NYC), `--upload-as SLUG` (override
city slug). The script auto-detects column names across schema
variations, streams the CSV (handles huge files), filters to NYC
counties by default, and POSTs the aggregated JudgeRecord[] to
`/api/upload`.

### Quality gates

```bash
npx tsc --noEmit             # type check
npx @biomejs/biome check .   # lint + format
node scrapers/verify-links.mjs   # verify external links still work
```

## Cost to run

Free tier on Cloudflare (Workers + R2). See issue tracker if you want to deploy your own.

## License

AGPL-3.0. Uses data from [Free Law Project](https://free.law/about/) (CourtListener) and various municipal open-data portals. See individual source cards on the live site for dataset-level attribution.

## Contributing

Issues and PRs welcome. If you can find a city with real judge-keyed case data that isn't here yet, please open an issue with the API endpoint.

#!/usr/bin/env bash
# JudgeSearch SF refresh watcher — runs scrapers/fetch-sf-hf.mjs --upload,
# pings Telegram on failure or when it succeeds with notably-different counts
# (so the user knows the dataset shifted under them, not just that it ran).
#
# Designed for ~/Library/LaunchAgents/com.barklee.judgesearch-sf-watcher.plist
# (weekly cadence, Sundays 06:30 local). Pure-shell job — per the LLM-vs-Shell
# rule this lives in launchd, NOT openclaw cron.
#
# Credentials (read from $HOME/.config/judgesearch/credentials.env):
#   UPLOAD_SECRET       — JudgeSearch worker /api/upload bearer (required)
#   TELEGRAM_BOT_TOKEN  — optional; falls back to log-only on failure
#   TELEGRAM_CHAT_ID    — optional; required to deliver Telegram alerts
#
# Exits non-zero on scraper or upload failure (so launchd shows it red).
# Successful runs that produced 0 judges or fewer than the previous run
# also exit non-zero — those are silent-failure modes the protective merge
# would otherwise hide.

set -euo pipefail

REPO="${HOME}/judge-search"
CREDS="${HOME}/.config/judgesearch/credentials.env"
STATE_DIR="${HOME}/.cache/judgesearch"
STATE_FILE="${STATE_DIR}/sf-watcher.last.json"
LOG="${HOME}/Library/Logs/judgesearch-sf-watcher.out.log"
WORKER_URL="${WORKER_URL:-https://judge-search.barkleesanders.workers.dev}"

mkdir -p "$STATE_DIR" "$(dirname "$LOG")"

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }
log() { printf "[%s] %s\n" "$(ts)" "$*" | tee -a "$LOG" >&2; }

notify() {
  local subject="$1"
  local body="$2"
  if [[ -n "${TELEGRAM_BOT_TOKEN:-}" && -n "${TELEGRAM_CHAT_ID:-}" ]]; then
    curl -sS --max-time 10 -X POST \
      "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      -d chat_id="${TELEGRAM_CHAT_ID}" \
      -d parse_mode=Markdown \
      -d text="⚖️ *JudgeSearch SF* — ${subject}
${body}" \
      -o /dev/null || log "telegram send failed"
  else
    log "telegram unconfigured — would have sent: ${subject} :: ${body}"
  fi
}

if [[ ! -r "$CREDS" ]]; then
  log "missing credentials file at $CREDS — bootstrap with the example template"
  notify "Watcher disabled" "Credentials file missing: \`${CREDS}\`. Run setup once."
  exit 1
fi

# Source credentials safely (no command exec from the file)
set -a
# shellcheck source=/dev/null
. "$CREDS"
set +a

if [[ -z "${UPLOAD_SECRET:-}" ]]; then
  log "UPLOAD_SECRET unset in $CREDS"
  notify "Watcher disabled" "\`UPLOAD_SECRET\` missing in credentials file."
  exit 1
fi

cd "$REPO"
log "starting SF refresh — repo=$REPO worker=$WORKER_URL"

# Run scraper with upload (already includes its own logging)
TMP_OUT=$(mktemp)
trap 'rm -f "$TMP_OUT"' EXIT

if ! UPLOAD_SECRET="$UPLOAD_SECRET" /usr/bin/env node scrapers/fetch-sf-hf.mjs \
      --upload --worker "$WORKER_URL" >"$TMP_OUT" 2>&1; then
  EXIT_CODE=$?
  log "scraper failed with exit $EXIT_CODE"
  tail -40 "$TMP_OUT" | tee -a "$LOG"
  notify "❌ Refresh failed" "Exit $EXIT_CODE — see \`${LOG}\` for the tail. Site is on stale data; protective merge kept the previous payload."
  exit "$EXIT_CODE"
fi

# Append scraper output to log
cat "$TMP_OUT" >>"$LOG"

# Parse the temp JSON the scraper just wrote
TMP_JSON="${REPO}/scrapers/.tmp-san-francisco.json"
if [[ ! -r "$TMP_JSON" ]]; then
  log "scraper succeeded but $TMP_JSON not found"
  notify "⚠️ Refresh suspect" "Upload returned 200 but local JSON missing. Investigate."
  exit 1
fi

JUDGE_COUNT=$(/usr/bin/python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(len(d.get('judges',[])))" "$TMP_JSON")
TOTAL_CASES=$(/usr/bin/python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('total_cases',0))" "$TMP_JSON")

log "ok — judges=$JUDGE_COUNT cases=$TOTAL_CASES"

# Compare to last run; alert on big drops (silent-failure protection)
PREV_JUDGES=0
PREV_CASES=0
if [[ -r "$STATE_FILE" ]]; then
  PREV_JUDGES=$(/usr/bin/python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('judges',0))" "$STATE_FILE" 2>/dev/null || echo 0)
  PREV_CASES=$(/usr/bin/python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('cases',0))" "$STATE_FILE" 2>/dev/null || echo 0)
fi

# Persist current run state
cat >"$STATE_FILE" <<EOF
{"judges": $JUDGE_COUNT, "cases": $TOTAL_CASES, "ran_at": "$(ts)"}
EOF

# Alert thresholds: zero judges OR cases dropped >40% from previous run
if [[ "$JUDGE_COUNT" -eq 0 ]]; then
  notify "❌ Zero judges returned" "Scraper succeeded but produced 0 judges — dataset schema may have shifted. Worker still serving the previous payload (protective merge)."
  exit 1
fi

if [[ "$PREV_CASES" -gt 0 ]]; then
  # Integer math: drop% = (prev - curr) * 100 / prev
  DROP=$(( (PREV_CASES - TOTAL_CASES) * 100 / PREV_CASES ))
  if (( DROP > 40 )); then
    notify "⚠️ Big drop in case count" "Cases fell from $PREV_CASES → $TOTAL_CASES (~${DROP}% drop). Worker auto-kept the previous payload via protective merge — manual review recommended."
    exit 1
  fi
fi

# Quiet success — no Telegram noise unless something to act on.
log "done"
exit 0

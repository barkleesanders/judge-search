// Run: npm test   (node --test, no framework, no build — Node >= 23)
//
// Every gate here is paired with its own negative control: a test that proves
// the check goes RED on a known-bad input. A guard nobody has watched fail is
// not a guard, and "0 findings" from a disabled check is indistinguishable from
// "0 findings" from clean data.

import assert from "node:assert/strict";
import { test } from "node:test";
import {
	type AuditableJudge,
	auditJudges,
	CW_NON_JUDGE_ROWS,
	dedupeById,
	isCourtWatchNonJudgeRow,
	looksNonPersonal,
} from "./audit.ts";

const judge = (over: Partial<AuditableJudge> = {}): AuditableJudge => ({
	id: "cw-254",
	name: "CLARK, BRANTLEY SCOTT JR",
	city: "Bay County",
	court: "Bay County, FL",
	total_cases: 98,
	fta_count: 23,
	rearrest_count: 65,
	revocation_count: 10,
	...over,
});

// ── The exclusion list ──

test("excludes the two known non-person rows", () => {
	assert.equal(
		isCourtWatchNonJudgeRow(266, "AFFIDAVIT NOT COMPLETED AT FIRST APPEARANCE"),
		true,
	);
	assert.equal(
		isCourtWatchNonJudgeRow(269, "BONDED PRIOR TO FIRST APPEARANCE"),
		true,
	);
	assert.equal(Object.keys(CW_NON_JUDGE_ROWS).length, 2);
});

test("NEGATIVE CONTROL: a renumbered upstream must not delete a real judge", () => {
	// If CourtWatch reassigns id 266 to a real person, the pair no longer
	// matches and the row survives. This is the whole reason the list is keyed
	// on id AND name rather than on id alone.
	assert.equal(isCourtWatchNonJudgeRow(266, "CLARK, BRANTLEY SCOTT JR"), false);
	assert.equal(isCourtWatchNonJudgeRow(269, "SMILEY, ELIJAH"), false);
	// ...and a renamed sentinel at an unknown id is likewise not silently dropped.
	assert.equal(
		isCourtWatchNonJudgeRow(999, "BONDED PRIOR TO FIRST APPEARANCE"),
		false,
	);
});

test("real ALL-CAPS judges are never excluded", () => {
	// The 15 real Bay County judges stored ALL-CAPS "LAST, FIRST". A name-shaped
	// rule would delete every one of them.
	for (const name of [
		"CAMPBELL, TIMOTHY CRAIG",
		"CLARK, BRANTLEY SCOTT JR",
		"COLLIER, DEVIN",
		"DYER, WILLIAM",
		"GAY, SHONNA YOUNG",
		"GRAMMER, JOE",
		"HENRY, WILLIAM",
		"JEFCOAT, SHALLA",
		"MALLORY, PETER A",
		"PATTERSON, CHRISTOPHER N",
		"REGISTER, TIMOTHY",
		"SMILEY, ELIJAH",
		"SMITH, TRACY",
		"STEPHENSON, DUSTIN",
		"VANN, SHANE",
	]) {
		assert.equal(looksNonPersonal(name), false, `${name} must not be flagged`);
		for (const id of [266, 269])
			assert.equal(isCourtWatchNonJudgeRow(id, name), false);
	}
});

test("the shape signal flags both sentinels and nothing else seen live", () => {
	assert.equal(looksNonPersonal("BONDED PRIOR TO FIRST APPEARANCE"), true);
	assert.equal(
		looksNonPersonal("AFFIDAVIT NOT COMPLETED AT FIRST APPEARANCE"),
		true,
	);
	assert.equal(looksNonPersonal("John Beamer"), false);
	assert.equal(looksNonPersonal("Celia Thacker Dorn"), false);
});

// ── auditJudges ──

test("a clean set passes", () => {
	const r = auditJudges("miami", [
		judge(),
		judge({ id: "cw-255", name: "SMITH, TRACY" }),
	]);
	assert.equal(r.ok, true);
	assert.deepEqual(r.errors, []);
	assert.equal(r.stats.duplicate_ids, 0);
});

test("NEGATIVE CONTROL: duplicate id goes RED", () => {
	const r = auditJudges("los-angeles", [judge(), judge()]);
	assert.equal(r.ok, false);
	assert.equal(r.stats.duplicate_ids, 1);
	assert.match(r.errors.join("\n"), /duplicate id cw-254 appears 2x/);
});

test("NEGATIVE CONTROL: duplicate name+city WARNS but does not fail", () => {
	// Two ids, one name. Never auto-merged: merging would invent a judge with
	// the combined caseload, dropping one would erase a real person's record.
	const r = auditJudges("miami", [
		judge({ id: "cw-222", name: "John Beamer", city: "Orange County" }),
		judge({ id: "cw-242", name: "John Beamer", city: "Orange County" }),
	]);
	assert.equal(r.stats.duplicate_name_city, 1);
	assert.match(r.warnings.join("\n"), /needs a human, do not merge or drop/);
	assert.equal(r.ok, true, "a name collision is not itself a publish blocker");
});

test("NEGATIVE CONTROL: county/court mismatch goes RED", () => {
	// The exact live bug: feed says Bay, court string says Orange County.
	const rows = [judge({ court: "Circuit Court, Orange County, FL" })];
	const r = auditJudges("miami", rows, { jurisdictionOf: () => "Bay" });
	assert.equal(r.ok, false);
	assert.match(
		r.errors.join("\n"),
		/feed says jurisdiction "Bay" but court reads/,
	);
});

test("REGRESSION: a jurisdictionOf derived from row.court is a tautology", () => {
	// Documents the bug this option's first wiring actually shipped with. Parsing
	// the county back out of the court string makes the check compare a value to
	// itself, so it passes on data that is entirely wrong. Both rows below are
	// mislabelled; the tautological reader calls them clean, the honest reader
	// (the feed's own value) calls them RED.
	const rows = [
		judge({ id: "cw-253", court: "Circuit Court, Orange County, FL" }),
		judge({ id: "cw-254", court: "Circuit Court, Orange County, FL" }),
	];
	const tautological = (row: AuditableJudge) =>
		row.court.endsWith(" County, FL")
			? row.court.slice(0, -" County, FL".length)
			: undefined;
	assert.equal(
		auditJudges("miami", rows, { jurisdictionOf: tautological }).ok,
		true,
		"the tautology passes — which is exactly why it must never be used",
	);

	// The feed said Bay for both. That is the reading that catches it.
	const feed: Record<string, string> = { "cw-253": "Bay", "cw-254": "Bay" };
	const honest = auditJudges("miami", rows, {
		jurisdictionOf: (row) => feed[row.id],
	});
	assert.equal(honest.ok, false);
	assert.equal(honest.errors.length, 2, "every mislabelled row is named");
});

test("matching county/court passes, and reports how many rows it checked", () => {
	const r = auditJudges("miami", [judge()], { jurisdictionOf: () => "Bay" });
	assert.equal(r.ok, true);
	assert.equal(r.stats.jurisdiction_checked, 1);
	assert.deepEqual(r.unmeasured, []);
});

test("an unrunnable check reports UNMEASURED, never a pass", () => {
	// Three outcomes, never two.
	const noFn = auditJudges("chicago", [judge()]);
	assert.equal(noFn.stats.jurisdiction_checked, 0);
	assert.match(noFn.unmeasured.join("\n"), /no jurisdictionOf/);

	const noData = auditJudges("chicago", [judge()], {
		jurisdictionOf: () => undefined,
	});
	assert.equal(noData.stats.jurisdiction_checked, 0);
	assert.match(noData.unmeasured.join("\n"), /no row carried a jurisdiction/);
});

test("NEGATIVE CONTROL: impossible counts go RED", () => {
	assert.equal(
		auditJudges("x", [judge({ fta_count: 99, total_cases: 5 })]).ok,
		false,
	);
	assert.equal(auditJudges("x", [judge({ total_cases: -1 })]).ok, false);
	assert.equal(
		auditJudges("x", [judge({ rearrest_count: Number.NaN })]).ok,
		false,
	);
	assert.equal(auditJudges("x", [judge({ name: "  " })]).ok, false);
});

test("NEGATIVE CONTROL: a fresh non-person row is surfaced even though no rule drops it", () => {
	// This is the case the exclusion list CANNOT catch — a new sentinel string at
	// a new id. It must not be silently published.
	const r = auditJudges("miami", [
		judge(),
		judge({ id: "cw-300", name: "NO AFFIDAVIT ON FILE" }),
	]);
	assert.equal(r.stats.non_personal_names, 1);
	assert.match(r.warnings.join("\n"), /does not look like a person's name/);
});

// ── dedupeById ──

test("identical twins collapse silently (the live cl-14533 case)", () => {
	const a = judge({
		id: "cl-14533",
		name: "Barbara R. Johnson",
		city: "Los Angeles",
	});
	const res = dedupeById([a, { ...a }]);
	assert.equal(res.rows.length, 1);
	assert.equal(res.identicalDropped, 1);
	assert.deepEqual(res.conflicts, []);
});

test("NEGATIVE CONTROL: an id reused by DIFFERENT rows is reported, not silently picked", () => {
	const res = dedupeById([
		judge({ id: "cl-1", name: "Alice Adams" }),
		judge({ id: "cl-1", name: "Bob Baker" }),
	]);
	assert.equal(res.rows.length, 1);
	assert.equal(res.identicalDropped, 0);
	assert.match(res.conflicts.join("\n"), /reused by rows that differ/);
});

test("dedupe preserves order and leaves distinct ids alone", () => {
	const res = dedupeById([
		judge({ id: "a", name: "A" }),
		judge({ id: "b", name: "B" }),
		judge({ id: "c", name: "C" }),
	]);
	assert.deepEqual(
		res.rows.map((r) => r.id),
		["a", "b", "c"],
	);
	assert.equal(res.identicalDropped, 0);
});

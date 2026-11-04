// Wide-event tests (native observability rollout, 2026-11-04).
// Run: npm test (node --test src/*.test.ts)

import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import worker from "./index.ts";
import { emitWideEvent } from "./wide-event.ts";

let logged: string[] = [];
const originalLog = console.log;
function capture() {
	logged = [];
	console.log = (msg?: unknown) => {
		logged.push(String(msg));
	};
}

afterEach(() => {
	console.log = originalLog;
});

describe("emitWideEvent", () => {
	it("emits one structured http_request event", () => {
		capture();
		const req = new Request("https://example.com/hello?token=secret", {
			headers: { "cf-ray": "ray-123", "user-agent": "node-test" },
		});
		emitWideEvent(req, 200, Date.now());
		assert.equal(logged.length, 1);
		const event = JSON.parse(logged[0]);
		assert.equal(event.event, "http_request");
		assert.equal(event.method, "GET");
		// Pathname only: the query string (and its token) never reaches the log.
		assert.equal(event.path, "/hello");
		assert.equal(event.status, 200);
		assert.equal(event.outcome, "ok");
		assert.equal(event.request_id, "ray-123");
		assert.equal(typeof event.duration_ms, "number");
	});

	it("marks an error event with outcome=error", () => {
		capture();
		emitWideEvent(
			new Request("https://example.com/boom"),
			500,
			Date.now(),
			"Error",
		);
		assert.equal(logged.length, 1);
		const event = JSON.parse(logged[0]);
		assert.equal(event.status, 500);
		assert.equal(event.outcome, "error");
		assert.equal(event.error, "Error");
	});

	it("the default export emits one event per fetch", async () => {
		capture();
		const res = await worker.fetch(
			new Request("https://fixture.test/"),
			{} as Parameters<typeof worker.fetch>[1],
		);
		assert.equal(res.status, 200);
		assert.equal(logged.length, 1);
		const event = JSON.parse(logged[0]);
		assert.equal(event.event, "http_request");
		assert.equal(event.path, "/");
		assert.equal(event.status, 200);
	});
});

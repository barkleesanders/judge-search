// Test helpers for asserting over source text.
//
// Structural assertions need the exact body of one function, and the two ways
// of approximating that are both wrong in ways that pass silently:
//   - a fixed character window shrinks when someone adds a comment (breaks for
//     an unrelated reason) AND matches an identical line in a neighbouring
//     block (too weak);
//   - an extractor that returns "" on failure makes every `includes` check
//     false and every `not.includes` check true — one broken helper becomes a
//     suite-wide false negative.
// Brace-matching, and throwing, is the fix for both.

/** Slice from `decl` through its matching closing brace, inclusive. */
export function bodyOf(decl: string, src: string): string {
	const start = src.indexOf(decl);
	if (start === -1) throw new Error(`declaration not found: ${decl}`);
	let depth = 0;
	let seen = false;
	for (let i = start; i < src.length; i++) {
		if (src[i] === "{") {
			depth++;
			seen = true;
		} else if (src[i] === "}" && seen && --depth === 0) {
			return src.slice(start, i + 1);
		}
	}
	throw new Error(`unbalanced braces after: ${decl}`);
}

/**
 * The page's own inline <script> — the largest one without a `src`, so a CDN
 * tag can never be mistaken for it. Throws below a size floor: an empty or
 * truncated extraction would make every assertion over it vacuously true.
 */
export function clientScript(html: string, minChars = 1000): string {
	let best = "";
	for (const m of html.matchAll(/<script(\s[^>]*)?>([\s\S]*?)<\/script>/g)) {
		if (/\ssrc\s*=/.test(m[1] ?? "")) continue;
		if (m[2].length > best.length) best = m[2];
	}
	if (best.length < minChars)
		throw new Error(
			`inline <script> came back ${best.length} chars (< ${minChars}) — extraction has drifted, so any assertion over it is vacuous`,
		);
	return best;
}

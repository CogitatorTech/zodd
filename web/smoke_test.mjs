// Smoke test for the playground WASM module. Run with `make web-test`
// (which builds the module and stages it as web/zodd.wasm first).

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const wasmPath = fileURLToPath(new URL("./zodd.wasm", import.meta.url));
const bytes = await readFile(wasmPath);
const { instance } = await WebAssembly.instantiate(bytes, {});
const exports = instance.exports;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function run(source) {
  const sourceBytes = encoder.encode(source);
  // Zero-length allocations return a dangling pointer; pass (0, 0) instead.
  let ptr = 0;
  if (sourceBytes.length > 0) {
    ptr = exports.alloc(sourceBytes.length);
    if (ptr === 0) throw new Error("alloc failed");
    // Create the view after alloc: memory growth detaches earlier views.
    new Uint8Array(exports.memory.buffer, ptr, sourceBytes.length).set(sourceBytes);
  }
  const status = exports.run(ptr, sourceBytes.length);
  if (ptr !== 0) exports.dealloc(ptr, sourceBytes.length);
  // Re-view after run for the same reason.
  const out = decoder.decode(
    new Uint8Array(exports.memory.buffer, exports.outputPtr(), exports.outputLen()),
  );
  return { status, out };
}

function expect(condition, message) {
  if (!condition) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

// Success path: recursion plus a stored query.
const closure = run(`
  edge(1, 2). edge(2, 3). edge(3, 4).
  path(X, Y) :- edge(X, Y).
  path(X, Z) :- path(X, Y), edge(Y, Z).
  ?- path(1, X).
`);
expect(closure.status === 0, `closure status is ${closure.status}`);
expect(closure.out.includes("?- path:"), "stored query header missing");
expect(closure.out.includes("(1, 2)"), "(1, 2) missing from closure output");
expect(closure.out.includes("(1, 4)"), "(1, 4) missing from closure output");
expect(!closure.out.includes("(2, 3)"), "unbound pair leaked into a bound query");

// Strings, negation, and aggregates; no stored query dumps derived relations.
const features = run(`
  node("a"). node("b"). blocked("b").
  safe(X) :- node(X), not blocked(X).
  edge("a", "b"). edge("a", "c").
  deg(N, count(M)) :- edge(N, M).
`);
expect(features.status === 0, `features status is ${features.status}`);
expect(features.out.includes('("a")'), "negation result missing");
expect(features.out.includes('("a", 2)'), "aggregate result missing");

// Error path: must report text with a location, never trap.
const bad = run("edge(1, X).");
expect(bad.status !== 0, "non-ground fact accepted");
expect(bad.out.includes("error:"), "error name missing from output");
expect(bad.out.includes("1:1:"), "line:column missing from diagnostic");

// Unstratifiable program: reported, not trapped.
const cycle = run("q(1). p(X) :- q(X), not p(X).");
expect(cycle.status !== 0, "negation cycle accepted");
expect(cycle.out.includes("NegationCycle"), "cycle error name missing");

// Empty program: no results, no trap.
const empty = run("");
expect(empty.status === 0, `empty status is ${empty.status}`);
expect(empty.out.includes("(no results)"), "empty program output missing");

// Repeated calls reuse the module cleanly.
const again = run("f(1). g(X) :- f(X). ?- g(1).");
expect(again.status === 0, `repeat status is ${again.status}`);
expect(again.out.includes("(1)"), "repeat run output missing");

console.log("WASM smoke test OK");

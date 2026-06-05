// Smoke test for the web frontend Wasm module. Run with `make web-test`
// (which builds the module and stages it as web/zodd.wasm first).

import {readFile} from "node:fs/promises";
import {fileURLToPath} from "node:url";
import {createRequire} from "node:module";

const require = createRequire(import.meta.url);
const {parseTuple, parseOutputToTables, cleanValue} = require("./main.js");


const wasmPath = fileURLToPath(new URL("./zodd.wasm", import.meta.url));
const bytes = await readFile(wasmPath);
const {instance} = await WebAssembly.instantiate(bytes, {});
const exports = instance.exports;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// Calls a Wasm export taking (ptr, len) pairs, one per string argument.
function call(fnName, strings) {
    const buffers = strings.map((s) => encoder.encode(s));
    const ptrs = buffers.map((bytes) => {
        // Zero-length allocations return a dangling pointer; pass (0, 0) instead.
        if (bytes.length === 0) return 0;
        const ptr = exports.alloc(bytes.length);
        if (ptr === 0) throw new Error("alloc failed");
        // Create the view after alloc: memory growth detaches earlier views.
        new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
        return ptr;
    });
    const args = [];
    buffers.forEach((bytes, i) => args.push(ptrs[i], bytes.length));
    const status = exports[fnName](...args);
    buffers.forEach((bytes, i) => {
        if (ptrs[i] !== 0) exports.dealloc(ptrs[i], bytes.length);
    });
    // Re-view after the call for the same reason.
    const out = decoder.decode(
        new Uint8Array(exports.memory.buffer, exports.outputPtr(), exports.outputLen()),
    );
    return {status, out};
}

function run(source) {
    return call("run", [source]);
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

// Comparison operators filter rule bodies.
const compare = run(`
  age(1, 17). age(2, 30). age(3, 18).
  adult(X) :- age(X, A), A >= 18.
  ?- adult(X).
`);
expect(compare.status === 0, `compare status is ${compare.status}`);
expect(compare.out.includes("(2)"), "adult(2) missing from comparison output");
expect(!compare.out.includes("(1)"), "minor leaked through the comparison filter");

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

// Plan rendering.
const closureSource = `
  edge(1, 2). edge(2, 3). edge(3, 4).
  path(X, Y) :- edge(X, Y).
  path(X, Z) :- path(X, Y), edge(Y, Z).
`;
const plan = call("explainPlan", [closureSource]);
expect(plan.status === 0, `plan status is ${plan.status}`);
expect(plan.out.includes("scan edge -> (X, Y)"), "scan step missing from plan");
expect(plan.out.includes("join edge on (Y)"), "join step missing from plan");
expect(plan.out.includes("head path(X, Z)"), "head projection missing from plan");

// Plan errors are reported, not trapped.
const badPlan = call("explainPlan", ["p(X) :- q(Y)."]);
expect(badPlan.status !== 0, "unsafe rule accepted by explainPlan");

// Proof tree for a derived tuple.
const proof = call("explain", [closureSource, "path(1, 3)"]);
expect(proof.status === 0, `explain status is ${proof.status}`);
expect(proof.out.includes("path(1, 3)"), "explained tuple missing from proof");
expect(proof.out.includes("via rule"), "rule line missing from proof");
expect(proof.out.includes("edge(1, 2) (fact)"), "fact leaf missing from proof");

// Tuples outside the result set are reported, not trapped.
const missing = call("explain", [closureSource, "path(3, 1)"]);
expect(missing.status !== 0, "absent tuple accepted by explain");
expect(missing.out.includes("not in the result set"), "absent tuple message missing");

// Malformed atoms are reported, not trapped.
const badAtom = call("explain", [closureSource, "path(1, "]);
expect(badAtom.status !== 0, "malformed atom accepted by explain");

// --- Regression and Unit Tests for JS logic in main.js ------------------------
console.log("Running JS utility unit tests...");

// 1. parseTuple
const t1 = parseTuple("1, 2, 3");
expect(t1.length === 3 && t1[0] === "1" && t1[1] === "2" && t1[2] === "3", "simple parseTuple failed");

const t2 = parseTuple('"hello", 42, "world, test"');
expect(t2.length === 3 && t2[0] === '"hello"' && t2[1] === "42" && t2[2] === '"world, test"', "parseTuple with commas inside quotes failed");

const t3 = parseTuple("");
expect(t3.length === 0, "empty parseTuple should return empty array");

const t4 = parseTuple("   ");
expect(t4.length === 0, "whitespace-only parseTuple should return empty array");

const t5 = parseTuple(String.raw`"line\n", "quote\"", "path\\root"`);
expect(
    t5.length === 3 && t5[0] === String.raw`"line\n"` && t5[1] === String.raw`"quote\""` && t5[2] === String.raw`"path\\root"`,
    "parseTuple should preserve escaped string contents",
);

// 2. cleanValue
expect(cleanValue('"hello"') === "hello", "cleanValue unquote failed");
expect(cleanValue('"hello\\nworld"') === "hello\nworld", "cleanValue newline escaped failed");
expect(cleanValue(String.raw`"quote\""`) === "quote\"", "cleanValue escaped quote failed");
expect(cleanValue(String.raw`"path\\root"`) === "path\\root", "cleanValue escaped backslash failed");
expect(cleanValue("42") === "42", "cleanValue number unquoted failed");

// 3. parseOutputToTables
const textOutput = `
?- path:
  (1, 2)
  (1, 3)
`;
const htmlTable = parseOutputToTables(textOutput);
expect(htmlTable.includes('<table class="output-table-el">'), "tabular format missing from output table HTML");
expect(htmlTable.includes('<h4 class="output-table-title">?- path</h4>'), "title missing from output table HTML");
expect(htmlTable.includes("Col 1") && htmlTable.includes("Col 2"), "table columns headers missing");
expect(htmlTable.includes('data-atom="path(1, 2)"'), "explain atom attribute missing from table rows");

// String values keep their quotes inside the atom attribute.
const htmlStrings = parseOutputToTables('safe:\n  ("a")\n');
expect(htmlStrings.includes('data-atom="safe(&quot;a&quot;)"'), "quoted atom attribute missing");

const htmlEscapedStrings = parseOutputToTables(String.raw`msg:
  ("line\n", "quote\"", "path\\root")
`);
expect(htmlEscapedStrings.includes("line\n"), "escaped newline should render as a newline in table cells");
expect(
    htmlEscapedStrings.includes(String.raw`data-atom="msg(&quot;line\n&quot;, &quot;quote\&quot;&quot;, &quot;path\\root&quot;)"`),
    "escaped atom attribute missing",
);

const emptyOutput = `(no results)\n`;
const htmlEmpty = parseOutputToTables(emptyOutput);
expect(htmlEmpty.includes("No results"), "empty output handling failed");

console.log("JS utility unit tests SUCCESS");

console.log("Wasm smoke test SUCCESS");

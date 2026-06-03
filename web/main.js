"use strict";

// --- Example programs -------------------------------------------------------

const EXAMPLES = [
  {
    name: "Transitive closure",
    source: `% A directed graph and its transitive closure.
edge(1, 2).
edge(2, 3).
edge(3, 4).

path(X, Y) :- edge(X, Y).
path(X, Z) :- path(X, Y), edge(Y, Z).

% Which nodes are reachable from node 1?
?- path(1, X).
`,
  },
  {
    name: "Negation (RBAC)",
    source: `% Role inheritance with explicit denials.
inherits("admin", "editor").
inherits("editor", "viewer").
grants("viewer", "read").
grants("editor", "write").
grants("admin", "configure").
denied("editor", "configure").

role(R, S) :- inherits(R, S).
role(R, T) :- role(R, S), inherits(S, T).

has_perm(R, P) :- grants(R, P).
has_perm(R, P) :- role(R, S), grants(S, P).

allowed(R, P) :- has_perm(R, P), not denied(R, P).

?- allowed("editor", X).
?- allowed("admin", X).
`,
  },
  {
    name: "Aggregates (packages)",
    source: `% A package registry: yanked packages taint their dependents.
package("app"). package("http"). package("json").
package("io"). package("core"). package("leftpad").

dep("app", "http"). dep("app", "json").
dep("http", "io"). dep("json", "io").
dep("io", "core").
dep("json", "leftpad").

yanked("leftpad").

needs(P, D) :- dep(P, D).
needs(P, D) :- needs(P, M), dep(M, D).

tainted(P) :- yanked(P).
tainted(P) :- needs(P, D), yanked(D).
safe(P) :- package(P), not tainted(P).

fanout(P, count(D)) :- needs(P, D).

?- safe(X).
?- fanout(X, N).
`,
  },
  {
    name: "Same generation",
    source: `% Two people are in the same generation if they share an
% ancestor at the same depth.
parent("alice", "carol").
parent("bob", "carol").
parent("carol", "eve").
parent("dave", "eve").

sg(X, Y) :- parent(X, P), parent(Y, P).
sg(X, Y) :- parent(X, P), parent(Y, Q), sg(P, Q).

?- sg("alice", X).
`,
  },
];

// --- Wasm glue ---------------------------------------------------------------

let wasm = null;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function loadWasm() {
  const imports = {};
  try {
    if (typeof WebAssembly.instantiateStreaming === "function") {
      const { instance } = await WebAssembly.instantiateStreaming(fetch("zodd.wasm"), imports);
      return instance.exports;
    }
  } catch {
    // Fall through to ArrayBuffer instantiation (e.g. file:// or MIME issues).
  }
  const bytes = await (await fetch("zodd.wasm")).arrayBuffer();
  const { instance } = await WebAssembly.instantiate(bytes, imports);
  return instance.exports;
}

function runProgram(source) {
  const sourceBytes = encoder.encode(source);
  // Zero-length allocations return a dangling pointer; pass (0, 0) instead.
  let ptr = 0;
  if (sourceBytes.length > 0) {
    ptr = wasm.alloc(sourceBytes.length);
    if (ptr === 0) throw new Error("Wasm allocation failed");
    // Views must be created after each call into Wasm: memory growth
    // detaches previously created typed arrays.
    new Uint8Array(wasm.memory.buffer, ptr, sourceBytes.length).set(sourceBytes);
  }
  const status = wasm.run(ptr, sourceBytes.length);
  if (ptr !== 0) wasm.dealloc(ptr, sourceBytes.length);
  const out = decoder.decode(
    new Uint8Array(wasm.memory.buffer, wasm.outputPtr(), wasm.outputLen()),
  );
  return { status, out };
}

// --- Syntax highlighting ------------------------------------------------------

const TOKEN_RE = new RegExp(
  [
    "(%[^\\n]*)", // 1: comment
    '("(?:\\\\.|[^"\\\\\\n])*"?)', // 2: string
    "\\b(not|count|sum|min|max)\\b", // 3: keyword
    "\\b(\\d+)\\b", // 4: number
    "\\b([A-Z_][A-Za-z0-9_]*)\\b", // 5: variable
    "\\b([a-z][A-Za-z0-9_]*)\\b", // 6: predicate
    "(\\?-|:-|[(),.])", // 7: punctuation
  ].join("|"),
  "g",
);

const TOKEN_CLASSES = [
  "tok-comment", "tok-string", "tok-keyword", "tok-number",
  "tok-variable", "tok-predicate", "tok-punct",
];

function escapeHtml(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function highlight(source) {
  let html = "";
  let last = 0;
  for (const match of source.matchAll(TOKEN_RE)) {
    html += escapeHtml(source.slice(last, match.index));
    for (let group = 1; group <= TOKEN_CLASSES.length; group++) {
      if (match[group] !== undefined) {
        html += `<span class="${TOKEN_CLASSES[group - 1]}">${escapeHtml(match[group])}</span>`;
        break;
      }
    }
    last = match.index + match[0].length;
  }
  html += escapeHtml(source.slice(last));
  return html;
}

// --- Permalinks ----------------------------------------------------------------

function encodeProgram(source) {
  const bytes = encoder.encode(source);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function decodeProgram(encoded) {
  const base64 = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(base64);
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return decoder.decode(bytes);
}

// --- UI wiring ------------------------------------------------------------------

const sourceEl = document.getElementById("source");
const highlightEl = document.getElementById("highlight");
const highlightCodeEl = document.getElementById("highlight-code");
const outputEl = document.getElementById("output");
const statusEl = document.getElementById("status");
const examplesEl = document.getElementById("examples");
const runEl = document.getElementById("run");
const shareEl = document.getElementById("share");
const dividerEl = document.getElementById("divider");
const editorPane = document.querySelector(".editor-pane");

function syncHighlight() {
  // A trailing newline keeps the backdrop the same height as the textarea.
  highlightCodeEl.innerHTML = highlight(sourceEl.value) + "\n";
  highlightEl.scrollTop = sourceEl.scrollTop;
  highlightEl.scrollLeft = sourceEl.scrollLeft;
}

function setSource(text) {
  sourceEl.value = text;
  syncHighlight();
}

function setStatus(text, kind) {
  statusEl.textContent = text;
  statusEl.className = kind || "";
}

function execute() {
  if (!wasm) return;
  const started = performance.now();
  let result;
  try {
    result = runProgram(sourceEl.value);
  } catch (err) {
    outputEl.textContent = `internal error: ${err}`;
    outputEl.className = "error";
    setStatus("trap", "error");
    return;
  }
  const elapsed = (performance.now() - started).toFixed(1);
  outputEl.textContent = result.out || "(no output)";
  outputEl.className = result.status === 0 ? "" : "error";
  setStatus(result.status === 0 ? `ok, ${elapsed} ms` : `error, ${elapsed} ms`, result.status === 0 ? "ok" : "error");
}

function share() {
  const url = new URL(window.location.href);
  url.hash = "program=" + encodeProgram(sourceEl.value);
  history.replaceState(null, "", url);
  const copied = navigator.clipboard?.writeText(url.href);
  if (copied) {
    copied.then(
      () => setStatus("link copied", "ok"),
      () => setStatus("link in address bar", "ok"),
    );
  } else {
    setStatus("link in address bar", "ok");
  }
}

// Examples dropdown.
for (const [index, example] of EXAMPLES.entries()) {
  const option = document.createElement("option");
  option.value = String(index);
  option.textContent = example.name;
  examplesEl.appendChild(option);
}
examplesEl.addEventListener("change", () => {
  setSource(EXAMPLES[Number(examplesEl.value)].source);
});

// Editor events.
sourceEl.addEventListener("input", syncHighlight);
sourceEl.addEventListener("scroll", syncHighlight);
sourceEl.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    execute();
  }
});

runEl.addEventListener("click", execute);
shareEl.addEventListener("click", share);

// Resizable split view.
dividerEl.addEventListener("pointerdown", (event) => {
  event.preventDefault();
  dividerEl.classList.add("dragging");
  dividerEl.setPointerCapture(event.pointerId);
  const onMove = (move) => {
    const bounds = document.getElementById("split").getBoundingClientRect();
    const fraction = Math.min(0.85, Math.max(0.15, (move.clientX - bounds.left) / bounds.width));
    editorPane.style.flexBasis = `${(fraction * 100).toFixed(1)}%`;
  };
  const onUp = () => {
    dividerEl.classList.remove("dragging");
    dividerEl.removeEventListener("pointermove", onMove);
    dividerEl.removeEventListener("pointerup", onUp);
  };
  dividerEl.addEventListener("pointermove", onMove);
  dividerEl.addEventListener("pointerup", onUp);
});

// Initial program: a permalink if present, the first example otherwise.
(function initSource() {
  const hash = window.location.hash;
  if (hash.startsWith("#program=")) {
    try {
      setSource(decodeProgram(hash.slice("#program=".length)));
      return;
    } catch {
      // Bad permalink; fall back to the first example.
    }
  }
  setSource(EXAMPLES[0].source);
})();

// Load the Wasm module, then run the initial program.
loadWasm()
  .then((exports) => {
    wasm = exports;
    setStatus("ready", "ok");
    execute();
  })
  .catch((err) => {
    outputEl.textContent = `Failed to load zodd.wasm: ${err}\n\nBuild it with: make web`;
    outputEl.className = "error";
    setStatus("load failed", "error");
  });

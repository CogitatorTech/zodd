"use strict";

// --- Example programs -------------------------------------------------------

const EXAMPLES = [
  {
    name: "Transitive closure",
    source: `% A directed graph and its transitive closure.
%
% Base facts: define a directed graph with edges between nodes.
edge(1, 2).
edge(2, 3).
edge(3, 4).

% Rules: compute path as the transitive closure of edge.
% Base case: any direct edge is a path.
path(X, Y) :- edge(X, Y).
% Recursive case: if a path exists from X to Y and an edge from Y to Z, then a path exists from X to Z.
path(X, Z) :- path(X, Y), edge(Y, Z).

% Query: which nodes X are reachable from node 1?
?- path(1, X).
`,
  },
  {
    name: "Bestseller join",
    source: `% Which bestsellers cost what, and who wrote them?
%
% Base facts: map authors to books, list bestselling books, and associate books with prices.
author("Ursula K. Le Guin", "A Wizard of Earthsea").
author("Toni Morrison", "Beloved").
author("Ursula K. Le Guin", "The Left Hand of Darkness").
author("Terry Pratchett", "Mort").

bestseller("A Wizard of Earthsea").
bestseller("The Left Hand of Darkness").

price("A Wizard of Earthsea", 14).
price("Beloved", 17).
price("The Left Hand of Darkness", 15).
price("Mort", 12).

% Rule: perform a three-way join to match authors, bestsellers, and prices.
q(Name, Book, Dollars) :-
    author(Name, Book), bestseller(Book), price(Book, Dollars).

% Query: retrieve the names, books, and prices of all bestselling books.
?- q(Name, Book, Dollars).
`,
  },
  {
    name: "Network reachability",
    source: `% Which network zones can talk through routing and firewall rules?
%
% Base facts: define network links and firewall block rules.
link("internet", "dmz").
link("dmz", "app_tier").
link("app_tier", "db_tier").
link("dmz", "monitoring").
link("db_tier", "logging").
link("logging", "monitoring").
link("monitoring", "pci_zone").

blocked("internet", "app_tier").
blocked("internet", "db_tier").
blocked("internet", "pci_zone").
blocked("dmz", "db_tier").
blocked("dmz", "pci_zone").
blocked("app_tier", "pci_zone").

% Rules:
% Compute transitive physical reachability between zones.
reachable(A, B) :- link(A, B).
reachable(A, C) :- reachable(A, B), link(B, C).

% Compute allowed communication: paths that are reachable and not blocked by firewall rules.
allowed(A, B) :- reachable(A, B), not blocked(A, B).

% Define exposure: any network zone reachable from the internet.
exposure(Z) :- allowed("internet", Z).

% Queries:
% Query 1: which zones are exposed to the internet?
?- exposure(Z).
% Query 2: which zones can be accessed from the DMZ?
?- allowed("dmz", Z).
`,
  },
  {
    name: "Knowledge graph",
    source: `% A medical ontology: type hierarchy and property inheritance.
%
% Base facts: define disease hierarchy, symptoms, drug targets, and target associations.
is_a("heart_disease", "cardiovascular").
is_a("arrhythmia", "heart_disease").
is_a("hypertension", "cardiovascular").
is_a("asthma", "respiratory").

has_symptom("cardiovascular", "fatigue").
has_symptom("heart_disease", "chest_pain").
has_symptom("heart_disease", "shortness_of_breath").
has_symptom("arrhythmia", "irregular_heartbeat").
has_symptom("hypertension", "high_bp").
has_symptom("respiratory", "shortness_of_breath").
has_symptom("asthma", "wheezing").

targets("lisinopril", "ace_enzyme").
targets("metoprolol", "beta_receptor").
targets("amlodipine", "calcium_channel").

associated_with("ace_enzyme", "hypertension").
associated_with("ace_enzyme", "heart_disease").
associated_with("beta_receptor", "arrhythmia").
associated_with("beta_receptor", "heart_disease").
associated_with("calcium_channel", "hypertension").
associated_with("calcium_channel", "arrhythmia").

% Rules:
% Transitive subtyping for the disease classification hierarchy.
subtype(X, Y) :- is_a(X, Y).
subtype(X, Z) :- subtype(X, Y), is_a(Y, Z).

% Symptom inheritance: a disease has its own symptoms and inherits symptoms from its supertypes.
symptom(D, S) :- has_symptom(D, S).
symptom(D, S) :- subtype(D, D2), symptom(D2, S).

% Drug treatments: a drug treats a disease if the drug targets a protein associated with the disease.
treats(Drug, D) :- targets(Drug, P), associated_with(P, D).

% Side effects: a drug can trigger symptoms associated with the disease it treats.
side_effect(Drug, S) :- treats(Drug, D), symptom(D, S).

% Queries:
% Query 1: which drugs treat which diseases?
?- treats(Drug, D).
% Query 2: what are the potential side effects of metoprolol?
?- side_effect("metoprolol", S).
`,
  },
  {
    name: "Data lineage",
    source: `% PII flowing through an ETL pipeline; anonymization blocks it.
%
% Base facts: define initial PII sources, ETL transformations, anonymization boundaries, and public datasets.
source_pii("raw_users").
source_pii("raw_logs").

transform("raw_users", "user_profiles").
transform("user_profiles", "analytics_users").
transform("raw_orders", "order_details").
transform("analytics_users", "sales_report").
transform("order_details", "sales_report").
transform("order_details", "anonymized_orders").
transform("anonymized_orders", "public_dashboard").
transform("raw_logs", "enriched_logs").
transform("enriched_logs", "audit_trail").
transform("audit_trail", "log_summary").

anonymizes("order_details", "anonymized_orders").
anonymizes("audit_trail", "log_summary").

public_dataset("sales_report").
public_dataset("public_dashboard").
public_dataset("log_summary").

% Rules:
% Propagate PII status through transformations unless an anonymization step is applied.
contains_pii(D) :- source_pii(D).
contains_pii(D2) :-
    contains_pii(D1), transform(D1, D2), not anonymizes(D1, D2).

% Define a policy violation as a public dataset containing unanonymized PII.
violation(D) :- contains_pii(D), public_dataset(D).

% Queries:
% Query 1: which datasets contain PII?
?- contains_pii(D).
% Query 2: which datasets violate the privacy policy?
?- violation(D).
`,
  },
  {
    name: "RBAC authorization",
    source: `% Effective permissions through role inheritance and denials.
%
% Base facts: define user roles, role hierarchies, role permissions, and explicit denials.
user_role("alice", "viewer").
user_role("bob", "editor").
user_role("charlie", "superadmin").

role_hier("editor", "viewer").
role_hier("admin", "editor").
role_hier("superadmin", "admin").

role_perm("viewer", "read").
role_perm("editor", "write").
role_perm("admin", "delete").
role_perm("admin", "manage_users").
role_perm("superadmin", "audit").

denied("charlie", "delete").

% Rules:
% Resolve transitively inherited roles.
has_role(U, R) :- user_role(U, R).
has_role(U, R2) :- has_role(U, R1), role_hier(R1, R2).

% Map users to permissions through their active and inherited roles.
can_access(U, P) :- has_role(U, R), role_perm(R, P).

% Compute effective permissions by subtracting explicit user-specific denials.
effective(U, P) :- can_access(U, P), not denied(U, P).

% Queries:
% Query 1: what are the effective permissions of Charlie?
?- effective("charlie", P).
% Query 2: what are the effective permissions of Bob?
?- effective("bob", P).
`,
  },
  {
    name: "Taint analysis",
    source: `% Untrusted data flowing to security-sensitive sinks.
%
% Base facts: define taint sources, data flow pathways, sanitizers, and security-sensitive sinks.
source("v1").
source("v2").

flow("v1", "v3").
flow("v3", "v4").
flow("v2", "v5").
flow("v3", "v5").
flow("v6", "v7").

sanitized("v3", "v4").

sink("sql_query", "v5").
sink("html_render", "v4").
sink("html_render", "v3").
sink("log_message", "v7").

% Rules:
% Propagate taint state from sources along flow paths unless blocked by a sanitizer.
tainted(V) :- source(V).
tainted(V2) :- tainted(V1), flow(V1, V2), not sanitized(V1, V2).

% Define a vulnerability violation as tainted data reaching a security-sensitive sink.
violation(S, V) :- sink(S, V), tainted(V).

% Queries:
% Query 1: which variables are tainted?
?- tainted(V).
% Query 2: which sinks and variables trigger policy violations?
?- violation(S, V).
`,
  },
  {
    name: "Dependency resolution",
    source: `% Transitive dependencies and total install size per package.
%
% Base facts: define direct dependencies and package sizes (in kilobytes or megabytes).
direct_dep("app", "web_framework").
direct_dep("app", "logging").
direct_dep("web_framework", "http").
direct_dep("web_framework", "json").
direct_dep("http", "tls").
direct_dep("tls", "crypto").
direct_dep("tls", "base64").
direct_dep("crypto", "utils").
direct_dep("json", "utils").
direct_dep("logging", "utils").

size("app", 50).
size("web_framework", 200).
size("http", 120).
size("json", 80).
size("logging", 30).
size("crypto", 150).
size("tls", 90).
size("base64", 20).
size("utils", 10).

% Rules:
% Transitively compute dependencies of all packages.
dep(A, B) :- direct_dep(A, B).
dep(A, C) :- dep(A, B), direct_dep(B, C).

% Define the set of packages to install for package P (the package itself and its dependencies).
installs(P, P) :- size(P, _).
installs(P, D) :- dep(P, D).

% Compute total installation size by aggregating the sizes of all dependencies.
total_size(P, sum(S)) :- installs(P, D), size(D, S).

% Queries:
% Query 1: which packages does "app" transitively depend on?
?- dep("app", D).
% Query 2: what is the total installation size of each package?
?- total_size(P, S).
`,
  },
  {
    name: "Package registry",
    source: `% A package registry: yanked packages taint their dependents.
%
% Base facts: register packages, define direct dependencies, and list yanked (revoked) packages.
package("app"). package("http"). package("json").
package("io"). package("core"). package("leftpad").

dep("app", "http"). dep("app", "json").
dep("http", "io"). dep("json", "io").
dep("io", "core").
dep("json", "leftpad").

yanked("leftpad").

% Rules:
% Transitively compute requirements of packages.
needs(P, D) :- dep(P, D).
needs(P, D) :- needs(P, M), dep(M, D).

% Propagate taint: a package is tainted if it is yanked, or if it depends on a yanked package.
tainted(P) :- yanked(P).
tainted(P) :- needs(P, D), yanked(D).

% Define safe packages as registered packages that are not tainted.
safe(P) :- package(P), not tainted(P).

% Compute transitive dependency count (fanout) using count aggregation.
fanout(P, count(D)) :- needs(P, D).

% Queries:
% Query 1: which packages are safe to use?
?- safe(X).
% Query 2: what is the dependency fanout count for each package?
?- fanout(X, N).
`,
  },
  {
    name: "Same generation",
    source: `% Two people are in the same generation if they share an
% ancestor at the same depth.
%
% Base facts: define parent-child relationships.
parent("alice", "carol").
parent("bob", "carol").
parent("carol", "eve").
parent("dave", "eve").

% Rules:
% Base case: siblings (sharing the same parent P) are in the same generation.
sg(X, Y) :- parent(X, P), parent(Y, P).
% Recursive case: if X's parent is P, Y's parent is Q, and P and Q are in the same generation, then X and Y are in the same generation.
sg(X, Y) :- parent(X, P), parent(Y, Q), sg(P, Q).

% Query: which people are in the same generation as alice?
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
const loadEl = document.getElementById("load");
const fileEl = document.getElementById("file");
const themeEl = document.getElementById("theme");
const aboutEl = document.getElementById("about");
const aboutDialogEl = document.getElementById("about-dialog");
const aboutCloseEl = document.getElementById("about-close");
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

// Loading a Datalog script from a file.
loadEl.addEventListener("click", () => fileEl.click());
fileEl.addEventListener("change", async () => {
  const file = fileEl.files[0];
  if (!file) return;
  setSource(await file.text());
  // Reset so selecting the same file again still fires a change event.
  fileEl.value = "";
  execute();
});

// Light and dark theme toggle, persisted across visits.
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  themeEl.textContent = theme === "light" ? "☾" : "☀";
  themeEl.title = theme === "light" ? "Switch to the dark theme" : "Switch to the light theme";
}

applyTheme(localStorage.getItem("zodd-theme") ?? "dark");

themeEl.addEventListener("click", () => {
  const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
  localStorage.setItem("zodd-theme", next);
  applyTheme(next);
});

// About dialog.
aboutEl.addEventListener("click", () => aboutDialogEl.showModal());
aboutCloseEl.addEventListener("click", () => aboutDialogEl.close());
aboutDialogEl.addEventListener("click", (event) => {
  // A click on the backdrop targets the dialog element itself.
  if (event.target === aboutDialogEl) aboutDialogEl.close();
});

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

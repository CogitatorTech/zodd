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

// Calls a Wasm export taking (ptr, len) pairs, one per string argument.
function wasmCall(fnName, strings) {
  const buffers = strings.map((s) => encoder.encode(s));
  const ptrs = buffers.map((bytes) => {
    // Zero-length allocations return a dangling pointer; pass (0, 0) instead.
    if (bytes.length === 0) return 0;
    const ptr = wasm.alloc(bytes.length);
    if (ptr === 0) throw new Error("Wasm allocation failed");
    // Views must be created after each call into Wasm: memory growth
    // detaches previously created typed arrays.
    new Uint8Array(wasm.memory.buffer, ptr, bytes.length).set(bytes);
    return ptr;
  });
  const args = [];
  buffers.forEach((bytes, i) => args.push(ptrs[i], bytes.length));
  const status = wasm[fnName](...args);
  buffers.forEach((bytes, i) => {
    if (ptrs[i] !== 0) wasm.dealloc(ptrs[i], bytes.length);
  });
  const out = decoder.decode(
    new Uint8Array(wasm.memory.buffer, wasm.outputPtr(), wasm.outputLen()),
  );
  return { status, out };
}

function runProgram(source) {
  return wasmCall("run", [source]);
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

function escapeAttr(text) {
  return escapeHtml(text).replace(/"/g, "&quot;");
}

function highlight(source, errorLine = null) {
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

  if (errorLine !== null) {
    const lines = html.split("\n");
    if (errorLine >= 1 && errorLine <= lines.length) {
      lines[errorLine - 1] = `<span class="line-error">${lines[errorLine - 1]}</span>`;
    }
    return lines.join("\n");
  }

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

function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  return new Promise((resolve, reject) => {
    try {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.style.position = "fixed";
      textarea.style.top = "0";
      textarea.style.left = "0";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      const successful = document.execCommand("copy");
      document.body.removeChild(textarea);
      if (successful) {
        resolve();
      } else {
        reject(new Error("Copy command failed"));
      }
    } catch (err) {
      reject(err);
    }
  });
}

// --- UI wiring ------------------------------------------------------------------

let activeErrorLine = null;

let sourceEl, highlightEl, highlightCodeEl, outputEl, outputTableEl, viewTextEl, viewTableEl, statusEl, examplesEl, runEl, planEl, shareEl, loadEl, downloadEl, clearEl, clearOutputEl, telemetryInfoEl, fileEl, themeEl, aboutEl, aboutDialogEl, aboutCloseEl, dividerEl, editorPane;

function syncHighlight(errorLine = null) {
  activeErrorLine = errorLine;
  // A trailing newline keeps the backdrop the same height as the textarea.
  highlightCodeEl.innerHTML = highlight(sourceEl.value, activeErrorLine) + "\n";
  syncScroll();
  updateLineNumbers();
  updateDropdownSelection();
}

function syncScroll() {
  highlightEl.scrollTop = sourceEl.scrollTop;
  highlightEl.scrollLeft = sourceEl.scrollLeft;
  const gutter = document.querySelector(".editor-gutter");
  if (gutter) {
    gutter.scrollTop = sourceEl.scrollTop;
  }
}

function updateLineNumbers() {
  const el = document.getElementById("linenos");
  if (!el) return;
  const lineCount = sourceEl.value.split("\n").length || 1;
  let numbers = "";
  for (let i = 1; i <= lineCount; i++) {
    numbers += i + "\n";
  }
  el.textContent = numbers;
}

function updateDropdownSelection() {
  if (typeof document === "undefined" || !examplesEl) return;
  const currentSource = sourceEl.value;
  let matchedIndex = -1;
  for (let i = 0; i < EXAMPLES.length; i++) {
    if (EXAMPLES[i].source === currentSource) {
      matchedIndex = i;
      break;
    }
  }
  if (matchedIndex !== -1) {
    examplesEl.value = String(matchedIndex);
  } else {
    examplesEl.value = "custom";
  }
}

function setSource(text) {
  sourceEl.value = text;
  sourceEl.scrollTop = 0;
  sourceEl.scrollLeft = 0;
  syncHighlight(null);
  localStorage.setItem("zodd-source", text);
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
    outputEl.classList.add("error");
    outputTableEl.innerHTML = `<div class="output-table-no-results" style="color: var(--error);">${escapeHtml("internal error: " + err)}</div>`;
    setStatus("trap", "error");
    telemetryInfoEl.textContent = "Internal Trap";
    return;
  }
  const elapsed = (performance.now() - started).toFixed(1);
  outputEl.textContent = result.out || "(no output)";
  outputEl.classList.toggle("error", result.status !== 0);

  if (result.status === 0) {
    setStatus("SUCCESS", "ok");
    telemetryInfoEl.textContent = `Duration: ${elapsed} ms | Size: ${result.out.length} chars`;
    outputTableEl.innerHTML = parseOutputToTables(result.out);
    syncHighlight(null);
  } else {
    setStatus("error", "error");
    telemetryInfoEl.textContent = `Failed in ${elapsed} ms`;
    outputTableEl.innerHTML = `<div class="output-table-no-results" style="color: var(--error); white-space: pre-wrap; font-family: var(--mono);">${escapeHtml(result.out)}</div>`;

    // Attempt to parse the error line from the diagnostic message (formatted as "line:col: message")
    const match = result.out.match(/^(\d+):(\d+):/);
    if (match) {
      const errorLine = parseInt(match[1], 10);
      syncHighlight(errorLine);
    } else {
      syncHighlight(null);
    }
  }
}

// Shows engine-generated explanation text (a plan or a proof tree) in the
// text view, leaving the last run's table view intact.
function showExplanation(result, okStatus) {
  outputEl.textContent = result.out || "(no output)";
  outputEl.classList.toggle("error", result.status !== 0);
  setView("text");
  if (result.status === 0) {
    setStatus(okStatus, "ok");
  } else {
    setStatus("error", "error");
  }
}

function showPlan() {
  if (!wasm) return;
  try {
    showExplanation(wasmCall("explainPlan", [sourceEl.value]), "plan");
  } catch (err) {
    outputEl.textContent = `internal error: ${err}`;
    outputEl.classList.add("error");
    setStatus("trap", "error");
  }
}

function explainAtom(atom) {
  if (!wasm) return;
  try {
    showExplanation(wasmCall("explain", [sourceEl.value, atom]), "explained");
  } catch (err) {
    outputEl.textContent = `internal error: ${err}`;
    outputEl.classList.add("error");
    setStatus("trap", "error");
  }
}

function share() {
  const url = new URL(window.location.href);
  url.hash = "program=" + encodeProgram(sourceEl.value);
  history.replaceState(null, "", url);
  copyToClipboard(url.href)
    .then(() => setStatus("link copied", "ok"))
    .catch(() => setStatus("link in address bar", "ok"));
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  themeEl.textContent = theme === "light" ? "☾" : "☀";
  themeEl.title = theme === "light" ? "Switch to the dark theme" : "Switch to the light theme";
}

// --- Output View Toggling & Parsing -------------------------------------------

let currentView = "text"; // "text" or "table"

function setView(view) {
  currentView = view;
  if (view === "table") {
    viewTableEl.classList.add("active");
    viewTextEl.classList.remove("active");
    outputEl.classList.add("hidden");
    outputTableEl.classList.remove("hidden");
  } else {
    viewTextEl.classList.add("active");
    viewTableEl.classList.remove("active");
    outputTableEl.classList.add("hidden");
    outputEl.classList.remove("hidden");
  }
}

function parseOutputToTables(text) {
  if (!text || text.trim() === "" || text.trim() === "(no results)" || text.trim() === "(no output)") {
    return `<div class="output-table-no-results">No results returned.</div>`;
  }

  const lines = text.split("\n");
  const parts = [];
  let currentTable = null;
  let currentText = [];

  function flushText() {
    if (currentText.length > 0) {
      // Remove trailing empty line if it is just a spacing artifact
      if (currentText[currentText.length - 1] === "") {
        currentText.pop();
      }
      if (currentText.length > 0) {
        parts.push({
          type: "text",
          content: currentText.join("\n")
        });
      }
      currentText = [];
    }
  }

  function flushTable() {
    if (currentTable) {
      parts.push({
        type: "table",
        title: currentTable.title,
        rows: currentTable.rows,
        truncated: currentTable.truncated
      });
      currentTable = null;
    }
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    if (!trimmed) {
      if (currentTable) {
        continue;
      }
      if (currentText.length > 0) {
        currentText.push(line);
      }
      continue;
    }

    // Check if the line is a block header: e.g. "pred_name:" or "?- q:"
    if (trimmed.endsWith(":") && !trimmed.startsWith("(")) {
      flushText();
      flushTable();
      const title = trimmed.slice(0, -1).trim();
      currentTable = {
        title: title,
        rows: [],
        truncated: null
      };
      continue;
    }

    // Check if the line is a tuple: e.g. "(1, 2)" or "  (1, 2)"
    if (trimmed.startsWith("(") && trimmed.endsWith(")") && trimmed !== "(no results)" && trimmed !== "(no output)") {
      flushText();
      const content = trimmed.slice(1, -1).trim();
      const rowData = parseTuple(content);
      if (currentTable) {
        currentTable.rows.push(rowData);
      } else {
        currentTable = { title: "Results", rows: [rowData], truncated: null };
      }
      continue;
    }

    // Check if it is a truncation warning
    if (trimmed.startsWith("... (output truncated")) {
      if (currentTable) {
        currentTable.truncated = trimmed;
      } else {
        currentText.push(line);
      }
      continue;
    }

    // Otherwise, it is non-tabular text
    flushTable();
    currentText.push(line);
  }

  flushText();
  flushTable();

  if (parts.length === 0) {
    return `<div class="output-table-no-results">No results.</div>`;
  }

  let html = "";
  for (const part of parts) {
    if (part.type === "text") {
      html += `<pre class="output-table-text-block">${escapeHtml(part.content)}</pre>`;
    } else if (part.type === "table") {
      html += `<div class="output-table-group">`;
      html += `<div class="output-table-header-row">`;
      html += `<h4 class="output-table-title">${escapeHtml(part.title)}</h4>`;
      if (part.rows.length > 0) {
        html += `<button class="copy-table-btn" title="Copy table to clipboard">Copy</button>`;
      }
      html += `</div>`;
      if (part.rows.length === 0) {
        html += `<div class="output-table-no-results">No rows.</div>`;
      } else {
        html += `<table class="output-table-el">`;
        const arity = part.rows[0].length;
        html += `<thead><tr>`;
        html += `<th class="index-col">#</th>`;
        for (let c = 1; c <= arity; c++) {
          html += `<th>Col ${c}</th>`;
        }
        html += `</tr></thead>`;

        // Rows of a named predicate carry their atom text so a click can
        // ask the engine to explain the tuple's derivation.
        const pred = part.title.replace(/^\?-\s*/, "");
        const explainable = /^[a-z][A-Za-z0-9_]*$/.test(pred);
        html += `<tbody>`;
        for (let r = 0; r < part.rows.length; r++) {
          const row = part.rows[r];
          if (explainable) {
            const atom = `${pred}(${row.join(", ")})`;
            html += `<tr data-atom="${escapeAttr(atom)}" title="Click to explain how this tuple was derived">`;
          } else {
            html += `<tr>`;
          }
          html += `<td class="index-col">${r + 1}</td>`;
          for (const val of row) {
            html += `<td>${escapeHtml(cleanValue(val))}</td>`;
          }
          html += `</tr>`;
        }
        html += `</tbody>`;
        html += `</table>`;
      }
      if (part.truncated) {
        html += `<div class="output-table-no-results">${escapeHtml(part.truncated)}</div>`;
      }
      html += `</div>`;
    }
  }

  return html;
}

function parseTuple(str) {
  if (!str.trim()) return [];
  const elements = [];
  let current = "";
  let inQuotes = false;
  let escape = false;
  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    if (escape) {
      current += char;
      escape = false;
    } else if (char === '\\') {
      escape = true;
    } else if (char === '"') {
      inQuotes = !inQuotes;
      current += char;
    } else if (char === ',' && !inQuotes) {
      elements.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  elements.push(current.trim());
  return elements;
}

function cleanValue(val) {
  if (val.startsWith('"') && val.endsWith('"')) {
    return val.slice(1, -1).replace(/\\(.)/g, (match, g1) => {
      switch (g1) {
        case "n": return "\n";
        case "t": return "\t";
        case '"': return '"';
        case '\\': return '\\';
        default: return g1;
      }
    });
  }
  return val;
}

// Wrap all DOM execution & side-effects
if (typeof document !== "undefined") {
  sourceEl = document.getElementById("source");
  highlightEl = document.getElementById("highlight");
  highlightCodeEl = document.getElementById("highlight-code");
  outputEl = document.getElementById("output");
  outputTableEl = document.getElementById("output-table");
  viewTextEl = document.getElementById("view-text");
  viewTableEl = document.getElementById("view-table");
  statusEl = document.getElementById("status");
  examplesEl = document.getElementById("examples");
  runEl = document.getElementById("run");
  planEl = document.getElementById("plan");
  shareEl = document.getElementById("share");
  loadEl = document.getElementById("load");
  downloadEl = document.getElementById("download");
  clearEl = document.getElementById("clear");
  clearOutputEl = document.getElementById("clear-output");
  telemetryInfoEl = document.getElementById("telemetry-info");
  fileEl = document.getElementById("file");
  themeEl = document.getElementById("theme");
  aboutEl = document.getElementById("about");
  aboutDialogEl = document.getElementById("about-dialog");
  aboutCloseEl = document.getElementById("about-close");
  dividerEl = document.getElementById("divider");
  editorPane = document.querySelector(".editor-pane");

  // Examples dropdown.
  const customOption = document.createElement("option");
  customOption.value = "custom";
  customOption.textContent = "[Custom / Edited]";
  customOption.disabled = true;
  customOption.hidden = true;
  examplesEl.appendChild(customOption);

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
  sourceEl.addEventListener("input", () => {
    syncHighlight(null);
    localStorage.setItem("zodd-source", sourceEl.value);
  });
  sourceEl.addEventListener("scroll", syncScroll);
  sourceEl.addEventListener("keydown", (event) => {
    if (event.key === "Tab" && !event.ctrlKey && !event.metaKey && !event.altKey) {
      event.preventDefault();
      const start = sourceEl.selectionStart;
      const end = sourceEl.selectionEnd;
      const val = sourceEl.value;
      sourceEl.value = val.substring(0, start) + "    " + val.substring(end);
      sourceEl.selectionStart = sourceEl.selectionEnd = start + 4;
      syncHighlight(null);
      localStorage.setItem("zodd-source", sourceEl.value);
    } else if (event.key === "Enter" && !event.ctrlKey && !event.metaKey && !event.altKey) {
      const start = sourceEl.selectionStart;
      const end = sourceEl.selectionEnd;
      if (start === end) {
        const val = sourceEl.value;
        const lastNewline = val.lastIndexOf("\n", start - 1);
        const lineStart = lastNewline + 1;
        const currentLine = val.substring(lineStart, start);
        const indentMatch = currentLine.match(/^\s*/);
        const indent = indentMatch ? indentMatch[0] : "";
        if (indent.length > 0) {
          event.preventDefault();
          const insert = "\n" + indent;
          sourceEl.value = val.substring(0, start) + insert + val.substring(start);
          sourceEl.selectionStart = sourceEl.selectionEnd = start + insert.length;
          syncHighlight(null);
          localStorage.setItem("zodd-source", sourceEl.value);
        }
      }
    } else if ((event.key === "(" || event.key === "[" || event.key === "{" || event.key === '"') && !event.ctrlKey && !event.metaKey && !event.altKey) {
      event.preventDefault();
      const start = sourceEl.selectionStart;
      const end = sourceEl.selectionEnd;
      const val = sourceEl.value;
      const pairs = { "(": ")", "[": "]", "{": "}", '"': '"' };
      const closingChar = pairs[event.key];
      if (start !== end) {
        const selectedText = val.substring(start, end);
        const insert = event.key + selectedText + closingChar;
        sourceEl.value = val.substring(0, start) + insert + val.substring(end);
        sourceEl.selectionStart = start + 1;
        sourceEl.selectionEnd = end + 1;
      } else {
        if (event.key === '"' && val.charAt(start) === '"') {
          sourceEl.selectionStart = sourceEl.selectionEnd = start + 1;
        } else {
          const insert = event.key + closingChar;
          sourceEl.value = val.substring(0, start) + insert + val.substring(start);
          sourceEl.selectionStart = sourceEl.selectionEnd = start + 1;
        }
      }
      syncHighlight(null);
      localStorage.setItem("zodd-source", sourceEl.value);
    } else if ((event.key === ")" || event.key === "]" || event.key === "}" || event.key === '"') && !event.ctrlKey && !event.metaKey && !event.altKey) {
      const start = sourceEl.selectionStart;
      const val = sourceEl.value;
      if (start === sourceEl.selectionEnd && val.charAt(start) === event.key) {
        event.preventDefault();
        sourceEl.selectionStart = sourceEl.selectionEnd = start + 1;
      }
    } else if (event.key === "Backspace" && !event.ctrlKey && !event.metaKey && !event.altKey) {
      const start = sourceEl.selectionStart;
      const end = sourceEl.selectionEnd;
      if (start === end && start > 0) {
        const val = sourceEl.value;
        const charBefore = val.charAt(start - 1);
        const charAfter = val.charAt(start);
        if (
          (charBefore === "(" && charAfter === ")") ||
          (charBefore === "[" && charAfter === "]") ||
          (charBefore === "{" && charAfter === "}") ||
          (charBefore === '"' && charAfter === '"')
        ) {
          event.preventDefault();
          sourceEl.value = val.substring(0, start - 1) + val.substring(start + 1);
          sourceEl.selectionStart = sourceEl.selectionEnd = start - 1;
          syncHighlight(null);
          localStorage.setItem("zodd-source", sourceEl.value);
        }
      }
    } else if ((event.ctrlKey || event.metaKey) && event.key === "/") {
      event.preventDefault();
      const start = sourceEl.selectionStart;
      const end = sourceEl.selectionEnd;
      const val = sourceEl.value;
      const lastNewline = val.lastIndexOf("\n", start - 1);
      const lineStart = lastNewline + 1;
      const nextNewline = val.indexOf("\n", end);
      const lineEnd = nextNewline === -1 ? val.length : nextNewline;
      const lineText = val.substring(lineStart, lineEnd);
      let newLineText;
      let offset;
      if (lineText.trim().startsWith("%")) {
        newLineText = lineText.replace(/^\s*% ?/, (match) => {
          const indent = match.match(/^\s*/)[0];
          return indent;
        });
        offset = newLineText.length - lineText.length;
      } else {
        const indentMatch = lineText.match(/^\s*/);
        const indent = indentMatch ? indentMatch[0] : "";
        const content = lineText.substring(indent.length);
        newLineText = indent + "% " + content;
        offset = 2;
      }
      sourceEl.value = val.substring(0, lineStart) + newLineText + val.substring(lineEnd);
      sourceEl.selectionStart = start + offset;
      sourceEl.selectionEnd = end + offset;
      syncHighlight(null);
      localStorage.setItem("zodd-source", sourceEl.value);
    } else if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      execute();
    }
  });

  runEl.addEventListener("click", execute);
  planEl.addEventListener("click", showPlan);
  shareEl.addEventListener("click", share);

  downloadEl.addEventListener("click", () => {
    const blob = new Blob([sourceEl.value], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "program.dl";
    a.click();
    URL.revokeObjectURL(url);
    setStatus("downloaded", "ok");
  });

  clearEl.addEventListener("click", () => {
    setSource("");
    setStatus("CLEARED", "cleared");
  });

  clearOutputEl.addEventListener("click", () => {
    outputEl.textContent = "";
    outputEl.classList.remove("error");
    outputTableEl.innerHTML = "";
    setStatus("CLEARED", "cleared");
    telemetryInfoEl.textContent = "";
  });

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
      dividerEl.releasePointerCapture(event.pointerId);
      dividerEl.removeEventListener("pointermove", onMove);
      dividerEl.removeEventListener("pointerup", onUp);
      dividerEl.removeEventListener("pointercancel", onUp);
    };
    dividerEl.addEventListener("pointermove", onMove);
    dividerEl.addEventListener("pointerup", onUp);
    dividerEl.addEventListener("pointercancel", onUp);
  });

  // Initial program: a permalink if present, the autosaved progress if available, or the first example otherwise.
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
    const saved = localStorage.getItem("zodd-source");
    if (saved !== null) {
      setSource(saved);
      return;
    }
    setSource(EXAMPLES[0].source);
  })();

  viewTextEl.addEventListener("click", () => setView("text"));
  viewTableEl.addEventListener("click", () => setView("table"));

  // Delegated listener: clicking a result row explains its derivation.
  outputTableEl.addEventListener("click", (event) => {
    if (event.target.closest(".copy-table-btn")) return;
    const row = event.target.closest("tr[data-atom]");
    if (!row) return;
    explainAtom(row.dataset.atom);
  });

  // Delegated listener to copy a table's data in TSV format (ignoring the index column)
  outputTableEl.addEventListener("click", (event) => {
    const btn = event.target.closest(".copy-table-btn");
    if (!btn) return;

    const group = btn.closest(".output-table-group");
    if (!group) return;

    const table = group.querySelector(".output-table-el");
    if (!table) return;

    const rows = table.querySelectorAll("tbody tr");
    const headers = table.querySelectorAll("thead th");

    let tsvLines = [];

    // Headers (skipping index column)
    let headerCols = [];
    for (let i = 0; i < headers.length; i++) {
      if (headers[i].classList.contains("index-col")) continue;
      headerCols.push(headers[i].textContent);
    }
    tsvLines.push(headerCols.join("\t"));

    // Rows (skipping index column)
    for (let r = 0; r < rows.length; r++) {
      const cells = rows[r].querySelectorAll("td");
      let rowCols = [];
      for (let c = 0; c < cells.length; c++) {
        if (cells[c].classList.contains("index-col")) continue;
        rowCols.push(cells[c].textContent);
      }
      tsvLines.push(rowCols.join("\t"));
    }

    const tsvText = tsvLines.join("\n");
    copyToClipboard(tsvText).then(() => {
      const originalText = btn.textContent;
      btn.textContent = "Copied!";
      btn.style.borderColor = "var(--ok)";
      btn.style.color = "var(--ok)";
      setTimeout(() => {
        btn.textContent = originalText;
        btn.style.borderColor = "";
        btn.style.color = "";
      }, 1500);
    }).catch((err) => {
      console.error("Clipboard copy failed: ", err);
    });
  });

  // Load the Wasm module, then run the initial program.
  loadWasm()
    .then((exports) => {
      wasm = exports;

      // Resolve and set version, build, and license metadata from Wasm
      try {
        const versionStr = decoder.decode(
          new Uint8Array(wasm.memory.buffer, wasm.versionPtr(), wasm.versionLen()),
        );
        const commitStr = decoder.decode(
          new Uint8Array(wasm.memory.buffer, wasm.commitPtr(), wasm.commitLen()),
        );
        const zigStr = decoder.decode(
          new Uint8Array(wasm.memory.buffer, wasm.zigVersionPtr(), wasm.zigVersionLen()),
        );
        const licenseStr = decoder.decode(
          new Uint8Array(wasm.memory.buffer, wasm.licensePtr(), wasm.licenseLen()),
        );

        document.getElementById("about-version").textContent = `${versionStr} (Zig ${zigStr})`;
        document.getElementById("about-build").textContent = `Wasm32 (${commitStr})`;
        document.getElementById("about-license").textContent = licenseStr;
      } catch (e) {
        // Fallback if functions are missing
      }

      setStatus("ready", "ok");
      execute();
    })
    .catch((err) => {
      outputEl.textContent = `Failed to load zodd.wasm: ${err}\n\nBuild it with: make web`;
      outputEl.classList.add("error");
      setStatus("load failed", "error");
    });
}

// --- Node exports for testing -------------------------------------------------
if (typeof exports !== "undefined") {
  exports.parseTuple = parseTuple;
  exports.cleanValue = cleanValue;
  exports.parseOutputToTables = parseOutputToTables;
  exports.highlight = highlight;
  exports.encodeProgram = encodeProgram;
  exports.decodeProgram = decodeProgram;
}

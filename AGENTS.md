# AGENTS.md

This file provides guidance to coding agents collaborating on this repository.

## Mission

Zodd is a small, embeddable [Datalog](https://en.wikipedia.org/wiki/Datalog) engine written in pure Zig.
It evaluates recursive rules over sets of tuples using semi-naive iteration, merge joins, and indexed extension primitives.
Zodd is designed to be embedded in Zig projects as a library.
Priorities, in order:

1. Correctness of relations, variables, joins, extensions, and fixed-point iteration.
2. Minimal public API for use as a library from other Zig projects.
3. Small dependency footprint and maintainable, well-tested code.
4. Cross-platform support (Linux, macOS, and Windows).

## Core Rules

- Use English for code, comments, docs, and tests.
- Prefer small, focused changes over large refactoring.
- Add comments only when they clarify non-obvious behavior.
- Do not add features, error handling, or abstractions beyond what is needed for the current task.
- Keep the dependency set small: do not add new Zig packages or C libraries without prior discussion.

## Writing Style

- Use Oxford commas in inline lists: "a, b, and c" not "a, b, c".
- Do not use em dashes. Restructure the sentence, or use a colon or semicolon instead.
- Avoid colorful adjectives and adverbs. Write "Datalog engine" not "blazing-fast Datalog engine", "merge join" not "efficient merge join".
- Use noun phrases for checklist items, not imperative verbs. Write "redundant index detection" not "detect redundant indexes".
- Headings in Markdown files must be in the title case: "Build from Source" not "Build from source". Minor words (a, an, the, and, but, or, for, in,
  on, at, to, by, of, is, are, was, were, be) stay lowercase unless they are the first word.

## Repository Layout

- `src/lib.zig`: Public API entry point. Re-exports `Relation`, `Variable`, `Iteration`, `Database`, join helpers, and extend primitives.
- `src/zodd/relation.zig`: Immutable `Relation` type (sorted, deduplicated tuples).
- `src/zodd/variable.zig`: Mutable `Variable` type for fixed-point iteration, plus the `gallop` search helper.
- `src/zodd/iteration.zig`: `Iteration` driver for semi-naive evaluation.
- `src/zodd/join.zig`: Merge-join algorithms (`joinHelper`, `joinInto`, `joinAnti`).
- `src/zodd/extend.zig`: Leaper-based extension primitives (`ExtendWith`, `FilterAnti`, `ExtendAnti`, `extendInto`).
- `src/zodd/secondary_index.zig`: Indexes for keyed lookups.
- `src/zodd/aggregate.zig`: Group-by and aggregation operations.
- `src/zodd/frontend/`: Datalog frontend. `program.zig` is the public `Database` API; `token.zig` and `parser.zig` parse textual Datalog;
  `ast.zig` and `builder.zig` hold the shared IR and the programmatic builder; `analyze.zig` checks safety and stratification;
  `dyntuple.zig`, `plan.zig`, `join_runtime.zig`, and `evaluator.zig` compile and run rules on the engine core; `explain.zig` renders rule
  plans and provenance proof trees; `magic.zig` builds the demand-transformed (magic sets) program behind `Database.queryDemand`.
- `src/cli/main.zig`: The `zodd` CLI executable (`run`, `query`, `plan`, `explain`, and `repl` subcommands), built via `zig build cli`.
- `tests/`: Non-unit tests (`integration_tests.zig`, `regression_tests.zig`, `property_tests.zig`, `incremental_tests.zig`, `frontend_tests.zig`).
- `web/`: Web frontend. `zodd_wasm.zig` is the Wasm wrapper built by `zig build wasm`; `index.html`, `main.js`, and `style.css` are the UI;
  `smoke_test.mjs` is the Node.js smoke test run by `make web-test`.
- `examples/`: Self-contained example programs (`e1_network_reachability.zig` through `e8_comparison_filters.zig`) built as executables via
  `build.zig`.
- `.github/workflows/`: CI workflows (`tests.yml` for unit and integration tests plus the Wasm smoke test, `docs.yml` for deploying the website:
  the web frontend at the site root and API docs under `/api`, and `release.yml` for publishing the web frontend Docker image to GHCR).
- `Dockerfile` / `.dockerignore`: Docker image for the web frontend (nginx serving the playground at the root and API docs under `/api`),
  built and published by `release.yml`.
- `build.zig` / `build.zig.zon`: Zig build configuration and package metadata.
- `Makefile`: GNU Make wrapper around `zig build` targets.
- `docs/`: Generated API docs land in `docs/api/` (produced by `make docs`).

## Architecture

### Evaluation Pipeline

A Datalog program flows through: base data is loaded into a `Relation` (`relation.zig`). Derived predicates use a `Variable` (`variable.zig`) driven
by an `Iteration` (`iteration.zig`) loop that calls `changed()` until a fixed point. Each iteration extends tuples via `join` (`join.zig`) or `extend`
(`extend.zig`), optionally using indexes (`index.zig`) or aggregates (`aggregate.zig`). Every primitive takes a `std.mem.Allocator` directly; there is
no wrapper context type.

### Relations and Variables Split

- `relation.zig` is the immutable, sorted, deduplicated tuple container used for base facts and finalized results.
- `variable.zig` is the mutable counterpart used inside fixed-point loops; it tracks stable, recent, and to-add tuple sets for semi-naive evaluation.
- New join shapes go in `join.zig`. New leaper-style extensions go in `extend.zig`.

### Indexing and Aggregation

`index.zig` provides keyed lookups used by the extend primitives. `aggregate.zig` provides group-by reductions.
When adding a new join or extension shape, consider whether it needs an index variant and add it alongside the existing ones.

### Public API Surface

Everything re-exported from `src/lib.zig` is part of the public API.
Changes to names or signatures there are breaking.
The rest of `src/zodd/` is internal and may be refactored freely as long as the public surface and its behavior are preserved.

### Dependencies

Zodd depends on three sibling Zig packages declared in `build.zig.zon`:

- `ordered`: sorted container primitives, linked into the `zodd` module for all builds.
- `minish`: property-testing framework, used only by `tests/property_tests.zig` and lazy-loaded in `build.zig`.
- `chilli`: CLI framework, used only by the `zodd` CLI executable (`src/cli/main.zig`).

Please do not add further dependencies without prior discussion.

## Zig Conventions

- Zig version: 0.16.0 (as declared in `build.zig.zon` and the Makefile's `ZIG_LOCAL` path). CI pins the version declared in `build.zig.zon`.
- Formatting is enforced by `zig fmt`. Run `make format` before committing.
- Naming follows Zig standard-library conventions: `camelCase` for functions (e.g. `joinInto`, `extendInto`, `fromSlice`), `snake_case` for local
  variables and struct fields, `PascalCase` for types and structs (e.g. `Relation`, `Variable`, `Database`), and `SCREAMING_SNAKE_CASE` for
  top-level compile-time constants.

## Required Validation

Run the relevant targets for any change:

| Target         | Command                                        | What It Runs                                                          |
|----------------|------------------------------------------------|-----------------------------------------------------------------------|
| Unit tests     | `make test`                                    | Inline `test` blocks in `src/` plus every file under `tests/`         |
| Lint           | `make lint`                                    | Checks Zig formatting with `zig fmt --check` over `src/` and `tests/` |
| Examples       | `make example`                                 | Builds and runs every example under `examples/`                       |
| Single example | `make example EXAMPLE=e1_network_reachability` | Runs one example program                                              |
| Docs           | `make docs`                                    | Generates API docs into `docs/api`                                    |
| Everything     | `make all`                                     | Runs `build`, `test`, `lint`, and `docs`                              |

## First Contribution Flow

1. Read the relevant module under `src/zodd/` (often `relation.zig`, `variable.zig`, `join.zig`, or `extend.zig`).
2. Implement the smallest change that covers the requirement.
3. Add or update inline `test` blocks in the changed Zig module, or extend a test file under `tests/`, to cover the new behavior.
4. Run `make test` and `make lint`.
5. If public behavior changed, also run `make example` to ensure no example regresses.

Good first tasks:

- Add a new join or extension shape in `src/zodd/join.zig` or `src/zodd/extend.zig` (with an inline `test` block and, if appropriate, an integration
  test under `tests/`).
- Improve an existing index strategy in `src/zodd/index.zig`.
- Add a new aggregate operation in `src/zodd/aggregate.zig`.
- Add a new example under `examples/` demonstrating a Datalog pattern, and list it in `examples/README.md`.

## Testing Expectations

- Unit tests live as inline `test` blocks in the module they cover (`src/lib.zig` and `src/zodd/*.zig`). They are discovered automatically via
  `std.testing.refAllDecls(@This())` in `src/lib.zig`.
- Non-unit tests live under `tests/` (`integration_tests.zig`, `regression_tests.zig`, `property_tests.zig`, `incremental_tests.zig`) and are
  auto-discovered by `build.zig`.
- Property tests use the `minish` dependency and should use fixed seeds so failures are reproducible in CI.
- Every new relation, variable operation, join, extension, index, or aggregate must ship with at least one `test` block that exercises it.
- No public API change is complete without a test covering the new or changed behavior.

## Change Design Checklist

Before coding:

1. Identify which module(s) the change touches (`relation`, `variable`, `iteration`, `join`, `extend`, `index`, `aggregate`, or `database`).
2. Consider whether a new join or extension needs a matching index or anti-variant.
3. Check whether the change is public-API-visible (like re-exported from `src/lib.zig`); if so, treat it as a breaking or additive API change
   deliberately.
4. Check cross-platform implications, especially for anything that touches the filesystem, timing, or OS-specific types.

Before submitting:

1. `make test` passes.
2. `make lint` passes.
3. `make example` still succeeds when touching relations, variables, joins, extensions, or iteration.
4. Docs updated (`make docs`) if the public API surface changed, and `ROADMAP.md` ticked/updated if a listed item was implemented.

## Commit and PR Hygiene

- Keep commits scoped to one logical change.
- PR descriptions should include:
    1. Behavioral change summary.
    2. Tests added or updated.
    3. Whether examples were run locally (yes/no), and on which OS.

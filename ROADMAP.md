## Project Roadmap

This document lists the completed and planned features for Zodd.

> [!IMPORTANT]
> This roadmap is a work in progress and is subject to change.

### Core Features

- [x] `Relation` - sorted, deduplicated tuple collections
- [x] `Variable` - incremental update lifecycle
- [x] `Iteration` - fixed-point computation context
- [x] `gallop` - exponential and binary search for filtering
- [x] `joinHelper` - merge-join on key-value tuples
- [x] `joinInto` - high-level join between variables
- [x] `Leaper` - Treefrog Leapjoin interface
- [x] `ExtendWith` - propose values from a relation
- [x] `FilterAnti` - negation (filter out matching tuples)
- [x] `ExtendAnti` - anti-join (filter to keep non-matching values)

### Other Features

- [x] Negation primitives (anti-join and anti-extend)
- [x] Aggregations
- [x] Recursion limits
- [x] Persistence
- [x] Secondary indices
- [x] Incremental maintenance (`Database.update` after `addFact`/`retract`: additions propagate as semi-naive deltas, deletions and
  negation/aggregate effects recompute only the affected strata; tuple-level deletion (DRed) is a possible future refinement)
- [x] Parallel execution (opt-in worker threads per fixed-point round via `Database.parallelism`; identical results to sequential evaluation)
- [x] CLI (`zodd run`, `query` with demand-driven evaluation, `plan`, `explain`, and a `repl`, built on Chilli)
- [ ] Streaming input
- [x] Rule DSL (textual Datalog frontend with a parser, a builder API, stratified negation, and aggregates)
- [x] Comparison operators (`<`, `<=`, `>`, `>=`, `=`, and `!=` as body filters)
- [x] Arithmetic in comparison filters (`+`, `-`, `*`, `/`, and parentheses on either side of a comparison)
- [x] Arithmetic assignments (`D2 is D + 1` binds a fresh variable per tuple; recursive use requires an iteration limit)
- [x] Query planner
- [x] Explain (rule plan rendering and tuple provenance proof trees)
- [x] Magic sets (demand-driven queries via `Database.queryDemand`, with a full-evaluation fallback for negation and aggregates)
- [x] Fact retraction (`Database.retract` removes a base fact and recomputes on the next solve or query)
- [x] Predicate arity up to 16 columns

### Development and Testing

- [x] Unit tests in each module
- [x] Integration, regression, property-based tests, etc. in `tests` directory
- [ ] Benchmarks

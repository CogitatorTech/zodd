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
- [x] Incremental maintenance
- [ ] Parallel execution
- [ ] CLI
- [ ] Streaming input
- [x] Rule DSL (textual Datalog frontend with a parser, a builder API, stratified negation, and aggregates)
- [x] Comparison operators (`<`, `<=`, `>`, `>=`, `=`, and `!=` as body filters)
- [x] Query planner
- [x] Explain (rule plan rendering and tuple provenance proof trees)
- [ ] Magic sets

### Development and Testing

- [x] Unit tests in each module
- [x] Integration, regression, property-based tests, etc. in `tests` directory
- [ ] Benchmarks

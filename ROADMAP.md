## Project Roadmap

This document outlines the features implemented in Zodd and the future goals for the project.

> [!IMPORTANT]
> This roadmap is a work in progress and is subject to change.

### Core Features

- [x] `Relation` - sorted, deduplicated tuple collections
- [x] `Variable` - incremental update lifecycle (stable/recent/to_add)
- [x] `Iteration` - fixed-point computation context
- [x] `gallop` - exponential and binary search for filtering
- [x] `joinHelper` - merge-join on key-value tuples
- [x] `joinInto` - high-level join between variables
- [x] Treefrog Leapjoin interface (`Leaper`)
- [x] `ExtendWith` - propose values from a relation
- [x] `FilterAnti` - negation (filter out matching tuples)
- [x] `ExtendAnti` - set difference (propose non-matching values)

### Testing

- [x] Unit tests in each module
- [x] Integration, regression, and property-based tests in `tests` directory

### Extensions

- [ ] Stratified negation
- [x] Aggregation
- [x] Recursion limits
- [ ] Rule DSL
- [x] Persistence
- [ ] Secondary indices
- [ ] Incremental maintenance
- [ ] Query planner
- [ ] Parallel execution
- [ ] CLI interface
- [ ] Magic sets
- [ ] Benchmarks
- [ ] WASM support
- [ ] Streaming input

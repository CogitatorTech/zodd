## Project Roadmap

This document outlines the features implemented in Zodd and the future goals for the project.

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

### Extra Features

- [x] Negation primitives (anti-join and anti-extend)
- [x] Aggregations
- [x] Recursion limits
- [x] Persistence
- [x] Secondary indices
- [x] Incremental maintenance
- [x] Parallel execution
- [ ] CLI interface
- [ ] Streaming input
- [ ] Rule DSL
- [ ] Query planner
- [ ] Magic sets

### Development and Testing

- [x] Unit tests in each module
- [x] Integration, regression, property-based tests, etc. in `tests` directory
- [ ] Benchmarks

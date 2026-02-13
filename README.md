<div align="center">
  <picture>
    <img alt="Zodd Logo" src="logo.svg" height="15%" width="15%">
  </picture>
<br>

<h2>Zodd</h2>

[![Tests](https://img.shields.io/github/actions/workflow/status/CogitatorTech/zodd/tests.yml?label=tests&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/zodd/actions/workflows/tests.yml)
[![License](https://img.shields.io/badge/license-MIT-007ec6?label=license&style=flat&labelColor=282c34&logo=open-source-initiative)](https://github.com/CogitatorTech/zodd/blob/main/LICENSE)
[![Examples](https://img.shields.io/badge/examples-view-green?style=flat&labelColor=282c34&logo=zig)](https://github.com/CogitatorTech/zodd/tree/main/examples)
[![Docs](https://img.shields.io/badge/docs-read-blue?style=flat&labelColor=282c34&logo=read-the-docs)](https://CogitatorTech.github.io/zodd/)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange?logo=zig&labelColor=282c34)](https://ziglang.org/download/)
[![Release](https://img.shields.io/github/release/CogitatorTech/zodd.svg?label=release&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/zodd/releases/latest)

A small embeddable Datalog engine in Zig

</div>

---

Zodd is a small [Datalog](https://en.wikipedia.org/wiki/Datalog) engine written in pure Zig.

### What is Datalog?

Datalog is a declarative logic programming language for deductive databases.
In contrast to SQL, which needs explicit joins and subqueries, Datalog lets you express recursive relationships naturally.
Instead of defining a schema and queries in a relational database,
you define a set of facts (base data) and rules (logical implications), and a Datalog engine automatically computes all derivable conclusions
iteratively.

Below is a Datalog program that defines a directed graph and computes its transitive closure.
The [Simple Example](#simple-example) section shows how to implement this using Zodd in Zig.

```prolog
% Facts: a graph (with four nodes and three edges)
edge(1, 2).
edge(2, 3).
edge(3, 4).

% Rule: transitive closure of the graph
% The transitive closure is the set of all node pairs (X, Y) where node Y is
% reachable from node X through one or more directed edges.
reachable(X, Y) :- edge(X, Y).
reachable(X, Z) :- reachable(X, Y), edge(Y, Z).

% Query: find all pairs of nodes that are reachable from each other
?- reachable(X, Y).

%% Output:
% X = 1, Y = 2
% X = 1, Y = 3
% X = 1, Y = 4
% X = 2, Y = 3
% X = 2, Y = 4
% X = 3, Y = 4
```

Datalog is used in many application domains, especially when recursive querying over structured data is needed.
For example:

- Security and access control
    - Role-based authorization with hierarchical permission inheritance and explicit denials
    - Network reachability analysis through routing policies and firewall rules
    - Taint analysis to trace untrusted data through program flows and detect vulnerabilities
- Data governance and compliance
    - Data lineage tracking through ETL pipelines for GDPR and CCPA compliance
    - PII propagation analysis with anonymization checkpoints
- Healthcare and life sciences
    - Medical ontology reasoning with type hierarchies and property inheritance
    - Drug-disease relationship inference and side effect prediction
- Software engineering
    - Dependency resolution with transitive closure and cycle detection
    - Points-to analysis and other static analyses over program representations

### Why Zodd?

- Written in pure Zig with a simple API
- Implements semi-naive evaluation for efficient recursive query processing
- Uses immutable, sorted, and deduplicated relations as core data structures
- Supports parallel execution for joins and variable updates
- Provides primitives for multi-way joins, anti-joins, secondary indexes, and aggregation

See [ROADMAP.md](ROADMAP.md) for the list of implemented and planned features.

> [!IMPORTANT]
> Zodd is in early development, so bugs and breaking changes are expected.
> Please use the [issues page](https://github.com/CogitatorTech/zodd/issues) to report bugs or request features.

---

### Getting Started

You can add Zodd to your project and start using it by following the steps below.

#### Installation

Run the following command in the root directory of your project to download Zodd:

```sh
zig fetch --save=zodd "https://github.com/CogitatorTech/zodd/archive/<branch_or_tag>.tar.gz"
```

Replace `<branch_or_tag>` with the desired branch or release tag, like `main` (for the developmental version) or `v0.1.0`.
This command will download Zodd and add it to Zig's global cache and update your project's `build.zig.zon` file.

> [!NOTE]
> Zodd is developed and tested with Zig version 0.15.2.

#### Adding to Build Script

Next, modify your `build.zig` file to make Zodd available to your build target as a module.

```zig
pub fn build(b: *std.Build) void {
    // ... The existing setup ...

    const zodd_dep = b.dependency("zodd", .{});
    const zodd_module = zodd_dep.module("zodd");
    exe.root_module.addImport("zodd", zodd_module);
}
```

#### Simple Example

Finally, you can `@import("zodd")` and start using it in your Zig project.

```zig
const std = @import("std");
const zodd = @import("zodd");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = try zodd.ExecutionContext.initWithThreads(allocator, 4);
    defer ctx.deinit();

    const Edge = struct { u32, u32 };

    // Create base relation: edges in a graph
    var edges = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
    });
    defer edges.deinit();

    // Create variable for reachability (transitive closure)
    var reachable = zodd.Variable(Edge).init(&ctx);
    defer reachable.deinit();

    // Initialize with base edges
    try reachable.insertSlice(&ctx, edges.elements);

    // Fixed-point iteration: reachable(X,Z) :- reachable(X,Y), edge(Y,Z)
    while (try reachable.changed()) {
        var new_tuples = std.ArrayList(Edge).init(allocator);
        defer new_tuples.deinit();

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) {
                    try new_tuples.append(.{ r[0], e[1] });
                }
            }
        }

        if (new_tuples.items.len > 0) {
            const rel = try zodd.Relation(Edge).fromSlice(&ctx, new_tuples.items);
            try reachable.insert(rel);
        }
    }

    // Get final result
    var result = try reachable.complete();
    defer result.deinit();

    std.debug.print("Reachable pairs: {d}\n", .{result.len()});
}
```

---

### Documentation

You can find the API documentation for the latest release of Zodd [here](https://CogitatorTech.github.io/zodd/).

Alternatively, you can use the `make docs` command to generate the documentation for the current version of Zodd.
This will generate HTML documentation in the `docs/api` directory, which you can serve locally with `make docs-serve` and view in a web browser.

### Examples

Check out the [examples](examples) directory for example usages of Zodd.

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

Zodd is licensed under the MIT License (see [LICENSE](LICENSE)).

### Acknowledgements

* The logo is from [SVG Repo](https://www.svgrepo.com/svg/469003/gravity) with some modifications.
* This project uses the [Minish](https://github.com/CogitatorTech/minish) framework for property-based testing and
  the [Ordered](https://github.com/CogitatorTech/ordered) library for B-tree indices.
* Zodd is inspired and modeled after the [Datafrog](https://github.com/frankmcsherry/blog/blob/master/posts/2018-05-19.md) Datalog engine for Rust.

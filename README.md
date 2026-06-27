<div align="center">
  <picture>
    <img alt="Zodd Logo" src="logo.svg" height="25%" width="25%">
  </picture>
<br>

<h2>Zodd</h2>

[![Tests](https://img.shields.io/github/actions/workflow/status/CogitatorTech/zodd/tests.yml?label=tests&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/zodd/actions/workflows/tests.yml)
[![License](https://img.shields.io/badge/license-MIT-007ec6?label=license&style=flat&labelColor=282c34&logo=open-source-initiative)](https://github.com/CogitatorTech/zodd/blob/main/LICENSE)
[![Examples](https://img.shields.io/badge/examples-view-green?style=flat&labelColor=282c34&logo=zig)](https://github.com/CogitatorTech/zodd/tree/main/examples)
[![API Docs](https://img.shields.io/badge/docs-read-blue?style=flat&labelColor=282c34&logo=read-the-docs)](https://cogitatortech.github.io/zodd/api/#zodd.lib)
[![Web Frontend](https://img.shields.io/badge/web_frontend-try-orange?style=flat&labelColor=282c34&logo=webassembly)](https://CogitatorTech.github.io/zodd/)
[![Zig](https://img.shields.io/badge/zig-0.16.0-F7A41D?style=flat&labelColor=282c34&logo=zig)](https://ziglang.org/download/)
[![Release](https://img.shields.io/github/release/CogitatorTech/zodd.svg?label=release&style=flat&labelColor=282c34&logo=github)](https://github.com/CogitatorTech/zodd/releases/latest)

A small embeddable Datalog engine in Zig

</div>

---

**You can try Zodd in your web browser [here](https://CogitatorTech.github.io/zodd/).**

You can download and use the latest pre-built Docker image for the web version of Zodd from the
[GHCR](https://github.com/CogitatorTech/zodd/pkgs/container/zodd-web) and run it with the following command:

```bash
docker run -d -p 8085:80 --rm ghcr.io/cogitatortech/zodd-web:latest
```

Then open http://localhost:8085 in your browser.

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

Datalog is useful for recursive queries over structured data, such as:

- **Access control**: managing role hierarchies and permissions.
- **Data lineage**: tracking how data moves through a system.
- **Software analysis**: resolving package dependencies or analyzing source code structure.
- **Graph queries**: finding paths and connections between data points.

### Features

- **Pure Zig**: a simple API built specifically for Zig projects.
- **Semi-naive evaluation**: handles recursion efficiently.
- **Core operations**: supports multi-way joins, anti-joins, secondary indexes, and aggregation.
- **Built-in frontend**: includes a Datalog parser, program builder, negation, and comparison filters.

See [ROADMAP.md](ROADMAP.md) for the list of implemented and planned features.

> [!IMPORTANT]
> Zodd is in early development, so bugs and breaking changes are expected.
> Please use the [issues page](https://github.com/CogitatorTech/zodd/issues) to report bugs or request features.

---

### Getting Started

Follow the steps below to add Zodd to your project.

#### Installation

Run this command in your project root to download Zodd:

```sh
zig fetch --save=zodd "https://github.com/CogitatorTech/zodd/archive/<branch_or_tag>.tar.gz"
```

Replace `<branch_or_tag>` with the branch or tag you want to use, such as `main` or `v0.1.0`.

> [!NOTE]
> Zodd is developed and tested with Zig version 0.16.0.

#### Build Configuration

Add Zodd as a module dependency in your `build.zig` file:

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
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const Edge = struct { u32, u32 };

    // Base relation: edges in a graph
    var edges = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
    });
    defer edges.deinit();

    // Variable holding the reachability closure
    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();
    try reachable.insertSlice(edges.elements);

    // Fixed-point iteration: reachable(X, Z) :- reachable(X, Y), edge(Y, Z)
    while (try reachable.changed()) {
        var batch: std.ArrayList(Edge) = .empty;
        defer batch.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) try batch.append(allocator, .{ r[0], e[1] });
            }
        }

        if (batch.items.len > 0) {
            const rel = try zodd.Relation(Edge).fromSlice(allocator, batch.items);
            try reachable.insert(rel);
        }
    }

    var result = try reachable.complete();
    defer result.deinit();

    std.debug.print("Reachable pairs: {d}\n", .{result.len()});
}
```

---

### Documentation

You can find the API documentation for the latest release of Zodd [here](https://cogitatortech.github.io/zodd/api/#zodd.lib).

#### Examples

Check out the [examples](examples) directory for example usages of Zodd.

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

Zodd is licensed under the MIT License (see [LICENSE](LICENSE)).

### Acknowledgements

* The logo shows a directed graph whose edges form a Z, with a dashed arc for the derived fact `path(a, d)`.
* This project uses [Minish](https://github.com/CogitatorTech/minish) for property-based testing and
  [Ordered](https://github.com/CogitatorTech/ordered) for B-tree indices.
* Zodd is inspired by and modeled after the [Datafrog](https://github.com/frankmcsherry/blog/blob/master/posts/2018-05-19.md) Datalog engine for Rust.


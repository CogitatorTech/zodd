const std = @import("std");
const zodd = @import("zodd");

// Dependency Resolution for a Package Manager
//
// Resolves transitive package dependencies, detects circular dependencies,
// computes total install sizes, and supports reverse-dependency lookups.
//
// Datalog rules:
//   dep(A, B)    :- direct_dep(A, B).
//   dep(A, C)    :- dep(A, B), direct_dep(B, C).
//   circular(A)  :- dep(A, A).
//
// Uses:
//   - Variable + Relation for transitive closure
//   - aggregate for computing total install size per package
//   - SecondaryIndex for efficient reverse-dependency lookups

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - Dependency Resolution Example\n", .{});
    std.debug.print("===================================================\n\n", .{});

    // Package IDs:
    //   app=1, web-framework=2, http=3, json=4, logging=5,
    //   crypto=6, tls=7, base64=8, utils=9
    //
    // Dependency graph:
    //   app -> web-framework, logging
    //   web-framework -> http, json
    //   http -> tls
    //   tls -> crypto, base64
    //   crypto -> utils
    //   json -> utils
    //   logging -> utils

    const Pair = struct { u32, u32 };

    const direct_dep_data = [_]Pair{
        .{ 1, 2 }, // app -> web-framework
        .{ 1, 5 }, // app -> logging
        .{ 2, 3 }, // web-framework -> http
        .{ 2, 4 }, // web-framework -> json
        .{ 3, 7 }, // http -> tls
        .{ 7, 6 }, // tls -> crypto
        .{ 7, 8 }, // tls -> base64
        .{ 6, 9 }, // crypto -> utils
        .{ 4, 9 }, // json -> utils
        .{ 5, 9 }, // logging -> utils
    };

    // Package sizes in KB
    const SizeTuple = struct { u32, u32 };
    const size_data = [_]SizeTuple{
        .{ 1, 50 }, // app: 50 KB
        .{ 2, 200 }, // web-framework: 200 KB
        .{ 3, 120 }, // http: 120 KB
        .{ 4, 80 }, // json: 80 KB
        .{ 5, 30 }, // logging: 30 KB
        .{ 6, 150 }, // crypto: 150 KB
        .{ 7, 90 }, // tls: 90 KB
        .{ 8, 20 }, // base64: 20 KB
        .{ 9, 10 }, // utils: 10 KB
    };

    const pkgName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                1 => "app",
                2 => "web-framework",
                3 => "http",
                4 => "json",
                5 => "logging",
                6 => "crypto",
                7 => "tls",
                8 => "base64",
                9 => "utils",
                else => "unknown",
            };
        }
    }.get;

    std.debug.print("Direct dependencies:\n", .{});
    for (direct_dep_data) |d| {
        std.debug.print("  {s} -> {s}\n", .{ pkgName(d[0]), pkgName(d[1]) });
    }

    std.debug.print("\nPackage sizes:\n", .{});
    for (size_data) |s| {
        std.debug.print("  {s}: {} KB\n", .{ pkgName(s[0]), s[1] });
    }

    // -- Build relations --

    var direct_deps = try zodd.Relation(Pair).fromSlice(&ctx, &direct_dep_data);
    defer direct_deps.deinit();

    // -- Step 1: Compute transitive dependencies --
    //   dep(A, B) :- direct_dep(A, B).
    //   dep(A, C) :- dep(A, B), direct_dep(B, C).

    var dep = zodd.Variable(Pair).init(&ctx);
    defer dep.deinit();

    try dep.insertSlice(&ctx, direct_deps.elements);

    std.debug.print("\nComputing transitive dependencies...\n", .{});

    const PairList = std.ArrayListUnmanaged(Pair);
    var iteration: usize = 0;
    while (try dep.changed()) : (iteration += 1) {
        var results = PairList{};
        defer results.deinit(allocator);

        for (dep.recent.elements) |d| {
            const a = d[0];
            const b = d[1];

            // dep(A, B) + direct_dep(B, C) -> dep(A, C)
            for (direct_deps.elements) |dd| {
                if (dd[0] == b) {
                    try results.append(allocator, .{ a, dd[1] });
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Pair).fromSlice(&ctx, results.items);
            try dep.insert(rel);
        }

        if (iteration > 50) break;
    }

    var deps = try dep.complete();
    defer deps.deinit();

    std.debug.print("\nTransitive dependencies:\n", .{});
    for (deps.elements) |d| {
        std.debug.print("  {s} depends on {s}\n", .{ pkgName(d[0]), pkgName(d[1]) });
    }

    std.debug.print("\nTotal: {} dependency pairs\n", .{deps.len()});

    // -- Step 2: Detect circular dependencies --
    //   circular(A) :- dep(A, A).

    std.debug.print("\nCircular dependency check:\n", .{});
    var circular_count: usize = 0;
    for (deps.elements) |d| {
        if (d[0] == d[1]) {
            std.debug.print("  CIRCULAR: {s} depends on itself!\n", .{pkgName(d[0])});
            circular_count += 1;
        }
    }
    if (circular_count == 0) {
        std.debug.print("  No circular dependencies detected.\n", .{});
    }

    // -- Step 3: Compute total install size using aggregate --
    //
    // For each package, sum the sizes of all its transitive dependencies plus itself.
    // We build a relation of (package, dep_size) pairs and aggregate by summing.

    var pkg_sizes = try zodd.Relation(SizeTuple).fromSlice(&ctx, &size_data);
    defer pkg_sizes.deinit();

    // Build (package, dep_size) pairs: for each dep(A, B), look up size of B.
    var install_tuples = PairList{};
    defer install_tuples.deinit(allocator);

    // Add each package's own size
    for (size_data) |s| {
        try install_tuples.append(allocator, .{ s[0], s[1] });
    }

    // Add dependency sizes
    for (deps.elements) |d| {
        for (size_data) |s| {
            if (s[0] == d[1]) {
                try install_tuples.append(allocator, .{ d[0], s[1] });
                break;
            }
        }
    }

    var install_rel = try zodd.Relation(Pair).fromSlice(&ctx, install_tuples.items);
    defer install_rel.deinit();

    // Aggregate: sum sizes per package
    var total_sizes = try zodd.aggregateFn(
        Pair,
        u32,
        u32,
        &ctx,
        &install_rel,
        struct {
            fn key(tuple: *const Pair) u32 {
                return tuple.*[0];
            }
        }.key,
        0,
        struct {
            fn fold(acc: u32, tuple: *const Pair) u32 {
                return acc + tuple.*[1];
            }
        }.fold,
    );
    defer total_sizes.deinit();

    std.debug.print("\nTotal install sizes (package + all dependencies):\n", .{});
    for (total_sizes.elements) |ts| {
        std.debug.print("  {s}: {} KB\n", .{ pkgName(ts[0]), ts[1] });
    }

    // -- Step 4: Reverse-dependency lookup using SecondaryIndex --
    //
    // Build a secondary index on the transitive deps relation, keyed by the
    // dependency (the target), so we can efficiently answer "who depends on X?"

    const DepIndex = zodd.index.SecondaryIndex(
        Pair,
        u32,
        struct {
            fn extract(tuple: Pair) u32 {
                return tuple[1]; // index by the dependency (target)
            }
        }.extract,
        struct {
            fn compare(a: u32, b: u32) std.math.Order {
                return std.math.order(a, b);
            }
        }.compare,
        16,
    );

    var rev_index = DepIndex.init(&ctx);
    defer rev_index.deinit();

    try rev_index.insertSlice(deps.elements);

    // Query: who depends on "utils" (id=9)?
    std.debug.print("\nReverse-dependency query: who depends on '{s}'?\n", .{pkgName(9)});
    if (rev_index.get(9)) |dependents| {
        for (dependents.elements) |d| {
            std.debug.print("  {s}\n", .{pkgName(d[0])});
        }
        std.debug.print("  ({} packages depend on {s})\n", .{ dependents.len(), pkgName(9) });
    }

    // Query: who depends on "tls" (id=7)?
    std.debug.print("\nReverse-dependency query: who depends on '{s}'?\n", .{pkgName(7)});
    if (rev_index.get(7)) |dependents| {
        for (dependents.elements) |d| {
            std.debug.print("  {s}\n", .{pkgName(d[0])});
        }
        std.debug.print("  ({} packages depend on {s})\n", .{ dependents.len(), pkgName(7) });
    }
}

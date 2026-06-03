const std = @import("std");
const zodd = @import("zodd");

// Datalog Frontend
//
// Runs a textual Datalog program through the frontend: facts and rules are
// parsed from source, solved with stratified semi-naive evaluation, and
// queried through partial bindings. The same program can be built
// programmatically with `db.builder()`; see the frontend tests.
//
// The program models a package registry: transitive dependencies, packages
// that are safe to install (no dependency on a yanked package), and a
// dependency count per package.
//
// Datalog rules:
//   needs(P, D)        :- dep(P, D).
//   needs(P, D)        :- needs(P, M), dep(M, D).
//   safe(P)            :- package(P), not tainted(P).
//   tainted(P)         :- needs(P, D), yanked(D).
//   tainted(P)         :- yanked(P).
//   fanout(P, count(D)) :- needs(P, D).

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Zodd Datalog Engine - Datalog Frontend\n", .{});
    std.debug.print("======================================\n\n", .{});

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    // Facts and rules in textual Datalog. Facts can also be added with
    // `db.addFact("dep", &.{ .{ .str = "app" }, .{ .str = "http" } })`.
    try db.run(
        \\% The package graph.
        \\package("app"). package("http"). package("json").
        \\package("io"). package("core"). package("leftpad").
        \\
        \\dep("app", "http"). dep("app", "json").
        \\dep("http", "io"). dep("json", "io").
        \\dep("io", "core").
        \\dep("json", "leftpad").
        \\
        \\yanked("leftpad").
        \\
        \\% Transitive dependencies.
        \\needs(P, D) :- dep(P, D).
        \\needs(P, D) :- needs(P, M), dep(M, D).
        \\
        \\% A package is tainted if it is yanked or depends on a yanked
        \\% package; otherwise it is safe to install.
        \\tainted(P) :- yanked(P).
        \\tainted(P) :- needs(P, D), yanked(D).
        \\safe(P) :- package(P), not tainted(P).
        \\
        \\% Transitive dependency count per package.
        \\fanout(P, count(D)) :- needs(P, D).
    );

    try db.solve();

    std.debug.print("Transitive dependencies of \"app\":\n", .{});
    var needs = try db.query("needs", &.{ zodd.Value{ .str = "app" }, null });
    defer needs.deinit();
    while (needs.next()) |row| {
        std.debug.print("  app -> {s}\n", .{row.get(1).str});
    }

    std.debug.print("\nPackages safe to install:\n", .{});
    var safe = try db.query("safe", &.{null});
    defer safe.deinit();
    while (safe.next()) |row| {
        std.debug.print("  {s}\n", .{row.get(0).str});
    }

    std.debug.print("\nDependency counts:\n", .{});
    var fanout = try db.query("fanout", &.{ null, null });
    defer fanout.deinit();
    while (fanout.next()) |row| {
        std.debug.print("  {f}\n", .{row});
    }
}

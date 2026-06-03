//! Integration tests for the Datalog frontend: the parser, the builder, the
//! semantic analysis, and the stratified evaluator working together through
//! the public `Database` API.

const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "frontend: transitive closure matches a hand-written semi-naive loop" {
    const allocator = testing.allocator;

    const edges = [_][2]u64{
        .{ 1, 2 }, .{ 2, 3 }, .{ 3, 4 }, .{ 2, 5 }, .{ 5, 6 },
        .{ 6, 3 }, .{ 7, 1 }, .{ 4, 8 }, .{ 8, 9 }, .{ 9, 4 },
    };

    // Frontend evaluation.
    var db = zodd.Database.init(allocator);
    defer db.deinit();
    for (edges) |edge| {
        try db.addFact("edge", &.{ .{ .int = edge[0] }, .{ .int = edge[1] } });
    }
    try db.run(
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );
    try db.solve();

    var frontend_pairs: std.ArrayListUnmanaged([2]u64) = .empty;
    defer frontend_pairs.deinit(allocator);
    var it = try db.query("path", &.{ null, null });
    defer it.deinit();
    while (it.next()) |row| {
        try frontend_pairs.append(allocator, .{ row.get(0).int, row.get(1).int });
    }

    // Hand-written semi-naive loop over the engine core, as in the README.
    const Edge = struct { u64, u64 };
    var edge_tuples: [edges.len]Edge = undefined;
    for (edges, 0..) |edge, i| edge_tuples[i] = .{ edge[0], edge[1] };

    var base = try zodd.Relation(Edge).fromSlice(allocator, &edge_tuples);
    defer base.deinit();

    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();
    try reachable.insertSlice(base.elements);

    while (try reachable.changed()) {
        var batch: std.ArrayListUnmanaged(Edge) = .empty;
        defer batch.deinit(allocator);
        for (reachable.recent.elements) |r| {
            for (base.elements) |e| {
                if (e[0] == r[1]) try batch.append(allocator, .{ r[0], e[1] });
            }
        }
        if (batch.items.len > 0) {
            const rel = try zodd.Relation(Edge).fromSlice(allocator, batch.items);
            try reachable.insert(rel);
        }
    }

    var expected = try reachable.complete();
    defer expected.deinit();

    try testing.expectEqual(expected.len(), frontend_pairs.items.len);
    for (expected.elements, frontend_pairs.items) |want, got| {
        try testing.expectEqual(want[0], got[0]);
        try testing.expectEqual(want[1], got[1]);
    }
}

test "frontend: stratified negation over a role hierarchy" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\% Role inheritance and explicit denials.
        \\inherits("admin", "editor").
        \\inherits("editor", "viewer").
        \\grants("viewer", "read").
        \\grants("editor", "write").
        \\grants("admin", "configure").
        \\denied("editor", "configure").
        \\
        \\role(R, R2) :- inherits(R, R2).
        \\role(R, R3) :- role(R, R2), inherits(R2, R3).
        \\
        \\has_perm(R, P) :- grants(R, P).
        \\has_perm(R, P) :- role(R, R2), grants(R2, P).
        \\
        \\allowed(R, P) :- has_perm(R, P), not denied(R, P).
    );
    try db.solve();

    // The editor inherits read but is denied configure.
    var editor = try db.query("allowed", &.{ zodd.Value{ .str = "editor" }, null });
    defer editor.deinit();
    var perms: std.ArrayListUnmanaged([]const u8) = .empty;
    defer perms.deinit(allocator);
    while (editor.next()) |row| {
        try perms.append(allocator, row.get(1).str);
    }
    try testing.expectEqual(@as(usize, 2), perms.items.len);
    for (perms.items) |perm| {
        try testing.expect(!std.mem.eql(u8, perm, "configure"));
    }

    // The admin is not denied anything.
    var admin = try db.query("allowed", &.{ zodd.Value{ .str = "admin" }, null });
    defer admin.deinit();
    var admin_count: usize = 0;
    while (admin.next()) |_| admin_count += 1;
    try testing.expectEqual(@as(usize, 3), admin_count);
}

test "frontend: aggregates over derived relations" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\dep("app", "http"). dep("app", "json").
        \\dep("http", "io"). dep("json", "io").
        \\dep("io", "core").
        \\
        \\needs(P, D) :- dep(P, D).
        \\needs(P, D) :- needs(P, M), dep(M, D).
        \\
        \\fanout(P, count(D)) :- needs(P, D).
    );
    try db.solve();

    // app transitively needs http, json, io, and core.
    var app = try db.query("fanout", &.{ zodd.Value{ .str = "app" }, null });
    defer app.deinit();
    try testing.expectEqual(@as(u64, 4), app.next().?.get(1).int);

    var io = try db.query("fanout", &.{ zodd.Value{ .str = "io" }, null });
    defer io.deinit();
    try testing.expectEqual(@as(u64, 1), io.next().?.get(1).int);
}

test "frontend: builder and parser produce the same results" {
    const allocator = testing.allocator;

    // Parsed program.
    var parsed = zodd.Database.init(allocator);
    defer parsed.deinit();
    try parsed.run(
        \\edge(1, 2). edge(2, 3). edge(3, 1).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );
    try parsed.solve();

    // The same program through the builder.
    var built = zodd.Database.init(allocator);
    defer built.deinit();
    {
        var b = built.builder();
        const edge = try b.predicate("edge", 2);
        const path = try b.predicate("path", 2);
        try b.fact(edge, &.{ 1, 2 });
        try b.fact(edge, &.{ 2, 3 });
        try b.fact(edge, &.{ 3, 1 });

        var r1 = b.rule(path);
        const x1 = try r1.v("X");
        const y1 = try r1.v("Y");
        try r1.head(&.{ x1, y1 });
        try r1.pos(edge, &.{ x1, y1 });
        try r1.finish();

        var r2 = b.rule(path);
        const x2 = try r2.v("X");
        const y2 = try r2.v("Y");
        const z2 = try r2.v("Z");
        try r2.head(&.{ x2, z2 });
        try r2.pos(path, &.{ x2, y2 });
        try r2.pos(edge, &.{ y2, z2 });
        try r2.finish();
    }
    try built.solve();

    var parsed_it = try parsed.query("path", &.{ null, null });
    defer parsed_it.deinit();
    var built_it = try built.query("path", &.{ null, null });
    defer built_it.deinit();

    while (parsed_it.next()) |parsed_row| {
        const built_row = built_it.next().?;
        try testing.expectEqual(parsed_row.get(0).int, built_row.get(0).int);
        try testing.expectEqual(parsed_row.get(1).int, built_row.get(1).int);
    }
    try testing.expect(built_it.next() == null);
}

test "frontend: string atoms display round-trip" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\likes("alice", "tea"). likes("bob", "coffee"). likes("alice", 42).
    );

    var it = try db.query("likes", &.{ zodd.Value{ .str = "alice" }, null });
    defer it.deinit();

    var buffer: [128]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    while (it.next()) |row| {
        try writer.print("{f} ", .{row});
    }
    try testing.expectEqualStrings(
        "(\"alice\", 42) (\"alice\", \"tea\") ",
        writer.buffered(),
    );
}

test "frontend: unstratifiable programs are rejected" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\q(1).
        \\p(X) :- q(X), not p(X).
    );
    try testing.expectError(error.NegationCycle, db.solve());
    try testing.expect(db.lastDiagnostic() != null);
}

test "frontend: iteration limits surface from the engine" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );

    db.max_iterations = 1;
    try testing.expectError(error.MaxIterationsExceeded, db.solve());

    db.max_iterations = null;
    try db.solve();
    var it = try db.query("path", &.{ null, null });
    defer it.deinit();
    var count: usize = 0;
    while (it.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 10), count);
}

test "frontend: stored queries parse and survive analysis" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2).
        \\path(X, Y) :- edge(X, Y).
        \\?- path(1, X).
        \\?- path(_, _).
    );
    try db.solve();

    var it = try db.query("path", &.{ null, null });
    defer it.deinit();
    try testing.expect(it.next() != null);
}

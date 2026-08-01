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

test "frontend: comparison filters across joins, recursion, and aggregates" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\% Weighted edges; hop(X, Y) keeps only the light ones.
        \\edge(1, 2, 10). edge(2, 3, 50). edge(3, 4, 20). edge(1, 4, 99).
        \\hop(X, Y) :- edge(X, Y, W), W < 60.
        \\reach(X, Y) :- hop(X, Y).
        \\reach(X, Z) :- reach(X, Y), hop(Y, Z), X != Z.
        \\out_deg(N, count(M)) :- hop(N, M).
        \\busy(N) :- out_deg(N, D), D >= 1.
    );
    try db.solve();

    // The 99-weight edge is filtered, so 4 is reached only through 2 and 3.
    var reach = try db.query("reach", &.{ zodd.Value{ .int = 1 }, null });
    defer reach.deinit();
    var targets: [4]u64 = undefined;
    var n: usize = 0;
    while (reach.next()) |row| : (n += 1) {
        targets[n] = row.get(1).int;
    }
    try testing.expectEqualSlices(u64, &.{ 2, 3, 4 }, targets[0..n]);

    // Comparisons over aggregate results work across strata.
    var busy = try db.query("busy", &.{null});
    defer busy.deinit();
    var count: usize = 0;
    while (busy.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 3), count);
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

test "frontend: aggregate whose argument is also a group variable" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    // The body binds MAX_ARITY (16) distinct variables, and the aggregated
    // variable A is also a group variable, so the aggregate projection must
    // not grow past MAX_ARITY columns.
    try db.run(
        \\e(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16).
        \\e(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17).
        \\e(2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16).
        \\t(A, count(A)) :- e(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P).
    );
    try db.solve();

    var it = try db.query("t", &.{ null, null });
    defer it.deinit();
    var counts: [3]u64 = .{ 0, 0, 0 };
    var rows: usize = 0;
    while (it.next()) |row| {
        counts[@intCast(row.get(0).int)] = row.get(1).int;
        rows += 1;
    }
    try testing.expectEqual(@as(usize, 2), rows);
    try testing.expectEqual(@as(u64, 2), counts[1]);
    try testing.expectEqual(@as(u64, 1), counts[2]);
}

test "frontend: query after a failed solve reports the failure" {
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

    // The failed solve must not leave partial results behind: querying
    // re-solves and surfaces the same failure instead of returning
    // an incomplete relation.
    try testing.expectError(error.MaxIterationsExceeded, db.query("path", &.{ null, null }));
}

test "frontend: analyze failure does not serve stale results" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2).
        \\path(X, Y) :- edge(X, Y).
    );
    try db.solve();

    // Adding an unsafe rule makes the next solve fail during analysis;
    // queries must then report the failure rather than answer from the
    // previous program version.
    try db.run("bad(X) :- not edge(X, 1).");
    try testing.expectError(error.UnsafeHeadVariable, db.solve());
    try testing.expectError(error.UnsafeHeadVariable, db.query("path", &.{ null, null }));
}

test "frontend: fact with a huge arity is rejected, not a crash" {
    const allocator = testing.allocator;

    var source: std.ArrayListUnmanaged(u8) = .empty;
    defer source.deinit(allocator);
    try source.appendSlice(allocator, "p(1");
    for (1..70_000) |_| {
        try source.appendSlice(allocator, ", 1");
    }
    try source.appendSlice(allocator, ").");

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try testing.expectError(error.ArityTooLarge, db.run(source.items));
}

test "frontend: rule with too many wildcard variables is rejected" {
    const allocator = testing.allocator;

    // 8,250 literals with 8 wildcards each lower to 66,000 distinct
    // variables, past the u16 VarId range.
    var source: std.ArrayListUnmanaged(u8) = .empty;
    defer source.deinit(allocator);
    try source.appendSlice(allocator, "t(X) :- q(X, 1, 1, 1, 1, 1, 1, 1)");
    for (0..8_250) |_| {
        try source.appendSlice(allocator, ", q(_, _, _, _, _, _, _, _)");
    }
    try source.appendSlice(allocator, ".");

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try db.run(source.items);
    try testing.expectError(error.TooManyVariables, db.solve());
}

test "frontend: arithmetic in comparison filters" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2, 30). edge(2, 3, 60). edge(3, 4, 70).
        \\light(X, Y) :- edge(X, Y, W), W * 2 < 125.
        \\heavy(X, Y) :- edge(X, Y, W), W + 10 >= 70.
        \\middle(X) :- edge(X, _, W), (W + 10) * 2 = 140.
        \\precedence(X) :- edge(X, _, _), 2 + 3 * 4 = 14.
    );
    try db.solve();

    var light = try db.query("light", &.{ null, null });
    defer light.deinit();
    var light_count: usize = 0;
    while (light.next()) |row| {
        try testing.expect(row.get(0).int == 1 or row.get(0).int == 2);
        light_count += 1;
    }
    try testing.expectEqual(@as(usize, 2), light_count);

    var heavy = try db.query("heavy", &.{ null, null });
    defer heavy.deinit();
    var heavy_count: usize = 0;
    while (heavy.next()) |row| {
        try testing.expect(row.get(0).int == 2 or row.get(0).int == 3);
        heavy_count += 1;
    }
    try testing.expectEqual(@as(usize, 2), heavy_count);

    var middle = try db.query("middle", &.{null});
    defer middle.deinit();
    const middle_row = middle.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 2), middle_row.get(0).int);
    try testing.expect(middle.next() == null);

    // 2 + 3 * 4 must parse as 2 + (3 * 4): every edge source qualifies.
    var prec = try db.query("precedence", &.{null});
    defer prec.deinit();
    var prec_count: usize = 0;
    while (prec.next()) |_| prec_count += 1;
    try testing.expectEqual(@as(usize, 3), prec_count);
}

test "frontend: arithmetic failure filters the tuple instead of erroring" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\v(0). v(3). v("s").
        \\division(X) :- v(X), 6 / X > 1.
        \\subtraction(X) :- v(X), X - 1 < 5.
        \\overflow(X) :- v(X), X * 4611686018427387904 >= 0.
    );
    try db.solve();

    // Division by zero and string operands fail the filter; only X = 3
    // passes either rule. The overflow rule keeps nothing: X = 3 pushes the
    // product past the 63-bit atom range, X = 0 gives 0 >= 0 but 0 * ... is
    // fine, so X = 0 stays.
    var division = try db.query("division", &.{null});
    defer division.deinit();
    const div_row = division.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 3), div_row.get(0).int);
    try testing.expect(division.next() == null);

    var subtraction = try db.query("subtraction", &.{null});
    defer subtraction.deinit();
    const sub_row = subtraction.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 3), sub_row.get(0).int);
    try testing.expect(subtraction.next() == null);

    var overflow = try db.query("overflow", &.{null});
    defer overflow.deinit();
    const ovf_row = overflow.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 0), ovf_row.get(0).int);
    try testing.expect(overflow.next() == null);
}

test "frontend: oversized arithmetic expressions are rejected" {
    const allocator = testing.allocator;

    // Too many operands on one comparison side.
    var wide: std.ArrayListUnmanaged(u8) = .empty;
    defer wide.deinit(allocator);
    try wide.appendSlice(allocator, "p(X) :- v(X), 1");
    for (0..70) |_| {
        try wide.appendSlice(allocator, " + 1");
    }
    try wide.appendSlice(allocator, " < X.");

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try db.run("v(1).");
    try testing.expectError(error.ExpressionTooLarge, db.run(wide.items));

    // Too deeply parenthesized.
    var deep: std.ArrayListUnmanaged(u8) = .empty;
    defer deep.deinit(allocator);
    try deep.appendSlice(allocator, "p(X) :- v(X), ");
    for (0..40) |_| {
        try deep.appendSlice(allocator, "(");
    }
    try deep.appendSlice(allocator, "1");
    for (0..40) |_| {
        try deep.appendSlice(allocator, ")");
    }
    try deep.appendSlice(allocator, " < X.");
    try testing.expectError(error.ExpressionTooLarge, db.run(deep.items));
}

test "frontend: unbound variable inside an arithmetic expression is unsafe" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\v(1).
        \\bad(X) :- v(X), X + Y < 10.
    );
    try testing.expectError(error.UnsafeComparisonVariable, db.solve());
}

test "frontend: is-assignments compute per-tuple values" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3). edge(3, 4).
        \\dist(1, 0).
        \\dist(Y, D2) :- dist(X, D), edge(X, Y), D2 is D + 1.
    );
    db.max_iterations = 10;
    try db.solve();

    var it = try db.query("dist", &.{ null, null });
    defer it.deinit();
    var hops: [5]u64 = @splat(99);
    var rows: usize = 0;
    while (it.next()) |row| {
        hops[@intCast(row.get(0).int)] = row.get(1).int;
        rows += 1;
    }
    try testing.expectEqual(@as(usize, 4), rows);
    try testing.expectEqual(@as(u64, 0), hops[1]);
    try testing.expectEqual(@as(u64, 1), hops[2]);
    try testing.expectEqual(@as(u64, 2), hops[3]);
    try testing.expectEqual(@as(u64, 3), hops[4]);
}

test "frontend: assignments chain and feed comparisons" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\v(1). v(40).
        \\r(X, B) :- v(X), A is X + 1, B is A * 2, B < 20.
    );
    try db.solve();

    // X = 1 gives B = 4; X = 40 gives B = 82, filtered by B < 20.
    var it = try db.query("r", &.{ null, null });
    defer it.deinit();
    const row = it.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 1), row.get(0).int);
    try testing.expectEqual(@as(u64, 4), row.get(1).int);
    try testing.expect(it.next() == null);
}

test "frontend: assignment failure derives nothing for the tuple" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\v(0). v(3). v("s").
        \\dec(X, Y) :- v(X), Y is X - 1.
    );
    try db.solve();

    // X = 0 underflows and X = "s" is not an integer; only X = 3 derives.
    var it = try db.query("dec", &.{ null, null });
    defer it.deinit();
    const row = it.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 3), row.get(0).int);
    try testing.expectEqual(@as(u64, 2), row.get(1).int);
    try testing.expect(it.next() == null);
}

test "frontend: unsound assignments are rejected" {
    const allocator = testing.allocator;

    // An unbound variable on the right-hand side.
    {
        var db = zodd.Database.init(allocator);
        defer db.deinit();
        try db.run(
            \\v(1).
            \\bad(X, Y) :- v(X), Y is Z + 1.
        );
        try testing.expectError(error.UnsafeAssignmentVariable, db.solve());
    }

    // A target already bound by a positive literal.
    {
        var db = zodd.Database.init(allocator);
        defer db.deinit();
        try db.run(
            \\v(1).
            \\bad(X) :- v(X), X is 1 + 1.
        );
        try testing.expectError(error.InvalidAssignment, db.solve());
    }

    // A target assigned twice.
    {
        var db = zodd.Database.init(allocator);
        defer db.deinit();
        try db.run(
            \\v(1).
            \\bad(X, A) :- v(X), A is X + 1, A is X + 2.
        );
        try testing.expectError(error.InvalidAssignment, db.solve());
    }
}

test "frontend: recursive arithmetic requires an iteration limit" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    // On a cyclic graph this program would count hops forever; solving
    // without an iteration limit must be refused up front.
    try db.run(
        \\edge(1, 2). edge(2, 1).
        \\dist(1, 0).
        \\dist(Y, D2) :- dist(X, D), edge(X, Y), D2 is D + 1.
    );
    try testing.expectError(error.IterationLimitRequired, db.solve());

    db.max_iterations = 5;
    try testing.expectError(error.MaxIterationsExceeded, db.solve());
}

test "frontend: predicates up to arity 16" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\wide(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16).
        \\wide(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17).
        \\ends(A, P) :- wide(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P).
    );
    try db.solve();

    var it = try db.query("ends", &.{ null, null });
    defer it.deinit();
    var lasts: [2]u64 = undefined;
    var rows: usize = 0;
    while (it.next()) |row| : (rows += 1) {
        try testing.expectEqual(@as(u64, 1), row.get(0).int);
        lasts[rows] = row.get(1).int;
    }
    try testing.expectEqual(@as(usize, 2), rows);
    try testing.expectEqualSlices(u64, &.{ 16, 17 }, &lasts);

    // Arity 17 stays out of range.
    var over = zodd.Database.init(allocator);
    defer over.deinit();
    try testing.expectError(
        error.ArityTooLarge,
        over.run("p(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)."),
    );
}

test "frontend: retract removes a base fact and recomputes" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );
    try db.solve();

    var before = try db.query("path", &.{ null, null });
    defer before.deinit();
    var count: usize = 0;
    while (before.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 3), count);

    // Retracting edge(2, 3) removes path(2, 3) and path(1, 3).
    try testing.expect(try db.retract("edge", &.{ .{ .int = 2 }, .{ .int = 3 } }));
    var after = try db.query("path", &.{ null, null });
    defer after.deinit();
    const row = after.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 1), row.get(0).int);
    try testing.expectEqual(@as(u64, 2), row.get(1).int);
    try testing.expect(after.next() == null);

    // A second retract of the same fact, or of a fact never added, removes
    // nothing.
    try testing.expect(!try db.retract("edge", &.{ .{ .int = 2 }, .{ .int = 3 } }));
    try testing.expect(!try db.retract("edge", &.{ .{ .int = 9 }, .{ .int = 9 } }));

    // Unknown predicates and arity mismatches are errors.
    try testing.expectError(error.UnknownPredicate, db.retract("nope", &.{.{ .int = 1 }}));
    try testing.expectError(error.ArityMismatch, db.retract("edge", &.{.{ .int = 1 }}));
}

test "frontend: queryDemand matches query on recursive programs" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3). edge(3, 4). edge(2, 5). edge(5, 6).
        \\edge(6, 3). edge(7, 1). edge(4, 8). edge(8, 9). edge(9, 4).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );

    // Demand-driven answers for a bound first argument match the full
    // evaluation, for every source node.
    var source: u64 = 1;
    while (source <= 9) : (source += 1) {
        var full = try db.query("path", &.{ zodd.Value{ .int = source }, null });
        defer full.deinit();
        var expected: std.ArrayListUnmanaged(u64) = .empty;
        defer expected.deinit(allocator);
        while (full.next()) |row| {
            try expected.append(allocator, row.get(1).int);
        }

        var demand = try db.queryDemand("path", &.{ zodd.Value{ .int = source }, null });
        defer demand.deinit();
        var got: std.ArrayListUnmanaged(u64) = .empty;
        defer got.deinit(allocator);
        while (demand.next()) |row| {
            try testing.expectEqual(source, row.get(0).int);
            try got.append(allocator, row.get(1).int);
        }

        try testing.expectEqualSlices(u64, expected.items, got.items);
    }
}

test "frontend: queryDemand falls back when demand does not apply" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3).
        \\blocked(3).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
        \\open(X, Y) :- path(X, Y), not blocked(Y).
        \\deg(N, count(M)) :- edge(N, M).
    );

    // Negation in the cone: falls back to full evaluation, same answers.
    var open = try db.queryDemand("open", &.{ zodd.Value{ .int = 1 }, null });
    defer open.deinit();
    const open_row = open.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 2), open_row.get(1).int);
    try testing.expect(open.next() == null);

    // Aggregates in the cone.
    var deg = try db.queryDemand("deg", &.{ zodd.Value{ .int = 1 }, null });
    defer deg.deinit();
    try testing.expectEqual(@as(u64, 1), (deg.next() orelse return error.TestUnexpectedResult).get(1).int);

    // No bound argument.
    var all = try db.queryDemand("path", &.{ null, null });
    defer all.deinit();
    var count: usize = 0;
    while (all.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 3), count);

    // A base (non-derived) predicate.
    var base = try db.queryDemand("edge", &.{ zodd.Value{ .int = 1 }, null });
    defer base.deinit();
    try testing.expectEqual(@as(u64, 2), (base.next() orelse return error.TestUnexpectedResult).get(1).int);
}

test "frontend: queryDemand evaluates only the demanded cone" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    // Two disconnected chains; demand for a node in the first chain must
    // not derive paths in the second one. `path(9, X)` from the far end of
    // a chain demands far fewer tuples than the full closure.
    try db.run(
        \\edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).
        \\edge(6, 7). edge(7, 8). edge(8, 9). edge(9, 10).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );

    var it = try db.queryDemand("path", &.{ zodd.Value{ .int = 9 }, null });
    defer it.deinit();
    const row = it.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u64, 10), row.get(1).int);
    try testing.expect(it.next() == null);
    // An inline test in program.zig checks that this evaluation computed
    // only the demanded slice of the closure.
}

test "frontend: parallel evaluation matches sequential results" {
    const allocator = testing.allocator;

    // Mutually recursive rules across two predicates, negation and an
    // aggregate in later strata, and arithmetic: several rule evaluations
    // per round, so parallel workers get real overlap.
    var source: std.ArrayListUnmanaged(u8) = .empty;
    defer source.deinit(allocator);
    var seed: u64 = 0x9e3779b97f4a7c15;
    for (0..120) |_| {
        seed = seed *% 6364136223846793005 +% 1442695040888963407;
        const a = (seed >> 33) % 30;
        const b = (seed >> 13) % 30;
        try source.print(allocator, "edge({d}, {d}).\n", .{ a, b });
    }
    try source.appendSlice(allocator,
        \\blocked(7). blocked(13).
        \\even(X, Y) :- edge(X, Y).
        \\even(X, Z) :- odd(X, Y), edge(Y, Z).
        \\odd(X, Y) :- edge(X, Y).
        \\odd(X, Z) :- even(X, Y), edge(Y, Z), X != Z.
        \\open(X, Y) :- even(X, Y), not blocked(Y).
        \\fan(N, count(M)) :- open(N, M).
        \\big(N, C2) :- fan(N, C), C2 is C * 2, C2 > 2.
    );

    var sequential = zodd.Database.init(allocator);
    defer sequential.deinit();
    try sequential.run(source.items);
    try sequential.solve();

    var parallel = zodd.Database.init(allocator);
    defer parallel.deinit();
    parallel.parallelism = 4;
    try parallel.run(source.items);
    try parallel.solve();

    for ([_][]const u8{ "even", "odd", "open", "fan", "big" }) |pred| {
        var seq_rows: std.ArrayListUnmanaged([2]u64) = .empty;
        defer seq_rows.deinit(allocator);
        var seq_it = try sequential.query(pred, &.{ null, null });
        defer seq_it.deinit();
        while (seq_it.next()) |row| {
            try seq_rows.append(allocator, .{ row.get(0).int, row.get(1).int });
        }

        var par_it = try parallel.query(pred, &.{ null, null });
        defer par_it.deinit();
        var i: usize = 0;
        while (par_it.next()) |row| : (i += 1) {
            try testing.expect(i < seq_rows.items.len);
            try testing.expectEqual(seq_rows.items[i][0], row.get(0).int);
            try testing.expectEqual(seq_rows.items[i][1], row.get(1).int);
        }
        try testing.expectEqual(seq_rows.items.len, i);
        try testing.expect(seq_rows.items.len > 0);
    }
}

test "frontend: facts added after solve become visible to queries" {
    const allocator = testing.allocator;

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );
    try db.solve();

    try db.addFact("edge", &.{ .{ .int = 3 }, .{ .int = 4 } });

    // The new edge extends the closure: 3 old paths plus (3,4), (2,4),
    // and (1,4).
    var it = try db.query("path", &.{ null, null });
    defer it.deinit();
    var count: usize = 0;
    while (it.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 6), count);
}

test "frontend: update maintains mixed additions and retractions" {
    const allocator = testing.allocator;

    const rules =
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
        \\reachable(Y) :- path(1, Y).
        \\isolated(X) :- node(X), not reachable(X).
        \\fan(N, count(M)) :- path(N, M).
    ;

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try db.run(rules);
    try db.run("node(1). node(2). node(3). node(4). node(5).");
    try db.run("edge(1, 2). edge(2, 3). edge(4, 5).");
    try db.solve();

    // A batch of changes: connect 3 to 4, drop 4 to 5, add a node.
    try db.addFact("edge", &.{ .{ .int = 3 }, .{ .int = 4 } });
    try testing.expect(try db.retract("edge", &.{ .{ .int = 4 }, .{ .int = 5 } }));
    try db.addFact("node", &.{.{ .int = 6 }});
    try db.update();

    // A fresh database with the same final facts must agree on every
    // derived predicate.
    var fresh = zodd.Database.init(allocator);
    defer fresh.deinit();
    try fresh.run(rules);
    try fresh.run("node(1). node(2). node(3). node(4). node(5). node(6).");
    try fresh.run("edge(1, 2). edge(2, 3). edge(3, 4).");
    try fresh.solve();

    const preds = [_]struct { name: []const u8, arity: usize }{
        .{ .name = "path", .arity = 2 },
        .{ .name = "reachable", .arity = 1 },
        .{ .name = "isolated", .arity = 1 },
        .{ .name = "fan", .arity = 2 },
    };
    for (preds) |pred| {
        var pattern: [2]?zodd.Value = .{ null, null };
        var expect_it = try fresh.query(pred.name, pattern[0..pred.arity]);
        defer expect_it.deinit();
        var got_it = try db.query(pred.name, pattern[0..pred.arity]);
        defer got_it.deinit();
        while (expect_it.next()) |want| {
            const got = got_it.next() orelse return error.TestUnexpectedResult;
            for (0..pred.arity) |i| {
                try testing.expectEqual(want.get(i).int, got.get(i).int);
            }
        }
        try testing.expect(got_it.next() == null);
    }
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

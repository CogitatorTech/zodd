const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "incremental maintenance: monotonic updates" {
    const allocator = testing.allocator;
    const Tuple = struct { u32, u32 };

    var iter = zodd.Iteration(Tuple).init(allocator, 100);
    defer iter.deinit();

    const B = try iter.variable();
    const A = try iter.variable();

    try B.insertSlice(&[_]Tuple{.{ 1, 2 }});

    while (try iter.changed()) {
        if (B.recent.len() > 0) {
            const rel = try zodd.Relation(Tuple).fromSlice(allocator, B.recent.elements);
            try A.insert(rel);
        }
    }

    try testing.expectEqual(@as(usize, 1), A.totalLen());

    try B.insertSlice(&[_]Tuple{.{ 2, 3 }});

    iter.reset();

    while (try iter.changed()) {
        if (B.recent.len() > 0) {
            const rel = try zodd.Relation(Tuple).fromSlice(allocator, B.recent.elements);
            try A.insert(rel);
        }
    }

    try testing.expectEqual(@as(usize, 2), A.totalLen());

    var final_res = try A.complete();
    defer final_res.deinit();

    try testing.expectEqual(@as(usize, 2), final_res.len());
    try testing.expectEqual(final_res.elements[0][0], 1);
    try testing.expectEqual(final_res.elements[1][0], 2);
}

test "incremental maintenance: join with new data after reset" {
    const allocator = testing.allocator;
    const KV = struct { u32, u32 };
    const Out = struct { u32, u32, u32 };

    var iter = zodd.Iteration(KV).init(allocator, 100);
    defer iter.deinit();

    const edges = try iter.variable();
    const labels = try iter.variable();
    var joined = zodd.Variable(Out).init(allocator);
    defer joined.deinit();

    // Round 1: edges={1->2}, labels={1->100}
    try edges.insertSlice(&[_]KV{.{ 1, 2 }});
    try labels.insertSlice(&[_]KV{.{ 1, 100 }});

    while (try iter.changed()) {
        try zodd.joinInto(u32, u32, u32, Out, edges, labels, &joined, struct {
            fn logic(key: *const u32, edge_val: *const u32, label_val: *const u32) Out {
                return .{ key.*, edge_val.*, label_val.* };
            }
        }.logic);
    }

    try testing.expectEqual(@as(usize, 1), joined.totalLen());

    // Round 2: add edge 2->3 and label 2->200
    try edges.insertSlice(&[_]KV{.{ 2, 3 }});
    try labels.insertSlice(&[_]KV{.{ 2, 200 }});
    iter.reset();

    while (try iter.changed()) {
        try zodd.joinInto(u32, u32, u32, Out, edges, labels, &joined, struct {
            fn logic(key: *const u32, edge_val: *const u32, label_val: *const u32) Out {
                return .{ key.*, edge_val.*, label_val.* };
            }
        }.logic);
    }

    // Should have picked up the new join result
    try testing.expect(joined.totalLen() >= 2);
}

test "incremental maintenance: transitive closure re-convergence" {
    const allocator = testing.allocator;
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    // Phase 1: edges 1->2, 2->3
    var edges = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();

    try reachable.insertSlice(edges.elements);

    var iters: usize = 0;
    while (try reachable.changed()) {
        var new = EdgeList.empty;
        defer new.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) try new.append(allocator, .{ r[0], e[1] });
            }
        }
        if (new.items.len > 0) {
            try reachable.insert(try zodd.Relation(Edge).fromSlice(allocator, new.items));
        }
        iters += 1;
        if (iters > 10) break;
    }

    // 1->2, 1->3, 2->3 = 3 pairs
    try testing.expectEqual(@as(usize, 3), reachable.totalLen());

    // Phase 2: add edge 3->4
    var edges2 = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
    });
    defer edges2.deinit();

    try reachable.insertSlice(&[_]Edge{.{ 3, 4 }});

    iters = 0;
    while (try reachable.changed()) {
        var new = EdgeList.empty;
        defer new.deinit(allocator);

        for (reachable.recent.elements) |r| {
            // Forward join: recent × edges
            for (edges2.elements) |e| {
                if (e[0] == r[1]) try new.append(allocator, .{ r[0], e[1] });
            }
            // Backward join: stable × recent (to catch paths that can now reach through new edges)
            for (reachable.stable.items) |*stable_rel| {
                for (stable_rel.elements) |old| {
                    if (old[1] == r[0]) try new.append(allocator, .{ old[0], r[1] });
                }
            }
        }
        if (new.items.len > 0) {
            try reachable.insert(try zodd.Relation(Edge).fromSlice(allocator, new.items));
        }
        iters += 1;
        if (iters > 10) break;
    }

    // 1->2,1->3,1->4, 2->3,2->4, 3->4 = 6 pairs
    try testing.expectEqual(@as(usize, 6), reachable.totalLen());
}

test "incremental maintenance: iteration reset with multiple variables" {
    const allocator = testing.allocator;

    var iter = zodd.Iteration(u32).init(allocator, 50);
    defer iter.deinit();

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&[_]u32{ 10, 20 });
    try v2.insertSlice(&[_]u32{ 30, 40 });

    // Converge
    while (try iter.changed()) {}

    try testing.expectEqual(@as(usize, 2), v1.totalLen());
    try testing.expectEqual(@as(usize, 2), v2.totalLen());

    // Reset and add more data
    iter.reset();
    try v1.insertSlice(&[_]u32{ 50, 60 });

    const changed = try iter.changed();
    try testing.expect(changed);

    // Run to completion
    while (try iter.changed()) {}

    try testing.expectEqual(@as(usize, 4), v1.totalLen());
    try testing.expectEqual(@as(usize, 2), v2.totalLen());
}

test "incremental maintenance: interleaved updates match fresh solves" {
    const allocator = testing.allocator;

    const rules =
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
        \\dead(X) :- node(X), not alive(X).
        \\alive(X) :- path(_, X).
        \\load(N, count(M)) :- path(N, M).
    ;

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try db.run(rules);
    for (0..12) |i| {
        try db.addFact("node", &.{.{ .int = i }});
    }
    try db.solve();

    // A deterministic pseudo-random walk of additions and retractions;
    // after each step the maintained database must agree with a fresh one
    // built from the same fact set.
    var live_edges: std.ArrayListUnmanaged([2]u64) = .empty;
    defer live_edges.deinit(allocator);
    var seed: u64 = 0x853c49e6748fea9b;
    for (0..40) |_| {
        seed = seed *% 6364136223846793005 +% 1442695040888963407;
        const a = (seed >> 33) % 12;
        const b = (seed >> 13) % 12;
        const drop = live_edges.items.len > 4 and (seed >> 3) % 3 == 0;
        if (drop) {
            const victim = live_edges.swapRemove((seed >> 23) % live_edges.items.len);
            try testing.expect(try db.retract("edge", &.{ .{ .int = victim[0] }, .{ .int = victim[1] } }));
        } else {
            try db.addFact("edge", &.{ .{ .int = a }, .{ .int = b } });
            try live_edges.append(allocator, .{ a, b });
        }

        var fresh = zodd.Database.init(allocator);
        defer fresh.deinit();
        try fresh.run(rules);
        for (0..12) |i| {
            try fresh.addFact("node", &.{.{ .int = i }});
        }
        for (live_edges.items) |edge| {
            try fresh.addFact("edge", &.{ .{ .int = edge[0] }, .{ .int = edge[1] } });
        }
        try fresh.solve();

        for ([_]struct { name: []const u8, arity: usize }{
            .{ .name = "path", .arity = 2 },
            .{ .name = "dead", .arity = 1 },
            .{ .name = "load", .arity = 2 },
        }) |pred| {
            var pattern: [2]?zodd.Value = .{ null, null };
            var want_it = try fresh.query(pred.name, pattern[0..pred.arity]);
            defer want_it.deinit();
            var got_it = try db.query(pred.name, pattern[0..pred.arity]);
            defer got_it.deinit();
            while (want_it.next()) |want| {
                const got = got_it.next() orelse return error.TestUnexpectedResult;
                for (0..pred.arity) |col| {
                    try testing.expectEqual(want.get(col).int, got.get(col).int);
                }
            }
            try testing.expect(got_it.next() == null);
        }
    }
}

const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "incremental maintenance: monotonic updates" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var iter = zodd.Iteration(Tuple).init(&ctx, 100);
    defer iter.deinit();

    const B = try iter.variable();
    const A = try iter.variable();

    try B.insertSlice(&ctx, &[_]Tuple{.{ 1, 2 }});

    while (try iter.changed()) {
        if (B.recent.len() > 0) {
            const rel = try zodd.Relation(Tuple).fromSlice(&ctx, B.recent.elements);
            try A.insert(rel);
        }
    }

    try testing.expectEqual(@as(usize, 1), A.totalLen());

    try B.insertSlice(&ctx, &[_]Tuple{.{ 2, 3 }});

    iter.reset();

    while (try iter.changed()) {
        if (B.recent.len() > 0) {
            const rel = try zodd.Relation(Tuple).fromSlice(&ctx, B.recent.elements);
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
    var ctx = zodd.ExecutionContext.init(allocator);
    const KV = struct { u32, u32 };
    const Out = struct { u32, u32, u32 };

    var iter = zodd.Iteration(KV).init(&ctx, 100);
    defer iter.deinit();

    const edges = try iter.variable();
    const labels = try iter.variable();
    var joined = zodd.Variable(Out).init(&ctx);
    defer joined.deinit();

    // Round 1: edges={1->2}, labels={1->100}
    try edges.insertSlice(&ctx, &[_]KV{.{ 1, 2 }});
    try labels.insertSlice(&ctx, &[_]KV{.{ 1, 100 }});

    while (try iter.changed()) {
        try zodd.joinInto(u32, u32, u32, Out, &ctx, edges, labels, &joined, struct {
            fn logic(key: *const u32, edge_val: *const u32, label_val: *const u32) Out {
                return .{ key.*, edge_val.*, label_val.* };
            }
        }.logic);
    }

    try testing.expectEqual(@as(usize, 1), joined.totalLen());

    // Round 2: add edge 2->3 and label 2->200
    try edges.insertSlice(&ctx, &[_]KV{.{ 2, 3 }});
    try labels.insertSlice(&ctx, &[_]KV{.{ 2, 200 }});
    iter.reset();

    while (try iter.changed()) {
        try zodd.joinInto(u32, u32, u32, Out, &ctx, edges, labels, &joined, struct {
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
    var ctx = zodd.ExecutionContext.init(allocator);
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    // Phase 1: edges 1->2, 2->3
    var edges = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(&ctx);
    defer reachable.deinit();

    try reachable.insertSlice(&ctx, edges.elements);

    var iters: usize = 0;
    while (try reachable.changed()) {
        var new = EdgeList{};
        defer new.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) try new.append(allocator, .{ r[0], e[1] });
            }
        }
        if (new.items.len > 0) {
            try reachable.insert(try zodd.Relation(Edge).fromSlice(&ctx, new.items));
        }
        iters += 1;
        if (iters > 10) break;
    }

    // 1->2, 1->3, 2->3 = 3 pairs
    try testing.expectEqual(@as(usize, 3), reachable.totalLen());

    // Phase 2: add edge 3->4
    var edges2 = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
    });
    defer edges2.deinit();

    try reachable.insertSlice(&ctx, &[_]Edge{.{ 3, 4 }});

    iters = 0;
    while (try reachable.changed()) {
        var new = EdgeList{};
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
            try reachable.insert(try zodd.Relation(Edge).fromSlice(&ctx, new.items));
        }
        iters += 1;
        if (iters > 10) break;
    }

    // 1->2,1->3,1->4, 2->3,2->4, 3->4 = 6 pairs
    try testing.expectEqual(@as(usize, 6), reachable.totalLen());
}

test "incremental maintenance: iteration reset with multiple variables" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var iter = zodd.Iteration(u32).init(&ctx, 50);
    defer iter.deinit();

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&ctx, &[_]u32{ 10, 20 });
    try v2.insertSlice(&ctx, &[_]u32{ 30, 40 });

    // Converge
    while (try iter.changed()) {}

    try testing.expectEqual(@as(usize, 2), v1.totalLen());
    try testing.expectEqual(@as(usize, 2), v2.totalLen());

    // Reset and add more data
    iter.reset();
    try v1.insertSlice(&ctx, &[_]u32{ 50, 60 });

    const changed = try iter.changed();
    try testing.expect(changed);

    // Run to completion
    while (try iter.changed()) {}

    try testing.expectEqual(@as(usize, 4), v1.totalLen());
    try testing.expectEqual(@as(usize, 2), v2.totalLen());
}

const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "transitive closure: linear chain" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    var edges = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(&ctx);
    defer reachable.deinit();

    try reachable.insertSlice(&ctx, edges.elements);

    var iters: usize = 0;
    while (try reachable.changed()) : (iters += 1) {
        var results = EdgeList{};
        defer results.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) {
                    try results.append(allocator, .{ r[0], e[1] });
                }
            }
        }

        if (results.items.len > 0) {
            try reachable.insert(try zodd.Relation(Edge).fromSlice(&ctx, results.items));
        }

        if (iters > 10) break;
    }

    var result = try reachable.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 6), result.len());
}

test "transitive closure: diamond graph" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    var edges = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
        .{ 1, 2 },
        .{ 1, 3 },
        .{ 2, 4 },
        .{ 3, 4 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(&ctx);
    defer reachable.deinit();

    try reachable.insertSlice(&ctx, edges.elements);

    var iters: usize = 0;
    while (try reachable.changed()) : (iters += 1) {
        var results = EdgeList{};
        defer results.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) {
                    try results.append(allocator, .{ r[0], e[1] });
                }
            }
        }

        if (results.items.len > 0) {
            try reachable.insert(try zodd.Relation(Edge).fromSlice(&ctx, results.items));
        }

        if (iters > 10) break;
    }

    var result = try reachable.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 5), result.len());
}

test "transitive closure: cycle detection" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    var edges = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 1 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(&ctx);
    defer reachable.deinit();

    try reachable.insertSlice(&ctx, edges.elements);

    var iters: usize = 0;
    while (try reachable.changed()) : (iters += 1) {
        var results = EdgeList{};
        defer results.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (edges.elements) |e| {
                if (e[0] == r[1]) {
                    try results.append(allocator, .{ r[0], e[1] });
                }
            }
        }

        if (results.items.len > 0) {
            try reachable.insert(try zodd.Relation(Edge).fromSlice(&ctx, results.items));
        }

        if (iters > 20) break;
    }

    var result = try reachable.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 9), result.len());
}

test "same generation: parent-child hierarchy" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Pair = struct { u32, u32 };
    const PairList = std.ArrayListUnmanaged(Pair);

    var parent_child = try zodd.Relation(Pair).fromSlice(&ctx, &[_]Pair{
        .{ 1, 2 },
        .{ 1, 3 },
        .{ 2, 4 },
        .{ 2, 5 },
    });
    defer parent_child.deinit();

    var same_gen = zodd.Variable(Pair).init(&ctx);
    defer same_gen.deinit();

    try same_gen.insertSlice(&ctx, &[_]Pair{ .{ 1, 1 }, .{ 2, 2 }, .{ 3, 3 }, .{ 4, 4 }, .{ 5, 5 } });

    var iters: usize = 0;
    while (try same_gen.changed()) : (iters += 1) {
        var results = PairList{};
        defer results.deinit(allocator);

        for (same_gen.recent.elements) |sg| {
            const p1 = sg[0];
            const p2 = sg[1];

            for (parent_child.elements) |pc1| {
                if (pc1[0] == p1) {
                    for (parent_child.elements) |pc2| {
                        if (pc2[0] == p2) {
                            try results.append(allocator, .{ pc1[1], pc2[1] });
                        }
                    }
                }
            }
        }

        if (results.items.len > 0) {
            try same_gen.insert(try zodd.Relation(Pair).fromSlice(&ctx, results.items));
        }

        if (iters > 10) break;
    }

    var result = try same_gen.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 9), result.len());
}

test "aggregate: group sum integration" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var rel = try zodd.Relation(Tuple).fromSlice(&ctx, &[_]Tuple{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 2, 5 },
    });
    defer rel.deinit();

    const key_func = struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    };
    const folder = struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            return acc + t[1];
        }
    };

    var result = try zodd.aggregate.aggregate(Tuple, u32, u32, &ctx, &rel, key_func.key, 0, folder.fold);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.len());
    try testing.expectEqual(@as(u32, 1), result.elements[0][0]);
    try testing.expectEqual(@as(u32, 30), result.elements[0][1]);
    try testing.expectEqual(@as(u32, 2), result.elements[1][0]);
    try testing.expectEqual(@as(u32, 5), result.elements[1][1]);
}

test "joinInto: incremental updates integration" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };
    const Out = struct { u32, u32, u32 };

    var v1 = zodd.Variable(Tuple).init(&ctx);
    defer v1.deinit();

    var v2 = zodd.Variable(Tuple).init(&ctx);
    defer v2.deinit();

    var out = zodd.Variable(Out).init(&ctx);
    defer out.deinit();

    try v1.insertSlice(&ctx, &[_]Tuple{.{ 1, 10 }});
    try v2.insertSlice(&ctx, &[_]Tuple{ .{ 1, 100 }, .{ 2, 200 } });

    _ = try v1.changed();
    _ = try v2.changed();

    try zodd.joinInto(u32, u32, u32, Out, &ctx, &v1, &v2, &out, struct {
        fn logic(key: *const u32, v1_val: *const u32, v2_val: *const u32) Out {
            return .{ key.*, v1_val.*, v2_val.* };
        }
    }.logic);

    _ = try out.changed();
    try testing.expectEqual(@as(usize, 1), out.recent.len());

    _ = try v1.changed();
    _ = try v2.changed();
    _ = try out.changed();

    try v2.insertSlice(&ctx, &[_]Tuple{.{ 1, 101 }});
    _ = try v2.changed();

    try zodd.joinInto(u32, u32, u32, Out, &ctx, &v1, &v2, &out, struct {
        fn logic(key: *const u32, v1_val: *const u32, v2_val: *const u32) Out {
            return .{ key.*, v1_val.*, v2_val.* };
        }
    }.logic);

    _ = try out.changed();
    try testing.expectEqual(@as(usize, 1), out.recent.len());
    try testing.expectEqual(@as(u32, 101), out.recent.elements[0][2]);
}

test "extendInto: extend and anti integration" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;
    const Out = struct { u32, u32 };

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{ .{1}, .{2} });
    _ = try source.changed();

    var allow = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 2, 30 },
    });
    defer allow.deinit();

    var block = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
    });
    defer block.deinit();

    var output = zodd.Variable(Out).init(&ctx);
    defer output.deinit();

    var ext_allow = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &allow, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var ext_block = zodd.ExtendAnti(Tuple, u32, Val).init(&ctx, &block, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]zodd.Leaper(Tuple, Val){ ext_allow.leaper(), ext_block.leaper() };

    try zodd.extendInto(Tuple, Val, Out, &ctx, &source, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) Out {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();
    try testing.expectEqual(@as(usize, 2), output.recent.len());
    try testing.expectEqual(Out{ 1, 20 }, output.recent.elements[0]);
    try testing.expectEqual(Out{ 2, 30 }, output.recent.elements[1]);
}

test "SecondaryIndex: getRange randomized integration" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    const Index = zodd.index.SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[0];
        }
    }.extract, struct {
        fn cmp(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.cmp, 4);

    var idx = Index.init(&ctx);
    defer idx.deinit();

    var all = std.ArrayListUnmanaged(Tuple){};
    defer all.deinit(allocator);

    var prng = std.Random.DefaultPrng.init(0x5a5a5a5a);
    const rand = prng.random();

    var i: usize = 0;
    while (i < 50) : (i += 1) {
        const k = rand.intRangeAtMost(u32, 0, 20);
        const v = rand.intRangeAtMost(u32, 0, 1000);
        const t = Tuple{ k, v };
        try idx.insert(t);
        try all.append(allocator, t);
    }

    var r: usize = 0;
    while (r < 10) : (r += 1) {
        const a = rand.intRangeAtMost(u32, 0, 20);
        const b = rand.intRangeAtMost(u32, 0, 20);
        const start = @min(a, b);
        const end = @max(a, b);

        var expected_list = std.ArrayListUnmanaged(Tuple){};
        defer expected_list.deinit(allocator);

        for (all.items) |t| {
            if (t[0] >= start and t[0] <= end) {
                try expected_list.append(allocator, t);
            }
        }

        var expected = try zodd.Relation(Tuple).fromSlice(&ctx, expected_list.items);
        defer expected.deinit();

        var got = try idx.getRange(start, end);
        defer got.deinit();

        try testing.expectEqualSlices(Tuple, expected.elements, got.elements);
    }
}

test "integration: FilterAnti in extendInto" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;
    const Out = struct { u32, u32 };

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();


    try source.insertSlice(&ctx, &[_]Tuple{ .{1}, .{2}, .{3} });
    _ = try source.changed();


    var rel = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
        .{ 2, 20 },
        .{ 3, 30 },
    });
    defer rel.deinit();




    var filter_rel = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 2, 999 },
    });
    defer filter_rel.deinit();

    var output = zodd.Variable(Out).init(&ctx);
    defer output.deinit();

    var ext = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var anti = zodd.FilterAnti(Tuple, u32, u32).init(&ctx, &filter_rel, struct {
        fn f(t: *const Tuple) struct { u32, u32 } {
            return .{ t[0], 999 };
        }
    }.f);








    var leapers = [_]zodd.Leaper(Tuple, Val){ ext.leaper(), anti.leaper() };

    try zodd.extendInto(Tuple, Val, Out, &ctx, &source, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) Out {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();


    try testing.expectEqual(@as(usize, 2), output.recent.len());
    try testing.expectEqual(Out{ 1, 10 }, output.recent.elements[0]);
    try testing.expectEqual(Out{ 3, 30 }, output.recent.elements[1]);
}

test "integration: multi-way intersection" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;
    const Out = struct { u32, u32 };

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{ .{1}, .{2}, .{3}, .{4} });
    _ = try source.changed();


    var r1 = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 100 }, .{ 2, 200 }, .{ 3, 300 }, .{ 4, 400 },
    });
    defer r1.deinit();


    var r2 = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 100 }, .{ 2, 200 }, .{ 4, 999 },
    });
    defer r2.deinit();


    var r3 = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 2, 200 }, .{ 3, 300 },
    });
    defer r3.deinit();

    var output = zodd.Variable(Out).init(&ctx);
    defer output.deinit();

    const KeyFunc = struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    };

    var ext1 = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &r1, KeyFunc.f);
    var ext2 = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &r2, KeyFunc.f);
    var ext3 = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &r3, KeyFunc.f);









    var leapers = [_]zodd.Leaper(Tuple, Val){ ext1.leaper(), ext2.leaper(), ext3.leaper() };

    try zodd.extendInto(Tuple, Val, Out, &ctx, &source, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) Out {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();








    try testing.expectEqual(@as(usize, 1), output.recent.len());
    try testing.expectEqual(Out{ 2, 200 }, output.recent.elements[0]);
}

test "integration: persistence round-trip" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var original = try zodd.Relation(Tuple).fromSlice(&ctx, &[_]Tuple{
        .{ 1, 10 }, .{ 2, 20 }, .{ 3, 30 },
    });
    defer original.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    try original.save(buffer.writer(allocator));

    var fbs = std.io.fixedBufferStream(buffer.items);
    var loaded = try zodd.Relation(Tuple).load(&ctx, fbs.reader());
    defer loaded.deinit();

    try testing.expectEqual(original.len(), loaded.len());
    try testing.expectEqualSlices(Tuple, original.elements, loaded.elements);
}

test "integration: empty-input transitive closure" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Edge = struct { u32, u32 };

    var edges = zodd.Relation(Edge).empty(&ctx);
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(&ctx);
    defer reachable.deinit();

    try reachable.insert(edges);







    try reachable.insertSlice(&ctx, &[_]Edge{});

    while (try reachable.changed()) {
        _ = struct {
            fn fail(_: *const Edge, _: *const u32, _: *const u32) Edge {
                unreachable;
            }
        }.fail(undefined, undefined, undefined);
    }

    var res = try reachable.complete();
    defer res.deinit();

    try testing.expectEqual(@as(usize, 0), res.len());
}

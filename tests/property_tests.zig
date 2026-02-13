const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");
const minish = @import("minish");
const gen = minish.gen;

test "property: relation always sorted after fromSlice" {
    try minish.check(
        testing.allocator,
        gen.list(u32, gen.intRange(u32, 0, 1000), 0, 50),
        struct {
            fn prop(data: []const u32) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel = try zodd.Relation(u32).fromSlice(&ctx, data);
                defer rel.deinit();

                if (rel.elements.len > 1) {
                    for (1..rel.elements.len) |i| {
                        try testing.expect(rel.elements[i - 1] <= rel.elements[i]);
                    }
                }
            }
        }.prop,
        .{ .num_runs = 100, .seed = 0xdeadbeef },
    );
}

test "property: relation always deduplicated after fromSlice" {
    try minish.check(
        testing.allocator,
        gen.list(u32, gen.intRange(u32, 0, 50), 0, 30),
        struct {
            fn prop(data: []const u32) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel = try zodd.Relation(u32).fromSlice(&ctx, data);
                defer rel.deinit();

                if (rel.elements.len > 1) {
                    for (1..rel.elements.len) |i| {
                        try testing.expect(rel.elements[i - 1] != rel.elements[i]);
                    }
                }
            }
        }.prop,
        .{ .num_runs = 100, .seed = 0xcafebabe },
    );
}

test "property: relation merge is commutative" {
    const TwoLists = struct { []const u32, []const u32 };
    const two_lists_gen = gen.tuple2(
        []const u32,
        []const u32,
        gen.list(u32, gen.intRange(u32, 0, 100), 0, 20),
        gen.list(u32, gen.intRange(u32, 0, 100), 0, 20),
    );

    try minish.check(
        testing.allocator,
        two_lists_gen,
        struct {
            fn prop(lists: TwoLists) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel1a = try zodd.Relation(u32).fromSlice(&ctx, lists[0]);
                var rel2a = try zodd.Relation(u32).fromSlice(&ctx, lists[1]);
                var merged_ab = try rel1a.merge(&rel2a);
                defer merged_ab.deinit();

                var rel1b = try zodd.Relation(u32).fromSlice(&ctx, lists[0]);
                var rel2b = try zodd.Relation(u32).fromSlice(&ctx, lists[1]);
                var merged_ba = try rel2b.merge(&rel1b);
                defer merged_ba.deinit();

                try testing.expectEqualSlices(u32, merged_ab.elements, merged_ba.elements);
            }
        }.prop,
        .{ .num_runs = 50, .seed = 0xfeedface },
    );
}

test "property: variable deduplicates across rounds" {
    try minish.check(
        testing.allocator,
        gen.list(u32, gen.intRange(u32, 0, 50), 1, 30),
        struct {
            fn prop(data: []const u32) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var v = zodd.Variable(u32).init(&ctx);
                defer v.deinit();

                try v.insertSlice(&ctx, data);
                while (try v.changed()) {}

                var result = try v.complete();
                defer result.deinit();

                if (result.elements.len > 1) {
                    for (1..result.elements.len) |i| {
                        try testing.expect(result.elements[i - 1] < result.elements[i]);
                    }
                }
            }
        }.prop,
        .{ .num_runs = 50, .seed = 0xbaadf00d },
    );
}

test "property: variable totalLen matches complete().len" {
    try minish.check(
        testing.allocator,
        gen.list(u32, gen.intRange(u32, 0, 100), 1, 30),
        struct {
            fn prop(data: []const u32) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var v = zodd.Variable(u32).init(&ctx);

                try v.insertSlice(&ctx, data);
                while (try v.changed()) {}

                const total_before = v.totalLen();
                var result = try v.complete();
                defer result.deinit();
                defer v.deinit();

                try testing.expectEqual(total_before, result.len());
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0x12345678 },
    );
}

test "property: transitive closure reaches expected nodes" {
    const Edge = struct { u32, u32 };
    const edges_gen = gen.list(
        Edge,
        gen.tuple2(u32, u32, gen.intRange(u32, 0, 5), gen.intRange(u32, 0, 5)),
        1,
        10,
    );

    try minish.check(
        testing.allocator,
        edges_gen,
        struct {
            fn prop(edges: []const Edge) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var edges_rel = try zodd.Relation(Edge).fromSlice(&ctx, edges);
                defer edges_rel.deinit();

                var reachable = zodd.Variable(Edge).init(&ctx);
                defer reachable.deinit();

                try reachable.insertSlice(&ctx, edges_rel.elements);

                var iters: usize = 0;
                const EdgeList = std.ArrayListUnmanaged(Edge);
                while (try reachable.changed()) : (iters += 1) {
                    var results = EdgeList{};
                    defer results.deinit(testing.allocator);

                    for (reachable.recent.elements) |r| {
                        for (edges_rel.elements) |e| {
                            if (e[0] == r[1]) {
                                try results.append(testing.allocator, .{ r[0], e[1] });
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

                try testing.expect(result.len() >= edges_rel.len());
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xabcdef01 },
    );
}

test "property: relation merge is idempotent" {
    try minish.check(
        testing.allocator,
        gen.list(u32, gen.intRange(u32, 0, 100), 0, 30),
        struct {
            fn prop(data: []const u32) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel1 = try zodd.Relation(u32).fromSlice(&ctx, data);
                defer rel1.deinit();

                var rel2 = try zodd.Relation(u32).fromSlice(&ctx, data);
                var merged = try rel2.merge(&rel1);
                defer merged.deinit();

                var expected = try zodd.Relation(u32).fromSlice(&ctx, data);
                defer expected.deinit();

                try testing.expectEqualSlices(u32, expected.elements, merged.elements);
            }
        }.prop,
        .{ .num_runs = 50, .seed = 0x11223344 },
    );
}

test "property: gallop returns suffix at target" {
    const Pair = struct { []const u32, u32 };
    const gen_pair = gen.tuple2(
        []const u32,
        u32,
        gen.list(u32, gen.intRange(u32, 0, 200), 0, 40),
        gen.intRange(u32, 0, 200),
    );

    try minish.check(
        testing.allocator,
        gen_pair,
        struct {
            fn prop(input: Pair) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel = try zodd.Relation(u32).fromSlice(&ctx, input[0]);
                defer rel.deinit();

                const target = input[1];
                const slice = zodd.gallop(u32, rel.elements, target);

                if (rel.elements.len == 0) {
                    try testing.expectEqual(@as(usize, 0), slice.len);
                    return;
                }

                if (slice.len > 0) {
                    try testing.expect(slice[0] >= target);
                }

                for (slice) |v| {
                    try testing.expect(v >= target);
                }
            }
        }.prop,
        .{ .num_runs = 50, .seed = 0x55667788 },
    );
}

test "property: relation merge is associative" {
    const ThreeLists = struct { []const u32, []const u32, []const u32 };
    const lists_gen = gen.tuple3(
        []const u32,
        []const u32,
        []const u32,
        gen.list(u32, gen.intRange(u32, 0, 50), 0, 15),
        gen.list(u32, gen.intRange(u32, 0, 50), 0, 15),
        gen.list(u32, gen.intRange(u32, 0, 50), 0, 15),
    );

    try minish.check(
        testing.allocator,
        lists_gen,
        struct {
            fn prop(lists: ThreeLists) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var a1 = try zodd.Relation(u32).fromSlice(&ctx, lists[0]);
                var b1 = try zodd.Relation(u32).fromSlice(&ctx, lists[1]);
                var c1 = try zodd.Relation(u32).fromSlice(&ctx, lists[2]);

                var ab = try a1.merge(&b1);
                var ab_c = try ab.merge(&c1);
                defer ab_c.deinit();

                var a2 = try zodd.Relation(u32).fromSlice(&ctx, lists[0]);
                var b2 = try zodd.Relation(u32).fromSlice(&ctx, lists[1]);
                var c2 = try zodd.Relation(u32).fromSlice(&ctx, lists[2]);

                var bc = try b2.merge(&c2);
                var a_bc = try a2.merge(&bc);
                defer a_bc.deinit();

                try testing.expectEqualSlices(u32, ab_c.elements, a_bc.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0x8899aabb },
    );
}

test "property: joinHelper matches naive join" {
    const Tuple = struct { u32, u32 };
    const Pair = struct { []const Tuple, []const Tuple };
    const pair_gen = gen.tuple2(
        []const Tuple,
        []const Tuple,
        gen.list(Tuple, gen.tuple2(u32, u32, gen.intRange(u32, 0, 20), gen.intRange(u32, 0, 20)), 0, 10),
        gen.list(Tuple, gen.tuple2(u32, u32, gen.intRange(u32, 0, 20), gen.intRange(u32, 0, 20)), 0, 10),
    );

    try minish.check(
        testing.allocator,
        pair_gen,
        struct {
            fn prop(p: Pair) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel1 = try zodd.Relation(Tuple).fromSlice(&ctx, p[0]);
                defer rel1.deinit();

                var rel2 = try zodd.Relation(Tuple).fromSlice(&ctx, p[1]);
                defer rel2.deinit();

                const Result = struct { u32, u32, u32 };
                var expected_list = std.ArrayListUnmanaged(Result){};
                defer expected_list.deinit(testing.allocator);

                for (rel1.elements) |t1| {
                    for (rel2.elements) |t2| {
                        if (t1[0] == t2[0]) {
                            try expected_list.append(testing.allocator, .{ t1[0], t1[1], t2[1] });
                        }
                    }
                }

                var expected = try zodd.Relation(Result).fromSlice(&ctx, expected_list.items);
                defer expected.deinit();

                const ResultList = std.ArrayListUnmanaged(Result);
                const Context = struct {
                    results: *ResultList,
                    alloc: std.mem.Allocator,

                    fn callback(self: @This(), key: *const u32, v1: *const u32, v2: *const u32) void {
                        self.results.append(self.alloc, .{ key.*, v1.*, v2.* }) catch {};
                    }
                };

                var got_list = ResultList{};
                defer got_list.deinit(testing.allocator);

                zodd.joinHelper(u32, u32, u32, &rel1, &rel2, Context{ .results = &got_list, .alloc = testing.allocator }, Context.callback);

                var got = try zodd.Relation(Result).fromSlice(&ctx, got_list.items);
                defer got.deinit();

                try testing.expectEqualSlices(Result, expected.elements, got.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0x99aabbcc },
    );
}

test "property: joinAnti matches naive filter" {
    const Tuple = struct { u32, u32 };
    const Pair = struct { []const Tuple, []const Tuple };
    const pair_gen = gen.tuple2(
        []const Tuple,
        []const Tuple,
        gen.list(Tuple, gen.tuple2(u32, u32, gen.intRange(u32, 0, 20), gen.intRange(u32, 0, 20)), 0, 12),
        gen.list(Tuple, gen.tuple2(u32, u32, gen.intRange(u32, 0, 20), gen.intRange(u32, 0, 20)), 0, 12),
    );

    try minish.check(
        testing.allocator,
        pair_gen,
        struct {
            fn prop(p: Pair) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var input = zodd.Variable(Tuple).init(&ctx);
                defer input.deinit();

                var filter = zodd.Variable(Tuple).init(&ctx);
                defer filter.deinit();

                var output = zodd.Variable(Tuple).init(&ctx);
                defer output.deinit();

                try input.insertSlice(&ctx, p[0]);
                try filter.insertSlice(&ctx, p[1]);

                _ = try input.changed();
                _ = try filter.changed();

                try zodd.joinAnti(u32, u32, u32, Tuple, &ctx, &input, &filter, &output, struct {
                    fn logic(key: *const u32, val: *const u32) Tuple {
                        return .{ key.*, val.* };
                    }
                }.logic);

                _ = try output.changed();

                var expected_list = std.ArrayListUnmanaged(Tuple){};
                defer expected_list.deinit(testing.allocator);

                for (input.recent.elements) |t| {
                    var found = false;
                    for (filter.recent.elements) |f| {
                        if (f[0] == t[0]) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        try expected_list.append(testing.allocator, t);
                    }
                }

                var expected = try zodd.Relation(Tuple).fromSlice(&ctx, expected_list.items);
                defer expected.deinit();

                var got = try zodd.Relation(Tuple).fromSlice(&ctx, output.recent.elements);
                defer got.deinit();

                try testing.expectEqualSlices(Tuple, expected.elements, got.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xa1b2c3d4 },
    );
}

test "property: extendInto matches naive extend" {
    const Tuple = u32;
    const KV = struct { u32, u32 };
    const Pair = struct { []const u32, []const KV };
    const pair_gen = gen.tuple2(
        []const u32,
        []const KV,
        gen.list(u32, gen.intRange(u32, 0, 10), 0, 10),
        gen.list(KV, gen.tuple2(u32, u32, gen.intRange(u32, 0, 10), gen.intRange(u32, 0, 10)), 0, 15),
    );

    try minish.check(
        testing.allocator,
        pair_gen,
        struct {
            fn prop(p: Pair) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var source = zodd.Variable(Tuple).init(&ctx);
                defer source.deinit();

                var rel = try zodd.Relation(KV).fromSlice(&ctx, p[1]);
                defer rel.deinit();

                var output = zodd.Variable(KV).init(&ctx);
                defer output.deinit();

                try source.insertSlice(&ctx, p[0]);
                _ = try source.changed();

                var ext = zodd.ExtendWith(Tuple, u32, u32).init(&ctx, &rel, struct {
                    fn f(t: *const Tuple) u32 {
                        return t.*;
                    }
                }.f);

                var leapers = [_]zodd.Leaper(Tuple, u32){ext.leaper()};

                try zodd.extendInto(Tuple, u32, KV, &ctx, &source, &leapers, &output, struct {
                    fn logic(t: *const Tuple, v: *const u32) KV {
                        return .{ t.*, v.* };
                    }
                }.logic);

                _ = try output.changed();

                var expected_list = std.ArrayListUnmanaged(KV){};
                defer expected_list.deinit(testing.allocator);

                for (source.recent.elements) |t| {
                    for (rel.elements) |kv| {
                        if (kv[0] == t) {
                            try expected_list.append(testing.allocator, kv);
                        }
                    }
                }

                var expected = try zodd.Relation(KV).fromSlice(&ctx, expected_list.items);
                defer expected.deinit();

                var got = try zodd.Relation(KV).fromSlice(&ctx, output.recent.elements);
                defer got.deinit();

                try testing.expectEqualSlices(KV, expected.elements, got.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xb2c3d4e5 },
    );
}

test "property: SecondaryIndex get matches naive filter" {
    const Tuple = struct { u32, u32 };
    const List = []const Tuple;

    const list_gen = gen.list(
        Tuple,
        gen.tuple2(u32, u32, gen.intRange(u32, 0, 10), gen.intRange(u32, 0, 50)),
        0,
        20,
    );

    try minish.check(
        testing.allocator,
        list_gen,
        struct {
            fn prop(data: List) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
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

                for (data) |t| {
                    try idx.insert(t);
                }

                var rel = try zodd.Relation(Tuple).fromSlice(&ctx, data);
                defer rel.deinit();

                var i: usize = 0;
                while (i < rel.elements.len) : (i += 1) {
                    const key = rel.elements[i][0];

                    var expected_list = std.ArrayListUnmanaged(Tuple){};
                    defer expected_list.deinit(testing.allocator);

                    for (data) |t| {
                        if (t[0] == key) {
                            try expected_list.append(testing.allocator, t);
                        }
                    }

                    var expected = try zodd.Relation(Tuple).fromSlice(&ctx, expected_list.items);
                    defer expected.deinit();

                    const got_ptr = idx.get(key).?;
                    try testing.expectEqualSlices(Tuple, expected.elements, got_ptr.elements);
                }
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xc1d2e3f4 },
    );
}

test "property: aggregate matches naive sum" {
    const Tuple = struct { u32, u32 };
    const List = []const Tuple;

    const list_gen = gen.list(
        Tuple,
        gen.tuple2(u32, u32, gen.intRange(u32, 0, 10), gen.intRange(u32, 0, 50)),
        0,
        20,
    );

    try minish.check(
        testing.allocator,
        list_gen,
        struct {
            fn prop(data: List) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel = try zodd.Relation(Tuple).fromSlice(&ctx, data);
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

                var map = std.AutoHashMap(u32, u32).init(testing.allocator);
                defer map.deinit();

                for (rel.elements) |t| {
                    const entry = try map.getOrPut(t[0]);
                    if (!entry.found_existing) {
                        entry.value_ptr.* = 0;
                    }
                    entry.value_ptr.* += t[1];
                }

                var expected_list = std.ArrayListUnmanaged(struct { u32, u32 }){};
                defer expected_list.deinit(testing.allocator);

                var it = map.iterator();
                while (it.next()) |entry| {
                    try expected_list.append(testing.allocator, .{ entry.key_ptr.*, entry.value_ptr.* });
                }

                var expected = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, expected_list.items);
                defer expected.deinit();

                try testing.expectEqualSlices(struct { u32, u32 }, expected.elements, result.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xd4e5f6a7 },
    );
}

test "property: persistence round-trip" {
    const Tuple = struct { u32, u32 };
    const List = []const Tuple;

    const list_gen = gen.list(
        Tuple,
        gen.tuple2(u32, u32, gen.intRange(u32, 0, 100), gen.intRange(u32, 0, 100)),
        0,
        50,
    );

    try minish.check(
        testing.allocator,
        list_gen,
        struct {
            fn prop(data: List) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);

                var original = try zodd.Relation(Tuple).fromSlice(&ctx, data);
                defer original.deinit();

                var buffer = std.ArrayListUnmanaged(u8){};
                defer buffer.deinit(testing.allocator);

                try original.save(buffer.writer(testing.allocator));

                var fbs = std.io.fixedBufferStream(buffer.items);
                var loaded = try zodd.Relation(Tuple).load(&ctx, fbs.reader());
                defer loaded.deinit();

                try testing.expectEqual(original.len(), loaded.len());
                try testing.expectEqualSlices(Tuple, original.elements, loaded.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xf00dcafe },
    );
}

test "property: aggregate count matches naive count" {
    const Tuple = struct { u32, u32 };
    const List = []const Tuple;

    const list_gen = gen.list(
        Tuple,
        gen.tuple2(u32, u32, gen.intRange(u32, 0, 10), gen.intRange(u32, 0, 50)),
        0,
        50,
    );

    try minish.check(
        testing.allocator,
        list_gen,
        struct {
            fn prop(data: List) !void {
                var ctx = zodd.ExecutionContext.init(testing.allocator);
                var rel = try zodd.Relation(Tuple).fromSlice(&ctx, data);
                defer rel.deinit();

                const key_func = struct {
                    fn f(t: *const Tuple) u32 {
                        return t[0];
                    }
                }.f;

                var result = try zodd.aggregateFn(
                    Tuple,
                    u32,
                    u32,
                    &ctx,
                    &rel,
                    key_func,
                    0,
                    struct {
                        fn count(acc: u32, _: *const Tuple) u32 {
                            return acc + 1;
                        }
                    }.count,
                );
                defer result.deinit();

                var map = std.AutoHashMap(u32, u32).init(testing.allocator);
                defer map.deinit();

                for (rel.elements) |t| {
                    const g = try map.getOrPut(t[0]);
                    if (!g.found_existing) g.value_ptr.* = 0;
                    g.value_ptr.* += 1;
                }

                var expected_list = std.ArrayListUnmanaged(struct { u32, u32 }){};
                defer expected_list.deinit(testing.allocator);

                var it = map.iterator();
                while (it.next()) |entry| {
                    try expected_list.append(testing.allocator, .{ entry.key_ptr.*, entry.value_ptr.* });
                }

                const sort = struct {
                    fn lessThan(_: void, a: struct { u32, u32 }, b: struct { u32, u32 }) bool {
                        return a[0] < b[0];
                    }
                };
                std.sort.block(struct { u32, u32 }, expected_list.items, {}, sort.lessThan);

                var expected = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, expected_list.items);
                defer expected.deinit();

                try testing.expectEqualSlices(struct { u32, u32 }, expected.elements, result.elements);
            }
        }.prop,
        .{ .num_runs = 30, .seed = 0xbeefbabe },
    );
}

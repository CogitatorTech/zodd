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
                var rel = try zodd.Relation(u32).fromSlice(testing.allocator, data);
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
                var rel = try zodd.Relation(u32).fromSlice(testing.allocator, data);
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
                var rel1a = try zodd.Relation(u32).fromSlice(testing.allocator, lists[0]);
                var rel2a = try zodd.Relation(u32).fromSlice(testing.allocator, lists[1]);
                var merged_ab = try rel1a.merge(&rel2a);
                defer merged_ab.deinit();

                var rel1b = try zodd.Relation(u32).fromSlice(testing.allocator, lists[0]);
                var rel2b = try zodd.Relation(u32).fromSlice(testing.allocator, lists[1]);
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
                var v = zodd.Variable(u32).init(testing.allocator);
                defer v.deinit();

                try v.insertSlice(data);
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
                var v = zodd.Variable(u32).init(testing.allocator);

                try v.insertSlice(data);
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
                var edges_rel = try zodd.Relation(Edge).fromSlice(testing.allocator, edges);
                defer edges_rel.deinit();

                var reachable = zodd.Variable(Edge).init(testing.allocator);
                defer reachable.deinit();

                try reachable.insertSlice(edges_rel.elements);

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
                        try reachable.insert(try zodd.Relation(Edge).fromSlice(testing.allocator, results.items));
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

const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "transitive closure: linear chain" {
    const allocator = testing.allocator;
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    var edges = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();

    try reachable.insertSlice(edges.elements);

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
            try reachable.insert(try zodd.Relation(Edge).fromSlice(allocator, results.items));
        }

        if (iters > 10) break;
    }

    var result = try reachable.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 6), result.len());
}

test "transitive closure: diamond graph" {
    const allocator = testing.allocator;
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    var edges = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
        .{ 1, 2 },
        .{ 1, 3 },
        .{ 2, 4 },
        .{ 3, 4 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();

    try reachable.insertSlice(edges.elements);

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
            try reachable.insert(try zodd.Relation(Edge).fromSlice(allocator, results.items));
        }

        if (iters > 10) break;
    }

    var result = try reachable.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 5), result.len());
}

test "transitive closure: cycle detection" {
    const allocator = testing.allocator;
    const Edge = struct { u32, u32 };
    const EdgeList = std.ArrayListUnmanaged(Edge);

    var edges = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 1 },
    });
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();

    try reachable.insertSlice(edges.elements);

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
            try reachable.insert(try zodd.Relation(Edge).fromSlice(allocator, results.items));
        }

        if (iters > 20) break;
    }

    var result = try reachable.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 9), result.len());
}

test "same generation: parent-child hierarchy" {
    const allocator = testing.allocator;
    const Pair = struct { u32, u32 };
    const PairList = std.ArrayListUnmanaged(Pair);

    var parent_child = try zodd.Relation(Pair).fromSlice(allocator, &[_]Pair{
        .{ 1, 2 },
        .{ 1, 3 },
        .{ 2, 4 },
        .{ 2, 5 },
    });
    defer parent_child.deinit();

    var same_gen = zodd.Variable(Pair).init(allocator);
    defer same_gen.deinit();

    try same_gen.insertSlice(&[_]Pair{ .{ 1, 1 }, .{ 2, 2 }, .{ 3, 3 }, .{ 4, 4 }, .{ 5, 5 } });

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
            try same_gen.insert(try zodd.Relation(Pair).fromSlice(allocator, results.items));
        }

        if (iters > 10) break;
    }

    var result = try same_gen.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 9), result.len());
}

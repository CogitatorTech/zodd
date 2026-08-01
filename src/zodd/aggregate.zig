//! # Aggregation
//!
//! The module provides primitives for grouping and aggregating tuples.
//!
//! It supports standard operations like sum, count, min, max via a generic
//! folder interface. The algorithm sorts tuples by the grouping key, then
//! folds values per group.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;

/// Aggregate tuples by key using a folder.
pub fn aggregate(
    comptime Tuple: type,
    comptime Key: type,
    comptime AggVal: type,
    allocator: Allocator,
    input: *const Relation(Tuple),
    key_func: fn (*const Tuple) Key,
    init_val: AggVal,
    folder: fn (AggVal, *const Tuple) AggVal,
) Allocator.Error!Relation(struct { Key, AggVal }) {
    const ResultTuple = struct { Key, AggVal };

    if (input.len() == 0) {
        return Relation(ResultTuple).empty(allocator);
    }

    const Intermediate = struct { Key, *const Tuple };
    var intermediates = try allocator.alloc(Intermediate, input.len());
    defer allocator.free(intermediates);

    for (input.elements, 0..) |*t, i| {
        intermediates[i] = .{ key_func(t), t };
    }

    const sortContext = struct {
        pub fn lessThan(_: void, a: Intermediate, b: Intermediate) bool {
            return switch (std.math.order(a[0], b[0])) {
                .lt => true,
                .gt => false,
                // Tuples within a group keep their input order (the pointers
                // index the sorted input), so order-sensitive folders are
                // deterministic despite the unstable sort.
                .eq => @intFromPtr(a[1]) < @intFromPtr(b[1]),
            };
        }
    };
    std.sort.pdq(Intermediate, intermediates, {}, sortContext.lessThan);

    var results = std.ArrayListUnmanaged(ResultTuple).empty;
    defer results.deinit(allocator);

    if (intermediates.len > 0) {
        var current_key = intermediates[0][0];
        var current_acc = init_val;

        for (intermediates) |item| {
            if (std.math.order(item[0], current_key) != .eq) {
                try results.append(allocator, .{ current_key, current_acc });
                current_key = item[0];
                current_acc = init_val;
            }
            current_acc = folder(current_acc, item[1]);
        }
        try results.append(allocator, .{ current_key, current_acc });
    }

    return Relation(ResultTuple).fromSlice(allocator, results.items);
}

test "aggregate: sum by key" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var data = try Relation(Tuple).fromSlice(allocator, &[_]Tuple{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 2, 5 },
        .{ 2, 6 },
        .{ 3, 100 },
    });
    defer data.deinit();

    const sum_folder = struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            return acc + t[1];
        }
    };
    const key_func = struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    };

    var result = try aggregate(Tuple, u32, u32, allocator, &data, key_func.key, 0, sum_folder.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.len());
    const res = result.elements;

    try std.testing.expectEqual(res[0].@"0", 1);
    try std.testing.expectEqual(res[0].@"1", 30);

    try std.testing.expectEqual(res[1].@"0", 2);
    try std.testing.expectEqual(res[1].@"1", 11);

    try std.testing.expectEqual(res[2].@"0", 3);
    try std.testing.expectEqual(res[2].@"1", 100);
}

test "aggregate: count" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var data = try Relation(Tuple).fromSlice(allocator, &[_]Tuple{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 2, 5 },
    });
    defer data.deinit();

    const count_folder = struct {
        fn fold(acc: usize, _: *const Tuple) usize {
            return acc + 1;
        }
    };
    const key_func = struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    };

    var result = try aggregate(Tuple, u32, usize, allocator, &data, key_func.key, 0, count_folder.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.len());
    try std.testing.expectEqual(result.elements[0].@"1", 2);
    try std.testing.expectEqual(result.elements[1].@"1", 1);
}

test "aggregate: min per key" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var data = try Relation(Tuple).fromSlice(allocator, &[_]Tuple{
        .{ 1, 30 }, .{ 1, 10 },  .{ 1, 20 },
        .{ 2, 5 },  .{ 2, 500 },
    });
    defer data.deinit();

    var result = try aggregate(Tuple, u32, u32, allocator, &data, struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    }.key, std.math.maxInt(u32), struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            return @min(acc, t[1]);
        }
    }.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.len());
    try std.testing.expectEqual(@as(u32, 10), result.elements[0].@"1");
    try std.testing.expectEqual(@as(u32, 5), result.elements[1].@"1");
}

test "aggregate: max per key" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var data = try Relation(Tuple).fromSlice(allocator, &[_]Tuple{
        .{ 1, 30 }, .{ 1, 10 },  .{ 1, 20 },
        .{ 2, 5 },  .{ 2, 500 },
    });
    defer data.deinit();

    var result = try aggregate(Tuple, u32, u32, allocator, &data, struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    }.key, 0, struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            return @max(acc, t[1]);
        }
    }.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.len());
    try std.testing.expectEqual(@as(u32, 30), result.elements[0].@"1");
    try std.testing.expectEqual(@as(u32, 500), result.elements[1].@"1");
}

test "aggregate: fold order within a group follows the input order" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    // Group on the second column so groups interleave in the sorted input
    // and the group sort has real work to do; an order-sensitive folder
    // then exposes any within-group reordering. For group key j, the first
    // tuple in input order is (j, j).
    var tuples: [1000]Tuple = undefined;
    for (&tuples, 0..) |*t, i| {
        t.* = .{ @intCast(i), @intCast(i % 10) };
    }
    var data = try Relation(Tuple).fromSlice(allocator, &tuples);
    defer data.deinit();

    const sentinel = std.math.maxInt(u32);
    var result = try aggregate(Tuple, u32, u32, allocator, &data, struct {
        fn key(t: *const Tuple) u32 {
            return t[1];
        }
    }.key, sentinel, struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            // First value per group: order-sensitive on purpose.
            return if (acc == sentinel) t[0] else acc;
        }
    }.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 10), result.len());
    for (result.elements, 0..) |row, j| {
        try std.testing.expectEqual(@as(u32, @intCast(j)), row.@"1");
    }
}

test "aggregate: empty input produces empty relation" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var empty_rel = Relation(Tuple).empty(allocator);
    defer empty_rel.deinit();

    var result = try aggregate(Tuple, u32, u32, allocator, &empty_rel, struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    }.key, 0, struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            return acc + t[1];
        }
    }.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.len());
}

test "aggregate: single group produces one row" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var data = try Relation(Tuple).fromSlice(allocator, &[_]Tuple{
        .{ 7, 1 }, .{ 7, 2 }, .{ 7, 3 },
    });
    defer data.deinit();

    var result = try aggregate(Tuple, u32, u32, allocator, &data, struct {
        fn key(t: *const Tuple) u32 {
            return t[0];
        }
    }.key, 0, struct {
        fn fold(acc: u32, t: *const Tuple) u32 {
            return acc + t[1];
        }
    }.fold);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.len());
    try std.testing.expectEqual(@as(u32, 7), result.elements[0].@"0");
    try std.testing.expectEqual(@as(u32, 6), result.elements[0].@"1");
}

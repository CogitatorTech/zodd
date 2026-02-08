//! Group-by and aggregation primitives for relations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;

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
            return std.math.order(a[0], b[0]) == .lt;
        }
    };
    std.sort.pdq(Intermediate, intermediates, {}, sortContext.lessThan);

    var results = std.ArrayListUnmanaged(ResultTuple){};
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

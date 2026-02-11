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

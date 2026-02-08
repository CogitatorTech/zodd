const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "regression: totalLen includes to_add batches" {
    const allocator = testing.allocator;

    var v = zodd.Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3 });

    try testing.expectEqual(@as(usize, 3), v.totalLen());

    _ = try v.changed();

    try testing.expectEqual(@as(usize, 3), v.totalLen());

    try v.insertSlice(&[_]u32{ 4, 5 });

    try testing.expectEqual(@as(usize, 5), v.totalLen());
}

test "regression: Iteration cleanup handles variables" {
    const allocator = testing.allocator;

    var iter = zodd.Iteration(u32).init(allocator, null);

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&[_]u32{ 1, 2, 3 });
    try v2.insertSlice(&[_]u32{ 4, 5 });

    _ = try iter.changed();

    iter.deinit();
}

test "regression: intersection correctness with sorted values" {
    const allocator = testing.allocator;
    const KV = struct { u32, u32 };

    var rel = try zodd.Relation(KV).fromSlice(allocator, &[_]KV{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 1, 30 },
        .{ 2, 100 },
        .{ 2, 200 },
    });
    defer rel.deinit();

    var ext = zodd.ExtendWith(u32, u32, u32).init(allocator, &rel, struct {
        fn f(t: *const u32) u32 {
            return t.*;
        }
    }.f);

    const tuple1: u32 = 1;
    const cnt1 = ext.leaper().count(&tuple1);
    try testing.expectEqual(@as(usize, 3), cnt1);

    const tuple2: u32 = 2;
    const cnt2 = ext.leaper().count(&tuple2);
    try testing.expectEqual(@as(usize, 2), cnt2);

    const tuple3: u32 = 99;
    const cnt3 = ext.leaper().count(&tuple3);
    try testing.expectEqual(@as(usize, 0), cnt3);
}

test "regression: variable deduplication across multiple rounds" {
    const allocator = testing.allocator;

    var v = zodd.Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3 });
    _ = try v.changed();

    try v.insertSlice(&[_]u32{ 2, 3, 4, 5 });
    const changed1 = try v.changed();
    try testing.expect(changed1);

    try testing.expectEqual(@as(usize, 2), v.recent.len());

    try v.insertSlice(&[_]u32{ 1, 2, 3, 4, 5 });
    const changed2 = try v.changed();

    try testing.expect(!changed2);

    _ = try v.changed();

    var result = try v.complete();
    defer result.deinit();
    try testing.expectEqual(@as(usize, 5), result.len());
}

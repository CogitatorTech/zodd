const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

// =============================================================================
// Regression tests for fixed issues
// =============================================================================

// Regression test for issue #1: totalLen should include to_add batches.
// Previously, totalLen() did not count pending items in to_add.
test "regression: totalLen includes to_add batches" {
    const allocator = testing.allocator;

    var v = zodd.Variable(u32).init(allocator);
    defer v.deinit();

    // Insert some items but don't call changed() yet
    try v.insertSlice(&[_]u32{ 1, 2, 3 });

    // totalLen should include the pending to_add items
    try testing.expectEqual(@as(usize, 3), v.totalLen());

    // Now call changed() to move items to recent
    _ = try v.changed();

    // totalLen should still be 3 (now in recent)
    try testing.expectEqual(@as(usize, 3), v.totalLen());

    // Add more items
    try v.insertSlice(&[_]u32{ 4, 5 });

    // totalLen should include both recent (via stable after changed) and to_add
    // After changed(): recent moves to stable, so totalLen = stable + recent + to_add
    try testing.expectEqual(@as(usize, 5), v.totalLen());
}

// Regression test for issue #2: Iteration.deinit should cleanup variables.
// Previously, users had to manually call deinit() and destroy() on variables.
test "regression: Iteration cleanup handles variables" {
    const allocator = testing.allocator;

    var iter = zodd.Iteration(u32).init(allocator);

    // Create variables - they should be cleaned up by iter.deinit()
    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&[_]u32{ 1, 2, 3 });
    try v2.insertSlice(&[_]u32{ 4, 5 });

    _ = try iter.changed();

    // This should cleanup all variables without leaking memory
    iter.deinit();

    // If we get here without a leak detected by the test allocator, the test passes
}

// Regression test for issue #3: Binary search in intersection.
// Tests that intersection works correctly with sorted data (validates the
// O(n log m) binary search implementation produces correct results).
test "regression: intersection correctness with sorted values" {
    const allocator = testing.allocator;
    const KV = struct { u32, u32 };

    // Create a relation with multiple values per key
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

    // Test count for key 1
    const tuple1: u32 = 1;
    const cnt1 = ext.leaper().count(&tuple1);
    try testing.expectEqual(@as(usize, 3), cnt1);

    // Test count for key 2
    const tuple2: u32 = 2;
    const cnt2 = ext.leaper().count(&tuple2);
    try testing.expectEqual(@as(usize, 2), cnt2);

    // Test count for non-existent key
    const tuple3: u32 = 99;
    const cnt3 = ext.leaper().count(&tuple3);
    try testing.expectEqual(@as(usize, 0), cnt3);
}

// Regression test for Variable deduplication.
// Ensures that duplicate values are correctly filtered across multiple rounds.
test "regression: variable deduplication across multiple rounds" {
    const allocator = testing.allocator;

    var v = zodd.Variable(u32).init(allocator);
    defer v.deinit();

    // Round 1: Insert initial values
    try v.insertSlice(&[_]u32{ 1, 2, 3 });
    _ = try v.changed();

    // Round 2: Insert overlapping values
    try v.insertSlice(&[_]u32{ 2, 3, 4, 5 });
    const changed1 = try v.changed();
    try testing.expect(changed1);
    // Only 4 and 5 should be new
    try testing.expectEqual(@as(usize, 2), v.recent.len());

    // Round 3: Insert same values again
    try v.insertSlice(&[_]u32{ 1, 2, 3, 4, 5 });
    const changed2 = try v.changed();
    // Nothing new should be added
    try testing.expect(!changed2);

    // Drain to stable
    _ = try v.changed();

    // Complete should have exactly 5 unique values
    var result = try v.complete();
    defer result.deinit();
    try testing.expectEqual(@as(usize, 5), result.len());
}

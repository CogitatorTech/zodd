//! # Runtime Join Primitives
//!
//! The module implements the relational operations the frontend evaluator
//! needs over `DynTuple` slices with runtime column counts: a prefix
//! merge-join, an anti-join filter, and a group-by aggregation.
//!
//! They mirror the comptime-generic algorithms in `join.zig` and
//! `aggregate.zig`, but key positions and widths are runtime values, which
//! the comptime APIs cannot express. All inputs must be sorted (the natural
//! `Relation(DynTuple)` order, which sorts any column prefix first).

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const dyntuple = @import("dyntuple.zig");
const interner = @import("interner.zig");
const DynTuple = dyntuple.DynTuple;

/// Selects one output column of a join: a column of the left or right tuple.
pub const OutCol = struct {
    right: bool,
    col: u8,
};

/// Merge-joins two sorted slices on their first `key_len` columns and
/// appends one output tuple per matching pair, assembled per `out_spec`.
/// With `key_len == 0` this is the full cross product.
pub fn mergeJoinPrefix(
    allocator: Allocator,
    left: []const DynTuple,
    right: []const DynTuple,
    key_len: usize,
    out_spec: []const OutCol,
    out: *std.ArrayListUnmanaged(DynTuple),
) Allocator.Error!void {
    var l = left;
    var r = right;

    while (l.len > 0 and r.len > 0) {
        switch (dyntuple.cmpPrefix(&l[0], &r[0], key_len)) {
            .lt => l = dyntuple.gallopPrefix(l, &r[0], key_len),
            .gt => r = dyntuple.gallopPrefix(r, &l[0], key_len),
            .eq => {
                const l_count = countPrefix(l, key_len);
                const r_count = countPrefix(r, key_len);

                for (l[0..l_count]) |*l_tuple| {
                    for (r[0..r_count]) |*r_tuple| {
                        var tuple = dyntuple.zero_tuple;
                        for (out_spec, 0..) |out_col, i| {
                            const source = if (out_col.right) r_tuple else l_tuple;
                            dyntuple.set(&tuple, i, dyntuple.get(source, out_col.col));
                        }
                        try out.append(allocator, tuple);
                    }
                }

                l = l[l_count..];
                r = r[r_count..];
            },
        }
    }
}

/// Number of leading tuples sharing the first tuple's `key_len`-column prefix.
fn countPrefix(slice: []const DynTuple, key_len: usize) usize {
    var count: usize = 1;
    while (count < slice.len and
        dyntuple.cmpPrefix(&slice[count], &slice[0], key_len) == .eq)
    {
        count += 1;
    }
    return count;
}

/// Appends every input tuple whose key (read from `key_cols`) has no match
/// in the sorted `filter` slice, whose key occupies its first
/// `key_cols.len` columns.
pub fn antiFilterPrefix(
    allocator: Allocator,
    input: []const DynTuple,
    key_cols: []const u8,
    filter: []const DynTuple,
    out: *std.ArrayListUnmanaged(DynTuple),
) Allocator.Error!void {
    for (input) |*tuple| {
        var probe = dyntuple.zero_tuple;
        for (key_cols, 0..) |src_col, i| {
            dyntuple.set(&probe, i, dyntuple.get(tuple, src_col));
        }

        const suffix = dyntuple.gallopPrefix(filter, &probe, key_cols.len);
        const found = suffix.len > 0 and
            dyntuple.cmpPrefix(&suffix[0], &probe, key_cols.len) == .eq;

        if (!found) {
            try out.append(allocator, tuple.*);
        }
    }
}

/// Groups a sorted, deduplicated slice by its first `group_len` columns and
/// folds column `value_col` per group, appending `[group..., result]`
/// tuples. Inputs are sets, so the fold runs once per distinct variable
/// binding. `count` and `sum` results are clamped into the integer payload
/// space so they never collide with string-tagged atoms.
pub fn aggregateDyn(
    allocator: Allocator,
    input: []const DynTuple,
    group_len: usize,
    value_col: usize,
    func: ast.AggFunc,
    out: *std.ArrayListUnmanaged(DynTuple),
) Allocator.Error!void {
    var start: usize = 0;
    while (start < input.len) {
        var end = start + 1;
        while (end < input.len and
            dyntuple.cmpPrefix(&input[end], &input[start], group_len) == .eq)
        {
            end += 1;
        }

        var acc: u64 = switch (func) {
            .count, .sum, .max => 0,
            .min => std.math.maxInt(u64),
        };
        for (input[start..end]) |*tuple| {
            const value = dyntuple.get(tuple, value_col);
            acc = switch (func) {
                .count => acc + 1,
                .sum => acc +| value,
                .min => @min(acc, value),
                .max => @max(acc, value),
            };
        }
        if (func == .count or func == .sum) {
            acc = @min(acc, interner.PAYLOAD_MASK);
        }

        var tuple = dyntuple.zero_tuple;
        var i: usize = 0;
        while (i < group_len) : (i += 1) {
            dyntuple.set(&tuple, i, dyntuple.get(&input[start], i));
        }
        dyntuple.set(&tuple, group_len, acc);
        try out.append(allocator, tuple);

        start = end;
    }
}

test "mergeJoinPrefix: joins on a shared key" {
    const allocator = std.testing.allocator;

    // Left: (key, a); right: (key, b); output: (key, a, b).
    const left = [_]DynTuple{
        dyntuple.fromSlice(&.{ 1, 10 }),
        dyntuple.fromSlice(&.{ 2, 20 }),
        dyntuple.fromSlice(&.{ 3, 30 }),
    };
    const right = [_]DynTuple{
        dyntuple.fromSlice(&.{ 2, 200 }),
        dyntuple.fromSlice(&.{ 3, 300 }),
        dyntuple.fromSlice(&.{ 3, 301 }),
        dyntuple.fromSlice(&.{ 4, 400 }),
    };
    const out_spec = [_]OutCol{
        .{ .right = false, .col = 0 },
        .{ .right = false, .col = 1 },
        .{ .right = true, .col = 1 },
    };

    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
    defer out.deinit(allocator);
    try mergeJoinPrefix(allocator, &left, &right, 1, &out_spec, &out);

    try std.testing.expectEqual(@as(usize, 3), out.items.len);
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&out.items[0], 0));
    try std.testing.expectEqual(@as(u64, 20), dyntuple.get(&out.items[0], 1));
    try std.testing.expectEqual(@as(u64, 200), dyntuple.get(&out.items[0], 2));
    try std.testing.expectEqual(@as(u64, 301), dyntuple.get(&out.items[2], 2));
}

test "mergeJoinPrefix: zero key length is a cross product" {
    const allocator = std.testing.allocator;

    const left = [_]DynTuple{
        dyntuple.fromSlice(&.{1}),
        dyntuple.fromSlice(&.{2}),
    };
    const right = [_]DynTuple{
        dyntuple.fromSlice(&.{10}),
        dyntuple.fromSlice(&.{20}),
        dyntuple.fromSlice(&.{30}),
    };
    const out_spec = [_]OutCol{
        .{ .right = false, .col = 0 },
        .{ .right = true, .col = 0 },
    };

    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
    defer out.deinit(allocator);
    try mergeJoinPrefix(allocator, &left, &right, 0, &out_spec, &out);

    try std.testing.expectEqual(@as(usize, 6), out.items.len);
}

test "antiFilterPrefix: keeps tuples without a match" {
    const allocator = std.testing.allocator;

    // Input: (a, b); filter on column 1.
    const input = [_]DynTuple{
        dyntuple.fromSlice(&.{ 1, 10 }),
        dyntuple.fromSlice(&.{ 2, 20 }),
        dyntuple.fromSlice(&.{ 3, 30 }),
    };
    const filter = [_]DynTuple{
        dyntuple.fromSlice(&.{20}),
    };

    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
    defer out.deinit(allocator);
    try antiFilterPrefix(allocator, &input, &.{1}, &filter, &out);

    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&out.items[0], 0));
    try std.testing.expectEqual(@as(u64, 3), dyntuple.get(&out.items[1], 0));
}

test "aggregateDyn: sum, count, min, and max" {
    const allocator = std.testing.allocator;

    // (group, value), sorted.
    const input = [_]DynTuple{
        dyntuple.fromSlice(&.{ 1, 10 }),
        dyntuple.fromSlice(&.{ 1, 20 }),
        dyntuple.fromSlice(&.{ 2, 5 }),
        dyntuple.fromSlice(&.{ 3, 100 }),
    };

    inline for (.{
        .{ ast.AggFunc.sum, [_]u64{ 30, 5, 100 } },
        .{ ast.AggFunc.count, [_]u64{ 2, 1, 1 } },
        .{ ast.AggFunc.min, [_]u64{ 10, 5, 100 } },
        .{ ast.AggFunc.max, [_]u64{ 20, 5, 100 } },
    }) |case| {
        var out: std.ArrayListUnmanaged(DynTuple) = .empty;
        defer out.deinit(allocator);
        try aggregateDyn(allocator, &input, 1, 1, case[0], &out);

        try std.testing.expectEqual(@as(usize, 3), out.items.len);
        for (case[1], 0..) |expected, i| {
            try std.testing.expectEqual(expected, dyntuple.get(&out.items[i], 1));
        }
    }
}

test "aggregateDyn: sum clamps into the integer payload space" {
    const allocator = std.testing.allocator;

    const big = interner.PAYLOAD_MASK;
    const input = [_]DynTuple{
        dyntuple.fromSlice(&.{ 1, big - 1 }),
        dyntuple.fromSlice(&.{ 1, big }),
    };

    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
    defer out.deinit(allocator);
    try aggregateDyn(allocator, &input, 1, 1, .sum, &out);

    try std.testing.expectEqual(@as(usize, 1), out.items.len);
    try std.testing.expectEqual(interner.PAYLOAD_MASK, dyntuple.get(&out.items[0], 1));
    try std.testing.expect(!interner.isStr(dyntuple.get(&out.items[0], 1)));
}

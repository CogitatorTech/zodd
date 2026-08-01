//! # Dynamic Tuple
//!
//! The module provides a fixed-width tuple type for the Datalog frontend.
//!
//! The engine core is comptime-generic over tuple types, while frontend
//! predicates have arities decided at runtime. `DynTuple` bridges the two: it
//! is a tuple struct of `MAX_ARITY` `u64` atoms, zero-padded past a
//! predicate's actual arity, so `Relation`, `Variable`, and `gallop` work on
//! it unchanged. Helpers in this module add the runtime-indexed operations
//! the frontend needs: column access, prefix comparison, prefix gallop, and
//! column-permuting projection (relayout).

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("../relation.zig").Relation;

/// A frontend value: either a raw integer or an interned-string id, encoded
/// by the interner's tag bit.
pub const Atom = u64;

/// Maximum predicate arity supported by the frontend. Each stored tuple
/// occupies `MAX_ARITY * 8` bytes regardless of the predicate's actual arity.
pub const MAX_ARITY = 16;

/// The single tuple type used by all frontend relations: a tuple struct of
/// `MAX_ARITY` `u64` fields. Columns at or past a predicate's arity are
/// always zero, so sorting and deduplication in `Relation` stay correct.
pub const DynTuple = @Tuple(&[_]type{Atom} ** MAX_ARITY);

/// A `DynTuple` with every column set to zero.
pub const zero_tuple: DynTuple = std.mem.zeroes(DynTuple);

/// Returns the value of column `col`. Tuple fields require comptime indices,
/// so this expands to a branch per column.
pub inline fn get(tuple: *const DynTuple, col: usize) Atom {
    inline for (0..MAX_ARITY) |i| {
        if (i == col) return tuple[i];
    }
    unreachable;
}

/// Sets column `col` to `value`.
pub inline fn set(tuple: *DynTuple, col: usize, value: Atom) void {
    inline for (0..MAX_ARITY) |i| {
        if (i == col) {
            tuple[i] = value;
            return;
        }
    }
    unreachable;
}

/// Builds a tuple from a slice of column values; remaining columns are zero.
pub fn fromSlice(values: []const Atom) DynTuple {
    std.debug.assert(values.len <= MAX_ARITY);
    var tuple = zero_tuple;
    for (values, 0..) |v, i| set(&tuple, i, v);
    return tuple;
}

/// Compares the first `prefix_len` columns of two tuples lexicographically.
pub fn cmpPrefix(a: *const DynTuple, b: *const DynTuple, prefix_len: usize) std.math.Order {
    var i: usize = 0;
    while (i < prefix_len) : (i += 1) {
        const order = std.math.order(get(a, i), get(b, i));
        if (order != .eq) return order;
    }
    return .eq;
}

/// Gallop search over a sorted slice, comparing only the first `prefix_len`
/// columns. Returns the suffix whose first element is `>= target` on that
/// prefix. Mirrors `zodd.gallop` but with a runtime column count.
pub fn gallopPrefix(slice: []const DynTuple, target: *const DynTuple, prefix_len: usize) []const DynTuple {
    if (slice.len == 0) return slice;
    if (cmpPrefix(&slice[0], target, prefix_len) != .lt) return slice;

    var step: usize = 1;
    var pos: usize = 0;

    while (true) {
        const next_pos = std.math.add(usize, pos, step) catch slice.len;
        if (next_pos >= slice.len or next_pos < pos) break;
        if (cmpPrefix(&slice[next_pos], target, prefix_len) != .lt) break;
        pos = next_pos;
        step = std.math.mul(usize, step, 2) catch std.math.maxInt(usize);
    }

    // Saturating arithmetic: `step` may have saturated above, in which case
    // `pos + step + 1` would overflow.
    const end_of_step = std.math.add(usize, pos, step) catch std.math.maxInt(usize);
    const upper = std.math.add(usize, end_of_step, 1) catch std.math.maxInt(usize);
    const end = @min(upper, slice.len);
    var lo = pos + 1;
    var hi = end;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (cmpPrefix(&slice[mid], target, prefix_len) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return slice[lo..];
}

/// Projects each tuple through a column selection and returns the sorted,
/// deduplicated result. Output column `i` takes the value of source column
/// `proj[i]`, so columns can be reordered, duplicated, or dropped. Output
/// columns beyond `proj.len` are zero.
pub fn relayout(
    allocator: Allocator,
    tuples: []const DynTuple,
    proj: []const u8,
) Allocator.Error!Relation(DynTuple) {
    std.debug.assert(proj.len <= MAX_ARITY);
    if (tuples.len == 0) return Relation(DynTuple).empty(allocator);

    const buffer = try allocator.alloc(DynTuple, tuples.len);
    defer allocator.free(buffer);

    for (tuples, 0..) |*tuple, row| {
        var out = zero_tuple;
        for (proj, 0..) |src_col, dest_col| {
            set(&out, dest_col, get(tuple, src_col));
        }
        buffer[row] = out;
    }

    return Relation(DynTuple).fromSlice(allocator, buffer);
}

test "DynTuple: get and set round-trip" {
    var tuple = zero_tuple;
    set(&tuple, 0, 10);
    set(&tuple, 3, 30);
    set(&tuple, MAX_ARITY - 1, 70);

    try std.testing.expectEqual(@as(Atom, 10), get(&tuple, 0));
    try std.testing.expectEqual(@as(Atom, 0), get(&tuple, 1));
    try std.testing.expectEqual(@as(Atom, 30), get(&tuple, 3));
    try std.testing.expectEqual(@as(Atom, 70), get(&tuple, MAX_ARITY - 1));
}

test "DynTuple: works with Relation sort and dedup" {
    const allocator = std.testing.allocator;

    var rel = try Relation(DynTuple).fromSlice(allocator, &[_]DynTuple{
        fromSlice(&.{ 2, 1 }),
        fromSlice(&.{ 1, 2 }),
        fromSlice(&.{ 2, 1 }),
        fromSlice(&.{ 1, 1 }),
    });
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 3), rel.len());
    try std.testing.expectEqual(@as(Atom, 1), get(&rel.elements[0], 0));
    try std.testing.expectEqual(@as(Atom, 1), get(&rel.elements[0], 1));
    try std.testing.expectEqual(@as(Atom, 2), get(&rel.elements[1], 1));
    try std.testing.expectEqual(@as(Atom, 2), get(&rel.elements[2], 0));
}

test "cmpPrefix: compares only the prefix" {
    const a = fromSlice(&.{ 1, 2, 9 });
    const b = fromSlice(&.{ 1, 2, 3 });

    try std.testing.expectEqual(std.math.Order.eq, cmpPrefix(&a, &b, 2));
    try std.testing.expectEqual(std.math.Order.gt, cmpPrefix(&a, &b, 3));
    try std.testing.expectEqual(std.math.Order.eq, cmpPrefix(&a, &b, 0));
}

test "gallopPrefix: finds the first matching prefix" {
    const slice = [_]DynTuple{
        fromSlice(&.{ 1, 5 }),
        fromSlice(&.{ 2, 1 }),
        fromSlice(&.{ 2, 7 }),
        fromSlice(&.{ 4, 0 }),
    };

    const target = fromSlice(&.{2});
    const result = gallopPrefix(&slice, &target, 1);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(Atom, 2), get(&result[0], 0));
    try std.testing.expectEqual(@as(Atom, 1), get(&result[0], 1));

    const missing = fromSlice(&.{3});
    const result2 = gallopPrefix(&slice, &missing, 1);
    try std.testing.expectEqual(@as(usize, 1), result2.len);
    try std.testing.expectEqual(@as(Atom, 4), get(&result2[0], 0));

    const beyond = fromSlice(&.{9});
    const result3 = gallopPrefix(&slice, &beyond, 1);
    try std.testing.expectEqual(@as(usize, 0), result3.len);
}

test "relayout: permutes columns and re-sorts" {
    const allocator = std.testing.allocator;

    const input = [_]DynTuple{
        fromSlice(&.{ 3, 1 }),
        fromSlice(&.{ 2, 5 }),
    };

    // Swap the two columns: output col 0 reads source col 1 and vice versa.
    var rel = try relayout(allocator, &input, &.{ 1, 0 });
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 2), rel.len());
    // (1, 3) sorts before (5, 2).
    try std.testing.expectEqual(@as(Atom, 1), get(&rel.elements[0], 0));
    try std.testing.expectEqual(@as(Atom, 3), get(&rel.elements[0], 1));
    try std.testing.expectEqual(@as(Atom, 5), get(&rel.elements[1], 0));
    try std.testing.expectEqual(@as(Atom, 2), get(&rel.elements[1], 1));
}

test "relayout: drops columns and dedups the projection" {
    const allocator = std.testing.allocator;

    const input = [_]DynTuple{
        fromSlice(&.{ 1, 2, 3 }),
        fromSlice(&.{ 4, 2, 6 }),
    };

    // Keep only column 1; both rows project to (2,) and dedup to one.
    var rel = try relayout(allocator, &input, &.{1});
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 1), rel.len());
    try std.testing.expectEqual(@as(Atom, 2), get(&rel.elements[0], 0));
}

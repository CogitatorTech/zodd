//! # Variable
//!
//! A Variable represents a relation that evolves during fixed-point iteration (semi-naive evaluation).
//!
//! The Variable maintains three states:
//! - **Stable**: The tuples processed in previous iterations.
//! - **Recent**: The tuples added in the current iteration (delta).
//! - **To Add**: The new tuples discovered in the current iteration.
//!
//! This structure allows join processing where only `recent` tuples are joined
//! with other relations to discover new facts.
//!
//! ## Usage
//!
//! ```zig
//! var v = Variable(Edge).init(allocator);
//! defer v.deinit();
//!
//! try v.insertSlice(initial_edges);
//! while (try v.changed()) {
//!     // Join logic here, populating v via `insert` / `insertSlice`.
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;
const shrinkOrCopy = @import("relation.zig").shrinkOrCopy;

pub fn Variable(comptime Tuple: type) type {
    return struct {
        const Self = @This();
        const Rel = Relation(Tuple);
        const RelList = std.ArrayListUnmanaged(Rel);

        /// The list of "stable" relations.
        stable: RelList,
        /// The "recent" relation (added in the last round).
        recent: Rel,
        /// The list of relations to be added for the next round.
        to_add: RelList,
        /// The allocator for internal structures.
        allocator: Allocator,

        /// Initializes a new variable.
        pub fn init(allocator: Allocator) Self {
            return Self{
                .stable = RelList.empty,
                .recent = Rel.empty(allocator),
                .to_add = RelList.empty,
                .allocator = allocator,
            };
        }

        /// Deinitializes the variable.
        pub fn deinit(self: *Self) void {
            for (self.stable.items) |*batch| {
                batch.deinit();
            }
            self.stable.deinit(self.allocator);

            self.recent.deinit();

            for (self.to_add.items) |*batch| {
                batch.deinit();
            }
            self.to_add.deinit(self.allocator);
        }

        /// Inserts a relation into the variable. The variable takes ownership
        /// of the relation's storage; do not `deinit` it after calling this.
        pub fn insert(self: *Self, relation: Rel) Allocator.Error!void {
            try self.to_add.append(self.allocator, relation);
        }

        /// Inserts a slice of tuples into the variable. The tuples are copied;
        /// the caller retains ownership of `tuples`.
        pub fn insertSlice(self: *Self, tuples: []const Tuple) Allocator.Error!void {
            const rel = try Rel.fromSlice(self.allocator, tuples);
            try self.insert(rel);
        }

        /// Processes pending updates and returns true if the variable has changed.
        pub fn changed(self: *Self) Allocator.Error!bool {
            if (!self.recent.isEmpty()) {
                var recent = self.recent;
                self.recent = Rel.empty(self.allocator);
                // `recent` now owns the tuples. If anything below fails we
                // must free them; on success we transfer ownership to
                // `self.stable` and null out `recent`.
                errdefer recent.deinit();

                while (self.stable.items.len > 0) {
                    const last = &self.stable.items[self.stable.items.len - 1];
                    if (last.len() <= 2 * recent.len()) {
                        var popped = self.stable.pop() orelse break;
                        errdefer popped.deinit();
                        recent = try recent.merge(&popped);
                    } else {
                        break;
                    }
                }

                try self.stable.append(self.allocator, recent);
                recent = Rel.empty(self.allocator);
            }

            if (self.to_add.items.len > 0) {
                var to_add = self.to_add.pop().?;
                errdefer to_add.deinit();

                while (self.to_add.items.len > 0) {
                    var more = self.to_add.pop().?;
                    errdefer more.deinit();
                    to_add = try to_add.merge(&more);
                }

                for (self.stable.items) |*batch| {
                    try self.filterAgainst(&to_add, batch);
                }

                self.recent = to_add;
                to_add = Rel.empty(self.allocator);
            }

            return !self.recent.isEmpty();
        }

        fn filterAgainst(self: *Self, target: *Rel, existing: *const Rel) Allocator.Error!void {
            if (target.elements.len == 0 or existing.elements.len == 0) return;

            var write_idx: usize = 0;
            var existing_slice: []const Tuple = existing.elements;

            for (target.elements) |elem| {
                existing_slice = gallop(Tuple, existing_slice, elem);

                const found = existing_slice.len > 0 and
                    Rel.compareTuples(existing_slice[0], elem) == .eq;

                if (!found) {
                    target.elements[write_idx] = elem;
                    write_idx += 1;
                }
            }

            if (write_idx < target.elements.len) {
                if (write_idx == 0) {
                    target.deinit();
                    target.* = Rel.empty(self.allocator);
                } else {
                    target.elements = try shrinkOrCopy(Tuple, self.allocator, target.elements, write_idx);
                }
            }
        }

        /// Returns the total number of elements in the variable.
        pub fn totalLen(self: Self) usize {
            var count: usize = self.recent.len();
            for (self.stable.items) |batch| {
                count += batch.len();
            }
            for (self.to_add.items) |batch| {
                count += batch.len();
            }
            return count;
        }

        /// Completes the variable and returns the final relation.
        ///
        /// **Destructive:** `complete` consumes the variable's internal
        /// batches and returns a single merged `Relation`. After this call
        /// the variable is left empty and should not be used again, other
        /// than to `deinit` (which remains safe).
        pub fn complete(self: *Self) Allocator.Error!Rel {
            if (!self.recent.isEmpty()) {
                try self.stable.append(self.allocator, self.recent);
                self.recent = Rel.empty(self.allocator);
            }

            if (self.to_add.items.len > 0) {
                var to_add = self.to_add.pop().?;
                errdefer to_add.deinit();

                while (self.to_add.items.len > 0) {
                    var more = self.to_add.pop().?;
                    errdefer more.deinit();
                    to_add = try to_add.merge(&more);
                }
                try self.stable.append(self.allocator, to_add);
                to_add = Rel.empty(self.allocator);
            }

            if (self.stable.items.len == 0) {
                return Rel.empty(self.allocator);
            }

            var result = self.stable.pop().?;
            errdefer result.deinit();

            while (self.stable.items.len > 0) {
                var batch = self.stable.pop().?;
                errdefer batch.deinit();
                result = try result.merge(&batch);
            }

            return result;
        }
    };
}

/// Gallop search for a value in a sorted slice.
pub fn gallop(comptime T: type, slice: []const T, target: T) []const T {
    const Rel = Relation(T);

    if (slice.len == 0) return slice;
    if (Rel.compareTuples(slice[0], target) != .lt) return slice;

    var step: usize = 1;
    var pos: usize = 0;

    while (true) {
        const next_pos = std.math.add(usize, pos, step) catch slice.len;
        if (next_pos >= slice.len or next_pos < pos) break;
        if (Rel.compareTuples(slice[next_pos], target) != .lt) break;
        pos = next_pos;
        const new_step = std.math.mul(usize, step, 2) catch std.math.maxInt(usize);
        step = new_step;
    }

    // Saturating arithmetic: `step` may be maxInt(usize) after the doubling
    // loop saturated, in which case `pos + step + 1` would overflow.
    const end_of_step = std.math.add(usize, pos, step) catch std.math.maxInt(usize);
    const upper = std.math.add(usize, end_of_step, 1) catch std.math.maxInt(usize);
    const end = @min(upper, slice.len);
    var lo = pos + 1;
    var hi = end;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (Rel.compareTuples(slice[mid], target) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return slice[lo..];
}

test "Variable: basic lifecycle" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3 });

    const changed1 = try v.changed();
    try std.testing.expect(changed1);
    try std.testing.expectEqual(@as(usize, 3), v.recent.len());

    const changed2 = try v.changed();
    try std.testing.expect(!changed2);
    try std.testing.expectEqual(@as(usize, 0), v.recent.len());
    try std.testing.expectEqual(@as(usize, 1), v.stable.items.len);
}

test "Variable: deduplication across rounds" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3 });
    _ = try v.changed();

    try v.insertSlice(&[_]u32{ 2, 3, 4, 5 });
    const changed = try v.changed();

    try std.testing.expect(changed);
    try std.testing.expectEqual(@as(usize, 2), v.recent.len());
}

test "Variable: complete" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);

    try v.insertSlice(&[_]u32{ 1, 2, 3 });
    _ = try v.changed();
    _ = try v.changed();

    try v.insertSlice(&[_]u32{ 4, 5 });
    _ = try v.changed();
    _ = try v.changed();

    var result = try v.complete();
    defer result.deinit();
    defer v.deinit();

    try std.testing.expectEqual(@as(usize, 5), result.len());
}

test "Variable: totalLen" {
    const allocator = std.testing.allocator;
    var v = Variable(u32).init(allocator);
    defer v.deinit();

    // Init: 0
    try std.testing.expectEqual(@as(usize, 0), v.totalLen());

    // Insert to_add: 3 items
    try v.insertSlice(&[_]u32{ 1, 2, 3 });
    try std.testing.expectEqual(@as(usize, 3), v.totalLen());

    // Changed: recent=3, stable=0, to_add=0 (moved to recent)
    _ = try v.changed();
    try std.testing.expectEqual(@as(usize, 3), v.totalLen());

    // Changed again: recent=0, stable=3 (moved to recent then merged to stable)
    // Wait, changed() moves recent -> stable.
    _ = try v.changed();
    try std.testing.expectEqual(@as(usize, 3), v.totalLen());

    // Add more
    try v.insertSlice(&[_]u32{4});
    try std.testing.expectEqual(@as(usize, 4), v.totalLen());
}

test "gallop: basic" {
    const slice = [_]u32{ 1, 3, 5, 7, 9, 11, 13, 15 };

    const result1 = gallop(u32, &slice, 6);
    try std.testing.expectEqual(@as(usize, 5), result1.len);
    try std.testing.expectEqual(@as(u32, 7), result1[0]);

    const result2 = gallop(u32, &slice, 1);
    try std.testing.expectEqual(@as(usize, 8), result2.len);

    const result3 = gallop(u32, &slice, 20);
    try std.testing.expectEqual(@as(usize, 0), result3.len);
}

test "gallop: target at the last element" {
    // Locks in the edge case where the galloping step lands on the final
    // index, which is where the `pos + step + 1` overflow path was reachable
    // in principle. The saturating-add fix keeps this correct.
    const slice = [_]u32{ 1, 2, 4, 8, 16, 32, 64, 128 };
    const result = gallop(u32, &slice, 128);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(u32, 128), result[0]);
}

test "gallop: target beyond saturated step" {
    // With 1024 elements the step doubles to 1024 before termination; the
    // boundary arithmetic must not overflow. Regression coverage for the
    // saturating `end` computation in `gallop`.
    var slice: [1024]u32 = undefined;
    for (&slice, 0..) |*x, i| x.* = @intCast(i * 2);
    const result = gallop(u32, &slice, 2045);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(u32, 2046), result[0]);
}

test "Variable: changed filters against stable batches" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3, 4, 5, 6, 7, 8 });
    _ = try v.changed();
    _ = try v.changed();

    try v.insertSlice(&[_]u32{ 2, 4, 6, 8, 9 });
    const changed = try v.changed();

    try std.testing.expect(changed);
    try std.testing.expectEqual(@as(usize, 1), v.recent.len());
    try std.testing.expectEqual(@as(u32, 9), v.recent.elements[0]);
}

test "Variable: changed with recent and to_add" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
    _ = try v.changed();

    try v.insertSlice(&[_]u32{ 3, 5, 11, 12 });
    const changed = try v.changed();

    try std.testing.expect(changed);
    try std.testing.expectEqual(@as(usize, 2), v.recent.len());
    try std.testing.expectEqual(@as(u32, 11), v.recent.elements[0]);
    try std.testing.expectEqual(@as(u32, 12), v.recent.elements[1]);
}

test "Variable: insertSlice with empty slice is a no-op" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{});

    // The empty insert lands as an empty Relation on the to_add queue; it
    // should merely vanish through changed() without error or leaks.
    const changed_result = try v.changed();
    try std.testing.expect(!changed_result);
    try std.testing.expectEqual(@as(usize, 0), v.totalLen());
}

test "Variable: changed returns false once no new facts arrive" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 2, 3 });
    try std.testing.expect(try v.changed());
    try std.testing.expect(!try v.changed());
    try std.testing.expect(!try v.changed());
}

test "Variable: changed merges multiple to_add batches into recent" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 1, 3, 5 });
    try v.insertSlice(&[_]u32{ 2, 4, 6 });
    try v.insertSlice(&[_]u32{ 5, 7 });

    try std.testing.expect(try v.changed());
    try std.testing.expectEqual(@as(usize, 7), v.recent.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 4, 5, 6, 7 }, v.recent.elements);
}

test "Variable: complete folds to_add when nothing has been processed yet" {
    const allocator = std.testing.allocator;

    var v = Variable(u32).init(allocator);
    defer v.deinit();

    try v.insertSlice(&[_]u32{ 2, 1 });
    try v.insertSlice(&[_]u32{ 3, 1 });

    var result = try v.complete();
    defer result.deinit();

    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3 }, result.elements);
}

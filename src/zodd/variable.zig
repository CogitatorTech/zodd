//! A Datalog variable representing a relation that evolves during iteration.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;

pub fn Variable(comptime Tuple: type) type {
    return struct {
        const Self = @This();
        const Rel = Relation(Tuple);
        const RelList = std.ArrayListUnmanaged(Rel);

        stable: RelList,
        recent: Rel,
        to_add: RelList,
        allocator: Allocator,

        pub fn init(allocator: Allocator) Self {
            return Self{
                .stable = RelList{},
                .recent = Rel.empty(allocator),
                .to_add = RelList{},
                .allocator = allocator,
            };
        }

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

        pub fn insert(self: *Self, relation: Rel) Allocator.Error!void {
            try self.to_add.append(self.allocator, relation);
        }

        pub fn insertSlice(self: *Self, tuples: []const Tuple) Allocator.Error!void {
            const rel = try Rel.fromSlice(self.allocator, tuples);
            try self.insert(rel);
        }

        pub fn changed(self: *Self) Allocator.Error!bool {
            if (!self.recent.isEmpty()) {
                var recent = self.recent;
                self.recent = Rel.empty(self.allocator);

                while (self.stable.items.len > 0) {
                    const last = &self.stable.items[self.stable.items.len - 1];
                    if (last.len() <= 2 * recent.len()) {
                        var popped = self.stable.pop() orelse break;
                        recent = try recent.merge(&popped);
                    } else {
                        break;
                    }
                }

                try self.stable.append(self.allocator, recent);
            }

            if (self.to_add.items.len > 0) {
                var to_add = self.to_add.pop().?;
                while (self.to_add.items.len > 0) {
                    var more = self.to_add.pop().?;
                    to_add = try to_add.merge(&more);
                }

                for (self.stable.items) |*batch| {
                    try self.filterAgainst(&to_add, batch);
                }

                self.recent = to_add;
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
                    target.elements = self.allocator.realloc(
                        target.elements,
                        write_idx,
                    ) catch target.elements[0..write_idx];
                }
            }
        }

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

        pub fn complete(self: *Self) Allocator.Error!Rel {
            if (!self.recent.isEmpty()) {
                try self.stable.append(self.allocator, self.recent);
                self.recent = Rel.empty(self.allocator);
            }

            if (self.to_add.items.len > 0) {
                var to_add = self.to_add.pop().?;
                while (self.to_add.items.len > 0) {
                    var more = self.to_add.pop().?;
                    to_add = try to_add.merge(&more);
                }
                try self.stable.append(self.allocator, to_add);
            }

            if (self.stable.items.len == 0) {
                return Rel.empty(self.allocator);
            }

            var result = self.stable.pop().?;
            while (self.stable.items.len > 0) {
                var batch = self.stable.pop().?;
                result = try result.merge(&batch);
            }

            return result;
        }
    };
}

pub fn gallop(comptime T: type, slice: []const T, target: T) []const T {
    const Rel = Relation(T);

    if (slice.len == 0) return slice;
    if (Rel.compareTuples(slice[0], target) != .lt) return slice;

    var step: usize = 1;
    var pos: usize = 0;

    while (true) {
        const next_pos = pos + step;
        if (next_pos >= slice.len or next_pos < pos) break;
        if (Rel.compareTuples(slice[next_pos], target) != .lt) break;
        pos = next_pos;
        step *|= 2;
    }

    const end = @min(pos + step + 1, slice.len);
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

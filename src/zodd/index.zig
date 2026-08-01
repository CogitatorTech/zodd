//! # Secondary Index
//!
//! The module implements B-tree based secondary indexes for relations.
//!
//! The index allows efficient lookups and range queries on attributes other than the primary sort key.
//! It uses an `id` -> `Relation` mapping, where `id` is the indexed attribute.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ordered = @import("ordered");
const Relation = @import("relation.zig").Relation;

pub fn SecondaryIndex(
    comptime Tuple: type,
    comptime Key: type,
    comptime key_extractor: fn (Tuple) Key,
    comptime key_compare: fn (Key, Key) std.math.Order,
    comptime BRANCHING_FACTOR: u16,
) type {
    return struct {
        const Self = @This();
        const Map = ordered.BTreeMap(Key, Relation(Tuple), key_compare, BRANCHING_FACTOR);

        /// Underlying B-tree map.
        map: Map,
        /// Allocator for the index.
        allocator: Allocator,

        /// Initializes a new secondary index.
        pub fn init(allocator: Allocator) Self {
            return Self{
                .map = Map.init(allocator),
                .allocator = allocator,
            };
        }

        /// Deinitializes the index.
        pub fn deinit(self: *Self) void {
            // Always free the map itself, even if we can't walk it. Walking
            // the map requires an allocation for the traversal stack, so an
            // OOM during deinit could leak the nested Relations, but we must
            // not let that also leak the B-tree structure.
            defer self.map.deinit();
            if (self.map.iterator()) |it| {
                var iter = it;
                defer iter.deinit();
                while (iter.next() catch null) |entry| {
                    var mut_rel = entry.value;
                    mut_rel.deinit();
                }
            } else |_| {}
        }

        /// Inserts a tuple into the index.
        pub fn insert(self: *Self, tuple: Tuple) !void {
            const key = key_extractor(tuple);
            if (self.map.getPtr(key)) |rel_ptr| {
                const single = try Relation(Tuple).fromSlice(self.allocator, &[_]Tuple{tuple});
                var mutable_single = single;
                errdefer mutable_single.deinit();
                var old_rel = rel_ptr.*;
                const new_rel = old_rel.merge(&mutable_single) catch |err| {
                    // A failed merge still consumes its inputs, so the map
                    // entry must not keep pointing at the old storage. The
                    // consumed relation is valid and empty.
                    rel_ptr.* = old_rel;
                    return err;
                };
                rel_ptr.* = new_rel;
            } else {
                var rel = try Relation(Tuple).fromSlice(self.allocator, &[_]Tuple{tuple});
                errdefer rel.deinit();
                try self.map.put(key, rel);
            }
        }

        /// Bulk insert multiple tuples.
        pub fn insertSlice(self: *Self, tuples: []const Tuple) !void {
            for (tuples) |t| {
                try self.insert(t);
            }
        }

        /// Returns the relation for a given key.
        pub fn get(self: *const Self, key: Key) ?*const Relation(Tuple) {
            return self.map.get(key);
        }

        /// Returns a relation covering the closed range [start_key, end_key].
        /// Both endpoints are inclusive: entries with `key == end_key` are
        /// returned.
        pub fn getRange(self: *Self, start_key: Key, end_key: Key) !Relation(Tuple) {
            var iter = try self.map.iterator();
            defer iter.deinit();

            var result_tuples = std.ArrayListUnmanaged(Tuple).empty;
            defer result_tuples.deinit(self.allocator);

            while (try iter.next()) |entry| {
                const k = entry.key;
                const order_start = key_compare(k, start_key);
                const order_end = key_compare(k, end_key);

                if (order_start == .lt) continue;
                if (order_end == .gt) break;

                try result_tuples.appendSlice(self.allocator, entry.value.elements);
            }

            return Relation(Tuple).fromSlice(self.allocator, result_tuples.items);
        }
    };
}

fn u32Compare(a: u32, b: u32) std.math.Order {
    return std.math.order(a, b);
}

test "SecondaryIndex: basic usage" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    const Index = SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[1];
        }
    }.extract, u32Compare, 4);

    var idx = Index.init(allocator);
    defer idx.deinit();

    try idx.insert(.{ 1, 10 });
    try idx.insert(.{ 2, 20 });
    try idx.insert(.{ 3, 10 });

    const rel10 = idx.get(10).?;
    try std.testing.expectEqual(@as(usize, 2), rel10.len());
    try std.testing.expectEqual(rel10.elements[0][0], 1);
    try std.testing.expectEqual(rel10.elements[1][0], 3);

    const rel20 = idx.get(20).?;
    try std.testing.expectEqual(@as(usize, 1), rel20.len());

    var range_rel = try idx.getRange(10, 20);
    defer range_rel.deinit();
    try std.testing.expectEqual(@as(usize, 3), range_rel.len());
}

test "SecondaryIndex: getRange empty and inverted" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    const Index = SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[0];
        }
    }.extract, u32Compare, 4);

    var idx = Index.init(allocator);
    defer idx.deinit();

    try idx.insert(.{ 1, 10 });
    try idx.insert(.{ 3, 30 });

    var empty = try idx.getRange(4, 5);
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.len());

    var inverted = try idx.getRange(5, 4);
    defer inverted.deinit();
    try std.testing.expectEqual(@as(usize, 0), inverted.len());
}

test "SecondaryIndex: getRange end is inclusive" {
    // Locks in the closed-interval [start, end] contract documented on
    // getRange. A regression here would be a silent behavior change.
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    const Index = SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[0];
        }
    }.extract, u32Compare, 4);

    var idx = Index.init(allocator);
    defer idx.deinit();

    try idx.insert(.{ 1, 10 });
    try idx.insert(.{ 2, 20 });
    try idx.insert(.{ 3, 30 });
    try idx.insert(.{ 4, 40 });

    // end_key == 3 must include the entry at key 3.
    var inclusive = try idx.getRange(2, 3);
    defer inclusive.deinit();
    try std.testing.expectEqual(@as(usize, 2), inclusive.len());

    // start == end picks out exactly one key.
    var point = try idx.getRange(3, 3);
    defer point.deinit();
    try std.testing.expectEqual(@as(usize, 1), point.len());
    try std.testing.expectEqual(@as(u32, 3), point.elements[0][0]);
}

test "SecondaryIndex: insert failure leaves the index memory-safe" {
    // A duplicate insert routes through Relation.merge, whose shrink step can
    // fail after the old relation's storage is gone. The map entry must never
    // keep pointing at that freed storage.
    const Tuple = struct { u32, u32 };
    const Index = SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[1];
        }
    }.extract, u32Compare, 4);

    // Fails every allocation, resize, and remap from `fail_index` onward
    // until disarmed. Unlike std.testing.FailingAllocator it can be disarmed
    // before deinit, so `deinit` (which allocates to walk the B-tree) can
    // still free everything and the testing allocator can flag any leak,
    // double free, or use-after-free from the insert path.
    const OneShotFailing = struct {
        inner: Allocator,
        fail_index: usize,
        index: usize = 0,

        const vtable = Allocator.VTable{
            .alloc = allocImpl,
            .resize = resizeImpl,
            .remap = remapImpl,
            .free = freeImpl,
        };

        fn allocator(self: *@This()) Allocator {
            return .{ .ptr = self, .vtable = &vtable };
        }

        fn armed(self: *const @This()) bool {
            return self.index >= self.fail_index;
        }

        fn allocImpl(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            defer self.index += 1;
            if (self.armed()) return null;
            return self.inner.rawAlloc(len, alignment, ret_addr);
        }

        fn resizeImpl(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.armed()) return false;
            return self.inner.rawResize(memory, alignment, new_len, ret_addr);
        }

        fn remapImpl(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.armed()) return null;
            return self.inner.rawRemap(memory, alignment, new_len, ret_addr);
        }

        fn freeImpl(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.inner.rawFree(memory, alignment, ret_addr);
        }
    };

    var fail_index: usize = 0;
    while (fail_index < 12) : (fail_index += 1) {
        var failing = OneShotFailing{ .inner = std.testing.allocator, .fail_index = fail_index };

        var idx = Index.init(failing.allocator());
        defer idx.deinit();

        idx.insert(.{ 1, 10 }) catch {};
        // Duplicate tuple: merge produces a shorter result, forcing the
        // fallible shrink after the old storage is consumed.
        idx.insert(.{ 1, 10 }) catch {};
        idx.insert(.{ 2, 10 }) catch {};

        // Whatever failed, lookups must not touch freed memory.
        if (idx.get(10)) |rel| {
            for (rel.elements) |t| {
                std.mem.doNotOptimizeAway(t);
            }
        }

        // Disarm before the deferred deinit: an allocation failure inside
        // deinit's B-tree walk is a separate, documented limitation.
        failing.fail_index = std.math.maxInt(usize);
    }
}

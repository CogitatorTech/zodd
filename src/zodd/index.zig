//! Secondary index implementation for relations using B-trees.

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

        map: Map,
        allocator: Allocator,

        pub fn init(allocator: Allocator) Self {
            return Self{
                .map = Map.init(allocator),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            var iter = self.map.iterator() catch return;
            defer iter.deinit();
            while (iter.next() catch null) |entry| {
                var mut_rel = entry.value;
                mut_rel.deinit();
            }
            self.map.deinit();
        }

        pub fn insert(self: *Self, tuple: Tuple) !void {
            const key = key_extractor(tuple);
            if (self.map.getPtr(key)) |rel_ptr| {
                const single = try Relation(Tuple).fromSlice(self.allocator, &[_]Tuple{tuple});
                var mutable_single = single;
                const new_rel = try rel_ptr.merge(&mutable_single);
                rel_ptr.* = new_rel;
            } else {
                const rel = try Relation(Tuple).fromSlice(self.allocator, &[_]Tuple{tuple});
                try self.map.put(key, rel);
            }
        }

        /// Bulk insert multiple tuples
        pub fn insertSlice(self: *Self, tuples: []const Tuple) !void {
            for (tuples) |t| {
                try self.insert(t);
            }
        }

        pub fn get(self: *const Self, key: Key) ?*const Relation(Tuple) {
            return self.map.get(key);
        }

        pub fn getRange(self: *Self, start_key: Key, end_key: Key) !Relation(Tuple) {
            var iter = try self.map.iterator();
            defer iter.deinit();

            var result_tuples = std.ArrayListUnmanaged(Tuple){};
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

//! Core Relation data structure: a sorted list of unique tuples.

const std = @import("std");
const mem = std.mem;
const sort = std.sort;
const Allocator = mem.Allocator;

pub fn Relation(comptime Tuple: type) type {
    return struct {
        const Self = @This();

        elements: []Tuple,
        allocator: Allocator,

        pub fn fromSlice(allocator: Allocator, input: []const Tuple) Allocator.Error!Self {
            if (input.len == 0) {
                return Self{
                    .elements = &[_]Tuple{},
                    .allocator = allocator,
                };
            }

            const elements = try allocator.alloc(Tuple, input.len);
            @memcpy(elements, input);

            sort.pdq(Tuple, elements, {}, lessThan);

            const unique_len = deduplicate(elements);

            if (unique_len < elements.len) {
                const shrunk = allocator.realloc(elements, unique_len) catch elements[0..unique_len];
                return Self{
                    .elements = shrunk,
                    .allocator = allocator,
                };
            }

            return Self{
                .elements = elements,
                .allocator = allocator,
            };
        }

        pub fn empty(allocator: Allocator) Self {
            return Self{
                .elements = &[_]Tuple{},
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.elements.len > 0) {
                self.allocator.free(self.elements);
            }
            self.elements = &[_]Tuple{};
        }

        pub fn len(self: Self) usize {
            return self.elements.len;
        }

        pub fn isEmpty(self: Self) bool {
            return self.elements.len == 0;
        }

        pub fn merge(self: *Self, other: *Self) Allocator.Error!Self {
            if (self.elements.len == 0) {
                const result = other.*;
                other.elements = &[_]Tuple{};
                self.deinit();
                return result;
            }
            if (other.elements.len == 0) {
                const result = self.*;
                self.elements = &[_]Tuple{};
                other.deinit();
                return result;
            }

            const total_len = self.elements.len + other.elements.len;
            const merged = try self.allocator.alloc(Tuple, total_len);

            @memcpy(merged[0..self.elements.len], self.elements);
            @memcpy(merged[self.elements.len..], other.elements);

            sort.pdq(Tuple, merged, {}, lessThan);
            const unique_len = deduplicate(merged);

            self.deinit();
            other.deinit();

            if (unique_len < merged.len) {
                const shrunk = self.allocator.realloc(merged, unique_len) catch merged[0..unique_len];
                return Self{
                    .elements = shrunk,
                    .allocator = self.allocator,
                };
            }

            return Self{
                .elements = merged,
                .allocator = self.allocator,
            };
        }

        fn lessThan(_: void, a: Tuple, b: Tuple) bool {
            return compareTuples(a, b) == .lt;
        }

        pub fn compareTuples(a: Tuple, b: Tuple) std.math.Order {
            const info = @typeInfo(Tuple);
            if (info == .@"struct" and info.@"struct".is_tuple) {
                inline for (0..info.@"struct".fields.len) |i| {
                    const a_field = a[i];
                    const b_field = b[i];
                    const order = std.math.order(a_field, b_field);
                    if (order != .eq) return order;
                }
                return .eq;
            } else {
                return std.math.order(a, b);
            }
        }

        fn deduplicate(elements: []Tuple) usize {
            if (elements.len <= 1) return elements.len;

            var write_idx: usize = 1;
            for (elements[1..]) |elem| {
                if (compareTuples(elements[write_idx - 1], elem) != .eq) {
                    elements[write_idx] = elem;
                    write_idx += 1;
                }
            }
            return write_idx;
        }
        pub fn save(self: Self, writer: anytype) !void {
            try writer.writeAll("ZODDREL");
            try writer.writeInt(u8, 1, .little);
            try writer.writeInt(u64, self.elements.len, .little);
            const bytes = std.mem.sliceAsBytes(self.elements);
            try writer.writeAll(bytes);
        }

        pub fn load(allocator: Allocator, reader: anytype) !Self {
            const magic = try reader.readBytesNoEof(7);
            if (!std.mem.eql(u8, &magic, "ZODDREL")) {
                return error.InvalidFormat;
            }
            const version = try reader.readInt(u8, .little);
            if (version != 1) {
                return error.UnsupportedVersion;
            }

            const length = try reader.readInt(u64, .little);
            if (length == 0) {
                return Self.empty(allocator);
            }

            const elements = try allocator.alloc(Tuple, length);
            errdefer allocator.free(elements);

            const bytes = std.mem.sliceAsBytes(elements);
            try reader.readNoEof(bytes);

            sort.pdq(Tuple, elements, {}, lessThan);
            const unique_len = deduplicate(elements);

            if (unique_len < elements.len) {
                const shrunk = allocator.realloc(elements, unique_len) catch elements[0..unique_len];
                return Self{
                    .elements = shrunk,
                    .allocator = allocator,
                };
            }

            return Self{
                .elements = elements,
                .allocator = allocator,
            };
        }
    };
}

test "Relation: empty" {
    const allocator = std.testing.allocator;
    var rel = Relation(u32).empty(allocator);
    defer rel.deinit();

    try std.testing.expect(rel.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), rel.len());
}

test "Relation: persistence" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var original = try Relation(Tuple).fromSlice(allocator, &[_]Tuple{
        .{ 1, 10 },
        .{ 2, 20 },
        .{ 3, 30 },
    });
    defer original.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    try original.save(buffer.writer(allocator));

    var fbs = std.io.fixedBufferStream(buffer.items);
    var loaded = try Relation(Tuple).load(allocator, fbs.reader());
    defer loaded.deinit();

    try std.testing.expectEqual(original.len(), loaded.len());
    try std.testing.expectEqualSlices(Tuple, original.elements, loaded.elements);
}

test "Relation: fromSlice sorts and deduplicates" {
    const allocator = std.testing.allocator;
    const input = [_]u32{ 5, 3, 3, 1, 5, 2, 1 };

    var rel = try Relation(u32).fromSlice(allocator, &input);
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 4), rel.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 5 }, rel.elements);
}

test "Relation: tuple type" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };
    const input = [_]Tuple{
        .{ 2, 1 },
        .{ 1, 2 },
        .{ 1, 2 },
        .{ 1, 1 },
    };

    var rel = try Relation(Tuple).fromSlice(allocator, &input);
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 3), rel.len());
    try std.testing.expectEqual(Tuple{ 1, 1 }, rel.elements[0]);
    try std.testing.expectEqual(Tuple{ 1, 2 }, rel.elements[1]);
    try std.testing.expectEqual(Tuple{ 2, 1 }, rel.elements[2]);
}

test "Relation: merge" {
    const allocator = std.testing.allocator;

    var rel1 = try Relation(u32).fromSlice(allocator, &[_]u32{ 1, 3, 5 });
    var rel2 = try Relation(u32).fromSlice(allocator, &[_]u32{ 2, 3, 4 });

    var merged = try rel1.merge(&rel2);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 5), merged.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 4, 5 }, merged.elements);
}

test "Relation: load normalizes order" {
    const allocator = std.testing.allocator;
    const Tuple = struct { u32, u32 };

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer = buffer.writer(allocator);
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 1, .little);
    const raw = [_]Tuple{
        .{ 2, 20 },
        .{ 1, 10 },
        .{ 2, 20 },
    };
    try writer.writeInt(u64, raw.len, .little);
    for (raw) |tuple| {
        const tuple_arr = [_]Tuple{tuple};
        try writer.writeAll(std.mem.sliceAsBytes(&tuple_arr));
    }

    var reader = std.io.fixedBufferStream(buffer.items);
    var rel = try Relation(Tuple).load(allocator, reader.reader());
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 2), rel.len());
    try std.testing.expectEqual(Tuple{ 1, 10 }, rel.elements[0]);
    try std.testing.expectEqual(Tuple{ 2, 20 }, rel.elements[1]);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;
const Variable = @import("variable.zig").Variable;
const gallop = @import("variable.zig").gallop;

pub fn Leaper(comptime Tuple: type, comptime Val: type) type {
    return struct {
        const Self = @This();
        const ValList = std.ArrayListUnmanaged(*const Val);

        ptr: *anyopaque,
        vtable: *const VTable,
        allocator: Allocator,
        had_error: bool = false,

        pub const VTable = struct {
            count: *const fn (ptr: *anyopaque, prefix: *const Tuple) usize,
            propose: *const fn (ptr: *anyopaque, prefix: *const Tuple, alloc: Allocator, values: *ValList, had_error: *bool) void,
            intersect: *const fn (ptr: *anyopaque, prefix: *const Tuple, values: *ValList) void,
        };

        pub fn count(self: Self, prefix: *const Tuple) usize {
            return self.vtable.count(self.ptr, prefix);
        }

        pub fn propose(self: *Self, prefix: *const Tuple, values: *ValList) void {
            self.vtable.propose(self.ptr, prefix, self.allocator, values, &self.had_error);
        }

        pub fn intersect(self: Self, prefix: *const Tuple, values: *ValList) void {
            self.vtable.intersect(self.ptr, prefix, values);
        }
    };
}

pub fn ExtendWith(
    comptime Tuple: type,
    comptime Key: type,
    comptime Val: type,
) type {
    return struct {
        const Self = @This();
        const Rel = Relation(struct { Key, Val });
        const LeaperType = Leaper(Tuple, Val);
        const ValList = std.ArrayListUnmanaged(*const Val);

        relation: *const Rel,
        key_func: *const fn (*const Tuple) Key,
        allocator: Allocator,

        cached_count: usize = 0,
        cached_start: usize = 0,

        pub fn init(allocator: Allocator, relation: *const Rel, key_func: *const fn (*const Tuple) Key) Self {
            return Self{
                .relation = relation,
                .key_func = key_func,
                .allocator = allocator,
            };
        }

        pub fn leaper(self: *Self) LeaperType {
            return LeaperType{
                .ptr = @ptrCast(self),
                .allocator = self.allocator,
                .vtable = &.{
                    .count = countImpl,
                    .propose = proposeImpl,
                    .intersect = intersectImpl,
                },
            };
        }

        fn countImpl(ptr: *anyopaque, prefix: *const Tuple) usize {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const key = self.key_func(prefix);

            const range = findKeyRange(Key, Val, self.relation.elements, key);
            self.cached_start = range.start;
            self.cached_count = range.count;
            return range.count;
        }

        fn proposeImpl(ptr: *anyopaque, prefix: *const Tuple, alloc: Allocator, values: *ValList, had_error: *bool) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            _ = prefix;

            const slice = self.relation.elements[self.cached_start..][0..self.cached_count];
            for (slice) |*elem| {
                values.append(alloc, &elem[1]) catch {
                    had_error.* = true;
                    return;
                };
            }
        }

        fn intersectImpl(ptr: *anyopaque, prefix: *const Tuple, values: *ValList) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const key = self.key_func(prefix);

            var write_idx: usize = 0;
            const range = findKeyRange(Key, Val, self.relation.elements, key);
            const range_slice = self.relation.elements[range.start..][0..range.count];

            for (values.items) |val| {
                if (binarySearchVal(Key, Val, range_slice, val.*)) {
                    values.items[write_idx] = val;
                    write_idx += 1;
                }
            }

            values.shrinkRetainingCapacity(write_idx);
        }
    };
}

pub fn FilterAnti(
    comptime Tuple: type,
    comptime Key: type,
    comptime Val: type,
) type {
    return struct {
        const Self = @This();
        const Rel = Relation(struct { Key, Val });
        const LeaperType = Leaper(Tuple, Val);
        const ValList = std.ArrayListUnmanaged(*const Val);

        relation: *const Rel,
        key_func: *const fn (*const Tuple) struct { Key, Val },
        allocator: Allocator,

        pub fn init(
            allocator: Allocator,
            relation: *const Rel,
            key_func: *const fn (*const Tuple) struct { Key, Val },
        ) Self {
            return Self{
                .relation = relation,
                .key_func = key_func,
                .allocator = allocator,
            };
        }

        pub fn leaper(self: *Self) LeaperType {
            return LeaperType{
                .ptr = @ptrCast(self),
                .allocator = self.allocator,
                .vtable = &.{
                    .count = countImpl,
                    .propose = proposeImpl,
                    .intersect = intersectImpl,
                },
            };
        }

        fn countImpl(ptr: *anyopaque, prefix: *const Tuple) usize {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const kv = self.key_func(prefix);

            const found = binarySearch(Key, Val, self.relation.elements, kv);
            return if (found) 0 else std.math.maxInt(usize);
        }

        fn proposeImpl(_: *anyopaque, _: *const Tuple, _: Allocator, _: *ValList, _: *bool) void {
            unreachable;
        }

        fn intersectImpl(_: *anyopaque, _: *const Tuple, _: *ValList) void {}
    };
}

pub fn ExtendAnti(
    comptime Tuple: type,
    comptime Key: type,
    comptime Val: type,
) type {
    return struct {
        const Self = @This();
        const Rel = Relation(struct { Key, Val });
        const LeaperType = Leaper(Tuple, Val);
        const ValList = std.ArrayListUnmanaged(*const Val);

        relation: *const Rel,
        key_func: *const fn (*const Tuple) Key,
        allocator: Allocator,

        pub fn init(allocator: Allocator, relation: *const Rel, key_func: *const fn (*const Tuple) Key) Self {
            return Self{
                .relation = relation,
                .key_func = key_func,
                .allocator = allocator,
            };
        }

        pub fn leaper(self: *Self) LeaperType {
            return LeaperType{
                .ptr = @ptrCast(self),
                .allocator = self.allocator,
                .vtable = &.{
                    .count = countImpl,
                    .propose = proposeImpl,
                    .intersect = intersectImpl,
                },
            };
        }

        fn countImpl(_: *anyopaque, _: *const Tuple) usize {
            return std.math.maxInt(usize);
        }

        fn proposeImpl(_: *anyopaque, _: *const Tuple, _: Allocator, _: *ValList, _: *bool) void {
            unreachable;
        }

        fn intersectImpl(ptr: *anyopaque, prefix: *const Tuple, values: *ValList) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const key = self.key_func(prefix);

            var write_idx: usize = 0;
            const range = findKeyRange(Key, Val, self.relation.elements, key);
            const range_slice = self.relation.elements[range.start..][0..range.count];

            for (values.items) |val| {
                // For anti-join, keep values NOT found in the relation
                if (!binarySearchVal(Key, Val, range_slice, val.*)) {
                    values.items[write_idx] = val;
                    write_idx += 1;
                }
            }

            values.shrinkRetainingCapacity(write_idx);
        }
    };
}

pub fn extendInto(
    comptime Tuple: type,
    comptime Val: type,
    comptime Result: type,
    source: *Variable(Tuple),
    leapers: []Leaper(Tuple, Val),
    output: *Variable(Result),
    logic: *const fn (*const Tuple, *const Val) Result,
) Allocator.Error!void {
    const ResultList = std.ArrayListUnmanaged(Result);
    const ValList = std.ArrayListUnmanaged(*const Val);

    var results = ResultList{};
    defer results.deinit(output.allocator);

    var values = ValList{};
    defer values.deinit(output.allocator);

    var had_error = false;

    for (source.recent.elements) |*tuple| {
        var min_index: usize = std.math.maxInt(usize);
        var min_count: usize = std.math.maxInt(usize);

        for (leapers, 0..) |leaper, i| {
            const cnt = leaper.count(tuple);
            if (cnt < min_count) {
                min_count = cnt;
                min_index = i;
            }
        }

        if (min_count == 0) continue;

        values.clearRetainingCapacity();
        leapers[min_index].propose(tuple, &values);

        // Check if propose had an allocation error
        if (leapers[min_index].had_error) {
            had_error = true;
            break;
        }

        for (leapers, 0..) |leaper, i| {
            if (i != min_index) {
                leaper.intersect(tuple, &values);
            }
        }

        for (values.items) |val| {
            results.append(output.allocator, logic(tuple, val)) catch {
                had_error = true;
                break;
            };
        }

        if (had_error) break;
    }

    if (had_error) {
        return error.OutOfMemory;
    }

    if (results.items.len > 0) {
        const rel = try Relation(Result).fromSlice(output.allocator, results.items);
        try output.insert(rel);
    }
}

const KeyRange = struct { start: usize, count: usize };

fn findKeyRange(comptime Key: type, comptime Val: type, elements: []const struct { Key, Val }, key: Key) KeyRange {
    if (elements.len == 0) return .{ .start = 0, .count = 0 };

    var lo: usize = 0;
    var hi: usize = elements.len;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (std.math.order(elements[mid][0], key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    const start = lo;
    if (start >= elements.len or std.math.order(elements[start][0], key) != .eq) {
        return .{ .start = start, .count = 0 };
    }

    var cnt: usize = 0;
    for (elements[start..]) |elem| {
        if (std.math.order(elem[0], key) != .eq) break;
        cnt += 1;
    }

    return .{ .start = start, .count = cnt };
}

fn binarySearch(comptime Key: type, comptime Val: type, elements: []const struct { Key, Val }, target: struct { Key, Val }) bool {
    var lo: usize = 0;
    var hi: usize = elements.len;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const cmp = compareKV(Key, Val, elements[mid], target);
        switch (cmp) {
            .lt => lo = mid + 1,
            .gt => hi = mid,
            .eq => return true,
        }
    }
    return false;
}

fn compareKV(comptime Key: type, comptime Val: type, a: struct { Key, Val }, b: struct { Key, Val }) std.math.Order {
    const key_order = std.math.order(a[0], b[0]);
    if (key_order != .eq) return key_order;
    return std.math.order(a[1], b[1]);
}

/// Binary search for a value within a slice of (key,val) tuples.
/// Since tuples are sorted by (key, val), values within the same key range are sorted.
fn binarySearchVal(comptime Key: type, comptime Val: type, elements: []const struct { Key, Val }, target_val: Val) bool {
    var lo: usize = 0;
    var hi: usize = elements.len;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.math.order(elements[mid][1], target_val)) {
            .lt => lo = mid + 1,
            .gt => hi = mid,
            .eq => return true,
        }
    }
    return false;
}

test "ExtendWith: basic" {
    const allocator = std.testing.allocator;
    const KV = struct { u32, u32 };

    var rel = try Relation(KV).fromSlice(allocator, &[_]KV{
        .{ 1, 10 },
        .{ 1, 11 },
        .{ 2, 20 },
    });
    defer rel.deinit();

    var ext = ExtendWith(u32, u32, u32).init(allocator, &rel, struct {
        fn f(t: *const u32) u32 {
            return t.*;
        }
    }.f);

    const tuple: u32 = 1;
    const cnt = ext.leaper().count(&tuple);
    try std.testing.expectEqual(@as(usize, 2), cnt);
}

test "FilterAnti: filters matching tuples" {
    const allocator = std.testing.allocator;
    const KV = struct { u32, u32 };
    const Tuple = struct { u32, u32 };

    var rel = try Relation(KV).fromSlice(allocator, &[_]KV{
        .{ 1, 10 },
        .{ 2, 20 },
    });
    defer rel.deinit();

    var filter = FilterAnti(Tuple, u32, u32).init(allocator, &rel, struct {
        fn f(t: *const Tuple) KV {
            return .{ t[0], t[1] };
        }
    }.f);

    const present: Tuple = .{ 1, 10 };
    const absent: Tuple = .{ 3, 30 };

    try std.testing.expectEqual(@as(usize, 0), filter.leaper().count(&present));
    try std.testing.expectEqual(std.math.maxInt(usize), filter.leaper().count(&absent));
}

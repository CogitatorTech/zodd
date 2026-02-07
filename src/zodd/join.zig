const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;
const Variable = @import("variable.zig").Variable;
const gallop = @import("variable.zig").gallop;

pub fn joinHelper(
    comptime Key: type,
    comptime Val1: type,
    comptime Val2: type,
    input1: *const Relation(struct { Key, Val1 }),
    input2: *const Relation(struct { Key, Val2 }),
    context: anytype,
    result: fn (@TypeOf(context), *const Key, *const Val1, *const Val2) void,
) void {
    const Tuple1 = struct { Key, Val1 };
    const Tuple2 = struct { Key, Val2 };
    var slice1: []const Tuple1 = input1.elements;
    var slice2: []const Tuple2 = input2.elements;

    while (slice1.len > 0 and slice2.len > 0) {
        const key1 = slice1[0][0];
        const key2 = slice2[0][0];

        const order = std.math.order(key1, key2);

        switch (order) {
            .lt => {
                slice1 = gallopKey(Key, Val1, slice1, key2);
            },
            .gt => {
                slice2 = gallopKey(Key, Val2, slice2, key1);
            },
            .eq => {
                const count1 = countMatchingKeys(Key, Val1, slice1, key1);
                const count2 = countMatchingKeys(Key, Val2, slice2, key2);

                for (slice1[0..count1]) |t1| {
                    for (slice2[0..count2]) |t2| {
                        result(context, &t1[0], &t1[1], &t2[1]);
                    }
                }

                slice1 = slice1[count1..];
                slice2 = slice2[count2..];
            },
        }
    }
}

fn countMatchingKeys(comptime Key: type, comptime Val: type, slice: []const struct { Key, Val }, key: Key) usize {
    var count: usize = 0;
    for (slice) |elem| {
        if (std.math.order(elem[0], key) != .eq) break;
        count += 1;
    }
    return count;
}

fn gallopKey(comptime Key: type, comptime Val: type, slice: []const struct { Key, Val }, target_key: Key) []const struct { Key, Val } {
    if (slice.len == 0) return slice;
    if (std.math.order(slice[0][0], target_key) != .lt) return slice;

    var step: usize = 1;
    var pos: usize = 0;

    while (pos + step < slice.len and std.math.order(slice[pos + step][0], target_key) == .lt) {
        pos += step;
        step *= 2;
    }

    const end = @min(pos + step + 1, slice.len);
    var lo = pos + 1;
    var hi = end;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (std.math.order(slice[mid][0], target_key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return slice[lo..];
}

pub fn joinInto(
    comptime Key: type,
    comptime Val1: type,
    comptime Val2: type,
    comptime Result: type,
    input1: *Variable(struct { Key, Val1 }),
    input2: *Variable(struct { Key, Val2 }),
    output: *Variable(Result),
    logic: fn (*const Key, *const Val1, *const Val2) Result,
) Allocator.Error!void {
    const ResultList = std.ArrayListUnmanaged(Result);
    var results = ResultList{};
    defer results.deinit(output.allocator);

    const Context = struct {
        results: *ResultList,
        alloc: Allocator,
        logic: *const fn (*const Key, *const Val1, *const Val2) Result,
        had_error: *bool,

        fn callback(self: @This(), key: *const Key, v1: *const Val1, v2: *const Val2) void {
            self.results.append(self.alloc, self.logic(key, v1, v2)) catch {
                self.had_error.* = true;
            };
        }
    };

    var had_error = false;
    const ctx = Context{ .results = &results, .alloc = output.allocator, .logic = &logic, .had_error = &had_error };

    for (input2.stable.items) |*batch2| {
        joinHelper(Key, Val1, Val2, &input1.recent, batch2, ctx, Context.callback);
    }

    for (input1.stable.items) |*batch1| {
        joinHelper(Key, Val1, Val2, batch1, &input2.recent, ctx, Context.callback);
    }

    joinHelper(Key, Val1, Val2, &input1.recent, &input2.recent, ctx, Context.callback);

    if (had_error) {
        return error.OutOfMemory;
    }

    if (results.items.len > 0) {
        const rel = try Relation(Result).fromSlice(output.allocator, results.items);
        try output.insert(rel);
    }
}

test "joinHelper: basic" {
    const Tuple1 = struct { u32, u32 };
    const Tuple2 = struct { u32, u32 };

    const allocator = std.testing.allocator;

    var input1 = try Relation(Tuple1).fromSlice(allocator, &[_]Tuple1{
        .{ 1, 10 },
        .{ 2, 20 },
        .{ 3, 30 },
    });
    defer input1.deinit();

    var input2 = try Relation(Tuple2).fromSlice(allocator, &[_]Tuple2{
        .{ 2, 200 },
        .{ 3, 300 },
        .{ 3, 301 },
        .{ 4, 400 },
    });
    defer input2.deinit();

    const ResultList = std.ArrayListUnmanaged(struct { u32, u32, u32 });
    const Context = struct {
        results: *ResultList,
        alloc: Allocator,

        fn callback(self: @This(), key: *const u32, v1: *const u32, v2: *const u32) void {
            self.results.append(self.alloc, .{ key.*, v1.*, v2.* }) catch {};
        }
    };

    var results = ResultList{};
    defer results.deinit(allocator);

    joinHelper(u32, u32, u32, &input1, &input2, Context{ .results = &results, .alloc = allocator }, Context.callback);

    try std.testing.expectEqual(@as(usize, 3), results.items.len);
}

test "joinInto: variable join" {
    const allocator = std.testing.allocator;

    const Tuple = struct { u32, u32 };
    var v1 = Variable(Tuple).init(allocator);
    defer v1.deinit();

    var v2 = Variable(Tuple).init(allocator);
    defer v2.deinit();

    var output = Variable(struct { u32, u32, u32 }).init(allocator);
    defer output.deinit();

    try v1.insertSlice(&[_]Tuple{ .{ 1, 10 }, .{ 2, 20 } });
    try v2.insertSlice(&[_]Tuple{ .{ 2, 200 }, .{ 3, 300 } });

    _ = try v1.changed();
    _ = try v2.changed();

    try joinInto(u32, u32, u32, struct { u32, u32, u32 }, &v1, &v2, &output, struct {
        fn logic(key: *const u32, v1_val: *const u32, v2_val: *const u32) struct { u32, u32, u32 } {
            return .{ key.*, v1_val.*, v2_val.* };
        }
    }.logic);

    _ = try output.changed();
    try std.testing.expectEqual(@as(usize, 1), output.recent.len());
}

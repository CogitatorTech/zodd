//! Join algorithms including merge-join and leapfrog trie join.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;
const Variable = @import("variable.zig").Variable;
const gallop = @import("variable.zig").gallop;
const ExecutionContext = @import("context.zig").ExecutionContext;

/// Helper function for joining two sorted relations.
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

    while (true) {
        const next_pos = std.math.add(usize, pos, step) catch slice.len;
        if (next_pos >= slice.len or next_pos < pos) break;
        if (std.math.order(slice[next_pos][0], target_key) != .lt) break;
        pos = next_pos;
        const new_step = std.math.mul(usize, step, 2) catch std.math.maxInt(usize);
        step = new_step;
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

/// Joins two variables and inserts the result into an output variable.
pub fn joinInto(
    comptime Key: type,
    comptime Val1: type,
    comptime Val2: type,
    comptime Result: type,
    ctx: *ExecutionContext,
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
    const cb_ctx = Context{ .results = &results, .alloc = output.allocator, .logic = &logic, .had_error = &had_error };

    if (ctx.pool != null and (input1.stable.items.len + input2.stable.items.len) > 0) {
        const Task = struct {
            left: *const Relation(struct { Key, Val1 }),
            right: *const Relation(struct { Key, Val2 }),
            results: std.ArrayListUnmanaged(Result) = .{},
            alloc: Allocator,
            had_error: bool = false,

            fn run(task: *@This()) void {
                const TaskContext = struct {
                    results: *std.ArrayListUnmanaged(Result),
                    alloc: Allocator,
                    logic: *const fn (*const Key, *const Val1, *const Val2) Result,
                    had_error: *bool,

                    fn callback(self: @This(), key: *const Key, v1: *const Val1, v2: *const Val2) void {
                        self.results.append(self.alloc, self.logic(key, v1, v2)) catch {
                            self.had_error.* = true;
                        };
                    }
                };

                const ctx_local = TaskContext{
                    .results = &task.results,
                    .alloc = task.alloc,
                    .logic = &logic,
                    .had_error = &task.had_error,
                };

                joinHelper(Key, Val1, Val2, task.left, task.right, ctx_local, TaskContext.callback);
            }
        };

        const task_count = input1.stable.items.len + input2.stable.items.len;
        const tasks = try ctx.allocator.alloc(Task, task_count);
        defer ctx.allocator.free(tasks);

        var idx: usize = 0;
        for (input2.stable.items) |*batch2| {
            tasks[idx] = .{ .left = &input1.recent, .right = batch2, .alloc = output.allocator };
            idx += 1;
        }
        for (input1.stable.items) |*batch1| {
            tasks[idx] = .{ .left = batch1, .right = &input2.recent, .alloc = output.allocator };
            idx += 1;
        }

        if (ctx.pool) |*pool| {
            var wg: std.Thread.WaitGroup = .{};
            for (tasks) |*task| {
                pool.*.spawnWg(&wg, Task.run, .{task});
            }
            wg.wait();
        }

        for (tasks) |*task| {
            defer task.results.deinit(output.allocator);
            if (task.had_error) {
                return error.OutOfMemory;
            }
            if (task.results.items.len > 0) {
                try results.appendSlice(output.allocator, task.results.items);
            }
        }
    } else {
        for (input2.stable.items) |*batch2| {
            joinHelper(Key, Val1, Val2, &input1.recent, batch2, cb_ctx, Context.callback);
        }

        for (input1.stable.items) |*batch1| {
            joinHelper(Key, Val1, Val2, batch1, &input2.recent, cb_ctx, Context.callback);
        }
    }

    joinHelper(Key, Val1, Val2, &input1.recent, &input2.recent, cb_ctx, Context.callback);

    if (had_error) {
        return error.OutOfMemory;
    }

    if (results.items.len > 0) {
        const rel = try Relation(Result).fromSlice(ctx, results.items);
        try output.insert(rel);
    }
}

/// Performs an anti-join between a variable and a filter variable.
pub fn joinAnti(
    comptime Key: type,
    comptime Val: type,
    comptime FilterVal: type,
    comptime Result: type,
    ctx: *ExecutionContext,
    input: *Variable(struct { Key, Val }),
    filter: *Variable(struct { Key, FilterVal }),
    output: *Variable(Result),
    logic: fn (*const Key, *const Val) Result,
) Allocator.Error!void {
    const ResultList = std.ArrayListUnmanaged(Result);
    var results = ResultList{};
    defer results.deinit(output.allocator);

    if (ctx.pool != null and input.recent.elements.len > 0) {
        const Task = struct {
            slice: []const struct { Key, Val },
            filter: *const Variable(struct { Key, FilterVal }),
            results: std.ArrayListUnmanaged(Result) = .{},
            alloc: Allocator,
            logic: *const fn (*const Key, *const Val) Result,
            had_error: bool = false,

            fn run(task: *@This()) void {
                for (task.slice) |tuple| {
                    const key = tuple[0];
                    var found = false;

                    {
                        const slice = gallopKey(Key, FilterVal, task.filter.recent.elements, key);
                        if (slice.len > 0 and countMatchingKeys(Key, FilterVal, slice, key) > 0) {
                            found = true;
                        }
                    }

                    if (!found) {
                        for (task.filter.stable.items) |*batch| {
                            const slice = gallopKey(Key, FilterVal, batch.elements, key);
                            if (slice.len > 0 and countMatchingKeys(Key, FilterVal, slice, key) > 0) {
                                found = true;
                                break;
                            }
                        }
                    }

                    if (!found) {
                        task.results.append(task.alloc, task.logic(&key, &tuple[1])) catch {
                            task.had_error = true;
                            return;
                        };
                    }
                }
            }
        };

        const chunk: usize = 128;
        const task_count = (input.recent.elements.len + chunk - 1) / chunk;
        const tasks = try ctx.allocator.alloc(Task, task_count);
        defer ctx.allocator.free(tasks);

        var i: usize = 0;
        while (i < task_count) : (i += 1) {
            const start = i * chunk;
            const end = @min(start + chunk, input.recent.elements.len);
            tasks[i] = .{
                .slice = input.recent.elements[start..end],
                .filter = filter,
                .alloc = output.allocator,
                .logic = &logic,
            };
        }

        if (ctx.pool) |*pool| {
            var wg: std.Thread.WaitGroup = .{};
            for (tasks) |*task| {
                pool.*.spawnWg(&wg, Task.run, .{task});
            }
            wg.wait();
        }

        for (tasks) |*task| {
            defer task.results.deinit(output.allocator);
            if (task.had_error) {
                return error.OutOfMemory;
            }
            if (task.results.items.len > 0) {
                try results.appendSlice(output.allocator, task.results.items);
            }
        }
    } else {
        for (input.recent.elements) |tuple| {
            const key = tuple[0];
            var found = false;

            {
                const slice = gallopKey(Key, FilterVal, filter.recent.elements, key);
                if (slice.len > 0 and countMatchingKeys(Key, FilterVal, slice, key) > 0) {
                    found = true;
                }
            }

            if (!found) {
                for (filter.stable.items) |*batch| {
                    const slice = gallopKey(Key, FilterVal, batch.elements, key);
                    if (slice.len > 0 and countMatchingKeys(Key, FilterVal, slice, key) > 0) {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                try results.append(output.allocator, logic(&key, &tuple[1]));
            }
        }
    }

    if (results.items.len > 0) {
        const rel = try Relation(Result).fromSlice(ctx, results.items);
        try output.insert(rel);
    }
}

test "joinHelper: basic" {
    const Tuple1 = struct { u32, u32 };
    const Tuple2 = struct { u32, u32 };

    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var input1 = try Relation(Tuple1).fromSlice(&ctx, &[_]Tuple1{
        .{ 1, 10 },
        .{ 2, 20 },
        .{ 3, 30 },
    });
    defer input1.deinit();

    var input2 = try Relation(Tuple2).fromSlice(&ctx, &[_]Tuple2{
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
    var ctx = ExecutionContext.init(allocator);

    const Tuple = struct { u32, u32 };
    var v1 = Variable(Tuple).init(&ctx);
    defer v1.deinit();

    var v2 = Variable(Tuple).init(&ctx);
    defer v2.deinit();

    var output = Variable(struct { u32, u32, u32 }).init(&ctx);
    defer output.deinit();

    try v1.insertSlice(&ctx, &[_]Tuple{ .{ 1, 10 }, .{ 2, 20 } });
    try v2.insertSlice(&ctx, &[_]Tuple{ .{ 2, 200 }, .{ 3, 300 } });

    _ = try v1.changed();
    _ = try v2.changed();

    try joinInto(u32, u32, u32, struct { u32, u32, u32 }, &ctx, &v1, &v2, &output, struct {
        fn logic(key: *const u32, v1_val: *const u32, v2_val: *const u32) struct { u32, u32, u32 } {
            return .{ key.*, v1_val.*, v2_val.* };
        }
    }.logic);

    _ = try output.changed();
    try std.testing.expectEqual(@as(usize, 1), output.recent.len());
}

test "joinAnti: simple negation" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var input = Variable(Tuple).init(&ctx);
    defer input.deinit();

    var filter = Variable(Tuple).init(&ctx);
    defer filter.deinit();

    var output = Variable(Tuple).init(&ctx);
    defer output.deinit();

    try input.insertSlice(&ctx, &[_]Tuple{ .{ 1, 10 }, .{ 2, 20 }, .{ 3, 30 } });
    try filter.insertSlice(&ctx, &[_]Tuple{.{ 2, 200 }});

    _ = try input.changed();
    _ = try filter.changed();

    try joinAnti(u32, u32, u32, Tuple, &ctx, &input, &filter, &output, struct {
        fn logic(key: *const u32, val: *const u32) Tuple {
            return .{ key.*, val.* };
        }
    }.logic);

    _ = try output.changed();

    try std.testing.expectEqual(@as(usize, 2), output.recent.len());
    try std.testing.expectEqual(output.recent.elements[0][0], 1);
    try std.testing.expectEqual(output.recent.elements[1][0], 3);
}

test "joinHelper: multiplicative matches" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const T1 = struct { u32, u32 };
    const T2 = struct { u32, u32 };

    var input1 = try Relation(T1).fromSlice(&ctx, &[_]T1{
        .{ 1, 10 },
        .{ 1, 11 },
        .{ 2, 20 },
    });
    defer input1.deinit();

    var input2 = try Relation(T2).fromSlice(&ctx, &[_]T2{
        .{ 1, 100 },
        .{ 1, 101 },
        .{ 2, 200 },
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

    try std.testing.expectEqual(@as(usize, 5), results.items.len);
}

test "joinInto: stable batches only" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var v1 = Variable(Tuple).init(&ctx);
    defer v1.deinit();

    var v2 = Variable(Tuple).init(&ctx);
    defer v2.deinit();

    var output = Variable(struct { u32, u32, u32 }).init(&ctx);
    defer output.deinit();

    try v1.insertSlice(&ctx, &[_]Tuple{.{ 1, 10 }});
    _ = try v1.changed();
    _ = try v1.changed();

    try v2.insertSlice(&ctx, &[_]Tuple{ .{ 1, 100 }, .{ 2, 200 } });
    _ = try v2.changed();
    _ = try v2.changed();

    try joinInto(u32, u32, u32, struct { u32, u32, u32 }, &ctx, &v1, &v2, &output, struct {
        fn logic(key: *const u32, v1_val: *const u32, v2_val: *const u32) struct { u32, u32, u32 } {
            return .{ key.*, v1_val.*, v2_val.* };
        }
    }.logic);

    const changed = try output.changed();
    try std.testing.expect(!changed);
    try std.testing.expectEqual(@as(usize, 0), output.recent.len());
}

test "joinAnti: parallel" {
    const allocator = std.testing.allocator;
    var ctx = try ExecutionContext.initWithThreads(allocator, 2);
    defer ctx.deinit();
    const Tuple = struct { u32, u32 };

    var input = Variable(Tuple).init(&ctx);
    defer input.deinit();

    var filter = Variable(Tuple).init(&ctx);
    defer filter.deinit();

    var output = Variable(Tuple).init(&ctx);
    defer output.deinit();

    try input.insertSlice(&ctx, &[_]Tuple{ .{ 1, 10 }, .{ 2, 20 }, .{ 3, 30 }, .{ 4, 40 } });
    try filter.insertSlice(&ctx, &[_]Tuple{ .{ 2, 200 }, .{ 4, 400 } });

    _ = try input.changed();
    _ = try filter.changed();

    try joinAnti(u32, u32, u32, Tuple, &ctx, &input, &filter, &output, struct {
        fn logic(key: *const u32, val: *const u32) Tuple {
            return .{ key.*, val.* };
        }
    }.logic);

    _ = try output.changed();

    try std.testing.expectEqual(@as(usize, 2), output.recent.len());
    try std.testing.expectEqual(@as(u32, 1), output.recent.elements[0][0]);
    try std.testing.expectEqual(@as(u32, 3), output.recent.elements[1][0]);
}

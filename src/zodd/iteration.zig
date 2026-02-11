//! Fixpoint iteration logic for semi-naive evaluation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Variable = @import("variable.zig").Variable;
const Relation = @import("relation.zig").Relation;
const ExecutionContext = @import("context.zig").ExecutionContext;

pub fn Iteration(comptime Tuple: type) type {
    return struct {
        const Self = @This();
        const Var = Variable(Tuple);
        const VarList = std.ArrayListUnmanaged(*Var);

        variables: VarList,
        allocator: Allocator,
        ctx: *ExecutionContext,
        max_iterations: usize,
        current_iteration: usize,

        pub fn init(ctx: *ExecutionContext, max_iterations: ?usize) Self {
            return Self{
                .variables = VarList{},
                .allocator = ctx.allocator,
                .ctx = ctx,
                .max_iterations = max_iterations orelse std.math.maxInt(usize),
                .current_iteration = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.variables.items) |v| {
                v.deinit();
                self.allocator.destroy(v);
            }
            self.variables.deinit(self.allocator);
        }

        pub fn variable(self: *Self) Allocator.Error!*Var {
            const v = try self.allocator.create(Var);
            v.* = Var.init(self.ctx);
            try self.variables.append(self.allocator, v);
            return v;
        }

        pub fn changed(self: *Self) !bool {
            if (self.current_iteration >= self.max_iterations) {
                return error.MaxIterationsExceeded;
            }
            self.current_iteration += 1;

            if (self.ctx.pool) |pool| {
                if (self.variables.items.len > 1) {
                    return self.changedParallel(pool);
                }
            }

            var any_changed = false;
            for (self.variables.items) |v| {
                if (try v.changed()) {
                    any_changed = true;
                }
            }
            return any_changed;
        }

        fn changedParallel(self: *Self, pool: *std.Thread.Pool) !bool {
            const count = self.variables.items.len;
            const Task = struct {
                var_ptr: *Var,
                changed: bool = false,
                err: ?anyerror = null,

                fn run(task: *@This()) void {
                    task.changed = task.var_ptr.changed() catch |err| {
                        task.err = err;
                        return;
                    };
                }
            };

            const tasks = try self.allocator.alloc(Task, count);
            defer self.allocator.free(tasks);

            var wg: std.Thread.WaitGroup = .{};
            for (self.variables.items, 0..) |v, i| {
                tasks[i] = .{ .var_ptr = v };
                pool.spawnWg(&wg, Task.run, .{&tasks[i]});
            }

            wg.wait();

            var any_changed = false;
            for (tasks) |task| {
                if (task.err) |err| return err;
                if (task.changed) any_changed = true;
            }
            return any_changed;
        }

        pub fn reset(self: *Self) void {
            self.current_iteration = 0;
        }
    };
}

test "Iteration: basic usage" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var iter = Iteration(u32).init(&ctx, null);
    defer iter.deinit();

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&ctx, &[_]u32{ 1, 2, 3 });
    try v2.insertSlice(&ctx, &[_]u32{ 4, 5 });

    const changed1 = try iter.changed();
    try std.testing.expect(changed1);

    const changed2 = try iter.changed();
    try std.testing.expect(!changed2);
}

test "Iteration: recursion limit" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var iter = Iteration(u32).init(&ctx, 1);
    defer iter.deinit();

    const v = try iter.variable();
    try v.insertSlice(&ctx, &[_]u32{1});

    _ = try iter.changed();

    try std.testing.expectError(error.MaxIterationsExceeded, iter.changed());
}

test "Iteration: reset" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    var iter = Iteration(u32).init(&ctx, 10);
    defer iter.deinit();

    const v = try iter.variable();
    try v.insertSlice(&ctx, &[_]u32{1});

    // Run some iterations
    _ = try iter.changed();
    try std.testing.expectEqual(@as(usize, 1), iter.current_iteration);

    iter.reset();
    try std.testing.expectEqual(@as(usize, 0), iter.current_iteration);

    // Can run again
    _ = try iter.changed();
    try std.testing.expectEqual(@as(usize, 1), iter.current_iteration);
}

test "Iteration: reset without new data" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var iter = Iteration(u32).init(&ctx, 10);
    defer iter.deinit();

    const v = try iter.variable();
    try v.insertSlice(&ctx, &[_]u32{1});

    _ = try iter.changed();
    const changed2 = try iter.changed();
    try std.testing.expect(!changed2);

    iter.reset();

    const changed3 = try iter.changed();
    try std.testing.expect(!changed3);
}

test "Iteration: parallel changed" {
    const allocator = std.testing.allocator;
    var ctx = try ExecutionContext.initWithThreads(allocator, 2);
    defer ctx.deinit();

    var iter = Iteration(u32).init(&ctx, null);
    defer iter.deinit();

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&ctx, &[_]u32{ 1, 2, 3 });
    try v2.insertSlice(&ctx, &[_]u32{ 4, 5 });

    const changed1 = try iter.changed();
    try std.testing.expect(changed1);

    const changed2 = try iter.changed();
    try std.testing.expect(!changed2);
}

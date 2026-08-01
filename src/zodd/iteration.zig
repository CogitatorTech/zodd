//! # Iteration Manager
//!
//! The manager handles the fixed-point iteration loop for semi-naive evaluation.
//!
//! It orchestrates the evolution of multiple `Variable` instances, checking for convergence
//! (when no new facts are added).

const std = @import("std");
const Allocator = std.mem.Allocator;
const Variable = @import("variable.zig").Variable;
const Relation = @import("relation.zig").Relation;

/// Errors returned by `Iteration.changed`.
pub const IterateError = Allocator.Error || error{MaxIterationsExceeded};

pub fn Iteration(comptime Tuple: type) type {
    return struct {
        const Self = @This();
        const Var = Variable(Tuple);
        const VarList = std.ArrayListUnmanaged(*Var);

        /// List of variables in the iteration.
        variables: VarList,
        /// Allocator for the iteration.
        allocator: Allocator,
        /// Maximum number of iterations allowed.
        max_iterations: usize,
        /// Current iteration count.
        current_iteration: usize,

        /// Initializes a new iteration. `max_iterations` of `null` means no
        /// limit; otherwise `changed` returns `error.MaxIterationsExceeded`
        /// after that many steps.
        pub fn init(allocator: Allocator, max_iterations: ?usize) Self {
            return Self{
                .variables = VarList.empty,
                .allocator = allocator,
                .max_iterations = max_iterations orelse std.math.maxInt(usize),
                .current_iteration = 0,
            };
        }

        /// Deinitializes the iteration and every `Variable` it owns.
        pub fn deinit(self: *Self) void {
            for (self.variables.items) |v| {
                v.deinit();
                self.allocator.destroy(v);
            }
            self.variables.deinit(self.allocator);
        }

        /// Creates a new variable owned by this iteration.
        pub fn variable(self: *Self) Allocator.Error!*Var {
            const v = try self.allocator.create(Var);
            errdefer self.allocator.destroy(v);
            v.* = Var.init(self.allocator);
            try self.variables.append(self.allocator, v);
            return v;
        }

        /// Runs one step of the iteration and returns true if any variable
        /// changed.
        pub fn changed(self: *Self) IterateError!bool {
            if (self.current_iteration >= self.max_iterations) {
                return error.MaxIterationsExceeded;
            }
            self.current_iteration += 1;

            var any_changed = false;
            for (self.variables.items) |v| {
                if (try v.changed()) {
                    any_changed = true;
                }
            }
            return any_changed;
        }

        /// Resets the iteration step counter. Does not touch the variables.
        pub fn reset(self: *Self) void {
            self.current_iteration = 0;
        }
    };
}

test "Iteration: basic usage" {
    const allocator = std.testing.allocator;

    var iter = Iteration(u32).init(allocator, null);
    defer iter.deinit();

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&[_]u32{ 1, 2, 3 });
    try v2.insertSlice(&[_]u32{ 4, 5 });

    const changed1 = try iter.changed();
    try std.testing.expect(changed1);

    const changed2 = try iter.changed();
    try std.testing.expect(!changed2);
}

test "Iteration: recursion limit" {
    const allocator = std.testing.allocator;

    var iter = Iteration(u32).init(allocator, 1);
    defer iter.deinit();

    const v = try iter.variable();
    try v.insertSlice(&[_]u32{1});

    _ = try iter.changed();

    try std.testing.expectError(error.MaxIterationsExceeded, iter.changed());
}

test "Iteration: reset" {
    const allocator = std.testing.allocator;
    var iter = Iteration(u32).init(allocator, 10);
    defer iter.deinit();

    const v = try iter.variable();
    try v.insertSlice(&[_]u32{1});

    _ = try iter.changed();
    try std.testing.expectEqual(@as(usize, 1), iter.current_iteration);

    iter.reset();
    try std.testing.expectEqual(@as(usize, 0), iter.current_iteration);

    _ = try iter.changed();
    try std.testing.expectEqual(@as(usize, 1), iter.current_iteration);
}

test "Iteration: reset without new data" {
    const allocator = std.testing.allocator;

    var iter = Iteration(u32).init(allocator, 10);
    defer iter.deinit();

    const v = try iter.variable();
    try v.insertSlice(&[_]u32{1});

    _ = try iter.changed();
    const changed2 = try iter.changed();
    try std.testing.expect(!changed2);

    iter.reset();

    const changed3 = try iter.changed();
    try std.testing.expect(!changed3);
}

test "Iteration: variable creation failure does not leak" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: Allocator) !void {
            var iter = Iteration(u32).init(allocator, null);
            defer iter.deinit();
            _ = try iter.variable();
            _ = try iter.variable();
        }
    }.run, .{});
}

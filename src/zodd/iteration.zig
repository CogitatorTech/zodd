const std = @import("std");
const Allocator = std.mem.Allocator;
const Variable = @import("variable.zig").Variable;
const Relation = @import("relation.zig").Relation;

pub fn Iteration(comptime Tuple: type) type {
    return struct {
        const Self = @This();
        const Var = Variable(Tuple);
        const VarList = std.ArrayListUnmanaged(*Var);

        variables: VarList,
        allocator: Allocator,
        max_iterations: usize,
        current_iteration: usize,

        pub fn init(allocator: Allocator, max_iterations: ?usize) Self {
            return Self{
                .variables = VarList{},
                .allocator = allocator,
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
            v.* = Var.init(self.allocator);
            try self.variables.append(self.allocator, v);
            return v;
        }

        pub fn changed(self: *Self) !bool {
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

    // First iteration ok
    _ = try iter.changed();

    // Second iteration should fail
    try std.testing.expectError(error.MaxIterationsExceeded, iter.changed());
}

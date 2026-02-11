//! Context management for parallel execution.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ExecutionContext = struct {
    allocator: Allocator,
    pool: ?*std.Thread.Pool = null,

    pub fn init(allocator: Allocator) ExecutionContext {
        return .{ .allocator = allocator, .pool = null };
    }

    pub fn initWithThreads(allocator: Allocator, worker_count: usize) !ExecutionContext {
        const pool = try allocator.create(std.Thread.Pool);
        errdefer allocator.destroy(pool);
        try std.Thread.Pool.init(pool, .{ .allocator = allocator, .n_jobs = worker_count });
        return .{ .allocator = allocator, .pool = pool };
    }

    pub fn deinit(self: *ExecutionContext) void {
        if (self.pool) |pool| {
            pool.deinit();
            self.allocator.destroy(pool);
        }
        self.pool = null;
    }

    pub fn hasParallel(self: *const ExecutionContext) bool {
        return self.pool != null;
    }
};

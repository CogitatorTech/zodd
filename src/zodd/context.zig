//! Context management for parallel execution.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ExecutionContext = struct {
    /// Allocator for the context.
    allocator: Allocator,
    /// Thread pool for parallel execution.
    pool: ?*std.Thread.Pool = null,

    /// Initializes a new execution context.
    pub fn init(allocator: Allocator) ExecutionContext {
        return .{ .allocator = allocator, .pool = null };
    }

    /// Initializes a new execution context with a thread pool.
    pub fn initWithThreads(allocator: Allocator, worker_count: usize) !ExecutionContext {
        const pool = try allocator.create(std.Thread.Pool);
        errdefer allocator.destroy(pool);
        try std.Thread.Pool.init(pool, .{ .allocator = allocator, .n_jobs = worker_count });
        return .{ .allocator = allocator, .pool = pool };
    }

    /// Deinitializes the execution context.
    pub fn deinit(self: *ExecutionContext) void {
        if (self.pool) |pool| {
            pool.deinit();
            self.allocator.destroy(pool);
        }
        self.pool = null;
    }

    /// Returns true if the context has a thread pool.
    pub fn hasParallel(self: *const ExecutionContext) bool {
        return self.pool != null;
    }
};

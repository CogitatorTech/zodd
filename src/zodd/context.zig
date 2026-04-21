//! # Execution Context
//!
//! The context manages shared resources for query execution, primarily the memory allocator
//! and optional thread pool.
//!
//! Users pass it to Zodd operations to access resources.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// WaitGroup is a no-op synchronization marker. Since `Pool.spawnWg` currently
/// runs work inline on the calling thread, no real waiting is needed. The type
/// exists so that call sites can continue to declare `var wg: WaitGroup = .{}`
/// and call `wg.wait()` without churn.
pub const WaitGroup = struct {
    pub fn wait(self: *WaitGroup) void {
        _ = self;
    }
};

/// Pool replaces the `std.Thread.Pool` removed in Zig 0.16. It currently
/// executes submitted work synchronously on the calling thread. This preserves
/// the call-site API so that the parallel code paths still compile and behave
/// correctly; real parallel execution can be layered back on once a stable
/// 0.16 concurrency story lands in user-facing std (today it lives behind
/// `std.Io` backends).
pub const Pool = struct {
    allocator: Allocator,

    pub const Options = struct {
        allocator: Allocator,
        n_jobs: ?usize = null,
    };

    pub fn init(self: *Pool, options: Options) !void {
        self.* = .{ .allocator = options.allocator };
    }

    pub fn deinit(self: *Pool) void {
        self.* = undefined;
    }

    pub fn spawnWg(self: *Pool, wg: *WaitGroup, comptime func: anytype, args: anytype) void {
        _ = self;
        _ = wg;
        @call(.auto, func, args);
    }
};

pub const ExecutionContext = struct {
    /// Allocator for the context.
    allocator: Allocator,
    /// Thread pool for parallel execution.
    pool: ?*Pool = null,

    /// Initializes a new execution context.
    pub fn init(allocator: Allocator) ExecutionContext {
        return .{ .allocator = allocator, .pool = null };
    }

    /// Initializes a new execution context with a thread pool.
    pub fn initWithThreads(allocator: Allocator, worker_count: usize) !ExecutionContext {
        const pool = try allocator.create(Pool);
        errdefer allocator.destroy(pool);
        try Pool.init(pool, .{ .allocator = allocator, .n_jobs = worker_count });
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

test "ExecutionContext: init has no pool" {
    var ctx = ExecutionContext.init(std.testing.allocator);
    defer ctx.deinit();

    try std.testing.expect(!ctx.hasParallel());
    try std.testing.expectEqual(@as(?*Pool, null), ctx.pool);
}

test "ExecutionContext: initWithThreads attaches a pool and deinit releases it" {
    var ctx = try ExecutionContext.initWithThreads(std.testing.allocator, 2);
    defer ctx.deinit();

    try std.testing.expect(ctx.hasParallel());
    try std.testing.expect(ctx.pool != null);
}

test "ExecutionContext: deinit is idempotent" {
    var ctx = try ExecutionContext.initWithThreads(std.testing.allocator, 1);
    ctx.deinit();
    // Second deinit should be a no-op; pool was nulled out.
    ctx.deinit();
    try std.testing.expect(!ctx.hasParallel());
}

test "Pool: spawnWg runs the submitted function" {
    var pool: Pool = undefined;
    try Pool.init(&pool, .{ .allocator = std.testing.allocator, .n_jobs = 2 });
    defer pool.deinit();

    var ran: bool = false;
    var wg: WaitGroup = .{};
    pool.spawnWg(&wg, struct {
        fn run(flag: *bool) void {
            flag.* = true;
        }
    }.run, .{&ran});
    wg.wait();

    try std.testing.expect(ran);
}

test "Pool: spawnWg across many tasks sees every one execute" {
    var pool: Pool = undefined;
    try Pool.init(&pool, .{ .allocator = std.testing.allocator, .n_jobs = 4 });
    defer pool.deinit();

    var counters = [_]u32{0} ** 16;
    var wg: WaitGroup = .{};
    for (&counters) |*c| {
        pool.spawnWg(&wg, struct {
            fn run(slot: *u32) void {
                slot.* += 1;
            }
        }.run, .{c});
    }
    wg.wait();

    for (counters) |c| {
        try std.testing.expectEqual(@as(u32, 1), c);
    }
}

test "WaitGroup: wait on unspawned group returns immediately" {
    var wg: WaitGroup = .{};
    wg.wait();
}

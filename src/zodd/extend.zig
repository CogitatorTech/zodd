//! # Extension Primitives
//!
//! The module provides mechanisms to extend tuples with new values, implementing semi-joins and anti-joins.
//!
//! A `Leaper` represents a strategy for finding matching values for a prefix.
//!
//! Components:
//! - `ExtendWith`: The struct extends a tuple by looking up values in a relation (semi-join).
//! - `FilterAnti`: The struct filters tuples that DO NOT match values in a relation (anti-join).
//! - `ExtendAnti`: The struct extends a tuple but excludes values present in a relation.
//!
//! The mechanics support "Leapfrog Trie Join" style execution.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Relation = @import("relation.zig").Relation;
const Variable = @import("variable.zig").Variable;
const gallop = @import("variable.zig").gallop;
const ExecutionContext = @import("context.zig").ExecutionContext;

/// Creates a Leaper interface type for a Tuple and Value type.
///
/// The Leaper proposes candidate values and verifies/intersects them based on a prefix tuple.
pub fn Leaper(comptime Tuple: type, comptime Val: type) type {
    return struct {
        const Self = @This();
        const ValList = std.ArrayListUnmanaged(*const Val);

        ptr: *anyopaque,
        vtable: *const VTable,
        allocator: Allocator,
        had_error: bool = false,

        pub const VTable = struct {
            /// Returns the estimated count of matching values.
            count: *const fn (ptr: *anyopaque, prefix: *const Tuple) usize,
            /// Proposes values for the extension.
            propose: *const fn (ptr: *anyopaque, prefix: *const Tuple, alloc: Allocator, values: *ValList, had_error: *bool) void,
            /// Intersects proposed values with the extension.
            intersect: *const fn (ptr: *anyopaque, prefix: *const Tuple, values: *ValList) void,
            /// Clones the leaper.
            clone: *const fn (ptr: *anyopaque, alloc: Allocator) Allocator.Error!*anyopaque,
            /// Destroys the leaper.
            destroy: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        };

        /// Returns the estimated count of matching values.
        pub fn count(self: Self, prefix: *const Tuple) usize {
            return self.vtable.count(self.ptr, prefix);
        }

        /// Proposes values for the extension.
        pub fn propose(self: *Self, prefix: *const Tuple, values: *ValList) void {
            self.vtable.propose(self.ptr, prefix, self.allocator, values, &self.had_error);
        }

        /// Intersects proposed values with the extension.
        pub fn intersect(self: Self, prefix: *const Tuple, values: *ValList) void {
            self.vtable.intersect(self.ptr, prefix, values);
        }

        /// Clones the leaper.
        pub fn clone(self: Self, alloc: Allocator) Allocator.Error!Self {
            const new_ptr = try self.vtable.clone(self.ptr, alloc);
            return Self{
                .ptr = new_ptr,
                .allocator = alloc,
                .vtable = self.vtable,
            };
        }

        /// Deinitializes the leaper.
        pub fn deinit(self: *Self) void {
            self.vtable.destroy(self.ptr, self.allocator);
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

        /// Initializes a new extend-with leaper.
        pub fn init(ctx: *ExecutionContext, relation: *const Rel, key_func: *const fn (*const Tuple) Key) Self {
            return Self{
                .relation = relation,
                .key_func = key_func,
                .allocator = ctx.allocator,
            };
        }

        /// Returns the type-erased leaper interface.
        pub fn leaper(self: *Self) LeaperType {
            return LeaperType{
                .ptr = @ptrCast(self),
                .allocator = self.allocator,
                .vtable = &.{
                    .count = countImpl,
                    .propose = proposeImpl,
                    .intersect = intersectImpl,
                    .clone = cloneImpl,
                    .destroy = destroyImpl,
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
            var slice: []const struct { Key, Val } = self.relation.elements[range.start..][0..range.count];

            for (values.items) |val| {
                slice = gallopValHelper(Key, Val, slice, val.*);
                if (slice.len > 0 and std.math.order(slice[0][1], val.*) == .eq) {
                    values.items[write_idx] = val;
                    write_idx += 1;
                }
            }

            values.shrinkRetainingCapacity(write_idx);
        }

        fn cloneImpl(ptr: *anyopaque, alloc: Allocator) Allocator.Error!*anyopaque {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const copy = try alloc.create(Self);
            copy.* = Self{
                .relation = self.relation,
                .key_func = self.key_func,
                .allocator = alloc,
                .cached_count = 0,
                .cached_start = 0,
            };
            return @ptrCast(copy);
        }

        fn destroyImpl(ptr: *anyopaque, alloc: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            alloc.destroy(self);
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

        /// Initializes a new filter-anti leaper.
        pub fn init(
            ctx: *ExecutionContext,
            relation: *const Rel,
            key_func: *const fn (*const Tuple) struct { Key, Val },
        ) Self {
            return Self{
                .relation = relation,
                .key_func = key_func,
                .allocator = ctx.allocator,
            };
        }

        /// Returns the type-erased leaper interface.
        pub fn leaper(self: *Self) LeaperType {
            return LeaperType{
                .ptr = @ptrCast(self),
                .allocator = self.allocator,
                .vtable = &.{
                    .count = countImpl,
                    .propose = proposeImpl,
                    .intersect = intersectImpl,
                    .clone = cloneImpl,
                    .destroy = destroyImpl,
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

        fn cloneImpl(ptr: *anyopaque, alloc: Allocator) Allocator.Error!*anyopaque {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const copy = try alloc.create(Self);
            copy.* = Self{
                .relation = self.relation,
                .key_func = self.key_func,
                .allocator = alloc,
            };
            return @ptrCast(copy);
        }

        fn destroyImpl(ptr: *anyopaque, alloc: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            alloc.destroy(self);
        }
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

        /// Initializes a new extend-anti leaper.
        pub fn init(ctx: *ExecutionContext, relation: *const Rel, key_func: *const fn (*const Tuple) Key) Self {
            return Self{
                .relation = relation,
                .key_func = key_func,
                .allocator = ctx.allocator,
            };
        }

        /// Returns the type-erased leaper interface.
        pub fn leaper(self: *Self) LeaperType {
            return LeaperType{
                .ptr = @ptrCast(self),
                .allocator = self.allocator,
                .vtable = &.{
                    .count = countImpl,
                    .propose = proposeImpl,
                    .intersect = intersectImpl,
                    .clone = cloneImpl,
                    .destroy = destroyImpl,
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
            var slice: []const struct { Key, Val } = self.relation.elements[range.start..][0..range.count];

            for (values.items) |val| {
                slice = gallopValHelper(Key, Val, slice, val.*);
                const found = slice.len > 0 and std.math.order(slice[0][1], val.*) == .eq;
                if (!found) {
                    values.items[write_idx] = val;
                    write_idx += 1;
                }
            }

            values.shrinkRetainingCapacity(write_idx);
        }

        fn cloneImpl(ptr: *anyopaque, alloc: Allocator) Allocator.Error!*anyopaque {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const copy = try alloc.create(Self);
            copy.* = Self{
                .relation = self.relation,
                .key_func = self.key_func,
                .allocator = alloc,
            };
            return @ptrCast(copy);
        }

        fn destroyImpl(ptr: *anyopaque, alloc: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            alloc.destroy(self);
        }
    };
}

/// Extends a variable into another variable using leapers.
pub fn extendInto(
    comptime Tuple: type,
    comptime Val: type,
    comptime Result: type,
    ctx: *ExecutionContext,
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

    if (ctx.pool != null and source.recent.elements.len > 0 and leapers.len > 0) {
        const chunk: usize = 128;
        const task_count = (source.recent.elements.len + chunk - 1) / chunk;

        const Task = struct {
            slice: []const Tuple,
            base_leapers: []Leaper(Tuple, Val),
            leapers: []Leaper(Tuple, Val) = &[_]Leaper(Tuple, Val){},
            results: std.ArrayListUnmanaged(Result) = .{},
            had_error: bool = false,
            logic_fn: *const fn (*const Tuple, *const Val) Result,

            fn run(task: *@This()) void {
                var local_values = std.ArrayListUnmanaged(*const Val){};
                defer local_values.deinit(task.base_leapers[0].allocator);

                for (task.slice) |*tuple| {
                    const sentinel = std.math.maxInt(usize);
                    var min_index: usize = sentinel;
                    var min_count: usize = sentinel;

                    for (task.leapers, 0..) |leaper, i| {
                        const cnt = leaper.count(tuple);
                        if (cnt < min_count) {
                            min_count = cnt;
                            min_index = i;
                        }
                    }

                    if (min_index == sentinel or min_count == 0 or min_count == sentinel) continue;

                    local_values.clearRetainingCapacity();
                    var min_leaper = &task.leapers[min_index];
                    min_leaper.had_error = false;
                    min_leaper.propose(tuple, &local_values);

                    if (min_leaper.had_error) {
                        task.had_error = true;
                        break;
                    }

                    for (task.leapers, 0..) |leaper, i| {
                        if (i != min_index) {
                            leaper.intersect(tuple, &local_values);
                        }
                    }

                    for (local_values.items) |val| {
                        task.results.append(min_leaper.allocator, task.logic_fn(tuple, val)) catch {
                            task.had_error = true;
                            break;
                        };
                    }

                    if (task.had_error) break;
                }
            }
        };

        const tasks = try ctx.allocator.alloc(Task, task_count);
        defer ctx.allocator.free(tasks);

        var t: usize = 0;
        while (t < task_count) : (t += 1) {
            const start = t * chunk;
            const end = @min(start + chunk, source.recent.elements.len);
            tasks[t] = .{
                .slice = source.recent.elements[start..end],
                .base_leapers = leapers,
                .logic_fn = logic,
            };
        }

        var t_idx: usize = 0;
        while (t_idx < task_count) : (t_idx += 1) {
            const task = &tasks[t_idx];
            const clones = try ctx.allocator.alloc(Leaper(Tuple, Val), leapers.len);
            var cloned: usize = 0;
            errdefer {
                for (clones[0..cloned]) |*leaper| {
                    leaper.deinit();
                }
                ctx.allocator.free(clones);
            }
            var i: usize = 0;
            while (i < leapers.len) : (i += 1) {
                clones[i] = try leapers[i].clone(ctx.allocator);
                cloned += 1;
            }
            task.leapers = clones;
        }

        if (ctx.pool) |*pool| {
            var wg: std.Thread.WaitGroup = .{};
            for (tasks) |*task| {
                pool.*.spawnWg(&wg, Task.run, .{task});
            }
            wg.wait();
        }

        for (tasks) |*task| {
            for (task.leapers) |*leaper| {
                leaper.deinit();
            }
            ctx.allocator.free(task.leapers);

            defer task.results.deinit(output.allocator);
            if (task.had_error) {
                return error.OutOfMemory;
            }
            if (task.results.items.len > 0) {
                try results.appendSlice(output.allocator, task.results.items);
            }
        }
    } else {
        for (source.recent.elements) |*tuple| {
            const sentinel = std.math.maxInt(usize);
            var min_index: usize = sentinel;
            var min_count: usize = sentinel;

            for (leapers, 0..) |leaper, i| {
                const cnt = leaper.count(tuple);
                if (cnt < min_count) {
                    min_count = cnt;
                    min_index = i;
                }
            }

            if (min_index == sentinel or min_count == 0 or min_count == sentinel) continue;

            values.clearRetainingCapacity();
            var min_leaper = &leapers[min_index];
            min_leaper.had_error = false;
            min_leaper.propose(tuple, &values);

            if (min_leaper.had_error) {
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
    }

    if (had_error) {
        return error.OutOfMemory;
    }

    if (results.items.len > 0) {
        const rel = try Relation(Result).fromSlice(ctx, results.items);
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

fn gallopValHelper(comptime Key: type, comptime Val: type, slice: []const struct { Key, Val }, target: Val) []const struct { Key, Val } {
    if (slice.len == 0) return slice;
    if (std.math.order(slice[0][1], target) != .lt) return slice;

    var step: usize = 1;
    var pos: usize = 0;

    while (true) {
        const next_pos = std.math.add(usize, pos, step) catch slice.len;
        if (next_pos >= slice.len or next_pos < pos) break;
        if (std.math.order(slice[next_pos][1], target) != .lt) break;
        pos = next_pos;
        const new_step = std.math.mul(usize, step, 2) catch std.math.maxInt(usize);
        step = new_step;
    }

    const end = @min(pos + step + 1, slice.len);
    var lo = pos + 1;
    var hi = end;

    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (std.math.order(slice[mid][1], target) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return slice[lo..];
}

test "ExtendWith: basic" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const KV = struct { u32, u32 };

    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{
        .{ 1, 10 },
        .{ 1, 11 },
        .{ 2, 20 },
    });
    defer rel.deinit();

    var ext = ExtendWith(u32, u32, u32).init(&ctx, &rel, struct {
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
    var ctx = ExecutionContext.init(allocator);
    const KV = struct { u32, u32 };
    const Tuple = struct { u32, u32 };

    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{
        .{ 1, 10 },
        .{ 2, 20 },
    });
    defer rel.deinit();

    var filter = FilterAnti(Tuple, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) KV {
            return .{ t[0], t[1] };
        }
    }.f);

    const present: Tuple = .{ 1, 10 };
    const absent: Tuple = .{ 3, 30 };

    try std.testing.expectEqual(@as(usize, 0), filter.leaper().count(&present));
    try std.testing.expectEqual(std.math.maxInt(usize), filter.leaper().count(&absent));
}

test "ExtendAnti: proposes absent values" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const KV = struct { u32, u32 }; // Key, Val

    // Relation contains {(1, 10), (1, 20)}
    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{
        .{ 1, 10 },
        .{ 1, 20 },
    });
    defer rel.deinit();

    var ext = ExtendAnti(u32, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const u32) u32 {
            return t.*;
        }
    }.f);

    const tuple: u32 = 1;
    var values = std.ArrayListUnmanaged(*const u32){};
    defer values.deinit(allocator);

    // Candidates to check: 10 (present), 15 (absent), 20 (present), 30 (absent)
    const v10: u32 = 10;
    const v15: u32 = 15;
    const v20: u32 = 20;
    const v30: u32 = 30;

    try values.append(allocator, &v10);
    try values.append(allocator, &v15);
    try values.append(allocator, &v20);
    try values.append(allocator, &v30);

    ext.leaper().intersect(&tuple, &values);

    // Should keep only 15 and 30
    try std.testing.expectEqual(@as(usize, 2), values.items.len);
    try std.testing.expectEqual(@as(u32, 15), values.items[0].*);
    try std.testing.expectEqual(@as(u32, 30), values.items[1].*);
}

test "extendInto: leapfrog join" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32; // We are extending Tuple(u32) with a new u32 value

    // Pattern: R(x, y) :- A(x), B(x, y), C(x, y)
    // A provides x. B and C constrain y.

    var A = Variable(Tuple).init(&ctx);
    defer A.deinit();
    try A.insertSlice(&ctx, &[_]Tuple{.{1}}); // x=1
    _ = try A.changed();

    // B = {(1, 10), (1, 20), (1, 30)}
    var R_B = try Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 1, 30 },
    });
    defer R_B.deinit();

    // C = {(1, 20), (1, 30), (1, 40)}
    var R_C = try Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 20 },
        .{ 1, 30 },
        .{ 1, 40 },
    });
    defer R_C.deinit();

    var output = Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    // Leapers for B and C
    var extB = ExtendWith(Tuple, u32, Val).init(&ctx, &R_B, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var extC = ExtendWith(Tuple, u32, Val).init(&ctx, &R_C, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]Leaper(Tuple, Val){ extB.leaper(), extC.leaper() };

    try extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &A, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();

    // Intersection of {10, 20, 30} and {20, 30, 40} is {20, 30}
    try std.testing.expectEqual(@as(usize, 2), output.recent.len());
    try std.testing.expectEqual(output.recent.elements[0][1], 20);
    try std.testing.expectEqual(output.recent.elements[1][1], 30);
}

test "extendInto: only anti leapers is harmless" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;

    var source = Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{.{1}});
    _ = try source.changed();

    const KV = struct { u32, u32 };
    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{});
    defer rel.deinit();

    var output = Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    var ext = ExtendAnti(Tuple, u32, Val).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]Leaper(Tuple, Val){ext.leaper()};

    try extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &source, leapers[0..], &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic);

    const changed = try output.changed();
    try std.testing.expect(!changed);
    try std.testing.expectEqual(@as(usize, 0), output.recent.len());
}

test "ExtendWith: count zero does not propose values" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const KV = struct { u32, u32 };
    const Tuple = u32;

    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{
        .{ 2, 20 },
    });
    defer rel.deinit();

    var ext = ExtendWith(Tuple, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t.*;
        }
    }.f);

    const tuple: Tuple = 1;
    const cnt = ext.leaper().count(&tuple);
    try std.testing.expectEqual(@as(usize, 0), cnt);

    var values = std.ArrayListUnmanaged(*const u32){};
    defer values.deinit(allocator);
    var leaper = ext.leaper();
    leaper.propose(&tuple, &values);
    try std.testing.expectEqual(@as(usize, 0), values.items.len);
}

test "FilterAnti and ExtendAnti: empty relation" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const KV = struct { u32, u32 };

    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{});
    defer rel.deinit();

    var filter = FilterAnti(Tuple, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) KV {
            return .{ t[0], 0 };
        }
    }.f);

    var ext = ExtendAnti(Tuple, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    const tuple: Tuple = .{1};
    try std.testing.expectEqual(std.math.maxInt(usize), filter.leaper().count(&tuple));

    const v10: u32 = 10;
    const v20: u32 = 20;
    var values = std.ArrayListUnmanaged(*const u32){};
    defer values.deinit(allocator);
    try values.append(allocator, &v10);
    try values.append(allocator, &v20);

    ext.leaper().intersect(&tuple, &values);
    try std.testing.expectEqual(@as(usize, 2), values.items.len);
}

test "extendInto: parallel" {
    const allocator = std.testing.allocator;
    var ctx = try ExecutionContext.initWithThreads(allocator, 2);
    defer ctx.deinit();
    const Tuple = struct { u32 };
    const Val = u32;

    var A = Variable(Tuple).init(&ctx);
    defer A.deinit();
    try A.insertSlice(&ctx, &[_]Tuple{.{1}});
    _ = try A.changed();

    var R_B = try Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 1, 30 },
    });
    defer R_B.deinit();

    var R_C = try Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 20 },
        .{ 1, 30 },
        .{ 1, 40 },
    });
    defer R_C.deinit();

    var output = Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    var extB = ExtendWith(Tuple, u32, Val).init(&ctx, &R_B, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var extC = ExtendWith(Tuple, u32, Val).init(&ctx, &R_C, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]Leaper(Tuple, Val){ extB.leaper(), extC.leaper() };

    try extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &A, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();
    try std.testing.expectEqual(@as(usize, 2), output.recent.len());
    try std.testing.expectEqual(output.recent.elements[0][1], 20);
    try std.testing.expectEqual(output.recent.elements[1][1], 30);
}

test "extendInto: clone failure cleans up" {
    const allocator = std.testing.allocator;
    var ctx = try ExecutionContext.initWithThreads(allocator, 2);
    defer ctx.deinit();

    const Tuple = struct { u32 };
    const Val = u32;

    var source = Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{.{1}});
    _ = try source.changed();

    const Counter = struct {
        clones: usize = 0,
        destroys: usize = 0,
        fail_after: usize = 0,
    };

    const State = struct {
        counter: *Counter,
        value: Val,
    };

    const VTable = Leaper(Tuple, Val).VTable;

    const Impl = struct {
        fn count(ptr: *anyopaque, _: *const Tuple) usize {
            _ = ptr;
            return 1;
        }

        fn propose(ptr: *anyopaque, _: *const Tuple, alloc: Allocator, values: *std.ArrayListUnmanaged(*const Val), had_error: *bool) void {
            const state: *State = @ptrCast(@alignCast(ptr));
            values.append(alloc, &state.value) catch {
                had_error.* = true;
                return;
            };
        }

        fn intersect(_: *anyopaque, _: *const Tuple, _: *std.ArrayListUnmanaged(*const Val)) void {}

        fn clone(ptr: *anyopaque, alloc: Allocator) Allocator.Error!*anyopaque {
            const state: *State = @ptrCast(@alignCast(ptr));
            if (state.counter.clones >= state.counter.fail_after) return error.OutOfMemory;
            const new_state = try alloc.create(State);
            new_state.* = .{ .counter = state.counter, .value = state.value };
            state.counter.clones += 1;
            return @ptrCast(new_state);
        }

        fn destroy(ptr: *anyopaque, alloc: Allocator) void {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.counter.destroys += 1;
            alloc.destroy(state);
        }
    };

    var counter = Counter{ .fail_after = 1 };

    const makeLeaper = struct {
        fn make(alloc: Allocator, counter_ptr: *Counter, value: Val) !Leaper(Tuple, Val) {
            const state = try alloc.create(State);
            state.* = .{ .counter = counter_ptr, .value = value };
            return .{
                .ptr = @ptrCast(state),
                .allocator = alloc,
                .vtable = &VTable{
                    .count = Impl.count,
                    .propose = Impl.propose,
                    .intersect = Impl.intersect,
                    .clone = Impl.clone,
                    .destroy = Impl.destroy,
                },
            };
        }
    };

    var leaper1 = try makeLeaper.make(allocator, &counter, 10);
    defer leaper1.deinit();
    var leaper2 = try makeLeaper.make(allocator, &counter, 20);
    defer leaper2.deinit();

    var leapers = [_]Leaper(Tuple, Val){ leaper1, leaper2 };

    var output = Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    try std.testing.expectError(error.OutOfMemory, extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &source, leapers[0..], &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic));

    try std.testing.expectEqual(counter.clones, counter.destroys);
}

test "Leaper clone uses clone allocator" {
    const allocator = std.testing.allocator;

    const CountingAlloc = struct {
        const Self = @This();
        const Align = std.mem.Alignment;

        backing: Allocator,
        alloc_count: usize = 0,
        free_count: usize = 0,

        fn wrap(self: *Self) Allocator {
            return Allocator{
                .ptr = self,
                .vtable = &.{
                    .alloc = allocFn,
                    .resize = resizeFn,
                    .remap = remapFn,
                    .free = freeFn,
                },
            };
        }

        fn allocFn(ctx: *anyopaque, len: usize, alignment: Align, ret_addr: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.alloc_count += 1;
            return self.backing.vtable.alloc(self.backing.ptr, len, alignment, ret_addr);
        }

        fn resizeFn(ctx: *anyopaque, buf: []u8, alignment: Align, new_len: usize, ret_addr: usize) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            return self.backing.vtable.resize(self.backing.ptr, buf, alignment, new_len, ret_addr);
        }

        fn remapFn(ctx: *anyopaque, buf: []u8, alignment: Align, new_len: usize, ret_addr: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            return self.backing.vtable.remap(self.backing.ptr, buf, alignment, new_len, ret_addr);
        }

        fn freeFn(ctx: *anyopaque, buf: []u8, alignment: Align, ret_addr: usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.free_count += 1;
            self.backing.vtable.free(self.backing.ptr, buf, alignment, ret_addr);
        }
    };

    var base_count = CountingAlloc{ .backing = allocator };
    var clone_count = CountingAlloc{ .backing = allocator };

    var ctx = ExecutionContext.init(base_count.wrap());
    const KV = struct { u32, u32 };

    var rel = try Relation(KV).fromSlice(&ctx, &[_]KV{.{ 1, 10 }});
    defer rel.deinit();

    var ext = ExtendWith(u32, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const u32) u32 {
            return t.*;
        }
    }.f);

    var leaper = ext.leaper();

    const alloc_before = clone_count.alloc_count;
    const free_before = clone_count.free_count;

    var cloned = try leaper.clone(clone_count.wrap());
    cloned.deinit();

    try std.testing.expectEqual(alloc_before + 1, clone_count.alloc_count);
    try std.testing.expectEqual(free_before + 1, clone_count.free_count);
}

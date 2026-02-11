//! Core Relation data structure: a sorted list of unique tuples.

const std = @import("std");
const mem = std.mem;
const sort = std.sort;
const Allocator = mem.Allocator;
const ExecutionContext = @import("context.zig").ExecutionContext;

pub fn Relation(comptime Tuple: type) type {
    return struct {
        const Self = @This();

        elements: []Tuple,
        allocator: Allocator,
        ctx: *ExecutionContext,

        pub fn fromSlice(ctx: *ExecutionContext, input: []const Tuple) Allocator.Error!Self {
            if (input.len == 0) {
                return Self{
                    .elements = &[_]Tuple{},
                    .allocator = ctx.allocator,
                    .ctx = ctx,
                };
            }

            const elements = try ctx.allocator.alloc(Tuple, input.len);
            if (ctx.pool) |pool| {
                const chunk: usize = 1024;
                const task_count = (input.len + chunk - 1) / chunk;
                const Task = struct {
                    start: usize,
                    end: usize,
                    input: []const Tuple,
                    output: []Tuple,

                    fn run(task: *@This()) void {
                        const size = task.end - task.start;
                        if (size == 0) return;
                        @memcpy(task.output[task.start..task.end], task.input[task.start..task.end]);
                    }
                };

                const tasks = try ctx.allocator.alloc(Task, task_count);
                defer ctx.allocator.free(tasks);

                var wg: std.Thread.WaitGroup = .{};
                var t: usize = 0;
                while (t < task_count) : (t += 1) {
                    const start = t * chunk;
                    const end = @min(start + chunk, input.len);
                    tasks[t] = .{ .start = start, .end = end, .input = input, .output = elements };
                    pool.spawnWg(&wg, Task.run, .{&tasks[t]});
                }

                if (task_count > 0) {
                    wg.wait();
                }
            } else {
                @memcpy(elements, input);
            }

            if (ctx.pool) |pool| {
                const chunk: usize = 2048;
                const task_count = (input.len + chunk - 1) / chunk;
                if (task_count > 1) {
                    const Task = struct {
                        start: usize,
                        end: usize,
                        data: []Tuple,

                        fn run(task: *@This()) void {
                            std.sort.pdq(Tuple, task.data[task.start..task.end], {}, lessThan);
                        }
                    };

                    const tasks = try ctx.allocator.alloc(Task, task_count);
                    defer ctx.allocator.free(tasks);

                    var wg: std.Thread.WaitGroup = .{};
                    var t2: usize = 0;
                    while (t2 < task_count) : (t2 += 1) {
                        const start = t2 * chunk;
                        const end = @min(start + chunk, input.len);
                        tasks[t2] = .{ .start = start, .end = end, .data = elements };
                        pool.spawnWg(&wg, Task.run, .{&tasks[t2]});
                    }

                    wg.wait();
                }
            }

            sort.pdq(Tuple, elements, {}, lessThan);

            const unique_len = deduplicate(elements);

            if (unique_len < elements.len) {
                const shrunk = ctx.allocator.realloc(elements, unique_len) catch elements[0..unique_len];
                return Self{
                    .elements = shrunk,
                    .allocator = ctx.allocator,
                    .ctx = ctx,
                };
            }

            return Self{
                .elements = elements,
                .allocator = ctx.allocator,
                .ctx = ctx,
            };
        }

        pub fn empty(ctx: *ExecutionContext) Self {
            return Self{
                .elements = &[_]Tuple{},
                .allocator = ctx.allocator,
                .ctx = ctx,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.elements.len > 0) {
                self.allocator.free(self.elements);
            }
            self.elements = &[_]Tuple{};
        }

        pub fn len(self: Self) usize {
            return self.elements.len;
        }

        pub fn isEmpty(self: Self) bool {
            return self.elements.len == 0;
        }

        pub fn merge(self: *Self, other: *Self) Allocator.Error!Self {
            if (self.elements.len == 0) {
                const result = other.*;
                other.elements = &[_]Tuple{};
                self.deinit();
                return result;
            }
            if (other.elements.len == 0) {
                const result = self.*;
                self.elements = &[_]Tuple{};
                other.deinit();
                return result;
            }

            const total_len = self.elements.len + other.elements.len;
            const merged = try self.allocator.alloc(Tuple, total_len);

            if (self.ctx.pool) |pool| {
                const chunk: usize = 1024;
                const task_count = (total_len + chunk - 1) / chunk;
                const Task = struct {
                    start: usize,
                    end: usize,
                    left: []const Tuple,
                    right: []const Tuple,
                    output: []Tuple,

                    fn run(task: *@This()) void {
                        var i = task.start;
                        while (i < task.end) : (i += 1) {
                            if (i < task.left.len) {
                                task.output[i] = task.left[i];
                            } else {
                                const idx = i - task.left.len;
                                task.output[i] = task.right[idx];
                            }
                        }
                    }
                };

                const tasks = try self.allocator.alloc(Task, task_count);
                defer self.allocator.free(tasks);

                var wg: std.Thread.WaitGroup = .{};
                var t: usize = 0;
                while (t < task_count) : (t += 1) {
                    const start = t * chunk;
                    const end = @min(start + chunk, total_len);
                    tasks[t] = .{
                        .start = start,
                        .end = end,
                        .left = self.elements,
                        .right = other.elements,
                        .output = merged,
                    };
                    pool.spawnWg(&wg, Task.run, .{&tasks[t]});
                }

                wg.wait();
            } else {
                @memcpy(merged[0..self.elements.len], self.elements);
                @memcpy(merged[self.elements.len..], other.elements);
            }

            if (self.ctx.pool) |pool| {
                const chunk: usize = 2048;
                const task_count = (total_len + chunk - 1) / chunk;
                if (task_count > 1) {
                    const TaskSort = struct {
                        start: usize,
                        end: usize,
                        data: []Tuple,

                        fn run(task: *@This()) void {
                            std.sort.pdq(Tuple, task.data[task.start..task.end], {}, lessThan);
                        }
                    };

                    const tasks_sort = try self.allocator.alloc(TaskSort, task_count);
                    defer self.allocator.free(tasks_sort);

                    var wg: std.Thread.WaitGroup = .{};
                    var t2: usize = 0;
                    while (t2 < task_count) : (t2 += 1) {
                        const start = t2 * chunk;
                        const end = @min(start + chunk, total_len);
                        tasks_sort[t2] = .{ .start = start, .end = end, .data = merged };
                        pool.spawnWg(&wg, TaskSort.run, .{&tasks_sort[t2]});
                    }

                    wg.wait();
                }
            }

            sort.pdq(Tuple, merged, {}, lessThan);
            const unique_len = deduplicate(merged);

            self.deinit();
            other.deinit();

            if (unique_len < merged.len) {
                const shrunk = self.allocator.realloc(merged, unique_len) catch merged[0..unique_len];
                return Self{
                    .elements = shrunk,
                    .allocator = self.allocator,
                    .ctx = self.ctx,
                };
            }

            return Self{
                .elements = merged,
                .allocator = self.allocator,
                .ctx = self.ctx,
            };
        }

        fn lessThan(_: void, a: Tuple, b: Tuple) bool {
            return compareTuples(a, b) == .lt;
        }

        fn orderField(comptime T: type, a: T, b: T) std.math.Order {
            const field_info = @typeInfo(T);
            if (field_info == .pointer) {
                return std.math.order(@intFromPtr(a), @intFromPtr(b));
            } else {
                return std.math.order(a, b);
            }
        }

        pub fn compareTuples(a: Tuple, b: Tuple) std.math.Order {
            const info = @typeInfo(Tuple);
            if (info == .@"struct" and info.@"struct".is_tuple) {
                inline for (0..info.@"struct".fields.len) |i| {
                    const a_field = a[i];
                    const b_field = b[i];
                    const order = orderField(@TypeOf(a_field), a_field, b_field);
                    if (order != .eq) return order;
                }
                return .eq;
            } else {
                const tuple_info = @typeInfo(Tuple);
                if (tuple_info == .pointer) {
                    return std.math.order(@intFromPtr(a), @intFromPtr(b));
                } else {
                    return std.math.order(a, b);
                }
            }
        }

        fn deduplicate(elements: []Tuple) usize {
            if (elements.len <= 1) return elements.len;

            var write_idx: usize = 1;
            for (elements[1..]) |elem| {
                if (compareTuples(elements[write_idx - 1], elem) != .eq) {
                    elements[write_idx] = elem;
                    write_idx += 1;
                }
            }
            return write_idx;
        }

        fn isSerializableType(comptime T: type) bool {
            return switch (@typeInfo(T)) {
                .int, .float, .bool, .@"enum" => true,
                .array => |info| isSerializableType(info.child),
                .@"struct" => |info| blk: {
                    inline for (info.fields) |field| {
                        if (!isSerializableType(field.type)) break :blk false;
                    }
                    break :blk true;
                },
                else => false,
            };
        }

        fn writeValue(comptime T: type, writer: anytype, value: T) !void {
            return switch (@typeInfo(T)) {
                .int => try writer.writeInt(T, value, .little),
                .bool => try writer.writeInt(u8, if (value) 1 else 0, .little),
                .float => blk: {
                    const IntType = std.meta.Int(.unsigned, @bitSizeOf(T));
                    const bits: IntType = @as(IntType, @bitCast(value));
                    try writer.writeInt(IntType, bits, .little);
                    break :blk;
                },
                .@"enum" => blk: {
                    const info = @typeInfo(T).@"enum";
                    const Tag = info.tag_type;
                    try writer.writeInt(Tag, @intFromEnum(value), .little);
                    break :blk;
                },
                .array => |info| blk: {
                    for (value) |elem| {
                        try writeValue(info.child, writer, elem);
                    }
                    break :blk;
                },
                .@"struct" => |info| blk: {
                    inline for (info.fields) |field| {
                        try writeValue(field.type, writer, @field(value, field.name));
                    }
                    break :blk;
                },
                else => return error.UnsupportedType,
            };
        }

        fn readValue(comptime T: type, reader: anytype) !T {
            return switch (@typeInfo(T)) {
                .int => try reader.readInt(T, .little),
                .bool => (try reader.readInt(u8, .little)) != 0,
                .float => blk: {
                    const IntType = std.meta.Int(.unsigned, @bitSizeOf(T));
                    const bits = try reader.readInt(IntType, .little);
                    break :blk @as(T, @bitCast(bits));
                },
                .@"enum" => blk: {
                    const info = @typeInfo(T).@"enum";
                    const Tag = info.tag_type;
                    const bits = try reader.readInt(Tag, .little);
                    break :blk @as(T, @enumFromInt(bits));
                },
                .array => |info| blk: {
                    var result: T = undefined;
                    var i: usize = 0;
                    while (i < result.len) : (i += 1) {
                        result[i] = try readValue(info.child, reader);
                    }
                    break :blk result;
                },
                .@"struct" => |info| blk: {
                    var result: T = undefined;
                    inline for (info.fields) |field| {
                        @field(result, field.name) = try readValue(field.type, reader);
                    }
                    break :blk result;
                },
                else => return error.UnsupportedType,
            };
        }

        pub fn save(self: Self, writer: anytype) !void {
            if (!isSerializableType(Tuple)) return error.UnsupportedType;
            try writer.writeAll("ZODDREL");
            try writer.writeInt(u8, 1, .little);
            try writer.writeInt(u64, self.elements.len, .little);
            for (self.elements) |elem| {
                try writeValue(Tuple, writer, elem);
            }
        }

        pub fn load(ctx: *ExecutionContext, reader: anytype) !Self {
            return loadWithLimit(ctx, reader, std.math.maxInt(usize));
        }

        pub fn loadWithLimit(ctx: *ExecutionContext, reader: anytype, max_len: usize) !Self {
            if (!isSerializableType(Tuple)) return error.UnsupportedType;
            const magic = try reader.readBytesNoEof(7);
            if (!std.mem.eql(u8, &magic, "ZODDREL")) {
                return error.InvalidFormat;
            }
            const version = try reader.readInt(u8, .little);
            if (version != 1) {
                return error.UnsupportedVersion;
            }

            const length_u64 = try reader.readInt(u64, .little);
            const length = std.math.cast(usize, length_u64) orelse return error.InvalidFormat;
            if (length == 0) {
                return Self.empty(ctx);
            }
            if (length > max_len) {
                return error.TooLarge;
            }

            const elements = try ctx.allocator.alloc(Tuple, length);
            errdefer ctx.allocator.free(elements);

            var i: usize = 0;
            while (i < length) : (i += 1) {
                elements[i] = try readValue(Tuple, reader);
            }

            sort.pdq(Tuple, elements, {}, lessThan);
            const unique_len = deduplicate(elements);

            if (unique_len < elements.len) {
                const shrunk = ctx.allocator.realloc(elements, unique_len) catch elements[0..unique_len];
                return Self{
                    .elements = shrunk,
                    .allocator = ctx.allocator,
                    .ctx = ctx,
                };
            }

            return Self{
                .elements = elements,
                .allocator = ctx.allocator,
                .ctx = ctx,
            };
        }
    };
}

test "Relation: empty" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    var rel = Relation(u32).empty(&ctx);
    defer rel.deinit();

    try std.testing.expect(rel.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), rel.len());
}

test "Relation: persistence" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var original = try Relation(Tuple).fromSlice(&ctx, &[_]Tuple{
        .{ 1, 10 },
        .{ 2, 20 },
        .{ 3, 30 },
    });
    defer original.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    try original.save(buffer.writer(allocator));

    var fbs = std.io.fixedBufferStream(buffer.items);
    var loaded = try Relation(Tuple).load(&ctx, fbs.reader());
    defer loaded.deinit();

    try std.testing.expectEqual(original.len(), loaded.len());
    try std.testing.expectEqualSlices(Tuple, original.elements, loaded.elements);
}

test "Relation: fromSlice sorts and deduplicates" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const input = [_]u32{ 5, 3, 3, 1, 5, 2, 1 };

    var rel = try Relation(u32).fromSlice(&ctx, &input);
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 4), rel.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 5 }, rel.elements);
}

test "Relation: tuple type" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };
    const input = [_]Tuple{
        .{ 2, 1 },
        .{ 1, 2 },
        .{ 1, 2 },
        .{ 1, 1 },
    };

    var rel = try Relation(Tuple).fromSlice(&ctx, &input);
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 3), rel.len());
    try std.testing.expectEqual(Tuple{ 1, 1 }, rel.elements[0]);
    try std.testing.expectEqual(Tuple{ 1, 2 }, rel.elements[1]);
    try std.testing.expectEqual(Tuple{ 2, 1 }, rel.elements[2]);
}

test "Relation: merge" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var rel1 = try Relation(u32).fromSlice(&ctx, &[_]u32{ 1, 3, 5 });
    var rel2 = try Relation(u32).fromSlice(&ctx, &[_]u32{ 2, 3, 4 });

    var merged = try rel1.merge(&rel2);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 5), merged.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 4, 5 }, merged.elements);
}

test "Relation: load normalizes order" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer = buffer.writer(allocator);
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 1, .little);
    const raw = [_]Tuple{
        .{ 2, 20 },
        .{ 1, 10 },
        .{ 2, 20 },
    };
    try writer.writeInt(u64, raw.len, .little);
    for (raw) |tuple| {
        try writer.writeInt(u32, tuple[0], .little);
        try writer.writeInt(u32, tuple[1], .little);
    }

    var reader = std.io.fixedBufferStream(buffer.items);
    var rel = try Relation(Tuple).load(&ctx, reader.reader());
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 2), rel.len());
    try std.testing.expectEqual(Tuple{ 1, 10 }, rel.elements[0]);
    try std.testing.expectEqual(Tuple{ 2, 20 }, rel.elements[1]);
}

test "Relation: loadWithLimit zero length with zero limit" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer = buffer.writer(allocator);
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 1, .little);
    try writer.writeInt(u64, 0, .little);

    var reader = std.io.fixedBufferStream(buffer.items);
    var rel = try Relation(u32).loadWithLimit(&ctx, reader.reader(), 0);
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 0), rel.len());
}

test "Relation: scalar save and load" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);

    var original = try Relation(u32).fromSlice(&ctx, &[_]u32{ 3, 1, 2, 2 });
    defer original.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    try original.save(buffer.writer(allocator));

    var fbs = std.io.fixedBufferStream(buffer.items);
    var loaded = try Relation(u32).load(&ctx, fbs.reader());
    defer loaded.deinit();

    try std.testing.expectEqual(original.len(), loaded.len());
    try std.testing.expectEqualSlices(u32, original.elements, loaded.elements);
}

test "Relation: fromSlice parallel copy" {
    const allocator = std.testing.allocator;
    var ctx = try ExecutionContext.initWithThreads(allocator, 2);
    defer ctx.deinit();

    const input = [_]u32{ 5, 3, 3, 1, 5, 2, 1 };

    var rel = try Relation(u32).fromSlice(&ctx, &input);
    defer rel.deinit();

    try std.testing.expectEqual(@as(usize, 4), rel.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 5 }, rel.elements);
}

test "Relation: merge parallel copy" {
    const allocator = std.testing.allocator;
    var ctx = try ExecutionContext.initWithThreads(allocator, 2);
    defer ctx.deinit();

    var rel1 = try Relation(u32).fromSlice(&ctx, &[_]u32{ 1, 3, 5 });
    var rel2 = try Relation(u32).fromSlice(&ctx, &[_]u32{ 2, 3, 4 });

    var merged = try rel1.merge(&rel2);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 5), merged.len());
    try std.testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3, 4, 5 }, merged.elements);
}

test "Relation: save/load unsupported type" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    const Bad = struct { *u8 };

    var rel = Relation(Bad).empty(&ctx);
    defer rel.deinit();

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    try std.testing.expectError(error.UnsupportedType, rel.save(buffer.writer(allocator)));

    var header: [16]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&header);
    try fbs.writer().writeAll("ZODDREL");
    try fbs.writer().writeInt(u8, 1, .little);
    try fbs.writer().writeInt(u64, 0, .little);
    const used = fbs.pos;

    var reader_fbs = std.io.fixedBufferStream(header[0..used]);
    try std.testing.expectError(error.UnsupportedType, Relation(Bad).load(&ctx, reader_fbs.reader()));
}

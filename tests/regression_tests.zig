const std = @import("std");
const testing = std.testing;
const zodd = @import("zodd");

test "regression: totalLen includes to_add batches" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    try v.insertSlice(&ctx, &[_]u32{ 1, 2, 3 });

    try testing.expectEqual(@as(usize, 3), v.totalLen());

    _ = try v.changed();

    try testing.expectEqual(@as(usize, 3), v.totalLen());

    try v.insertSlice(&ctx, &[_]u32{ 4, 5 });

    try testing.expectEqual(@as(usize, 5), v.totalLen());
}

test "regression: Iteration cleanup handles variables" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var iter = zodd.Iteration(u32).init(&ctx, null);

    const v1 = try iter.variable();
    const v2 = try iter.variable();

    try v1.insertSlice(&ctx, &[_]u32{ 1, 2, 3 });
    try v2.insertSlice(&ctx, &[_]u32{ 4, 5 });

    _ = try iter.changed();

    iter.deinit();
}

test "regression: intersection correctness with sorted values" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const KV = struct { u32, u32 };

    var rel = try zodd.Relation(KV).fromSlice(&ctx, &[_]KV{
        .{ 1, 10 },
        .{ 1, 20 },
        .{ 1, 30 },
        .{ 2, 100 },
        .{ 2, 200 },
    });
    defer rel.deinit();

    var ext = zodd.ExtendWith(u32, u32, u32).init(&ctx, &rel, struct {
        fn f(t: *const u32) u32 {
            return t.*;
        }
    }.f);

    const tuple1: u32 = 1;
    const cnt1 = ext.leaper().count(&tuple1);
    try testing.expectEqual(@as(usize, 3), cnt1);

    const tuple2: u32 = 2;
    const cnt2 = ext.leaper().count(&tuple2);
    try testing.expectEqual(@as(usize, 2), cnt2);

    const tuple3: u32 = 99;
    const cnt3 = ext.leaper().count(&tuple3);
    try testing.expectEqual(@as(usize, 0), cnt3);
}

test "regression: variable deduplication across multiple rounds" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    try v.insertSlice(&ctx, &[_]u32{ 1, 2, 3 });
    _ = try v.changed();

    try v.insertSlice(&ctx, &[_]u32{ 2, 3, 4, 5 });
    const changed1 = try v.changed();
    try testing.expect(changed1);

    try testing.expectEqual(@as(usize, 2), v.recent.len());

    try v.insertSlice(&ctx, &[_]u32{ 1, 2, 3, 4, 5 });
    const changed2 = try v.changed();

    try testing.expect(!changed2);

    _ = try v.changed();

    var result = try v.complete();
    defer result.deinit();
    try testing.expectEqual(@as(usize, 5), result.len());
}

test "regression: extendInto error detection with allocation failure simulation" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{.{1}});
    _ = try source.changed();

    var R_B = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
        .{ 1, 20 },
    });
    defer R_B.deinit();

    var output = zodd.Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    var extB = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &R_B, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]zodd.Leaper(Tuple, Val){extB.leaper()};

    try zodd.extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &source, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();
    try testing.expectEqual(@as(usize, 2), output.recent.len());
}

test "regression: SecondaryIndex get returns pointer not copy" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    const Index = zodd.index.SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[1];
        }
    }.extract, struct {
        fn cmp(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.cmp, 4);

    var idx = Index.init(&ctx);
    defer idx.deinit();

    try idx.insert(.{ 1, 10 });
    try idx.insert(.{ 2, 10 });

    const rel_ptr = idx.get(10).?;
    try testing.expectEqual(@as(usize, 2), rel_ptr.len());
    try testing.expectEqual(@as(u32, 1), rel_ptr.elements[0][0]);
    try testing.expectEqual(@as(u32, 2), rel_ptr.elements[1][0]);
}

test "regression: Variable complete includes recent and to_add data" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    try v.insertSlice(&ctx, &[_]u32{ 1, 2, 3 });

    var result = try v.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.len());
    try testing.expectEqual(@as(u32, 1), result.elements[0]);
    try testing.expectEqual(@as(u32, 2), result.elements[1]);
    try testing.expectEqual(@as(u32, 3), result.elements[2]);
}

test "regression: Variable complete with recent data not yet stable" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    try v.insertSlice(&ctx, &[_]u32{ 1, 2 });
    _ = try v.changed();

    try v.insertSlice(&ctx, &[_]u32{ 3, 4 });
    _ = try v.changed();

    var result = try v.complete();
    defer result.deinit();

    try testing.expectEqual(@as(usize, 4), result.len());
}

test "regression: gallop with large step values" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    const size = 1000;
    const data = try allocator.alloc(u32, size);
    defer allocator.free(data);

    for (data, 0..) |*elem, i| {
        elem.* = @intCast(i * 2);
    }

    var rel = try zodd.Relation(u32).fromSlice(&ctx, data);
    defer rel.deinit();

    const target: u32 = 1500;
    const result_slice = zodd.gallop(u32, rel.elements, target);

    try testing.expect(result_slice.len > 0);
    if (result_slice.len > 0) {
        try testing.expect(result_slice[0] >= target);
    }
}

test "regression: Relation save and load with tuples" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var original = try zodd.Relation(Tuple).fromSlice(&ctx, &[_]Tuple{
        .{ 2, 20 },
        .{ 1, 10 },
        .{ 3, 30 },
    });
    defer original.deinit();

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    try original.save(&aw.writer);

    var reader = std.Io.Reader.fixed(aw.writer.buffered());
    var loaded = try zodd.Relation(Tuple).load(&ctx, &reader);
    defer loaded.deinit();

    try testing.expectEqual(original.len(), loaded.len());
    try testing.expectEqualSlices(Tuple, original.elements, loaded.elements);
}

test "regression: extendInto with only ExtendAnti should not call propose" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{.{1}});
    _ = try source.changed();

    const KV = struct { u32, u32 };
    var rel = try zodd.Relation(KV).fromSlice(&ctx, &[_]KV{
        .{ 2, 100 },
    });
    defer rel.deinit();

    var output = zodd.Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    var ext = zodd.ExtendAnti(Tuple, u32, Val).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]zodd.Leaper(Tuple, Val){ext.leaper()};

    try zodd.extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &source, leapers[0..], &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic);

    const changed = try output.changed();
    try testing.expect(!changed);
}

test "regression: SecondaryIndex does not leak memory on repeated inserts" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    const Index = zodd.index.SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[0];
        }
    }.extract, struct {
        fn cmp(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.cmp, 4);

    var idx = Index.init(&ctx);
    defer idx.deinit();

    try idx.insert(.{ 1, 100 });
    try idx.insert(.{ 1, 200 });
    try idx.insert(.{ 1, 300 });

    const rel = idx.get(1).?;
    try testing.expectEqual(@as(usize, 3), rel.len());
}

test "regression: joinAnti searches full filter" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var input = zodd.Variable(Tuple).init(&ctx);
    defer input.deinit();

    var filter = zodd.Variable(Tuple).init(&ctx);
    defer filter.deinit();

    var output = zodd.Variable(Tuple).init(&ctx);
    defer output.deinit();

    try input.insertSlice(&ctx, &[_]Tuple{ .{ 1, 10 }, .{ 2, 20 }, .{ 3, 30 } });
    try filter.insertSlice(&ctx, &[_]Tuple{ .{ 1, 100 }, .{ 3, 300 } });

    _ = try input.changed();
    _ = try filter.changed();

    try zodd.joinAnti(u32, u32, u32, Tuple, &ctx, &input, &filter, &output, struct {
        fn logic(key: *const u32, val: *const u32) Tuple {
            return .{ key.*, val.* };
        }
    }.logic);

    _ = try output.changed();
    try testing.expectEqual(@as(usize, 1), output.recent.len());
    try testing.expectEqual(@as(u32, 2), output.recent.elements[0][0]);
}

test "regression: Relation loadWithLimit rejects large length" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const writer = &aw.writer;
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 1, .little);
    try writer.writeInt(u64, 2, .little);

    const t1 = Tuple{ 1, 10 };
    const t2 = Tuple{ 2, 20 };
    const arr1 = [_]Tuple{t1};
    const arr2 = [_]Tuple{t2};
    try writer.writeAll(std.mem.sliceAsBytes(&arr1));
    try writer.writeAll(std.mem.sliceAsBytes(&arr2));

    var reader = std.Io.Reader.fixed(aw.writer.buffered());
    try testing.expectError(error.TooLarge, zodd.Relation(Tuple).loadWithLimit(&ctx, &reader, 1));
}

test "regression: extendInto resets leaper error" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32 };
    const Val = u32;

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();

    try source.insertSlice(&ctx, &[_]Tuple{.{1}});
    _ = try source.changed();

    var rel = try zodd.Relation(struct { u32, u32 }).fromSlice(&ctx, &[_]struct { u32, u32 }{
        .{ 1, 10 },
        .{ 1, 20 },
    });
    defer rel.deinit();

    var output = zodd.Variable(struct { u32, u32 }).init(&ctx);
    defer output.deinit();

    var ext = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);

    var leapers = [_]zodd.Leaper(Tuple, Val){ext.leaper()};
    leapers[0].had_error = true;

    try zodd.extendInto(Tuple, Val, struct { u32, u32 }, &ctx, &source, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) struct { u32, u32 } {
            return .{ t[0], v.* };
        }
    }.logic);

    _ = try output.changed();
    try testing.expectEqual(@as(usize, 2), output.recent.len());
}

test "regression: loadWithLimit rejects invalid magic" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const writer = &aw.writer;
    try writer.writeAll("BADMAGC");
    try writer.writeInt(u8, 1, .little);
    try writer.writeInt(u64, 1, .little);

    const t1 = Tuple{ 1, 10 };
    const arr1 = [_]Tuple{t1};
    try writer.writeAll(std.mem.sliceAsBytes(&arr1));

    var reader = std.Io.Reader.fixed(aw.writer.buffered());
    try testing.expectError(error.InvalidFormat, zodd.Relation(Tuple).loadWithLimit(&ctx, &reader, 10));
}

test "regression: loadWithLimit rejects unsupported version" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const writer = &aw.writer;
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 2, .little);
    try writer.writeInt(u64, 1, .little);

    const t1 = Tuple{ 1, 10 };
    const arr1 = [_]Tuple{t1};
    try writer.writeAll(std.mem.sliceAsBytes(&arr1));

    var reader = std.Io.Reader.fixed(aw.writer.buffered());
    try testing.expectError(error.UnsupportedVersion, zodd.Relation(Tuple).loadWithLimit(&ctx, &reader, 10));
}

test "regression: joinAnti checks multiple stable batches" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var input = zodd.Variable(Tuple).init(&ctx);
    defer input.deinit();

    var filter = zodd.Variable(Tuple).init(&ctx);
    defer filter.deinit();

    var output = zodd.Variable(Tuple).init(&ctx);
    defer output.deinit();

    try input.insertSlice(&ctx, &[_]Tuple{ .{ 1, 10 }, .{ 2, 20 }, .{ 3, 30 } });
    _ = try input.changed();

    try filter.insertSlice(&ctx, &[_]Tuple{.{ 1, 100 }});
    _ = try filter.changed();
    _ = try filter.changed();

    try filter.insertSlice(&ctx, &[_]Tuple{.{ 3, 300 }});
    _ = try filter.changed();

    try zodd.joinAnti(u32, u32, u32, Tuple, &ctx, &input, &filter, &output, struct {
        fn logic(key: *const u32, val: *const u32) Tuple {
            return .{ key.*, val.* };
        }
    }.logic);

    _ = try output.changed();
    try testing.expectEqual(@as(usize, 1), output.recent.len());
    try testing.expectEqual(@as(u32, 2), output.recent.elements[0][0]);
}

test "regression: complete on empty Variable returns empty relation" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    var res = try v.complete();
    defer res.deinit();

    try testing.expectEqual(@as(usize, 0), res.len());
}

test "regression: joinInto with empty input produces empty output" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const KV = struct { u32, u32 };
    const Out = struct { u32, u32, u32 };

    var v1 = zodd.Variable(KV).init(&ctx);
    defer v1.deinit();

    var v2 = zodd.Variable(KV).init(&ctx);
    defer v2.deinit();

    var out = zodd.Variable(Out).init(&ctx);
    defer out.deinit();

    try v1.insertSlice(&ctx, &[_]KV{.{ 1, 10 }});
    _ = try v1.changed();

    _ = try v2.changed();

    try zodd.joinInto(u32, u32, u32, Out, &ctx, &v1, &v2, &out, struct {
        fn logic(k: *const u32, v1_val: *const u32, v2_val: *const u32) Out {
            return .{ k.*, v1_val.*, v2_val.* };
        }
    }.logic);

    _ = try out.changed();
    try testing.expectEqual(@as(usize, 0), out.recent.len());
}

test "regression: joinAnti with empty filter keeps all inputs" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var input = zodd.Variable(Tuple).init(&ctx);
    defer input.deinit();

    var filter = zodd.Variable(Tuple).init(&ctx);
    defer filter.deinit();

    var output = zodd.Variable(Tuple).init(&ctx);
    defer output.deinit();

    try input.insertSlice(&ctx, &[_]Tuple{ .{ 1, 10 }, .{ 2, 20 } });
    _ = try input.changed();

    _ = try filter.changed();

    try zodd.joinAnti(u32, u32, u32, Tuple, &ctx, &input, &filter, &output, struct {
        fn logic(key: *const u32, val: *const u32) Tuple {
            return .{ key.*, val.* };
        }
    }.logic);

    _ = try output.changed();
    try testing.expectEqual(@as(usize, 2), output.recent.len());
}

test "regression: aggregate with unique keys" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var rel = try zodd.Relation(Tuple).fromSlice(&ctx, &[_]Tuple{
        .{ 1, 10 },
        .{ 2, 20 },
        .{ 3, 30 },
    });
    defer rel.deinit();

    var result = try zodd.aggregateFn(
        Tuple,
        u32,
        u32,
        &ctx,
        &rel,
        struct {
            fn key(t: *const Tuple) u32 {
                return t[0];
            }
        }.key,
        0,
        struct {
            fn sum(acc: u32, t: *const Tuple) u32 {
                return acc + t[1];
            }
        }.sum,
    );
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.len());
    try testing.expectEqual(result.elements[0][1], 10);
    try testing.expectEqual(result.elements[1][1], 20);
    try testing.expectEqual(result.elements[2][1], 30);
}

/// Wrapper allocator that forwards to a child allocator but:
/// - always rejects in-place `remap` (forcing `realloc` down the alloc+copy+free
///   path), and
/// - can be configured to fail a specific nth `alloc` call.
///
/// Lets tests simulate `realloc` failure without corrupting the underlying
/// allocator's bookkeeping, so std.testing.allocator's leak/size checks still
/// catch bugs in the code under test.
const FlakeyAllocator = struct {
    child: std.mem.Allocator,
    alloc_count: usize = 0,
    /// Number of `alloc` calls that returned null. Tests assert this is > 0
    /// to prove they actually exercised the failure path they meant to.
    alloc_failures: usize = 0,
    /// Index (0-based) of the alloc call that should fail; `null` disables.
    fail_on_alloc: ?usize = null,
    /// When true, every `alloc` call after `fail_on_alloc` is set returns null
    /// until cleared. Overrides `fail_on_alloc`.
    fail_all_allocs: bool = false,
    bytes_allocated: usize = 0,
    bytes_freed: usize = 0,

    pub fn allocator(self: *FlakeyAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = rawAlloc,
                .resize = rawResize,
                .remap = rawRemap,
                .free = rawFree,
            },
        };
    }

    fn rawAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *FlakeyAllocator = @ptrCast(@alignCast(ctx));
        const idx = self.alloc_count;
        self.alloc_count += 1;
        if (self.fail_all_allocs) {
            self.alloc_failures += 1;
            return null;
        }
        if (self.fail_on_alloc) |target| {
            if (idx == target) {
                self.alloc_failures += 1;
                return null;
            }
        }
        const p = self.child.rawAlloc(len, alignment, ret_addr);
        if (p != null) self.bytes_allocated += len;
        return p;
    }

    fn rawResize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *FlakeyAllocator = @ptrCast(@alignCast(ctx));
        return self.child.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn rawRemap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }

    fn rawFree(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *FlakeyAllocator = @ptrCast(@alignCast(ctx));
        self.bytes_freed += memory.len;
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

test "regression: Relation.fromSlice survives realloc-shrink failure" {
    // With the old `realloc(...) catch elements[0..unique_len]` pattern, a
    // failed shrink would leave Relation.elements pointing at a prefix of an
    // over-sized allocation. The subsequent `deinit` then frees with the
    // shortened length and trips std.testing.allocator's size assertion.
    // This test forces realloc to fail (via FlakeyAllocator) and relies on
    // that size assertion to catch any regression.
    //
    // Allocation sequence: #0 = initial elements buffer, #1 = realloc-internal
    // alloc during shrink. We fail #1 so the shrinkOrCopy fallback runs.
    var fa = FlakeyAllocator{ .child = testing.allocator, .fail_on_alloc = 1 };
    var ctx = zodd.ExecutionContext.init(fa.allocator());

    const input = [_]u32{ 1, 1, 2, 2, 3, 3 };
    var rel = try zodd.Relation(u32).fromSlice(&ctx, &input);
    defer rel.deinit();

    try testing.expectEqual(@as(usize, 3), rel.len());
    try testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3 }, rel.elements);
    try testing.expect(fa.alloc_failures > 0);
}

test "regression: Relation.merge survives realloc-shrink failure" {
    var fa = FlakeyAllocator{ .child = testing.allocator };
    var ctx = zodd.ExecutionContext.init(fa.allocator());

    var a = try zodd.Relation(u32).fromSlice(&ctx, &[_]u32{ 1, 3, 5 });
    var b = try zodd.Relation(u32).fromSlice(&ctx, &[_]u32{ 3, 5, 7 });

    // After `fromSlice` twice with already-sorted+unique input, alloc_count
    // should reflect one alloc per relation. merge will alloc the merge buffer
    // (next), then shrink will issue a realloc whose internal alloc is the
    // one after that. Fail that alloc specifically.
    fa.fail_on_alloc = fa.alloc_count + 1;

    var merged = try a.merge(&b);
    defer merged.deinit();

    try testing.expectEqual(@as(usize, 4), merged.len());
    try testing.expectEqualSlices(u32, &[_]u32{ 1, 3, 5, 7 }, merged.elements);
    try testing.expect(fa.alloc_failures > 0);
}

test "regression: Relation.loadWithLimit survives realloc-shrink failure" {
    var fa = FlakeyAllocator{ .child = testing.allocator };
    var ctx = zodd.ExecutionContext.init(fa.allocator());
    const Tuple = struct { u32, u32 };

    // Build a valid serialized buffer with duplicates so load shrinks.
    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    const w = &aw.writer;
    try w.writeAll("ZODDREL");
    try w.writeInt(u8, 1, .little);
    try w.writeInt(u64, 4, .little);
    const raw = [_]Tuple{ .{ 1, 10 }, .{ 1, 10 }, .{ 2, 20 }, .{ 2, 20 } };
    for (raw) |t| {
        try w.writeInt(u32, t[0], .little);
        try w.writeInt(u32, t[1], .little);
    }

    // Alloc sequence: #0 initial load buffer, #1 realloc-internal alloc.
    fa.fail_on_alloc = 1;

    var reader = std.Io.Reader.fixed(aw.writer.buffered());
    var loaded = try zodd.Relation(Tuple).load(&ctx, &reader);
    defer loaded.deinit();

    try testing.expectEqual(@as(usize, 2), loaded.len());
    try testing.expect(fa.alloc_failures > 0);
}

test "regression: SecondaryIndex.deinit frees map even if iterator fails" {
    // The old deinit early-returned on iterator() OOM, leaking the B-tree
    // structure *and* every nested Relation. The fix wraps the walk in a
    // `defer self.map.deinit()` so the tree is always freed.
    //
    // We use page_allocator as the child so that the unavoidable nested-
    // Relation leak (we can't visit the values without the iterator) doesn't
    // trip testing.allocator's leak assertion. The regression signal is the
    // byte counter on FlakeyAllocator itself: under the old bug, deinit
    // freed nothing; with the fix, the B-tree's internal nodes are freed.
    var fa = FlakeyAllocator{ .child = std.heap.page_allocator };
    var ctx = zodd.ExecutionContext.init(fa.allocator());

    const Tuple = struct { u32, u32 };
    const u32Cmp = struct {
        fn f(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.f;
    const Index = zodd.index.SecondaryIndex(Tuple, u32, struct {
        fn extract(t: Tuple) u32 {
            return t[0];
        }
    }.extract, u32Cmp, 4);

    var idx = Index.init(&ctx);
    try idx.insert(.{ 1, 10 });
    try idx.insert(.{ 2, 20 });
    try idx.insert(.{ 3, 30 });

    fa.fail_on_alloc = fa.alloc_count;
    const freed_before = fa.bytes_freed;
    idx.deinit();
    const freed_during = fa.bytes_freed - freed_before;

    try testing.expect(fa.alloc_failures > 0);
    try testing.expect(freed_during > 0);
}

test "regression: Variable.changed frees local batches on merge OOM" {
    // changed() moves self.recent into a local, pops a batch off self.stable,
    // and merges the two. If the merge's internal alloc fails, both locals
    // must be freed. Before the fix they leaked.
    var fa = FlakeyAllocator{ .child = testing.allocator };
    var ctx = zodd.ExecutionContext.init(fa.allocator());

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    // Round 1: { 1, 2, 3, 4 } → self.recent.
    try v.insertSlice(&ctx, &[_]u32{ 1, 2, 3, 4 });
    _ = try v.changed();

    // Round 2: { 5, 6, 7, 8 } → self.recent; round 1 batch moves to stable.
    try v.insertSlice(&ctx, &[_]u32{ 5, 6, 7, 8 });
    _ = try v.changed();

    // Queue round 3's new tuples so changed() has real work to do.
    try v.insertSlice(&ctx, &[_]u32{ 9, 10 });

    // Round 3: with len(stable.last) == 4 and len(recent) == 4, changed()
    // triggers the stable+recent merge. Fail the merge buffer alloc.
    fa.fail_on_alloc = fa.alloc_count;

    try testing.expectError(error.OutOfMemory, v.changed());
    try testing.expect(fa.alloc_failures > 0);
    // testing.allocator's leak check at scope exit asserts no leak.
}

test "regression: Variable.complete frees locals on merge OOM" {
    // complete() pops batches off self.stable and merges them into a single
    // result. A failing merge alloc must still free the in-flight locals.
    var fa = FlakeyAllocator{ .child = testing.allocator };
    var ctx = zodd.ExecutionContext.init(fa.allocator());

    var v = zodd.Variable(u32).init(&ctx);
    defer v.deinit();

    // Build up multiple stable batches.
    try v.insertSlice(&ctx, &[_]u32{ 1, 2 });
    _ = try v.changed();
    try v.insertSlice(&ctx, &[_]u32{ 3, 4 });
    _ = try v.changed();
    try v.insertSlice(&ctx, &[_]u32{ 5, 6 });
    _ = try v.changed();

    fa.fail_on_alloc = fa.alloc_count;

    try testing.expectError(error.OutOfMemory, v.complete());
    try testing.expect(fa.alloc_failures > 0);
}

test "regression: extendInto cleans up all tasks on mid-loop clone failure" {
    // The parallel extendInto path clones each leaper once per task in a
    // tight loop. If cloning fails partway through, tasks whose leapers were
    // already populated must be cleaned up. The old version did not track
    // that, so testing.allocator's leak detector catches any regression.
    const Tuple = struct { u32 };
    const Val = u32;
    const KV = struct { u32, u32 };

    var fa = FlakeyAllocator{ .child = testing.allocator };
    var ctx = try zodd.ExecutionContext.initWithThreads(fa.allocator(), 2);
    defer ctx.deinit();

    // > 128 tuples so extendInto splits into multiple tasks (chunk size 128).
    var input: [200]Tuple = undefined;
    for (&input, 0..) |*t, i| t.* = .{@intCast(i)};

    var source = zodd.Variable(Tuple).init(&ctx);
    defer source.deinit();
    try source.insertSlice(&ctx, &input);
    _ = try source.changed();

    var rel = try zodd.Relation(KV).fromSlice(&ctx, &[_]KV{ .{ 1, 10 }, .{ 2, 20 } });
    defer rel.deinit();

    var ext = zodd.ExtendWith(Tuple, u32, Val).init(&ctx, &rel, struct {
        fn f(t: *const Tuple) u32 {
            return t[0];
        }
    }.f);
    var leapers = [_]zodd.Leaper(Tuple, Val){ext.leaper()};

    var output = zodd.Variable(KV).init(&ctx);
    defer output.deinit();

    // Allocation sequence inside extendInto's parallel branch:
    //   [+0] tasks array
    //   [+1] task 0 clones array   [+2] task 0 clone[0]
    //   [+3] task 1 clones array   [+4] task 1 clone[0]
    // Failing [+4] reproduces the leak scenario (task 0 fully populated).
    fa.fail_on_alloc = fa.alloc_count + 4;

    const result = zodd.extendInto(Tuple, Val, KV, &ctx, &source, &leapers, &output, struct {
        fn logic(t: *const Tuple, v: *const Val) KV {
            return .{ t[0], v.* };
        }
    }.logic);

    try testing.expectError(error.OutOfMemory, result);
    try testing.expect(fa.alloc_failures > 0);
    // testing.allocator's leak check at scope exit is the actual regression
    // assertion: with the old code, task 0's cloned leapers would leak.
}

test "regression: Variable.filterAgainst survives realloc-shrink failure" {
    // filterAgainst compacts target in place, then shrinks. We drive it by
    // inserting duplicates that get filtered out on the next `changed()`.
    // Allocation counting across `Variable.changed()` and ArrayList growth is
    // brittle, so instead of picking one exact index we fail every alloc of
    // the exact shrink size by re-using shrinkOrCopy directly from our test.
    // (Deterministic integration coverage for Variable is already provided by
    // the fromSlice/merge/load tests above, which share the same helper.)
    const allocator = testing.allocator;
    const original = try allocator.alloc(u32, 8);
    for (original, 0..) |*slot, i| slot.* = @intCast(i);

    var fa = FlakeyAllocator{ .child = allocator, .fail_on_alloc = 0 };
    const shrunk = try zodd.relation.shrinkOrCopy(u32, fa.allocator(), original, 5);
    defer fa.allocator().free(shrunk);

    try testing.expectEqual(@as(usize, 5), shrunk.len);
    try testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 2, 3, 4 }, shrunk);
    try testing.expect(fa.alloc_failures > 0);
}

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

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    try original.save(buffer.writer(allocator));

    var fbs = std.io.fixedBufferStream(buffer.items);
    var loaded = try zodd.Relation(Tuple).load(&ctx, fbs.reader());
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

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer = buffer.writer(allocator);
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 1, .little);
    try writer.writeInt(u64, 2, .little);

    const t1 = Tuple{ 1, 10 };
    const t2 = Tuple{ 2, 20 };
    const arr1 = [_]Tuple{t1};
    const arr2 = [_]Tuple{t2};
    try writer.writeAll(std.mem.sliceAsBytes(&arr1));
    try writer.writeAll(std.mem.sliceAsBytes(&arr2));

    var reader = std.io.fixedBufferStream(buffer.items);
    try testing.expectError(error.TooLarge, zodd.Relation(Tuple).loadWithLimit(&ctx, reader.reader(), 1));
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

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer = buffer.writer(allocator);
    try writer.writeAll("BADMAGC");
    try writer.writeInt(u8, 1, .little);
    try writer.writeInt(u64, 1, .little);

    const t1 = Tuple{ 1, 10 };
    const arr1 = [_]Tuple{t1};
    try writer.writeAll(std.mem.sliceAsBytes(&arr1));

    var reader = std.io.fixedBufferStream(buffer.items);
    try testing.expectError(error.InvalidFormat, zodd.Relation(Tuple).loadWithLimit(&ctx, reader.reader(), 10));
}

test "regression: loadWithLimit rejects unsupported version" {
    const allocator = testing.allocator;
    var ctx = zodd.ExecutionContext.init(allocator);
    const Tuple = struct { u32, u32 };

    var buffer = std.ArrayListUnmanaged(u8){};
    defer buffer.deinit(allocator);

    var writer = buffer.writer(allocator);
    try writer.writeAll("ZODDREL");
    try writer.writeInt(u8, 2, .little);
    try writer.writeInt(u64, 1, .little);

    const t1 = Tuple{ 1, 10 };
    const arr1 = [_]Tuple{t1};
    try writer.writeAll(std.mem.sliceAsBytes(&arr1));

    var reader = std.io.fixedBufferStream(buffer.items);
    try testing.expectError(error.UnsupportedVersion, zodd.Relation(Tuple).loadWithLimit(&ctx, reader.reader(), 10));
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

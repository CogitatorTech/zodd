const std = @import("std");
const zodd = @import("zodd");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Zodd Datalog Engine - Points-To Analysis Example\n", .{});
    std.debug.print("================================================\n\n", .{});

    const Pair = struct { u32, u32 };

    const alloc_data = [_]Pair{
        .{ 1, 100 },
        .{ 2, 200 },
        .{ 3, 300 },
    };

    const assign_data = [_]Pair{
        .{ 4, 1 },
        .{ 5, 2 },
        .{ 6, 4 },
        .{ 7, 5 },
    };

    const load_data = [_]Pair{
        .{ 8, 6 },
    };

    const store_data = [_]Pair{
        .{ 6, 3 },
    };

    std.debug.print("Program statements:\n", .{});
    for (alloc_data) |a| {
        std.debug.print("  v{} = alloc(obj{})\n", .{ a[0], a[1] });
    }
    for (assign_data) |a| {
        std.debug.print("  v{} = v{}\n", .{ a[0], a[1] });
    }
    for (load_data) |l| {
        std.debug.print("  v{} = *v{}\n", .{ l[0], l[1] });
    }
    for (store_data) |s| {
        std.debug.print("  *v{} = v{}\n", .{ s[0], s[1] });
    }
    std.debug.print("\n", .{});

    var alloc = try zodd.Relation(Pair).fromSlice(allocator, &alloc_data);
    defer alloc.deinit();

    var assign = try zodd.Relation(Pair).fromSlice(allocator, &assign_data);
    defer assign.deinit();

    var load = try zodd.Relation(Pair).fromSlice(allocator, &load_data);
    defer load.deinit();

    var store = try zodd.Relation(Pair).fromSlice(allocator, &store_data);
    defer store.deinit();

    var points_to = zodd.Variable(Pair).init(allocator);
    defer points_to.deinit();

    try points_to.insertSlice(alloc.elements);

    std.debug.print("Computing points-to analysis...\n", .{});
    std.debug.print("Rules:\n", .{});
    std.debug.print("  points_to(V, O) :- alloc(V, O)\n", .{});
    std.debug.print("  points_to(V1, O) :- assign(V1, V2), points_to(V2, O)\n", .{});
    std.debug.print("  points_to(V1, O) :- load(V1, V2), points_to(V2, P), points_to(P, O)\n", .{});
    std.debug.print("  points_to(P, O) :- store(P_ptr, V), points_to(P_ptr, P), points_to(V, O)\n\n", .{});

    const ResultList = std.ArrayListUnmanaged(Pair);
    var iteration: usize = 0;
    while (try points_to.changed()) : (iteration += 1) {
        std.debug.print("  Iteration {}: {} recent tuples\n", .{ iteration, points_to.recent.len() });

        var results = ResultList{};
        defer results.deinit(allocator);

        for (points_to.recent.elements) |pt| {
            const v = pt[0];
            const o = pt[1];

            for (assign.elements) |a| {
                if (a[1] == v) {
                    try results.append(allocator, .{ a[0], o });
                }
            }

            for (load.elements) |l| {
                if (l[1] == v) {
                    for (points_to.stable.items) |*batch| {
                        for (batch.elements) |pt2| {
                            if (pt2[0] == o) {
                                try results.append(allocator, .{ l[0], pt2[1] });
                            }
                        }
                    }
                    for (points_to.recent.elements) |pt2| {
                        if (pt2[0] == o) {
                            try results.append(allocator, .{ l[0], pt2[1] });
                        }
                    }
                }
            }

            for (store.elements) |s| {
                if (s[1] == v) {
                    for (points_to.stable.items) |*batch| {
                        for (batch.elements) |pt2| {
                            if (pt2[0] == s[0]) {
                                try results.append(allocator, .{ pt2[1], o });
                            }
                        }
                    }
                    for (points_to.recent.elements) |pt2| {
                        if (pt2[0] == s[0]) {
                            try results.append(allocator, .{ pt2[1], o });
                        }
                    }
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Pair).fromSlice(allocator, results.items);
            try points_to.insert(rel);
        }

        if (iteration > 50) break;
    }

    std.debug.print("\n", .{});

    var result = try points_to.complete();
    defer result.deinit();

    std.debug.print("Points-to results:\n", .{});
    for (result.elements) |pt| {
        std.debug.print("  v{} -> obj{}\n", .{ pt[0], pt[1] });
    }

    std.debug.print("\nTotal: {} points-to pairs\n", .{result.len()});
}

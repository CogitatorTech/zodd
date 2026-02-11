const std = @import("std");
const zodd = @import("zodd");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - Same Generation Example\n", .{});
    std.debug.print("==============================================\n\n", .{});

    const Pair = struct { u32, u32 };

    const parent_data = [_]Pair{
        .{ 1, 2 },
        .{ 1, 3 },
        .{ 2, 4 },
        .{ 2, 5 },
        .{ 3, 6 },
        .{ 3, 7 },
        .{ 4, 8 },
        .{ 5, 9 },
    };

    std.debug.print("Parent-Child relationships:\n", .{});
    for (parent_data) |p| {
        std.debug.print("  {} is parent of {}\n", .{ p[0], p[1] });
    }
    std.debug.print("\n", .{});

    var parent = try zodd.Relation(Pair).fromSlice(&ctx, &parent_data);
    defer parent.deinit();

    var same_gen = zodd.Variable(Pair).init(&ctx);
    defer same_gen.deinit();

    var initial = [_]Pair{
        .{ 1, 1 }, .{ 2, 2 }, .{ 3, 3 }, .{ 4, 4 },
        .{ 5, 5 }, .{ 6, 6 }, .{ 7, 7 }, .{ 8, 8 },
        .{ 9, 9 },
    };
    try same_gen.insertSlice(&ctx, &initial);

    std.debug.print("Computing same-generation relation...\n", .{});
    std.debug.print("Rule: same_gen(X,Y) :- same_gen(P1,P2), parent(P1,X), parent(P2,Y)\n\n", .{});

    const ResultList = std.ArrayListUnmanaged(Pair);
    var iteration: usize = 0;
    while (try same_gen.changed()) : (iteration += 1) {
        std.debug.print("  Iteration {}: {} recent tuples\n", .{ iteration, same_gen.recent.len() });

        var results = ResultList{};
        defer results.deinit(allocator);

        for (same_gen.recent.elements) |sg| {
            const p1 = sg[0];
            const p2 = sg[1];

            for (parent.elements) |pc1| {
                if (pc1[0] == p1) {
                    for (parent.elements) |pc2| {
                        if (pc2[0] == p2) {
                            try results.append(allocator, .{ pc1[1], pc2[1] });
                        }
                    }
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Pair).fromSlice(&ctx, results.items);
            try same_gen.insert(rel);
        }

        if (iteration > 50) break;
    }

    std.debug.print("\n", .{});

    var result = try same_gen.complete();
    defer result.deinit();

    std.debug.print("Same-generation pairs:\n", .{});
    for (result.elements) |r| {
        if (r[0] != r[1]) {
            std.debug.print("  {} and {} are in the same generation\n", .{ r[0], r[1] });
        }
    }

    std.debug.print("\nTotal: {} same-generation pairs (including reflexive)\n", .{result.len()});
}

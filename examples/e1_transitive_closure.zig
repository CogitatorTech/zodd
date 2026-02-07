const std = @import("std");
const zodd = @import("zodd");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Zodd Datalog Engine - Transitive Closure Example\n", .{});
    std.debug.print("================================================\n\n", .{});

    const Edge = struct { u32, u32 };
    const edges_data = [_]Edge{
        .{ 1, 2 },
        .{ 2, 3 },
        .{ 3, 4 },
        .{ 1, 3 },
        .{ 4, 5 },
    };

    std.debug.print("Input edges:\n", .{});
    for (edges_data) |e| {
        std.debug.print("  {} -> {}\n", .{ e[0], e[1] });
    }
    std.debug.print("\n", .{});

    var edges = try zodd.Relation(Edge).fromSlice(allocator, &edges_data);
    defer edges.deinit();

    var reachable = zodd.Variable(Edge).init(allocator);
    defer reachable.deinit();

    try reachable.insertSlice(edges.elements);

    std.debug.print("Computing transitive closure...\n", .{});

    const ResultList = std.ArrayListUnmanaged(Edge);
    var iteration: usize = 0;
    while (try reachable.changed()) : (iteration += 1) {
        std.debug.print("  Iteration {}: {} recent tuples\n", .{ iteration, reachable.recent.len() });

        var results = ResultList{};
        defer results.deinit(allocator);

        for (reachable.recent.elements) |r| {
            const x = r[0];
            const y = r[1];

            for (edges.elements) |e| {
                if (e[0] == y) {
                    try results.append(allocator, .{ x, e[1] });
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Edge).fromSlice(allocator, results.items);
            try reachable.insert(rel);
        }

        if (iteration > 100) {
            std.debug.print("  (reached iteration limit)\n", .{});
            break;
        }
    }

    std.debug.print("\n", .{});

    var result = try reachable.complete();
    defer result.deinit();

    std.debug.print("Reachability (transitive closure):\n", .{});
    for (result.elements) |r| {
        std.debug.print("  {} can reach {}\n", .{ r[0], r[1] });
    }

    std.debug.print("\nTotal: {} reachable pairs\n", .{result.len()});
}

const std = @import("std");
const zodd = @import("zodd");

// Arithmetic Assignments
//
// Counts network hops from a gateway with the `is` operator, which binds a
// fresh variable to an arithmetic expression per tuple. Recursive rules
// that use an assignment can derive new values forever (here the topology
// has a cycle), so the database requires `max_iterations` to be set, and a
// comparison filter bounds the hop count itself.
//
// Datalog rules:
//   reach("gw", 0).
//   reach(Y, H2)    :- reach(X, H), link(X, Y), H2 is H + 1, H2 < 5.
//   best(N, min(H)) :- reach(N, H).

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Zodd Datalog Engine - Arithmetic Assignments\n", .{});
    std.debug.print("============================================\n\n", .{});

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\% Directed links; d -> a closes a cycle.
        \\link("gw", "a"). link("a", "b"). link("b", "c").
        \\link("a", "c"). link("c", "d"). link("d", "a").
        \\
        \\% Hop counts from the gateway: H2 is H + 1 computes the next hop
        \\% count, and H2 < 5 keeps the search within a hop budget.
        \\reach("gw", 0).
        \\reach(Y, H2) :- reach(X, H), link(X, Y), H2 is H + 1, H2 < 5.
        \\
        \\% Shortest observed hop count per node.
        \\best(N, min(H)) :- reach(N, H).
    );

    // A recursive rule with an assignment must run under an iteration
    // limit; without one, `solve` returns error.IterationLimitRequired.
    db.max_iterations = 32;
    try db.solve();

    std.debug.print("Reachable within the hop budget:\n", .{});
    var best = try db.query("best", &.{ null, null });
    defer best.deinit();
    while (best.next()) |row| {
        std.debug.print("  {s}: {d} hop(s)\n", .{ row.get(0).str, row.get(1).int });
    }

    std.debug.print("\nAll hop counts observed for node c:\n", .{});
    var c_hops = try db.query("reach", &.{ zodd.Value{ .str = "c" }, null });
    defer c_hops.deinit();
    while (c_hops.next()) |row| {
        std.debug.print("  {d} hop(s)\n", .{row.get(1).int});
    }
}

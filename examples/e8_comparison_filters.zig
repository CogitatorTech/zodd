const std = @import("std");
const zodd = @import("zodd");

// Comparison Filters
//
// Uses comparison operators in rule bodies to monitor service latencies
// against SLA limits. Comparisons are filters: every comparison variable
// must also occur in a positive body literal. Equality (`=`) and
// inequality (`!=`) compare any values; the ordered operators (`<`, `<=`,
// `>`, `>=`) compare integers.
//
// Datalog rules:
//   breach(S, P)      :- latency(S, P, L), sla(S, Limit), L > Limit.
//   flagged(S)        :- breach(S, _).
//   healthy(S)        :- sla(S, _), not flagged(S).
//   worst(S, max(L))  :- latency(S, _, L).
//   slower_than(A, B) :- worst(A, LA), worst(B, LB), A != B, LA > LB.

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Zodd Datalog Engine - Comparison Filters\n", .{});
    std.debug.print("========================================\n\n", .{});

    var db = zodd.Database.init(allocator);
    defer db.deinit();

    // The same rules can be built programmatically: a comparison is added
    // with `r.cmp(l, .gt, limit)` on the rule builder.
    try db.run(
        \\% Observed latencies (milliseconds) per service and probe.
        \\latency("auth", 1, 12). latency("auth", 2, 18).
        \\latency("search", 1, 220). latency("search", 2, 250).
        \\latency("billing", 1, 95). latency("billing", 2, 80).
        \\
        \\% SLA limit per service.
        \\sla("auth", 50). sla("search", 200). sla("billing", 100).
        \\
        \\% A probe breaches when its latency exceeds the service's limit.
        \\breach(S, P) :- latency(S, P, L), sla(S, Limit), L > Limit.
        \\
        \\% A service is healthy when no probe breached its SLA.
        \\flagged(S) :- breach(S, _).
        \\healthy(S) :- sla(S, _), not flagged(S).
        \\
        \\% Worst observed latency per service, then a pairwise ordering:
        \\% comparisons also apply to aggregate results in a later stratum.
        \\worst(S, max(L)) :- latency(S, _, L).
        \\slower_than(A, B) :- worst(A, LA), worst(B, LB), A != B, LA > LB.
    );

    try db.solve();

    std.debug.print("SLA breaches:\n", .{});
    var breaches = try db.query("breach", &.{ null, null });
    defer breaches.deinit();
    while (breaches.next()) |row| {
        std.debug.print("  {s} (probe {d})\n", .{ row.get(0).str, row.get(1).int });
    }

    std.debug.print("\nHealthy services:\n", .{});
    var healthy = try db.query("healthy", &.{null});
    defer healthy.deinit();
    while (healthy.next()) |row| {
        std.debug.print("  {s}\n", .{row.get(0).str});
    }

    std.debug.print("\nWorst latency per service:\n", .{});
    var worst = try db.query("worst", &.{ null, null });
    defer worst.deinit();
    while (worst.next()) |row| {
        std.debug.print("  {s}: {d} ms\n", .{ row.get(0).str, row.get(1).int });
    }

    std.debug.print("\nPairwise ordering by worst latency:\n", .{});
    var slower = try db.query("slower_than", &.{ null, null });
    defer slower.deinit();
    while (slower.next()) |row| {
        std.debug.print("  {s} is slower than {s}\n", .{ row.get(0).str, row.get(1).str });
    }
}

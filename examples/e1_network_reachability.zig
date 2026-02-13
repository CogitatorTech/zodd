const std = @import("std");
const zodd = @import("zodd");

// Network Reachability Analysis
//
// Determines which network zones can communicate through routing policies and
// firewall rules. A common task in enterprise security auditing to identify
// unintended exposure paths. For example, verifying that the internet cannot
// reach the database tier, or that PCI zones are properly isolated.
//
// Datalog rules:
//   reachable(A, B) :- link(A, B).
//   reachable(A, C) :- reachable(A, B), link(B, C).
//   allowed(A, B)   :- reachable(A, B), NOT blocked(A, B).
//   exposure(Z)     :- allowed(internet, Z).

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - Network Reachability Analysis\n", .{});
    std.debug.print("===================================================\n\n", .{});

    // Network topology:
    //
    //   internet(1) --> dmz(2) --> app_tier(3) --> db_tier(4)
    //                    |                           |
    //                    v                           v
    //               monitoring(5) <------------- logging(6)
    //                    |
    //                    v
    //               pci_zone(7)
    //
    // Zone IDs:
    //   internet=1, dmz=2, app_tier=3, db_tier=4,
    //   monitoring=5, logging=6, pci_zone=7

    const Pair = struct { u32, u32 };

    // Network links (directional routing rules)
    const link_data = [_]Pair{
        .{ 1, 2 }, // internet -> dmz
        .{ 2, 3 }, // dmz -> app_tier
        .{ 3, 4 }, // app_tier -> db_tier
        .{ 2, 5 }, // dmz -> monitoring
        .{ 4, 6 }, // db_tier -> logging
        .{ 6, 5 }, // logging -> monitoring
        .{ 5, 7 }, // monitoring -> pci_zone
    };

    // Firewall deny rules: blocked(src_zone, dst_zone)
    const blocked_data = [_]Pair{
        .{ 1, 3 }, // block internet -> app_tier (must go through dmz)
        .{ 1, 4 }, // block internet -> db_tier
        .{ 1, 7 }, // block internet -> pci_zone
        .{ 2, 4 }, // block dmz -> db_tier (must go through app_tier)
        .{ 2, 7 }, // block dmz -> pci_zone
        .{ 3, 7 }, // block app_tier -> pci_zone
    };

    const zoneName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                1 => "internet",
                2 => "dmz",
                3 => "app_tier",
                4 => "db_tier",
                5 => "monitoring",
                6 => "logging",
                7 => "pci_zone",
                else => "unknown",
            };
        }
    }.get;

    std.debug.print("Network links:\n", .{});
    for (link_data) |l| {
        std.debug.print("  {s} -> {s}\n", .{ zoneName(l[0]), zoneName(l[1]) });
    }

    std.debug.print("\nFirewall deny rules:\n", .{});
    for (blocked_data) |b| {
        std.debug.print("  DENY {s} -> {s}\n", .{ zoneName(b[0]), zoneName(b[1]) });
    }

    // -- Build relations --

    var links = try zodd.Relation(Pair).fromSlice(&ctx, &link_data);
    defer links.deinit();

    var blocked = try zodd.Relation(Pair).fromSlice(&ctx, &blocked_data);
    defer blocked.deinit();

    // -- Step 1: Compute reachable zones (transitive routing) --
    //   reachable(A, B) :- link(A, B).
    //   reachable(A, C) :- reachable(A, B), link(B, C).

    var reachable = zodd.Variable(Pair).init(&ctx);
    defer reachable.deinit();

    try reachable.insertSlice(&ctx, links.elements);

    std.debug.print("\nComputing transitive reachability...\n", .{});

    const PairList = std.ArrayListUnmanaged(Pair);
    var iteration: usize = 0;
    while (try reachable.changed()) : (iteration += 1) {
        var results = PairList{};
        defer results.deinit(allocator);

        for (reachable.recent.elements) |r| {
            for (links.elements) |l| {
                if (l[0] == r[1]) {
                    try results.append(allocator, .{ r[0], l[1] });
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Pair).fromSlice(&ctx, results.items);
            try reachable.insert(rel);
        }

        if (iteration > 50) break;
    }

    var reach_result = try reachable.complete();
    defer reach_result.deinit();

    std.debug.print("\nAll reachable zone pairs (via routing):\n", .{});
    for (reach_result.elements) |r| {
        std.debug.print("  {s} -> {s}\n", .{ zoneName(r[0]), zoneName(r[1]) });
    }

    // -- Step 2: Apply firewall rules (anti-join) --
    //   allowed(A, B) :- reachable(A, B), NOT blocked(A, B).

    std.debug.print("\nApplying firewall rules...\n", .{});

    var allowed = PairList{};
    defer allowed.deinit(allocator);

    for (reach_result.elements) |r| {
        var is_blocked = false;
        for (blocked.elements) |b| {
            if (b[0] == r[0] and b[1] == r[1]) {
                is_blocked = true;
                break;
            }
        }
        if (is_blocked) {
            std.debug.print("  BLOCKED: {s} -> {s}\n", .{ zoneName(r[0]), zoneName(r[1]) });
        } else {
            try allowed.append(allocator, r);
        }
    }

    std.debug.print("\nAllowed communication paths:\n", .{});
    for (allowed.items) |a| {
        std.debug.print("  {s} -> {s}\n", .{ zoneName(a[0]), zoneName(a[1]) });
    }

    // -- Step 3: Identify internet-exposed zones --
    //   exposure(Z) :- allowed(internet, Z).

    std.debug.print("\nInternet-exposed zones:\n", .{});
    var exposure_count: usize = 0;
    for (allowed.items) |a| {
        if (a[0] == 1) { // internet
            std.debug.print("  {s} is reachable from the internet\n", .{zoneName(a[1])});
            exposure_count += 1;
        }
    }
    if (exposure_count == 0) {
        std.debug.print("  (none)\n", .{});
    }

    // -- Step 4: Security audit summary --

    std.debug.print("\nSecurity audit:\n", .{});
    const critical_zones = [_]u32{ 4, 7 }; // db_tier, pci_zone
    const critical_names = [_][]const u8{ "db_tier", "pci_zone" };

    for (critical_zones, 0..) |zone, idx| {
        var exposed = false;
        for (allowed.items) |a| {
            if (a[0] == 1 and a[1] == zone) {
                exposed = true;
                break;
            }
        }
        if (exposed) {
            std.debug.print("  WARNING: {s} is exposed to the internet!\n", .{critical_names[idx]});
        } else {
            std.debug.print("  OK: {s} is not reachable from the internet\n", .{critical_names[idx]});
        }
    }

    std.debug.print("\nTotal: {} reachable pairs, {} allowed after firewall, {} internet-exposed\n", .{
        reach_result.len(),
        allowed.items.len,
        exposure_count,
    });
}

const std = @import("std");
const zodd = @import("zodd");

// Data Lineage for GDPR/CCPA Compliance
//
// Tracks how sensitive data (PII) flows through ETL pipelines and data
// warehouse transformations. Identifies which downstream datasets contain
// PII, verifies that anonymization steps properly cleanse data, and flags
// compliance violations when PII appears in public-facing datasets.
//
// Datalog rules:
//   contains_pii(D)  :- source_pii(D).
//   contains_pii(D2) :- contains_pii(D1), transform(D1, D2),
//                        NOT anonymizes(D1, D2).
//   violation(D)     :- contains_pii(D), public_dataset(D).

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - Data Lineage for Compliance\n", .{});
    std.debug.print("=================================================\n\n", .{});

    // Data pipeline:
    //
    //   [raw_users] -----> [user_profiles] -----> [analytics_users]
    //       PII                                        |
    //                                                  v
    //   [raw_orders] ----> [order_details] -----> [sales_report] (PUBLIC)
    //                           |
    //                           v
    //                    [anonymized_orders] ---> [public_dashboard] (PUBLIC)
    //                     (anonymized)
    //
    //   [raw_logs] ------> [enriched_logs] -----> [audit_trail]
    //       PII                PII                     |
    //                                                  v
    //                                            [log_summary] (PUBLIC)
    //                                             (anonymized)

    const Pair = struct { u32, u32 };
    const Scalar = struct { u32 };

    // Dataset IDs:
    //   raw_users=1, user_profiles=2, analytics_users=3,
    //   raw_orders=4, order_details=5, sales_report=6,
    //   anonymized_orders=7, public_dashboard=8,
    //   raw_logs=9, enriched_logs=10, audit_trail=11, log_summary=12

    // Datasets containing PII at the source level
    const source_pii_data = [_]Scalar{
        .{1}, // raw_users
        .{9}, // raw_logs (contains IP addresses, user agents)
    };

    // ETL transformations: transform(source, destination)
    const transform_data = [_]Pair{
        .{ 1, 2 }, // raw_users -> user_profiles
        .{ 2, 3 }, // user_profiles -> analytics_users
        .{ 4, 5 }, // raw_orders -> order_details
        .{ 3, 6 }, // analytics_users -> sales_report
        .{ 5, 6 }, // order_details -> sales_report
        .{ 5, 7 }, // order_details -> anonymized_orders
        .{ 7, 8 }, // anonymized_orders -> public_dashboard
        .{ 9, 10 }, // raw_logs -> enriched_logs
        .{ 10, 11 }, // enriched_logs -> audit_trail
        .{ 11, 12 }, // audit_trail -> log_summary
    };

    // Anonymization steps: these block PII propagation
    const anonymize_data = [_]Pair{
        .{ 5, 7 }, // order_details -> anonymized_orders (PII stripped)
        .{ 11, 12 }, // audit_trail -> log_summary (PII stripped)
    };

    // Public-facing datasets
    const public_data = [_]Scalar{
        .{6}, // sales_report
        .{8}, // public_dashboard
        .{12}, // log_summary
    };

    const dsName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                1 => "raw_users",
                2 => "user_profiles",
                3 => "analytics_users",
                4 => "raw_orders",
                5 => "order_details",
                6 => "sales_report",
                7 => "anonymized_orders",
                8 => "public_dashboard",
                9 => "raw_logs",
                10 => "enriched_logs",
                11 => "audit_trail",
                12 => "log_summary",
                else => "unknown",
            };
        }
    }.get;

    std.debug.print("PII sources:\n", .{});
    for (source_pii_data) |s| {
        std.debug.print("  {s}\n", .{dsName(s[0])});
    }

    std.debug.print("\nETL transformations:\n", .{});
    for (transform_data) |t| {
        var is_anon = false;
        for (anonymize_data) |a| {
            if (a[0] == t[0] and a[1] == t[1]) {
                is_anon = true;
                break;
            }
        }
        if (is_anon) {
            std.debug.print("  {s} -> {s} [ANONYMIZED]\n", .{ dsName(t[0]), dsName(t[1]) });
        } else {
            std.debug.print("  {s} -> {s}\n", .{ dsName(t[0]), dsName(t[1]) });
        }
    }

    std.debug.print("\nPublic-facing datasets:\n", .{});
    for (public_data) |p| {
        std.debug.print("  {s}\n", .{dsName(p[0])});
    }

    // -- Build relations --

    var transforms = try zodd.Relation(Pair).fromSlice(&ctx, &transform_data);
    defer transforms.deinit();

    var anonymizes = try zodd.Relation(Pair).fromSlice(&ctx, &anonymize_data);
    defer anonymizes.deinit();

    // -- Step 1: Propagate PII through the pipeline --
    //   contains_pii(D) :- source_pii(D).
    //   contains_pii(D2) :- contains_pii(D1), transform(D1, D2),
    //                        NOT anonymizes(D1, D2).

    var contains_pii = zodd.Variable(Scalar).init(&ctx);
    defer contains_pii.deinit();
    try contains_pii.insertSlice(&ctx, &source_pii_data);

    std.debug.print("\nPropagating PII through ETL pipeline...\n", .{});

    // Use ExtendWith to propose destinations for PII-containing datasets
    var extend = zodd.ExtendWith(Scalar, u32, u32).init(&ctx, &transforms, &struct {
        fn key(tuple: *const Scalar) u32 {
            return tuple[0];
        }
    }.key);

    var iteration: usize = 0;
    while (try contains_pii.changed()) : (iteration += 1) {
        std.debug.print("  Iteration {}: {} datasets with PII\n", .{ iteration, contains_pii.recent.len() });

        // Use extendInto to find downstream datasets
        var proposed = zodd.Variable(Pair).init(&ctx);
        defer proposed.deinit();

        const leaper = extend.leaper();
        var leapers = [_]zodd.Leaper(Scalar, u32){leaper};

        try zodd.extendInto(
            Scalar,
            u32,
            Pair,
            &ctx,
            &contains_pii,
            &leapers,
            &proposed,
            &struct {
                fn logic(src: *const Scalar, dst: *const u32) Pair {
                    return .{ src[0], dst.* };
                }
            }.logic,
        );

        _ = try proposed.changed();

        // Filter out anonymized transformations
        const ScalarList = std.ArrayListUnmanaged(Scalar);
        var new_pii = ScalarList{};
        defer new_pii.deinit(allocator);

        for (proposed.recent.elements) |p| {
            var is_anon = false;
            for (anonymizes.elements) |a| {
                if (a[0] == p[0] and a[1] == p[1]) {
                    is_anon = true;
                    break;
                }
            }
            if (is_anon) {
                std.debug.print("    PII blocked: {s} -> {s} (anonymized)\n", .{ dsName(p[0]), dsName(p[1]) });
            } else {
                try new_pii.append(allocator, .{p[1]});
            }
        }

        if (new_pii.items.len > 0) {
            const rel = try zodd.Relation(Scalar).fromSlice(&ctx, new_pii.items);
            try contains_pii.insert(rel);
        }

        if (iteration > 50) break;
    }

    var pii_result = try contains_pii.complete();
    defer pii_result.deinit();

    std.debug.print("\nDatasets containing PII:\n", .{});
    for (pii_result.elements) |p| {
        std.debug.print("  {s}\n", .{dsName(p[0])});
    }

    // -- Step 2: Detect compliance violations --
    //   violation(D) :- contains_pii(D), public_dataset(D).

    std.debug.print("\nCompliance check (PII in public datasets):\n", .{});
    var violation_count: usize = 0;
    for (public_data) |pub_ds| {
        var has_pii = false;
        for (pii_result.elements) |p| {
            if (p[0] == pub_ds[0]) {
                has_pii = true;
                break;
            }
        }
        if (has_pii) {
            std.debug.print("  VIOLATION: {s} is public and contains PII!\n", .{dsName(pub_ds[0])});
            violation_count += 1;
        } else {
            std.debug.print("  OK: {s} is public and PII-free\n", .{dsName(pub_ds[0])});
        }
    }

    // -- Step 3: Trace PII lineage for a specific dataset --
    //
    // For the violated dataset (sales_report), trace back to find which original
    // PII sources contributed their data through non-anonymized paths.

    const trace_target: u32 = 6; // sales_report
    std.debug.print("\nPII lineage trace for '{s}':\n", .{dsName(trace_target)});
    std.debug.print("  Upstream PII sources: ", .{});
    var first = true;

    for (source_pii_data) |src| {
        // BFS from source through non-anonymized transforms to see if it reaches the target
        var frontier = std.ArrayListUnmanaged(u32){};
        defer frontier.deinit(allocator);
        try frontier.append(allocator, src[0]);

        var found = false;
        var step: usize = 0;
        while (frontier.items.len > 0 and step < 20) : (step += 1) {
            var next_frontier = std.ArrayListUnmanaged(u32){};
            defer next_frontier.deinit(allocator);

            for (frontier.items) |node| {
                if (node == trace_target) {
                    found = true;
                    break;
                }
                for (transform_data) |t| {
                    if (t[0] == node) {
                        var is_anon = false;
                        for (anonymize_data) |a| {
                            if (a[0] == t[0] and a[1] == t[1]) {
                                is_anon = true;
                                break;
                            }
                        }
                        if (!is_anon) {
                            try next_frontier.append(allocator, t[1]);
                        }
                    }
                }
            }
            if (found) break;

            frontier.clearRetainingCapacity();
            try frontier.appendSlice(allocator, next_frontier.items);
        }

        if (found) {
            if (!first) std.debug.print(", ", .{});
            std.debug.print("{s}", .{dsName(src[0])});
            first = false;
        }
    }
    std.debug.print("\n", .{});

    std.debug.print("\nSummary: {} datasets with PII, {} compliance violations, {} public datasets clean\n", .{
        pii_result.len(),
        violation_count,
        public_data.len - violation_count,
    });
}

const std = @import("std");
const zodd = @import("zodd");

// Taint Analysis for Security
//
// Tracks the flow of untrusted (tainted) data through a program to detect
// potential security vulnerabilities such as SQL injection and XSS.
//
// Datalog rules:
//   tainted(V)        :- source(V).
//   tainted(V2)       :- tainted(V1), flow(V1, V2), NOT sanitized_flow(V1, V2).
//   violation(V, S)   :- tainted(V), sink(S, V).
//
// Uses ExtendWith (leapfrog trie join) for taint propagation and
// FilterAnti for sanitizer filtering.

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - Taint Analysis Example\n", .{});
    std.debug.print("=============================================\n\n", .{});

    // Simulated program:
    //   v1 = readUserInput()         -- taint source
    //   v2 = readCookie()            -- taint source
    //   v3 = v1                      -- flow: v1 -> v3
    //   v4 = sanitize(v3)            -- sanitized flow: v3 -> v4
    //   v5 = v2 + v3                 -- flow: v2 -> v5, v3 -> v5
    //   v6 = config_value            -- clean variable (not tainted)
    //   v7 = v6                      -- flow: v6 -> v7
    //   sqlQuery(v5)                 -- sink: SQL query with v5
    //   htmlRender(v4)               -- sink: HTML render with v4
    //   htmlRender(v3)               -- sink: HTML render with v3
    //   logMessage(v7)               -- sink: log with v7

    const Pair = struct { u32, u32 };
    const Scalar = struct { u32 };

    // Taint sources
    const source_data = [_]Scalar{
        .{1}, // v1 = readUserInput()
        .{2}, // v2 = readCookie()
    };

    // Data flow edges: flow(from, to)
    const flow_data = [_]Pair{
        .{ 1, 3 }, // v3 = v1
        .{ 3, 4 }, // v4 = sanitize(v3) -- flow exists, but sanitized
        .{ 2, 5 }, // v5 = v2 + ...
        .{ 3, 5 }, // v5 = ... + v3
        .{ 6, 7 }, // v7 = v6
    };

    // Sanitized flows: these block taint propagation
    const sanitized_data = [_]Pair{
        .{ 3, 4 }, // sanitize() between v3 and v4
    };

    // Sinks: sink(sink_id, variable) -- security-sensitive operations
    const sink_data = [_]Pair{
        .{ 50, 5 }, // sqlQuery(v5)
        .{ 51, 4 }, // htmlRender(v4)
        .{ 52, 3 }, // htmlRender(v3)
        .{ 53, 7 }, // logMessage(v7)
    };

    const varName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                1 => "v1 (readUserInput)",
                2 => "v2 (readCookie)",
                3 => "v3",
                4 => "v4 (sanitized)",
                5 => "v5",
                6 => "v6 (config)",
                7 => "v7",
                else => "unknown",
            };
        }
    }.get;

    const sinkName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                50 => "sqlQuery",
                51 => "htmlRender",
                52 => "htmlRender",
                53 => "logMessage",
                else => "unknown",
            };
        }
    }.get;

    std.debug.print("Taint sources:\n", .{});
    for (source_data) |s| {
        std.debug.print("  {s}\n", .{varName(s[0])});
    }

    std.debug.print("\nData flows:\n", .{});
    for (flow_data) |f| {
        var is_sanitized = false;
        for (sanitized_data) |s| {
            if (s[0] == f[0] and s[1] == f[1]) {
                is_sanitized = true;
                break;
            }
        }
        if (is_sanitized) {
            std.debug.print("  v{} -> v{} [SANITIZED]\n", .{ f[0], f[1] });
        } else {
            std.debug.print("  v{} -> v{}\n", .{ f[0], f[1] });
        }
    }

    std.debug.print("\nSinks:\n", .{});
    for (sink_data) |s| {
        std.debug.print("  {s}(v{})\n", .{ sinkName(s[0]), s[1] });
    }

    // -- Build relations --

    var flow = try zodd.Relation(Pair).fromSlice(&ctx, &flow_data);
    defer flow.deinit();

    var sanitized = try zodd.Relation(Pair).fromSlice(&ctx, &sanitized_data);
    defer sanitized.deinit();

    // -- Step 1: Compute tainted variables using ExtendWith + FilterAnti --
    //   tainted(V)  :- source(V).
    //   tainted(V2) :- tainted(V1), flow(V1, V2), NOT sanitized(V1, V2).
    //
    // Source tuple type is Scalar = { u32 } (single tainted variable).
    // We extract the variable id as the Key, and ExtendWith on the flow relation
    // gives us the Val (destination variable). FilterAnti on sanitized blocks
    // sanitized flows.
    //
    // After extension, we get the destination variable, then wrap it back into
    // a Scalar and feed it into the tainted variable.

    var tainted = zodd.Variable(Scalar).init(&ctx);
    defer tainted.deinit();

    try tainted.insertSlice(&ctx, &source_data);

    std.debug.print("\nComputing taint propagation...\n", .{});

    // ExtendWith: extract key from Scalar (the tainted var id), look up in flow relation
    // to get destination variables.
    var extend = zodd.ExtendWith(Scalar, u32, u32).init(&ctx, &flow, &struct {
        fn key(tuple: *const Scalar) u32 {
            return tuple[0];
        }
    }.key);

    // FilterAnti: block flows that are sanitized.
    // FilterAnti's key_func extracts (Key, Val) from the source Scalar.
    // But FilterAnti needs to see both source and proposed value...
    // Actually FilterAnti checks if (key, val) pair exists in the relation.
    // The key_func returns {Key, Val} from the Tuple. For a Scalar source, we only
    // have the source variable, not the proposed destination yet.
    //
    // FilterAnti works in the intersect phase: after propose gives candidate values,
    // intersect with FilterAnti removes values that match the filter.
    // So we cannot use FilterAnti here because it needs a (key, val) from the tuple
    // alone (without the proposed value).
    //
    // Instead, we will: (1) use ExtendWith to propose destinations, then
    // (2) manually filter out sanitized flows in a second pass.

    var iteration: usize = 0;
    while (try tainted.changed()) : (iteration += 1) {
        std.debug.print("  Iteration {}: {} newly tainted variables\n", .{ iteration, tainted.recent.len() });

        // Use extendInto to propose destinations for recently tainted variables
        var proposed = zodd.Variable(Pair).init(&ctx);
        defer proposed.deinit();

        const leaper = extend.leaper();
        var leapers = [_]zodd.Leaper(Scalar, u32){leaper};

        try zodd.extendInto(
            Scalar,
            u32,
            Pair,
            &ctx,
            &tainted,
            &leapers,
            &proposed,
            &struct {
                fn logic(src: *const Scalar, dst: *const u32) Pair {
                    return .{ src[0], dst.* };
                }
            }.logic,
        );

        _ = try proposed.changed();

        // Filter out sanitized flows and convert back to Scalar
        const ScalarList = std.ArrayListUnmanaged(Scalar);
        var new_tainted = ScalarList{};
        defer new_tainted.deinit(allocator);

        for (proposed.recent.elements) |p| {
            var is_sanitized = false;
            for (sanitized.elements) |s| {
                if (s[0] == p[0] and s[1] == p[1]) {
                    is_sanitized = true;
                    break;
                }
            }
            if (!is_sanitized) {
                try new_tainted.append(allocator, .{p[1]});
            }
        }

        if (new_tainted.items.len > 0) {
            const rel = try zodd.Relation(Scalar).fromSlice(&ctx, new_tainted.items);
            try tainted.insert(rel);
        }

        if (iteration > 50) break;
    }

    var tainted_result = try tainted.complete();
    defer tainted_result.deinit();

    std.debug.print("\nTainted variables:\n", .{});
    for (tainted_result.elements) |t| {
        std.debug.print("  {s}\n", .{varName(t[0])});
    }

    // -- Step 2: Detect violations --
    //   violation(V, S) :- tainted(V), sink(S, V).

    std.debug.print("\nSecurity violations detected:\n", .{});
    var violation_count: usize = 0;
    for (sink_data) |s| {
        for (tainted_result.elements) |t| {
            if (t[0] == s[1]) {
                std.debug.print("  VIOLATION: {s}(v{}) -- v{} is tainted!\n", .{
                    sinkName(s[0]),
                    s[1],
                    s[1],
                });
                violation_count += 1;
            }
        }
    }

    if (violation_count == 0) {
        std.debug.print("  (none)\n", .{});
    }

    std.debug.print("\nSafe sinks:\n", .{});
    for (sink_data) |s| {
        var is_tainted = false;
        for (tainted_result.elements) |t| {
            if (t[0] == s[1]) {
                is_tainted = true;
                break;
            }
        }
        if (!is_tainted) {
            std.debug.print("  {s}(v{}) -- safe\n", .{ sinkName(s[0]), s[1] });
        }
    }

    std.debug.print("\nSummary: {} tainted variables, {} violations\n", .{ tainted_result.len(), violation_count });
}

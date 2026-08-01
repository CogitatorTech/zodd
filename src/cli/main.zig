//! # Zodd CLI
//!
//! A command-line interface for the Datalog frontend: run programs, answer
//! ad-hoc queries (demand-driven by default), print rule plans and proof
//! trees, and explore interactively in a REPL.

const std = @import("std");
const chilli = @import("chilli");
const zodd = @import("zodd");
const build_options = @import("build_options");

const io = std.Options.debug_io;

/// Rows printed per query before truncating, matching the web frontend.
const max_rows = 10_000;

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var root = try chilli.Command.init(allocator, .{
        .name = "zodd",
        .description = "An embeddable Datalog engine",
        .version = build_options.version,
        .exec = execRoot,
    });
    defer root.deinit();

    const run_cmd = try chilli.Command.init(allocator, .{
        .name = "run",
        .description = "Solve a program and answer its stored (?-) queries",
        .exec = execRun,
    });
    try run_cmd.addPositional(.{ .name = "file", .description = "Datalog source file", .is_required = true });
    try addCommonFlags(run_cmd);
    try root.addSubcommand(run_cmd);

    const query_cmd = try chilli.Command.init(allocator, .{
        .name = "query",
        .description = "Answer one goal against a program, demand-driven by default",
        .exec = execQuery,
    });
    try query_cmd.addPositional(.{ .name = "file", .description = "Datalog source file", .is_required = true });
    try query_cmd.addPositional(.{ .name = "goal", .description = "Goal, like 'path(1, X)'", .is_required = true });
    try query_cmd.addFlag(.{
        .name = "full",
        .description = "Evaluate the whole program instead of the demanded slice",
        .type = .Bool,
        .default_value = .{ .Bool = false },
    });
    try addCommonFlags(query_cmd);
    try root.addSubcommand(query_cmd);

    const plan_cmd = try chilli.Command.init(allocator, .{
        .name = "plan",
        .description = "Print the compiled join plan of every rule",
        .exec = execPlan,
    });
    try plan_cmd.addPositional(.{ .name = "file", .description = "Datalog source file", .is_required = true });
    try root.addSubcommand(plan_cmd);

    const explain_cmd = try chilli.Command.init(allocator, .{
        .name = "explain",
        .description = "Print the proof tree of a derived fact",
        .exec = execExplain,
    });
    try explain_cmd.addPositional(.{ .name = "file", .description = "Datalog source file", .is_required = true });
    try explain_cmd.addPositional(.{ .name = "fact", .description = "Ground fact, like 'path(1, 3)'", .is_required = true });
    try explain_cmd.addFlag(.{
        .name = "depth",
        .description = "Proof levels to expand",
        .type = .Int,
        .default_value = .{ .Int = 16 },
    });
    try addCommonFlags(explain_cmd);
    try root.addSubcommand(explain_cmd);

    const repl_cmd = try chilli.Command.init(allocator, .{
        .name = "repl",
        .description = "Interactive session; facts and rules end with '.', goals start with '?-'",
        .exec = execRepl,
    });
    try repl_cmd.addPositional(.{
        .name = "file",
        .description = "Datalog source file to preload",
        .default_value = .{ .String = "" },
    });
    try addCommonFlags(repl_cmd);
    try root.addSubcommand(repl_cmd);

    try root.run(init.args, null);
}

fn addCommonFlags(cmd: *chilli.Command) !void {
    try cmd.addFlag(.{
        .name = "max-iterations",
        .description = "Fixed-point rounds allowed per stratum (0 means no limit)",
        .type = .Int,
        .default_value = .{ .Int = 0 },
    });
    try cmd.addFlag(.{
        .name = "parallel",
        .shortcut = 'j',
        .description = "Worker threads per fixed-point round (0 means one per CPU)",
        .type = .Int,
        .default_value = .{ .Int = 1 },
    });
}

fn execRoot(ctx: chilli.CommandContext) !void {
    try ctx.command.printHelp();
}

/// Reads a source file, reporting the path on failure.
fn loadSource(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .unlimited) catch |err| {
        std.debug.print("error: cannot read '{s}': {t}\n", .{ path, err });
        std.process.exit(1);
    };
}

/// Loads a program into `db`, printing the diagnostic on failure.
fn loadProgram(db: *zodd.Database, source: []const u8, ctx: chilli.CommandContext) !void {
    const max_iterations = try ctx.getFlag("max-iterations", i64);
    if (max_iterations > 0) db.max_iterations = @intCast(max_iterations);
    const parallel = try ctx.getFlag("parallel", i64);
    if (parallel >= 0) db.parallelism = @intCast(parallel);
    db.run(source) catch |err| fatal(db, source, err);
}

/// Prints the database diagnostic (with line and column when available) to
/// stderr, or the bare error when there is none.
fn reportDiagnostic(db: *const zodd.Database, source: ?[]const u8, err: anyerror) void {
    const diag = db.lastDiagnostic() orelse {
        std.debug.print("error: {t}\n", .{err});
        return;
    };
    if (diag.message.len == 0) {
        std.debug.print("error: {t}\n", .{err});
        return;
    }
    if (source != null and diag.span != null) {
        const location = lineColumn(source.?, diag.span.?.start);
        std.debug.print("error at {d}:{d}: {s}\n", .{ location.line, location.column, diag.message });
    } else {
        std.debug.print("error: {s}\n", .{diag.message});
    }
}

/// Reports and exits: command handlers call this instead of propagating, so
/// the framework does not print a second, generic error message.
fn fatal(db: *const zodd.Database, source: ?[]const u8, err: anyerror) noreturn {
    reportDiagnostic(db, source, err);
    std.process.exit(1);
}

const Location = struct { line: usize, column: usize };

fn lineColumn(source: []const u8, offset: usize) Location {
    var line: usize = 1;
    var column: usize = 1;
    const end = @min(offset, source.len);
    for (source[0..end]) |byte| {
        if (byte == '\n') {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    return .{ .line = line, .column = column };
}

/// Appends "?- " and "." around a bare goal so it parses as a stored query.
/// Already-wrapped goals pass through.
fn normalizeGoal(allocator: std.mem.Allocator, goal: []const u8) ![]u8 {
    const trimmed = std.mem.trim(u8, goal, " \t\r\n");
    const without_prefix = if (std.mem.startsWith(u8, trimmed, "?-"))
        std.mem.trimStart(u8, trimmed["?-".len..], " \t")
    else
        trimmed;
    const without_dot = if (std.mem.endsWith(u8, without_prefix, "."))
        without_prefix[0 .. without_prefix.len - 1]
    else
        without_prefix;
    return std.fmt.allocPrint(allocator, "?- {s}.", .{without_dot});
}

/// Decodes the last stored query of `db` into a name and Value pattern.
const Goal = struct {
    name: []const u8,
    pattern: [16]?zodd.Value,
    arity: u16,
};

fn lastStoredGoal(db: *zodd.Database) ?Goal {
    if (db.program.queries.items.len == 0) return null;
    const stored = db.program.queries.items[db.program.queries.items.len - 1];
    const info = db.program.preds.items[stored.pred];
    var goal = Goal{
        .name = db.interner.resolve(info.name_atom).str,
        .pattern = @splat(null),
        .arity = info.arity,
    };
    for (stored.pattern, 0..) |maybe_atom, i| {
        if (maybe_atom) |atom| goal.pattern[i] = db.interner.resolve(atom);
    }
    return goal;
}

fn printRows(writer: *std.Io.Writer, it: *zodd.RowIterator) !usize {
    var printed: usize = 0;
    while (it.next()) |row| {
        if (printed == max_rows) {
            try writer.print("... (truncated at {d} rows)\n", .{max_rows});
            break;
        }
        try writer.print("{f}\n", .{row});
        printed += 1;
    }
    if (printed == 0) try writer.writeAll("(no rows)\n");
    return printed;
}

fn execRun(ctx: chilli.CommandContext) !void {
    const allocator = ctx.tmp_allocator;
    const path = try ctx.getArg("file", []const u8);
    const source = try loadSource(allocator, path);

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try loadProgram(&db, source, ctx);
    db.solve() catch |err| fatal(&db, source, err);

    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    defer stdout.flush() catch {};

    if (db.program.queries.items.len > 0) {
        for (db.program.queries.items) |stored| {
            const info = db.program.preds.items[stored.pred];
            const name = db.interner.resolve(info.name_atom).str;
            var pattern: [16]?zodd.Value = @splat(null);
            for (stored.pattern, 0..) |maybe_atom, i| {
                if (maybe_atom) |atom| pattern[i] = db.interner.resolve(atom);
            }
            try writer.print("?- {s}:\n", .{name});
            var it = try db.query(name, pattern[0..info.arity]);
            defer it.deinit();
            _ = try printRows(writer, &it);
        }
    } else {
        for (db.program.preds.items) |info| {
            if (!info.derived) continue;
            const name = db.interner.resolve(info.name_atom).str;
            try writer.print("{s}:\n", .{name});
            var pattern: [16]?zodd.Value = @splat(null);
            var it = try db.query(name, pattern[0..info.arity]);
            defer it.deinit();
            _ = try printRows(writer, &it);
        }
    }
}

fn execQuery(ctx: chilli.CommandContext) !void {
    const allocator = ctx.tmp_allocator;
    const path = try ctx.getArg("file", []const u8);
    const goal_text = try ctx.getArg("goal", []const u8);
    const full = try ctx.getFlag("full", bool);
    const source = try loadSource(allocator, path);

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    try loadProgram(&db, source, ctx);

    const normalized = try normalizeGoal(allocator, goal_text);
    db.run(normalized) catch |err| fatal(&db, normalized, err);
    const goal = lastStoredGoal(&db).?;

    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    defer stdout.flush() catch {};

    var it = (if (full)
        db.query(goal.name, goal.pattern[0..goal.arity])
    else
        db.queryDemand(goal.name, goal.pattern[0..goal.arity])) catch |err| fatal(&db, source, err);
    defer it.deinit();
    _ = try printRows(writer, &it);
}

fn execPlan(ctx: chilli.CommandContext) !void {
    const allocator = ctx.tmp_allocator;
    const path = try ctx.getArg("file", []const u8);
    const source = try loadSource(allocator, path);

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    db.run(source) catch |err| fatal(&db, source, err);

    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    defer stdout.flush() catch {};

    db.explainPlan(writer) catch |err| fatal(&db, source, err);
}

fn execExplain(ctx: chilli.CommandContext) !void {
    const allocator = ctx.tmp_allocator;
    const path = try ctx.getArg("file", []const u8);
    const fact_text = try ctx.getArg("fact", []const u8);
    const depth = try ctx.getFlag("depth", i64);
    const source = try loadSource(allocator, path);

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    db.track_provenance = true;
    try loadProgram(&db, source, ctx);

    const normalized = try normalizeGoal(allocator, fact_text);
    db.run(normalized) catch |err| fatal(&db, normalized, err);
    const goal = lastStoredGoal(&db).?;

    var values: [16]zodd.Value = undefined;
    for (goal.pattern[0..goal.arity], 0..) |slot, i| {
        values[i] = slot orelse {
            std.debug.print("error: explain needs a ground fact; '{s}' has free arguments\n", .{fact_text});
            std.process.exit(1);
        };
    }

    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    defer stdout.flush() catch {};

    db.explain(writer, goal.name, values[0..goal.arity], @intCast(@max(depth, 1))) catch |err| {
        stdout.flush() catch {};
        if (err == error.TupleNotFound) {
            std.debug.print("error: '{s}' is not in the result set\n", .{fact_text});
            std.process.exit(1);
        }
        fatal(&db, source, err);
    };
}

fn execRepl(ctx: chilli.CommandContext) !void {
    const allocator = ctx.tmp_allocator;
    const path = try ctx.getArg("file", []const u8);

    var db = zodd.Database.init(allocator);
    defer db.deinit();
    if (path.len > 0) {
        const source = try loadSource(allocator, path);
        try loadProgram(&db, source, ctx);
    } else {
        const max_iterations = try ctx.getFlag("max-iterations", i64);
        if (max_iterations > 0) db.max_iterations = @intCast(max_iterations);
    }

    var out_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout.interface;

    var in_buffer: [64 * 1024]u8 = undefined;
    var stdin = std.Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin.interface;

    try writer.writeAll("zodd repl; statements end with '.', goals start with '?-', :quit exits\n");
    while (true) {
        try writer.writeAll("zodd> ");
        try stdout.flush();
        const raw = (try reader.takeDelimiter('\n')) orelse break;
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0) continue;
        if (std.mem.eql(u8, line, ":quit") or std.mem.eql(u8, line, ":q")) break;

        if (std.mem.startsWith(u8, line, "?-")) {
            const normalized = try normalizeGoal(allocator, line);
            defer allocator.free(normalized);
            db.run(normalized) catch |err| {
                reportDiagnostic(&db, normalized, err);
                continue;
            };
            const goal = lastStoredGoal(&db).?;
            var it = db.queryDemand(goal.name, goal.pattern[0..goal.arity]) catch |err| {
                reportDiagnostic(&db, null, err);
                continue;
            };
            defer it.deinit();
            _ = try printRows(writer, &it);
        } else {
            const owned = try allocator.dupe(u8, line);
            defer allocator.free(owned);
            db.run(owned) catch |err| {
                reportDiagnostic(&db, owned, err);
                continue;
            };
            db.solve() catch |err| {
                reportDiagnostic(&db, owned, err);
                continue;
            };
        }
        try stdout.flush();
    }
    try writer.writeAll("\n");
    try stdout.flush();
}

test "normalizeGoal wraps bare goals and passes wrapped ones through" {
    const allocator = std.testing.allocator;

    const bare = try normalizeGoal(allocator, "path(1, X)");
    defer allocator.free(bare);
    try std.testing.expectEqualStrings("?- path(1, X).", bare);

    const dotted = try normalizeGoal(allocator, "path(1, X).");
    defer allocator.free(dotted);
    try std.testing.expectEqualStrings("?- path(1, X).", dotted);

    const wrapped = try normalizeGoal(allocator, "?- path(1, X).");
    defer allocator.free(wrapped);
    try std.testing.expectEqualStrings("?- path(1, X).", wrapped);

    const padded = try normalizeGoal(allocator, "  ?-  path(1, X)  ");
    defer allocator.free(padded);
    try std.testing.expectEqualStrings("?- path(1, X).", padded);
}

test "lineColumn locates offsets" {
    const source = "abc\ndef\nghi";
    try std.testing.expectEqual(Location{ .line = 1, .column = 1 }, lineColumn(source, 0));
    try std.testing.expectEqual(Location{ .line = 1, .column = 3 }, lineColumn(source, 2));
    try std.testing.expectEqual(Location{ .line = 2, .column = 1 }, lineColumn(source, 4));
    try std.testing.expectEqual(Location{ .line = 3, .column = 2 }, lineColumn(source, 9));
    try std.testing.expectEqual(Location{ .line = 3, .column = 4 }, lineColumn(source, 99));
}

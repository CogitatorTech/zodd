//! # Web Frontend Wasm Wrapper
//!
//! The module exposes the Datalog frontend to the web frontend over a
//! small C ABI. JavaScript writes UTF-8 source into linear memory with
//! `alloc`, calls `run`, and reads the rendered output back through
//! `outputPtr` and `outputLen`.
//!
//! Built for wasm32-freestanding, where a panic becomes an opaque trap, so
//! `run` catches every error (including out of memory) and renders it as
//! text instead. A fixed iteration limit turns non-terminating programs
//! into a reported error rather than a hung tab.

const std = @import("std");
const zodd = @import("zodd");
const build_options = @import("build_options");
const builtin = @import("builtin");

const allocator = std.heap.wasm_allocator;

/// Maximum predicate arity, mirroring the frontend's `MAX_ARITY`.
const max_arity = 8;

/// Fixed-point rounds allowed per stratum before reporting an error.
const iteration_limit = 100_000;

/// Total result rows printed before truncating the output.
const max_rows = 2000;

/// Proof tree levels expanded by `explain` before truncating.
const explain_depth_limit = 64;

/// The rendered output of the last `run` call, owned by this module.
var output: []u8 = &.{};

/// Allocates a buffer JavaScript can write source text into.
export fn alloc(len: usize) ?[*]u8 {
    const buffer = allocator.alloc(u8, len) catch return null;
    return buffer.ptr;
}

/// Frees a buffer obtained from `alloc`.
export fn dealloc(ptr: [*]u8, len: usize) void {
    allocator.free(ptr[0..len]);
}

/// Pointer to the output of the last `run` call.
export fn outputPtr() [*]const u8 {
    return output.ptr;
}

/// Length of the output of the last `run` call.
export fn outputLen() usize {
    return output.len;
}

/// Stores the result of an evaluation as the module's output text: 0 on
/// success, 1 on error with the error name appended.
fn capture(result: anyerror!void, buffer: *std.Io.Writer.Allocating) u32 {
    const status: u32 = if (result) |_| 0 else |err| blk: {
        buffer.writer.print("error: {s}\n", .{@errorName(err)}) catch {};
        break :blk 1;
    };
    output = buffer.toOwnedSlice() catch &.{};
    return status;
}

fn freeOutput() void {
    if (output.len != 0) {
        allocator.free(output);
        output = &.{};
    }
}

/// Parses, solves, and queries the given Datalog source. Returns 0 on
/// success and nonzero on error; either way the output text is available
/// through `outputPtr` and `outputLen`.
export fn run(source_ptr: [*]const u8, source_len: usize) u32 {
    freeOutput();
    var buffer = std.Io.Writer.Allocating.init(allocator);
    defer buffer.deinit();
    return capture(evaluate(source_ptr[0..source_len], &buffer.writer), &buffer);
}

/// Renders the compiled join plan of every rule in the given source.
export fn explainPlan(source_ptr: [*]const u8, source_len: usize) u32 {
    freeOutput();
    var buffer = std.Io.Writer.Allocating.init(allocator);
    defer buffer.deinit();
    return capture(evaluatePlan(source_ptr[0..source_len], &buffer.writer), &buffer);
}

/// Renders the proof tree of one derived tuple. The atom text is a ground
/// atom like `path(1, 3)`, parsed with the same parser as the program.
export fn explain(
    source_ptr: [*]const u8,
    source_len: usize,
    atom_ptr: [*]const u8,
    atom_len: usize,
) u32 {
    freeOutput();
    var buffer = std.Io.Writer.Allocating.init(allocator);
    defer buffer.deinit();
    return capture(
        evaluateExplain(source_ptr[0..source_len], atom_ptr[0..atom_len], &buffer.writer),
        &buffer,
    );
}

fn evaluate(source: []const u8, writer: *std.Io.Writer) !void {
    var db = zodd.Database.init(allocator);
    defer db.deinit();
    db.max_iterations = iteration_limit;

    db.run(source) catch |err| {
        try renderDiagnostic(writer, &db, source);
        return err;
    };
    db.solve() catch |err| {
        try renderDiagnostic(writer, &db, source);
        return err;
    };

    try renderResults(writer, &db);
}

fn evaluatePlan(source: []const u8, writer: *std.Io.Writer) !void {
    var db = zodd.Database.init(allocator);
    defer db.deinit();
    db.max_iterations = iteration_limit;

    db.run(source) catch |err| {
        try renderDiagnostic(writer, &db, source);
        return err;
    };
    if (db.program.rules.items.len == 0) {
        try writer.writeAll("(no rules)\n");
        return;
    }
    db.explainPlan(writer) catch |err| {
        try renderDiagnostic(writer, &db, source);
        return err;
    };
}

fn evaluateExplain(source: []const u8, atom: []const u8, writer: *std.Io.Writer) !void {
    var db = zodd.Database.init(allocator);
    defer db.deinit();
    db.max_iterations = iteration_limit;
    db.track_provenance = true;

    db.run(source) catch |err| {
        try renderDiagnostic(writer, &db, source);
        return err;
    };
    db.solve() catch |err| {
        try renderDiagnostic(writer, &db, source);
        return err;
    };

    // Parse the atom as a stored query, reusing the parser and interner.
    const query_text = try std.fmt.allocPrint(allocator, "?- {s}.", .{atom});
    defer allocator.free(query_text);
    db.run(query_text) catch |err| {
        try writer.print("cannot parse atom: {s}\n", .{atom});
        return err;
    };

    const stored = db.program.queries.items[db.program.queries.items.len - 1];
    const info = db.program.preds.items[stored.pred];
    const name = db.interner.resolve(info.name_atom).str;

    var values: [max_arity]zodd.Value = undefined;
    for (stored.pattern, 0..) |maybe_atom, i| {
        const ground = maybe_atom orelse return error.NonGroundAtom;
        values[i] = db.interner.resolve(ground);
    }

    db.explain(writer, name, values[0..info.arity], explain_depth_limit) catch |err| {
        if (err == error.TupleNotFound) {
            try writer.print("{s} is not in the result set\n", .{atom});
        }
        return err;
    };
}

fn renderDiagnostic(writer: *std.Io.Writer, db: *const zodd.Database, source: []const u8) !void {
    const diag = db.lastDiagnostic() orelse return;
    if (diag.span) |span| {
        const location = lineColumn(source, span.start);
        try writer.print("{d}:{d}: {s}\n", .{ location.line, location.column, diag.message });
    } else {
        try writer.print("{s}\n", .{diag.message});
    }
}

fn lineColumn(source: []const u8, offset: u32) struct { line: usize, column: usize } {
    const end = @min(@as(usize, offset), source.len);
    var line: usize = 1;
    var column: usize = 1;
    for (source[0..end]) |c| {
        if (c == '\n') {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    return .{ .line = line, .column = column };
}

/// Answers the program's stored queries, or prints every derived predicate
/// when the program has none.
fn renderResults(writer: *std.Io.Writer, db: *zodd.Database) !void {
    var printed: usize = 0;

    if (db.program.queries.items.len > 0) {
        for (db.program.queries.items) |stored| {
            const info = db.program.preds.items[stored.pred];
            const name = db.interner.resolve(info.name_atom).str;

            var pattern: [max_arity]?zodd.Value = @splat(null);
            for (stored.pattern, 0..) |maybe_atom, i| {
                if (maybe_atom) |atom| pattern[i] = db.interner.resolve(atom);
            }

            try writer.print("?- {s}:\n", .{name});
            var it = try db.query(name, pattern[0..info.arity]);
            defer it.deinit();
            if (!try printRows(writer, &it, &printed)) return;
        }
    } else {
        for (db.program.preds.items) |info| {
            if (!info.derived) continue;
            const name = db.interner.resolve(info.name_atom).str;
            const pattern: [max_arity]?zodd.Value = @splat(null);

            try writer.print("{s}:\n", .{name});
            var it = try db.query(name, pattern[0..info.arity]);
            defer it.deinit();
            if (!try printRows(writer, &it, &printed)) return;
        }
    }

    if (printed == 0) {
        try writer.writeAll("(no results)\n");
    }
}

fn printRows(writer: *std.Io.Writer, it: *zodd.RowIterator, printed: *usize) !bool {
    while (it.next()) |row| {
        if (printed.* >= max_rows) {
            try writer.print("... (output truncated at {d} rows)\n", .{max_rows});
            return false;
        }
        try writer.print("  {f}\n", .{row});
        printed.* += 1;
    }
    return true;
}

export fn versionPtr() [*]const u8 {
    return build_options.version.ptr;
}

export fn versionLen() usize {
    return build_options.version.len;
}

export fn commitPtr() [*]const u8 {
    return build_options.commit.ptr;
}

export fn commitLen() usize {
    return build_options.commit.len;
}

const target_name = @tagName(builtin.cpu.arch) ++ " " ++ @tagName(builtin.os.tag);

export fn zigVersionPtr() [*]const u8 {
    return builtin.zig_version_string.ptr;
}

export fn zigVersionLen() usize {
    return builtin.zig_version_string.len;
}

export fn targetPtr() [*]const u8 {
    return target_name.ptr;
}

export fn targetLen() usize {
    return target_name.len;
}

export fn licensePtr() [*]const u8 {
    return "MIT".ptr;
}

export fn licenseLen() usize {
    return "MIT".len;
}

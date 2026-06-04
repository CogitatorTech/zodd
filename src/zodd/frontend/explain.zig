//! # Explain
//!
//! The module renders compiled rule plans and tuple derivations as text.
//! `Database.explainPlan` writes the join plan of every rule, and
//! `Database.explain` writes the proof tree of a derived tuple from the
//! provenance recorded by the evaluator.

const std = @import("std");
const ast = @import("ast.zig");
const dyntuple = @import("dyntuple.zig");
const evaluator_mod = @import("evaluator.zig");
const interner_mod = @import("interner.zig");
const plan_mod = @import("plan.zig");
const DynTuple = dyntuple.DynTuple;
const Interner = interner_mod.Interner;
const WriteError = std.Io.Writer.Error;

fn predName(program: *const ast.Program, interner: *const Interner, pred: ast.PredId) []const u8 {
    return interner.resolve(program.preds.items[pred].name_atom).str;
}

fn writeValue(writer: *std.Io.Writer, interner: *const Interner, atom: dyntuple.Atom) WriteError!void {
    switch (interner.resolve(atom)) {
        .int => |v| try writer.print("{d}", .{v}),
        .str => |s| try writer.print("\"{s}\"", .{s}),
    }
}

fn writeVar(writer: *std.Io.Writer, rule: *const ast.Rule, var_id: ast.VarId) WriteError!void {
    if (var_id < rule.var_names.len) {
        try writer.writeAll(rule.var_names[var_id]);
    } else {
        try writer.writeAll("_");
    }
}

fn writeTerm(
    writer: *std.Io.Writer,
    interner: *const Interner,
    rule: *const ast.Rule,
    term: ast.Term,
) WriteError!void {
    switch (term) {
        .variable => |var_id| try writeVar(writer, rule, var_id),
        .constant => |value| try writeValue(writer, interner, value),
        .wildcard => try writer.writeAll("_"),
    }
}

fn writeVarList(writer: *std.Io.Writer, rule: *const ast.Rule, vars: []const ast.VarId) WriteError!void {
    try writer.writeAll("(");
    for (vars, 0..) |var_id, i| {
        if (i > 0) try writer.writeAll(", ");
        try writeVar(writer, rule, var_id);
    }
    try writer.writeAll(")");
}

fn writeAtomTemplate(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    rule: *const ast.Rule,
    atom: ast.Atom,
) WriteError!void {
    try writer.print("{s}(", .{predName(program, interner, atom.pred)});
    for (atom.terms, 0..) |term, i| {
        if (i > 0) try writer.writeAll(", ");
        try writeTerm(writer, interner, rule, term);
    }
    try writer.writeAll(")");
}

fn writeHeadTemplate(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    rule: *const ast.Rule,
) WriteError!void {
    switch (rule.head) {
        .plain => |atom| try writeAtomTemplate(writer, program, interner, rule, atom),
        .aggregate => |agg| {
            try writer.print("{s}(", .{predName(program, interner, agg.pred)});
            const arity = agg.group_terms.len + 1;
            var group_index: usize = 0;
            for (0..arity) |slot| {
                if (slot > 0) try writer.writeAll(", ");
                if (slot == agg.agg_slot) {
                    try writer.print("{s}(", .{@tagName(agg.func)});
                    try writeTerm(writer, interner, rule, agg.arg);
                    try writer.writeAll(")");
                } else {
                    try writeTerm(writer, interner, rule, agg.group_terms[group_index]);
                    group_index += 1;
                }
            }
            try writer.writeAll(")");
        },
    }
}

/// Writes a rule in source form: `head :- body.`.
pub fn writeRule(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    rule: *const ast.Rule,
) WriteError!void {
    try writeHeadTemplate(writer, program, interner, rule);
    try writer.writeAll(" :- ");
    for (rule.body, 0..) |literal, i| {
        if (i > 0) try writer.writeAll(", ");
        if (literal.negated) try writer.writeAll("not ");
        try writeAtomTemplate(writer, program, interner, rule, literal.atom);
    }
    try writer.writeAll(".");
}

fn writeChecks(
    writer: *std.Io.Writer,
    interner: *const Interner,
    load: *const plan_mod.AtomLoad,
) WriteError!void {
    if (load.const_checks.len == 0 and load.eq_checks.len == 0) return;
    try writer.writeAll(" where ");
    var first = true;
    for (load.const_checks) |check| {
        if (!first) try writer.writeAll(", ");
        first = false;
        try writer.print("col{d} = ", .{check.col});
        try writeValue(writer, interner, check.value);
    }
    for (load.eq_checks) |check| {
        if (!first) try writer.writeAll(", ");
        first = false;
        try writer.print("col{d} = col{d}", .{ check.col_a, check.col_b });
    }
}

/// Writes the compiled plan of one rule: the rule itself, its stratum, the
/// scan, join, and anti steps, and the head projection.
pub fn writePlan(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    rule: *const ast.Rule,
    plan: *const plan_mod.Plan,
) WriteError!void {
    const stratum = program.preds.items[rule.head.pred()].stratum;
    try writer.print("rule {d} (stratum {d}): ", .{ rule.index, stratum });
    try writeRule(writer, program, interner, rule);
    try writer.writeAll("\n");

    for (plan.steps) |step| {
        switch (step) {
            .scan => |load| {
                const pred = rule.body[load.lit].atom.pred;
                try writer.print("  scan {s}", .{predName(program, interner, pred)});
                try writeChecks(writer, interner, &load);
                try writer.writeAll(" -> ");
                try writeVarList(writer, rule, load.out_vars);
                try writer.writeAll("\n");
            },
            .join => |join| {
                const pred = rule.body[join.load.lit].atom.pred;
                try writer.print("  join {s} on ", .{predName(program, interner, pred)});
                try writeVarList(writer, rule, join.load.out_vars[0..join.key_len]);
                try writeChecks(writer, interner, &join.load);
                try writer.writeAll(" -> ");
                try writeVarList(writer, rule, join.out_vars);
                try writer.writeAll("\n");
            },
            .anti => |anti| {
                const pred = rule.body[anti.load.lit].atom.pred;
                try writer.print("  anti {s} on ", .{predName(program, interner, pred)});
                try writeVarList(writer, rule, anti.load.out_vars[0..anti.i_key_cols.len]);
                try writeChecks(writer, interner, &anti.load);
                try writer.writeAll("\n");
            },
        }
    }

    try writer.writeAll("  head ");
    try writeHeadTemplate(writer, program, interner, rule);
    try writer.writeAll("\n");
}

/// Writes a ground atom: `pred(value, ...)`.
fn writeTupleAtom(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    pred: ast.PredId,
    tuple: *const DynTuple,
) WriteError!void {
    try writer.print("{s}(", .{predName(program, interner, pred)});
    const arity = program.preds.items[pred].arity;
    var col: usize = 0;
    while (col < arity) : (col += 1) {
        if (col > 0) try writer.writeAll(", ");
        try writeValue(writer, interner, dyntuple.get(tuple, col));
    }
    try writer.writeAll(")");
}

/// Grounds a body atom by substituting the recorded variable binding.
fn groundAtom(atom: ast.Atom, binding: *const DynTuple) DynTuple {
    var tuple = dyntuple.zero_tuple;
    for (atom.terms, 0..) |term, col| {
        const value = switch (term) {
            .variable => |var_id| dyntuple.get(binding, var_id),
            .constant => |constant| constant,
            .wildcard => unreachable, // Lowered by analysis.
        };
        dyntuple.set(&tuple, col, value);
    }
    return tuple;
}

fn writeIndent(writer: *std.Io.Writer, depth: usize) WriteError!void {
    try writer.splatByteAll(' ', depth * 2);
}

/// Writes the proof tree of a derived tuple from recorded provenance.
/// `max_depth` bounds the expanded rule levels; null means no bound.
pub fn writeProof(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    evaluator: *const evaluator_mod.Evaluator,
    pred: ast.PredId,
    tuple: DynTuple,
    max_depth: ?usize,
) WriteError!void {
    try writeProofNode(writer, program, interner, evaluator, pred, tuple, 0, max_depth);
}

fn writeProofNode(
    writer: *std.Io.Writer,
    program: *const ast.Program,
    interner: *const Interner,
    evaluator: *const evaluator_mod.Evaluator,
    pred: ast.PredId,
    tuple: DynTuple,
    depth: usize,
    max_depth: ?usize,
) WriteError!void {
    try writeIndent(writer, depth);
    try writeTupleAtom(writer, program, interner, pred, &tuple);

    if (!program.preds.items[pred].derived) {
        try writer.writeAll(" (fact)\n");
        return;
    }
    const derivation = evaluator.derivationOf(pred, tuple) orelse {
        try writer.writeAll("\n");
        return;
    };

    switch (derivation) {
        .fact => try writer.writeAll(" (fact)\n"),
        .rule => |d| {
            if (max_depth != null and depth >= max_depth.?) {
                try writer.writeAll(" (depth limit)\n");
                return;
            }
            const rule = &program.rules.items[d.rule];
            try writer.writeAll("\n");
            try writeIndent(writer, depth + 1);
            try writer.print("via rule {d}: ", .{d.rule});
            try writeRule(writer, program, interner, rule);
            try writer.writeAll("\n");
            for (rule.body) |literal| {
                const premise = groundAtom(literal.atom, &d.binding);
                if (literal.negated) {
                    try writeIndent(writer, depth + 1);
                    try writer.writeAll("not ");
                    try writeTupleAtom(writer, program, interner, literal.atom.pred, &premise);
                    try writer.writeAll(" (absent)\n");
                } else {
                    try writeProofNode(writer, program, interner, evaluator, literal.atom.pred, premise, depth + 1, max_depth);
                }
            }
        },
        .aggregate => |rule_index| {
            const rule = &program.rules.items[rule_index];
            try writer.writeAll("\n");
            try writeIndent(writer, depth + 1);
            try writer.print("via rule {d} (aggregate): ", .{rule_index});
            try writeRule(writer, program, interner, rule);
            try writer.writeAll("\n");
        },
    }
}

// --- Tests -----------------------------------------------------------------

const Builder = @import("builder.zig").Builder;
const analyze_mod = @import("analyze.zig");

test "writeRule: plain, negated, and aggregate heads" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const node = try builder.predicate("node", 1);
    const blocked = try builder.predicate("blocked", 1);
    const safe = try builder.predicate("safe", 1);
    const edge = try builder.predicate("edge", 2);
    const deg = try builder.predicate("deg", 2);

    {
        // safe(X) :- node(X), not blocked(X).
        var r = builder.rule(safe);
        const x = try r.v("X");
        try r.head(&.{x});
        try r.pos(node, &.{x});
        try r.neg(blocked, &.{x});
        try r.finish();
    }
    {
        // deg(N, count(M)) :- edge(N, M).
        var r = builder.rule(deg);
        const n = try r.v("N");
        const m = try r.v("M");
        try r.aggHead(&.{n}, 1, .count, m);
        try r.pos(edge, &.{ n, m });
        try r.finish();
    }

    var buffer: [128]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try writeRule(&writer, &program, &interner, &program.rules.items[0]);
    try std.testing.expectEqualStrings("safe(X) :- node(X), not blocked(X).", writer.buffered());

    writer = std.Io.Writer.fixed(&buffer);
    try writeRule(&writer, &program, &interner, &program.rules.items[1]);
    try std.testing.expectEqualStrings("deg(N, count(M)) :- edge(N, M).", writer.buffered());
}

test "writePlan: scan, join, anti, and checks" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const edge = try builder.predicate("edge", 2);
    const blocked = try builder.predicate("blocked", 1);
    const path = try builder.predicate("path", 2);

    // path(X, Z) :- path(X, Y), edge(Y, Z), not blocked(Z).
    var r = builder.rule(path);
    const x = try r.v("X");
    const y = try r.v("Y");
    const z = try r.v("Z");
    try r.head(&.{ x, z });
    try r.pos(path, &.{ x, y });
    try r.pos(edge, &.{ y, z });
    try r.neg(blocked, &.{z});
    try r.finish();

    _ = try analyze_mod.analyze(&program, null);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const plan = try plan_mod.compile(arena.allocator(), &program.rules.items[0]);

    var buffer: [512]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try writePlan(&writer, &program, &interner, &program.rules.items[0], &plan);

    try std.testing.expectEqualStrings(
        \\rule 0 (stratum 1): path(X, Z) :- path(X, Y), edge(Y, Z), not blocked(Z).
        \\  scan path -> (X, Y)
        \\  join edge on (Y) -> (Y, X, Z)
        \\  anti blocked on (Z)
        \\  head path(X, Z)
        \\
    , writer.buffered());
}

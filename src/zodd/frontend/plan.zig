//! # Rule Planner
//!
//! The module compiles an analyzed rule into a join plan the evaluator
//! executes: a left-deep chain of scans and prefix merge-joins, followed by
//! comparison filters, anti-join filters for negated literals, and a head
//! projection (or aggregation).
//!
//! The intermediate result of a chain is a `DynTuple` relation whose columns
//! hold the rule variables bound so far (the layout). All column
//! permutations are precomputed here, once per rule; the evaluator only
//! shuffles tuples.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const dyntuple = @import("dyntuple.zig");
const join_runtime = @import("join_runtime.zig");

/// Errors produced while planning a rule.
pub const PlanError = error{
    /// A rule binds more distinct variables than `MAX_ARITY` columns.
    TooManyVariables,
} || Allocator.Error;

/// A constant constraint on a loaded atom: relation column must equal value.
pub const ConstCheck = struct {
    col: u8,
    value: dyntuple.Atom,
};

/// A repeated-variable constraint: two relation columns must be equal.
pub const EqCheck = struct {
    col_a: u8,
    col_b: u8,
};

/// Loads one body atom's relation into join shape: tuples passing the
/// constant and equality checks are projected so `out_vars[i]` sits in
/// column `i` (reading relation column `src_cols[i]`).
pub const AtomLoad = struct {
    /// Index of the literal in the rule body.
    lit: u16,
    src_cols: []u8,
    const_checks: []ConstCheck,
    eq_checks: []EqCheck,
    /// Variables of the output columns, key variables first.
    out_vars: []ast.VarId,
};

/// Joins the intermediate with a loaded atom on `key_len` shared variables.
pub const JoinStep = struct {
    load: AtomLoad,
    /// Relayout of the intermediate: key columns first, then the rest.
    i_proj: []u8,
    key_len: u8,
    /// Output assembly; defines the next layout `out_vars`.
    out_spec: []join_runtime.OutCol,
    out_vars: []ast.VarId,
};

/// Filters the intermediate against a negated literal's relation.
pub const AntiStep = struct {
    load: AtomLoad,
    /// Columns of the current layout forming the probe key, in the order of
    /// `load.out_vars`.
    i_key_cols: []u8,
};

/// One side of a comparison filter: a column of the current layout or a
/// constant.
pub const CmpArg = union(enum) {
    col: u8,
    constant: dyntuple.Atom,
};

/// Filters the intermediate with a comparison. Equality and inequality
/// compare raw atoms; ordered operators compare integers, and a string
/// operand fails the comparison.
pub const CmpStep = struct {
    op: ast.CmpOp,
    lhs: CmpArg,
    rhs: CmpArg,
};

pub const Step = union(enum) {
    scan: AtomLoad,
    join: JoinStep,
    anti: AntiStep,
    cmp: CmpStep,
};

/// One head column: a column of the final intermediate (or of the aggregate
/// result tuple) or a constant.
pub const HeadCol = union(enum) {
    col: u8,
    constant: dyntuple.Atom,
};

/// Aggregation over the final intermediate: project `proj` (group variables,
/// then the aggregated column, then all remaining columns for set-semantics
/// folding), group by the first `group_len` columns, then assemble the head
/// from the `[group..., result]` tuples.
pub const AggPlan = struct {
    proj: []u8,
    group_len: u8,
    func: ast.AggFunc,
    head_cols: []HeadCol,
};

pub const HeadKind = union(enum) {
    plain: []HeadCol,
    aggregate: AggPlan,
};

/// A compiled rule.
pub const Plan = struct {
    /// Scan and join steps in body order, then cmp steps, then anti steps.
    steps: []Step,
    head: HeadKind,
    /// Final intermediate width (number of layout columns).
    width: u8,
    /// Variables of the final intermediate's columns; every rule variable
    /// is bound here once the step chain completes.
    layout: []ast.VarId,
};

/// Compiles `rule` into a plan. Must run after `analyze` (wildcards lowered,
/// safety guaranteed). All plan storage is allocated from `arena`.
pub fn compile(arena: Allocator, rule: *const ast.Rule) PlanError!Plan {
    if (rule.var_count > dyntuple.MAX_ARITY) return error.TooManyVariables;

    var layout: std.ArrayListUnmanaged(ast.VarId) = .empty;
    var steps: std.ArrayListUnmanaged(Step) = .empty;

    // Positive literals first, in body order.
    var first = true;
    for (rule.body, 0..) |literal, lit_index| {
        if (literal.negated) continue;
        if (first) {
            const load = try buildLoad(arena, literal.atom, @intCast(lit_index), &.{});
            try steps.append(arena, .{ .scan = load });
            try layout.appendSlice(arena, load.out_vars);
            first = false;
        } else {
            const step = try buildJoin(arena, literal.atom, @intCast(lit_index), layout.items);
            try steps.append(arena, .{ .join = step });
            layout.clearRetainingCapacity();
            try layout.appendSlice(arena, step.out_vars);
        }
    }

    // Comparison filters after all positives; safety guarantees their
    // variables are bound by then. None of the remaining steps change the
    // layout, so the columns stay valid.
    for (rule.compares) |compare| {
        try steps.append(arena, .{ .cmp = .{
            .op = compare.op,
            .lhs = cmpArg(compare.lhs, layout.items),
            .rhs = cmpArg(compare.rhs, layout.items),
        } });
    }

    // Negated literals after all positives; safety guarantees their
    // variables are bound by then.
    for (rule.body, 0..) |literal, lit_index| {
        if (!literal.negated) continue;
        const shared = try sharedVars(arena, layout.items, literal.atom);
        const load = try buildLoad(arena, literal.atom, @intCast(lit_index), shared);
        const i_key_cols = try arena.alloc(u8, shared.len);
        for (shared, 0..) |var_id, i| {
            i_key_cols[i] = colOf(layout.items, var_id);
        }
        try steps.append(arena, .{ .anti = .{ .load = load, .i_key_cols = i_key_cols } });
    }

    const head = try buildHead(arena, rule, layout.items);

    return Plan{
        .steps = steps.items,
        .head = head,
        .width = @intCast(layout.items.len),
        .layout = layout.items,
    };
}

/// Builds the load spec for an atom: key variables first, then the
/// remaining distinct variables in first-occurrence order, with constant
/// and repeated-variable checks.
fn buildLoad(
    arena: Allocator,
    atom: ast.Atom,
    lit_index: u16,
    key_vars: []const ast.VarId,
) PlanError!AtomLoad {
    var const_checks: std.ArrayListUnmanaged(ConstCheck) = .empty;
    var eq_checks: std.ArrayListUnmanaged(EqCheck) = .empty;
    var distinct: std.ArrayListUnmanaged(ast.VarId) = .empty;
    var first_col: std.ArrayListUnmanaged(u8) = .empty;

    for (atom.terms, 0..) |term, col| {
        switch (term) {
            .constant => |value| try const_checks.append(arena, .{
                .col = @intCast(col),
                .value = value,
            }),
            .variable => |var_id| {
                if (indexOfVar(distinct.items, var_id)) |i| {
                    try eq_checks.append(arena, .{
                        .col_a = first_col.items[i],
                        .col_b = @intCast(col),
                    });
                } else {
                    try distinct.append(arena, var_id);
                    try first_col.append(arena, @intCast(col));
                }
            },
            .wildcard => unreachable, // Lowered by analysis.
        }
    }

    var out_vars: std.ArrayListUnmanaged(ast.VarId) = .empty;
    try out_vars.appendSlice(arena, key_vars);
    for (distinct.items) |var_id| {
        if (indexOfVar(key_vars, var_id) == null) {
            try out_vars.append(arena, var_id);
        }
    }

    const src_cols = try arena.alloc(u8, out_vars.items.len);
    for (out_vars.items, 0..) |var_id, i| {
        const distinct_index = indexOfVar(distinct.items, var_id).?;
        src_cols[i] = first_col.items[distinct_index];
    }

    return AtomLoad{
        .lit = lit_index,
        .src_cols = src_cols,
        .const_checks = const_checks.items,
        .eq_checks = eq_checks.items,
        .out_vars = out_vars.items,
    };
}

fn cmpArg(term: ast.Term, layout: []const ast.VarId) CmpArg {
    return switch (term) {
        .variable => |var_id| CmpArg{ .col = colOf(layout, var_id) },
        .constant => |value| CmpArg{ .constant = value },
        .wildcard => unreachable, // Rejected by the builder.
    };
}

fn buildJoin(
    arena: Allocator,
    atom: ast.Atom,
    lit_index: u16,
    layout: []const ast.VarId,
) PlanError!JoinStep {
    const shared = try sharedVars(arena, layout, atom);
    const key_len: u8 = @intCast(shared.len);
    const load = try buildLoad(arena, atom, lit_index, shared);

    // Relayout the intermediate: shared variables first, then the rest.
    const i_proj = try arena.alloc(u8, layout.len);
    var next: usize = 0;
    for (shared) |var_id| {
        i_proj[next] = colOf(layout, var_id);
        next += 1;
    }
    var rest_vars: std.ArrayListUnmanaged(ast.VarId) = .empty;
    for (layout, 0..) |var_id, col| {
        if (indexOfVar(shared, var_id) == null) {
            i_proj[next] = @intCast(col);
            next += 1;
            try rest_vars.append(arena, var_id);
        }
    }

    // Output: keys, then the intermediate's other columns, then the atom's
    // new variables.
    const new_count = load.out_vars.len - shared.len;
    var out_spec: std.ArrayListUnmanaged(join_runtime.OutCol) = .empty;
    var out_vars: std.ArrayListUnmanaged(ast.VarId) = .empty;
    for (shared, 0..) |var_id, i| {
        try out_spec.append(arena, .{ .right = false, .col = @intCast(i) });
        try out_vars.append(arena, var_id);
    }
    for (rest_vars.items, 0..) |var_id, i| {
        try out_spec.append(arena, .{ .right = false, .col = @intCast(shared.len + i) });
        try out_vars.append(arena, var_id);
    }
    for (0..new_count) |i| {
        try out_spec.append(arena, .{ .right = true, .col = @intCast(shared.len + i) });
        try out_vars.append(arena, load.out_vars[shared.len + i]);
    }

    return JoinStep{
        .load = load,
        .i_proj = i_proj,
        .key_len = key_len,
        .out_spec = out_spec.items,
        .out_vars = out_vars.items,
    };
}

fn buildHead(
    arena: Allocator,
    rule: *const ast.Rule,
    layout: []const ast.VarId,
) PlanError!HeadKind {
    switch (rule.head) {
        .plain => |atom| {
            const cols = try arena.alloc(HeadCol, atom.terms.len);
            for (atom.terms, 0..) |term, i| {
                cols[i] = switch (term) {
                    .variable => |var_id| HeadCol{ .col = colOf(layout, var_id) },
                    .constant => |value| HeadCol{ .constant = value },
                    .wildcard => unreachable,
                };
            }
            return HeadKind{ .plain = cols };
        },
        .aggregate => |agg| {
            // Distinct group variables, in first-occurrence order.
            var group_vars: std.ArrayListUnmanaged(ast.VarId) = .empty;
            for (agg.group_terms) |term| {
                if (term == .variable and indexOfVar(group_vars.items, term.variable) == null) {
                    try group_vars.append(arena, term.variable);
                }
            }
            const group_len: u8 = @intCast(group_vars.items.len);
            const arg_var = agg.arg.variable;

            // Projection: group variables, the aggregated column, then every
            // remaining layout column so folding sees distinct full bindings.
            var proj: std.ArrayListUnmanaged(u8) = .empty;
            for (group_vars.items) |var_id| {
                try proj.append(arena, colOf(layout, var_id));
            }
            try proj.append(arena, colOf(layout, arg_var));
            for (layout, 0..) |var_id, col| {
                if (var_id != arg_var and indexOfVar(group_vars.items, var_id) == null) {
                    try proj.append(arena, @intCast(col));
                }
            }

            // Head assembly over the [group..., result] aggregate tuples.
            const arity = agg.group_terms.len + 1;
            const head_cols = try arena.alloc(HeadCol, arity);
            var group_index: usize = 0;
            for (head_cols, 0..) |*head_col, slot| {
                if (slot == agg.agg_slot) {
                    head_col.* = HeadCol{ .col = group_len };
                    continue;
                }
                const term = agg.group_terms[group_index];
                group_index += 1;
                head_col.* = switch (term) {
                    .variable => |var_id| HeadCol{
                        .col = @intCast(indexOfVar(group_vars.items, var_id).?),
                    },
                    .constant => |value| HeadCol{ .constant = value },
                    .wildcard => unreachable,
                };
            }

            return HeadKind{ .aggregate = .{
                .proj = proj.items,
                .group_len = group_len,
                .func = agg.func,
                .head_cols = head_cols,
            } };
        },
    }
}

/// Variables present in both the layout and the atom, in layout order.
fn sharedVars(
    arena: Allocator,
    layout: []const ast.VarId,
    atom: ast.Atom,
) Allocator.Error![]ast.VarId {
    var shared: std.ArrayListUnmanaged(ast.VarId) = .empty;
    for (layout) |var_id| {
        for (atom.terms) |term| {
            if (term == .variable and term.variable == var_id) {
                try shared.append(arena, var_id);
                break;
            }
        }
    }
    return shared.items;
}

fn indexOfVar(vars: []const ast.VarId, var_id: ast.VarId) ?usize {
    for (vars, 0..) |existing, i| {
        if (existing == var_id) return i;
    }
    return null;
}

fn colOf(layout: []const ast.VarId, var_id: ast.VarId) u8 {
    return @intCast(indexOfVar(layout, var_id).?);
}

test "compile: transitive closure step rule" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const edge = try builder.predicate("edge", 2);
    const path = try builder.predicate("path", 2);
    _ = edge;

    // path(X, Z) :- path(X, Y), edge(Y, Z).
    var r = builder.rule(path);
    const x = try r.v("X");
    const y = try r.v("Y");
    const z = try r.v("Z");
    try r.head(&.{ x, z });
    try r.pos(path, &.{ x, y });
    try r.pos(1, &.{ y, z }); // edge
    try r.finish();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const plan = try compile(arena.allocator(), &program.rules.items[0]);

    try std.testing.expectEqual(@as(usize, 2), plan.steps.len);
    try std.testing.expect(plan.steps[0] == .scan);
    try std.testing.expect(plan.steps[1] == .join);

    const join = plan.steps[1].join;
    // Join key is Y, the only shared variable.
    try std.testing.expectEqual(@as(u8, 1), join.key_len);
    // Final layout binds all three variables.
    try std.testing.expectEqual(@as(usize, 3), join.out_vars.len);
    try std.testing.expectEqual(@as(u8, 3), plan.width);

    // Head projects X and Z from the final layout.
    const head = plan.head.plain;
    try std.testing.expectEqual(@as(usize, 2), head.len);
    try std.testing.expectEqual(x.variable, join.out_vars[head[0].col]);
    try std.testing.expectEqual(z.variable, join.out_vars[head[1].col]);
}

test "compile: constants and repeated variables become checks" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const t = try builder.predicate("t", 3);
    const out = try builder.predicate("out", 1);

    // out(X) :- t(X, X, 5).
    var r = builder.rule(out);
    const x = try r.v("X");
    try r.head(&.{x});
    try r.pos(t, &.{ x, x, try builder.int(5) });
    try r.finish();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const plan = try compile(arena.allocator(), &program.rules.items[0]);

    const load = plan.steps[0].scan;
    try std.testing.expectEqual(@as(usize, 1), load.const_checks.len);
    try std.testing.expectEqual(@as(u8, 2), load.const_checks[0].col);
    try std.testing.expectEqual(@as(u64, 5), load.const_checks[0].value);
    try std.testing.expectEqual(@as(usize, 1), load.eq_checks.len);
    try std.testing.expectEqual(@as(usize, 1), load.out_vars.len);
}

test "compile: comparisons become cmp steps before anti steps" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const person = try builder.predicate("person", 2);
    const blocked = try builder.predicate("blocked", 1);
    const adult = try builder.predicate("adult", 1);

    // adult(X) :- person(X, Age), Age >= 18, not blocked(X).
    var r = builder.rule(adult);
    const x = try r.v("X");
    const age = try r.v("Age");
    try r.head(&.{x});
    try r.pos(person, &.{ x, age });
    try r.cmp(age, .ge, try builder.int(18));
    try r.neg(blocked, &.{x});
    try r.finish();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const plan = try compile(arena.allocator(), &program.rules.items[0]);

    try std.testing.expectEqual(@as(usize, 3), plan.steps.len);
    try std.testing.expect(plan.steps[0] == .scan);
    try std.testing.expect(plan.steps[1] == .cmp);
    try std.testing.expect(plan.steps[2] == .anti);

    const cmp = plan.steps[1].cmp;
    try std.testing.expectEqual(ast.CmpOp.ge, cmp.op);
    // Age sits in layout column 1; the constant carries through.
    try std.testing.expectEqual(age.variable, plan.layout[cmp.lhs.col]);
    try std.testing.expectEqual(@as(u64, 18), cmp.rhs.constant);
}

test "compile: negation becomes a trailing anti step" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const node = try builder.predicate("node", 1);
    const blocked = try builder.predicate("blocked", 1);
    const safe = try builder.predicate("safe", 1);

    // safe(X) :- not blocked(X), node(X).  (anti runs after the scan)
    var r = builder.rule(safe);
    const x = try r.v("X");
    try r.head(&.{x});
    try r.neg(blocked, &.{x});
    try r.pos(node, &.{x});
    try r.finish();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const plan = try compile(arena.allocator(), &program.rules.items[0]);

    try std.testing.expectEqual(@as(usize, 2), plan.steps.len);
    try std.testing.expect(plan.steps[0] == .scan);
    try std.testing.expect(plan.steps[1] == .anti);
    try std.testing.expectEqual(@as(usize, 1), plan.steps[1].anti.i_key_cols.len);
}

test "compile: rejects too many variables" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const wide = try builder.predicate("wide", dyntuple.MAX_ARITY);
    const out = try builder.predicate("out", 1);

    // Bind MAX_ARITY + 1 distinct variables across two literals.
    var r = builder.rule(out);
    var terms_a: [dyntuple.MAX_ARITY]ast.Term = undefined;
    for (&terms_a, 0..) |*term, i| {
        var name_buf: [8]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "A{d}", .{i}) catch unreachable;
        term.* = try r.v(name);
    }
    var terms_b = terms_a;
    terms_b[0] = try r.v("Extra");

    try r.head(&.{terms_a[0]});
    try r.pos(wide, &terms_a);
    try r.pos(wide, &terms_b);
    try r.finish();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try std.testing.expectError(error.TooManyVariables, compile(arena.allocator(), &program.rules.items[0]));
}

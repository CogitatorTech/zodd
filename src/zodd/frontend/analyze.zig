//! # Semantic Analysis
//!
//! The module validates a Datalog program and assigns evaluation strata.
//!
//! Checks performed:
//! - Wildcard lowering: each `_` occurrence becomes a fresh rule variable.
//! - Safety (range restriction): every head variable, every variable in a
//!   negated literal, and every aggregated variable must occur in a positive
//!   body literal.
//! - Stratification: the predicate dependency graph must have no cycle
//!   through a negative edge (negation or aggregation). Strata are assigned
//!   so negated and aggregated predicates are fully computed before use.
//!
//! Stratification runs Tarjan's strongly connected components algorithm;
//! a negative edge inside a component is exactly a recursion-through-negation
//! cycle. Components are emitted dependencies-first, so strata are assigned
//! in one pass over the emission order.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");

/// Errors reported by analysis. `Diagnostic` carries the details.
pub const AnalyzeError = error{
    UnsafeHeadVariable,
    UnsafeNegatedVariable,
    UnsafeAggregate,
    NegationCycle,
} || Allocator.Error;

/// Where an analysis error occurred. The message is owned by the program's
/// arena and lives as long as the program.
pub const Diagnostic = struct {
    message: []const u8 = "",
    span: ?ast.Span = null,
    rule_index: ?u32 = null,
};

/// Result of a successful analysis.
pub const Analysis = struct {
    /// Number of strata; predicate strata are stored in `PredInfo.stratum`.
    stratum_count: u16,
};

/// A dependency edge from a rule head to a body predicate.
const Edge = struct {
    to: ast.PredId,
    negative: bool,
};

/// Validates `program` in place: lowers wildcards, recomputes `derived`
/// flags, checks safety, and assigns `PredInfo.stratum`. On error, fills
/// `diagnostic` (when provided) with location details. Idempotent.
pub fn analyze(
    program: *ast.Program,
    diagnostic: ?*Diagnostic,
) AnalyzeError!Analysis {
    lowerWildcards(program);

    for (program.preds.items) |*info| {
        info.derived = false;
        info.stratum = 0;
    }
    for (program.rules.items) |rule| {
        program.preds.items[rule.head.pred()].derived = true;
    }

    for (program.rules.items) |*rule| {
        try checkSafety(program, rule, diagnostic);
    }

    return stratify(program, diagnostic);
}

/// Replaces every wildcard term with a fresh rule-scoped variable.
fn lowerWildcards(program: *ast.Program) void {
    for (program.rules.items) |*rule| {
        switch (rule.head) {
            .plain => |atom| lowerTerms(atom.terms, &rule.var_count),
            .aggregate => |agg| lowerTerms(agg.group_terms, &rule.var_count),
        }
        for (rule.body) |literal| {
            lowerTerms(literal.atom.terms, &rule.var_count);
        }
    }
}

fn lowerTerms(terms: []ast.Term, var_count: *u16) void {
    for (terms) |*term| {
        if (term.* == .wildcard) {
            term.* = ast.Term{ .variable = var_count.* };
            var_count.* += 1;
        }
    }
}

fn checkSafety(
    program: *ast.Program,
    rule: *const ast.Rule,
    diagnostic: ?*Diagnostic,
) AnalyzeError!void {
    var bound = try std.DynamicBitSetUnmanaged.initEmpty(
        program.allocator(),
        rule.var_count,
    );
    // Arena-owned; freed with the program.

    for (rule.body) |literal| {
        if (literal.negated) continue;
        for (literal.atom.terms) |term| {
            if (term == .variable) bound.set(term.variable);
        }
    }

    switch (rule.head) {
        .plain => |atom| {
            for (atom.terms) |term| {
                if (term == .variable and !bound.isSet(term.variable)) {
                    return fail(program, diagnostic, rule, error.UnsafeHeadVariable, "head variable not bound by a positive body literal");
                }
            }
        },
        .aggregate => |agg| {
            for (agg.group_terms) |term| {
                if (term == .variable and !bound.isSet(term.variable)) {
                    return fail(program, diagnostic, rule, error.UnsafeHeadVariable, "aggregate group variable not bound by a positive body literal");
                }
            }
            if (agg.arg != .variable or !bound.isSet(agg.arg.variable)) {
                return fail(program, diagnostic, rule, error.UnsafeAggregate, "aggregated variable not bound by a positive body literal");
            }
        },
    }

    for (rule.body) |literal| {
        if (!literal.negated) continue;
        for (literal.atom.terms) |term| {
            if (term == .variable and !bound.isSet(term.variable)) {
                return fail(program, diagnostic, rule, error.UnsafeNegatedVariable, "variable in negated literal not bound by a positive body literal");
            }
        }
    }
}

fn fail(
    program: *ast.Program,
    diagnostic: ?*Diagnostic,
    rule: *const ast.Rule,
    err: AnalyzeError,
    message: []const u8,
) AnalyzeError {
    if (diagnostic) |diag| {
        diag.* = .{
            .message = std.fmt.allocPrint(
                program.allocator(),
                "rule {d}: {s}",
                .{ rule.index, message },
            ) catch message,
            .span = rule.span,
            .rule_index = rule.index,
        };
    }
    return err;
}

/// Tarjan's strongly connected components over the predicate dependency
/// graph, followed by stratum assignment in emission (dependencies-first)
/// order.
fn stratify(program: *ast.Program, diagnostic: ?*Diagnostic) AnalyzeError!Analysis {
    const n = program.preds.items.len;
    if (n == 0) return Analysis{ .stratum_count = 0 };

    const arena = program.allocator();

    // Build adjacency lists: head -> body predicate, negative for negated
    // literals and for every body literal of an aggregate rule.
    var adjacency = try arena.alloc(std.ArrayListUnmanaged(Edge), n);
    for (adjacency) |*list| list.* = .empty;
    for (program.rules.items) |rule| {
        const head_pred = rule.head.pred();
        const aggregate_head = rule.head == .aggregate;
        for (rule.body) |literal| {
            try adjacency[head_pred].append(arena, .{
                .to = literal.atom.pred,
                .negative = literal.negated or aggregate_head,
            });
        }
    }

    var tarjan = Tarjan{
        .adjacency = adjacency,
        .index = try arena.alloc(u32, n),
        .lowlink = try arena.alloc(u32, n),
        .on_stack = try std.DynamicBitSetUnmanaged.initEmpty(arena, n),
        .stack = .empty,
        .scc_id = try arena.alloc(u32, n),
        .scc_members = .empty,
        .scc_offsets = .empty,
        .arena = arena,
    };
    @memset(tarjan.index, undefined_index);
    for (0..n) |node| {
        if (tarjan.index[node] == undefined_index) {
            try tarjan.strongConnect(@intCast(node));
        }
    }

    // Components are emitted dependencies-first, so a single pass assigns
    // strata: crossing a negative edge requires a strictly higher stratum.
    const scc_count = tarjan.scc_offsets.items.len;
    const scc_stratum = try arena.alloc(u16, scc_count);
    @memset(scc_stratum, 0);

    for (0..scc_count) |scc| {
        const members = tarjan.sccMembers(scc);
        var stratum: u16 = 0;
        for (members) |member| {
            for (adjacency[member].items) |edge| {
                const target_scc = tarjan.scc_id[edge.to];
                if (target_scc == scc) {
                    if (edge.negative) {
                        if (diagnostic) |diag| {
                            diag.* = .{
                                .message = std.fmt.allocPrint(
                                    arena,
                                    "negation or aggregation through recursion involving predicate {d}",
                                    .{member},
                                ) catch "negation cycle",
                            };
                        }
                        return error.NegationCycle;
                    }
                    continue;
                }
                const required: u16 = scc_stratum[target_scc] + @intFromBool(edge.negative);
                stratum = @max(stratum, required);
            }
        }
        scc_stratum[scc] = stratum;
        for (members) |member| {
            program.preds.items[member].stratum = stratum;
        }
    }

    var max_stratum: u16 = 0;
    for (scc_stratum) |s| max_stratum = @max(max_stratum, s);
    return Analysis{ .stratum_count = max_stratum + 1 };
}

const undefined_index = std.math.maxInt(u32);

const Tarjan = struct {
    adjacency: []const std.ArrayListUnmanaged(Edge),
    index: []u32,
    lowlink: []u32,
    on_stack: std.DynamicBitSetUnmanaged,
    stack: std.ArrayListUnmanaged(u32),
    /// Members of all components, in emission order.
    scc_members: std.ArrayListUnmanaged(u32),
    /// Start offset of each component within `scc_members`.
    scc_offsets: std.ArrayListUnmanaged(u32),
    scc_id: []u32,
    arena: Allocator,
    counter: u32 = 0,

    fn sccMembers(self: *const Tarjan, scc: usize) []const u32 {
        const start: usize = self.scc_offsets.items[scc];
        const end: usize = if (scc + 1 < self.scc_offsets.items.len)
            self.scc_offsets.items[scc + 1]
        else
            self.scc_members.items.len;
        return self.scc_members.items[start..end];
    }

    /// Recursive step; depth is bounded by the number of predicates.
    fn strongConnect(self: *Tarjan, node: u32) Allocator.Error!void {
        self.index[node] = self.counter;
        self.lowlink[node] = self.counter;
        self.counter += 1;
        try self.stack.append(self.arena, node);
        self.on_stack.set(node);

        for (self.adjacency[node].items) |edge| {
            if (self.index[edge.to] == undefined_index) {
                try self.strongConnect(edge.to);
                self.lowlink[node] = @min(self.lowlink[node], self.lowlink[edge.to]);
            } else if (self.on_stack.isSet(edge.to)) {
                self.lowlink[node] = @min(self.lowlink[node], self.index[edge.to]);
            }
        }

        if (self.lowlink[node] == self.index[node]) {
            const scc: u32 = @intCast(self.scc_offsets.items.len);
            try self.scc_offsets.append(self.arena, @intCast(self.scc_members.items.len));
            while (true) {
                const member = self.stack.pop().?;
                self.on_stack.unset(member);
                self.scc_id[member] = scc;
                try self.scc_members.append(self.arena, member);
                if (member == node) break;
            }
        }
    }
};

test "analyze: strata for negation" {
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

    // safe(X) :- node(X), not blocked(X).
    var r = builder.rule(safe);
    const x = try r.v("X");
    try r.head(&.{x});
    try r.pos(node, &.{x});
    try r.neg(blocked, &.{x});
    try r.finish();

    const analysis = try analyze(&program, null);
    try std.testing.expectEqual(@as(u16, 2), analysis.stratum_count);
    try std.testing.expectEqual(@as(u16, 0), program.preds.items[node].stratum);
    try std.testing.expectEqual(@as(u16, 0), program.preds.items[blocked].stratum);
    try std.testing.expectEqual(@as(u16, 1), program.preds.items[safe].stratum);
}

test "analyze: recursion without negation stays in one stratum" {
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

    var r1 = builder.rule(path);
    {
        const x = try r1.v("X");
        const y = try r1.v("Y");
        try r1.head(&.{ x, y });
        try r1.pos(edge, &.{ x, y });
        try r1.finish();
    }
    var r2 = builder.rule(path);
    {
        const x = try r2.v("X");
        const y = try r2.v("Y");
        const z = try r2.v("Z");
        try r2.head(&.{ x, z });
        try r2.pos(path, &.{ x, y });
        try r2.pos(edge, &.{ y, z });
        try r2.finish();
    }

    const analysis = try analyze(&program, null);
    try std.testing.expectEqual(@as(u16, 1), analysis.stratum_count);
    try std.testing.expectEqual(@as(u16, 0), program.preds.items[path].stratum);
}

test "analyze: rejects negation through recursion" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const q = try builder.predicate("q", 1);
    const p = try builder.predicate("p", 1);

    // p(X) :- q(X), not p(X).
    var r = builder.rule(p);
    const x = try r.v("X");
    try r.head(&.{x});
    try r.pos(q, &.{x});
    try r.neg(p, &.{x});
    try r.finish();

    var diag = Diagnostic{};
    try std.testing.expectError(error.NegationCycle, analyze(&program, &diag));
    try std.testing.expect(diag.message.len > 0);
}

test "analyze: rejects unsafe head and negated variables" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const q = try builder.predicate("q", 1);
    const p = try builder.predicate("p", 2);

    // p(X, Y) :- q(X).  (Y unbound)
    var r = builder.rule(p);
    const x = try r.v("X");
    const y = try r.v("Y");
    try r.head(&.{ x, y });
    try r.pos(q, &.{x});
    try r.finish();

    var diag = Diagnostic{};
    try std.testing.expectError(error.UnsafeHeadVariable, analyze(&program, &diag));
    try std.testing.expectEqual(@as(?u32, 0), diag.rule_index);
}

test "analyze: rejects unsafe negated variable" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const q = try builder.predicate("q", 1);
    const b = try builder.predicate("b", 1);
    const p = try builder.predicate("p", 1);

    // p(X) :- q(X), not b(Z).  (Z unbound)
    var r = builder.rule(p);
    const x = try r.v("X");
    const z = try r.v("Z");
    try r.head(&.{x});
    try r.pos(q, &.{x});
    try r.neg(b, &.{z});
    try r.finish();

    try std.testing.expectError(error.UnsafeNegatedVariable, analyze(&program, null));
}

test "analyze: aggregates force a higher stratum" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const emp = try builder.predicate("emp", 2);
    const total = try builder.predicate("total", 2);

    // total(D, sum(S)) :- emp(D, S).
    var r = builder.rule(total);
    const d = try r.v("D");
    const s = try r.v("S");
    try r.aggHead(&.{d}, 1, .sum, s);
    try r.pos(emp, &.{ d, s });
    try r.finish();

    const analysis = try analyze(&program, null);
    try std.testing.expectEqual(@as(u16, 2), analysis.stratum_count);
    try std.testing.expect(program.preds.items[total].stratum > program.preds.items[emp].stratum);
}

test "analyze: wildcard lowering is idempotent" {
    const allocator = std.testing.allocator;
    const Builder = @import("builder.zig").Builder;
    const Interner = @import("interner.zig").Interner;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();
    var builder = Builder{ .program = &program, .interner = &interner };

    const edge = try builder.predicate("edge", 2);
    const source = try builder.predicate("source", 1);

    // source(X) :- edge(X, _).
    var r = builder.rule(source);
    const x = try r.v("X");
    try r.head(&.{x});
    try r.pos(edge, &.{ x, Builder.wild });
    try r.finish();

    _ = try analyze(&program, null);
    const var_count_after_first = program.rules.items[0].var_count;
    try std.testing.expectEqual(@as(u16, 2), var_count_after_first);

    _ = try analyze(&program, null);
    try std.testing.expectEqual(var_count_after_first, program.rules.items[0].var_count);
}

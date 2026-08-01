//! # Magic Sets
//!
//! The module builds a demand-transformed program for a single query with
//! bound arguments: adorned copies of the relevant derived predicates are
//! guarded by magic predicates seeded from the query's bindings, so
//! evaluation computes only tuples the query can actually reach.
//!
//! The transformation covers positive rules (including comparisons and
//! assignments). It reports `error.DemandUnsupported` when it cannot apply:
//! no bound argument, a non-derived query predicate, or negation or
//! aggregation in the relevant rule cone, where a naive rewrite would be
//! unsound. Callers fall back to full evaluation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const builder_mod = @import("builder.zig");
const interner_mod = @import("interner.zig");
const Interner = interner_mod.Interner;

pub const DemandError = builder_mod.BuildError || error{DemandUnsupported};

/// A demand-transformed program. The program shares the source interner;
/// `query_pred` is the adorned predicate holding the query's answers.
pub const Demand = struct {
    program: ast.Program,
    query_pred: ast.PredId,
};

/// Bound-argument mask of a query pattern or literal; bit `i` set means
/// argument `i` is bound.
const Mask = u32;

fn boundCount(mask: Mask) u16 {
    return @popCount(mask);
}

/// Builds the demand transformation of `?- pred(pattern)` over an analyzed
/// `source` program (wildcards lowered, safety checked). On success the
/// caller owns the returned program.
pub fn transform(
    allocator: Allocator,
    source: *const ast.Program,
    interner: *Interner,
    pred: ast.PredId,
    pattern: []const ?u64,
) DemandError!Demand {
    if (!source.preds.items[pred].derived) return error.DemandUnsupported;

    var query_mask: Mask = 0;
    for (pattern, 0..) |slot, i| {
        if (slot != null) query_mask |= @as(Mask, 1) << @intCast(i);
    }
    if (query_mask == 0) return error.DemandUnsupported;

    try checkCone(allocator, source, pred);

    var state = State{
        .source = source,
        .interner = interner,
        .program = ast.Program.init(allocator),
        .adorned = .empty,
        .magic = .empty,
        .edb = .empty,
        .worklist = .empty,
        .allocator = allocator,
    };
    errdefer state.deinit();
    defer {
        state.adorned.deinit(allocator);
        state.magic.deinit(allocator);
        state.edb.deinit(allocator);
        state.worklist.deinit(allocator);
    }
    var b = builder_mod.Builder{ .program = &state.program, .interner = interner };
    state.builder = &b;

    // Seed the query's magic predicate with the bound pattern values.
    const query_pred = try state.adornedPred(pred, query_mask);
    var seed_buf: std.ArrayListUnmanaged(u64) = .empty;
    defer seed_buf.deinit(allocator);
    for (pattern) |slot| {
        if (slot) |value| try seed_buf.append(allocator, value);
    }
    try b.fact((try state.magicPred(pred, query_mask)).?, seed_buf.items);

    while (state.worklist.pop()) |item| {
        try state.emitAdorned(item.pred, item.mask);
    }

    return Demand{ .program = state.program, .query_pred = query_pred };
}

/// Rejects programs whose relevant rule cone the rewrite cannot express
/// soundly: negated literals or aggregate heads.
fn checkCone(allocator: Allocator, source: *const ast.Program, pred: ast.PredId) DemandError!void {
    var in_cone = try std.DynamicBitSetUnmanaged.initEmpty(allocator, source.preds.items.len);
    defer in_cone.deinit(allocator);
    in_cone.set(pred);

    var changed = true;
    while (changed) {
        changed = false;
        for (source.rules.items) |rule| {
            if (!in_cone.isSet(rule.head.pred())) continue;
            for (rule.body) |literal| {
                if (!in_cone.isSet(literal.atom.pred)) {
                    in_cone.set(literal.atom.pred);
                    changed = true;
                }
            }
        }
    }

    for (source.rules.items) |rule| {
        if (!in_cone.isSet(rule.head.pred())) continue;
        if (rule.head == .aggregate) return error.DemandUnsupported;
        for (rule.body) |literal| {
            if (literal.negated) return error.DemandUnsupported;
        }
    }
}

const WorkItem = struct { pred: ast.PredId, mask: Mask };

const State = struct {
    source: *const ast.Program,
    interner: *Interner,
    program: ast.Program,
    builder: *builder_mod.Builder = undefined,
    /// (source pred, mask) to adorned predicate in the new program.
    adorned: std.AutoHashMapUnmanaged(u64, ast.PredId),
    /// (source pred, mask) to magic predicate in the new program.
    magic: std.AutoHashMapUnmanaged(u64, ast.PredId),
    /// Source EDB pred to its copy in the new program.
    edb: std.AutoHashMapUnmanaged(ast.PredId, ast.PredId),
    worklist: std.ArrayListUnmanaged(WorkItem),
    allocator: Allocator,

    fn deinit(self: *State) void {
        self.program.deinit();
    }

    fn key(pred: ast.PredId, mask: Mask) u64 {
        return (@as(u64, pred) << 32) | mask;
    }

    fn sourceName(self: *State, pred: ast.PredId) []const u8 {
        return self.interner.resolve(self.source.preds.items[pred].name_atom).str;
    }

    /// Formats the b/f adornment string of `mask` over `arity` arguments.
    fn adornment(mask: Mask, arity: u16, buf: []u8) []const u8 {
        for (0..arity) |i| {
            buf[i] = if (mask & (@as(Mask, 1) << @intCast(i)) != 0) 'b' else 'f';
        }
        return buf[0..arity];
    }

    /// The adorned copy of a derived predicate, registering it on the
    /// worklist on first use. `$` keeps generated names unparseable.
    fn adornedPred(self: *State, pred: ast.PredId, mask: Mask) DemandError!ast.PredId {
        if (self.adorned.get(key(pred, mask))) |id| return id;
        const info = self.source.preds.items[pred];
        var adorn_buf: [dyntuple_max]u8 = undefined;
        const name = try std.fmt.allocPrint(self.program.allocator(), "{s}$adorned${s}", .{
            self.sourceName(pred),
            adornment(mask, info.arity, &adorn_buf),
        });
        const id = try self.builder.predicate(name, info.arity);
        try self.adorned.put(self.allocator, key(pred, mask), id);
        try self.worklist.append(self.allocator, .{ .pred = pred, .mask = mask });

        // Base facts of a mixed predicate hold regardless of demand.
        for (self.source.facts.items) |fact| {
            if (fact.pred == pred) try self.builder.fact(id, fact.row);
        }
        return id;
    }

    /// The magic predicate of an adorned derived predicate, or null for the
    /// all-free adornment (whose demand is everything).
    fn magicPred(self: *State, pred: ast.PredId, mask: Mask) DemandError!?ast.PredId {
        if (mask == 0) return null;
        if (self.magic.get(key(pred, mask))) |id| return id;
        const info = self.source.preds.items[pred];
        var adorn_buf: [dyntuple_max]u8 = undefined;
        const name = try std.fmt.allocPrint(self.program.allocator(), "{s}$magic${s}", .{
            self.sourceName(pred),
            adornment(mask, info.arity, &adorn_buf),
        });
        const id = try self.builder.predicate(name, boundCount(mask));
        try self.magic.put(self.allocator, key(pred, mask), id);
        return id;
    }

    /// The copy of a non-derived predicate, with its facts.
    fn edbPred(self: *State, pred: ast.PredId) DemandError!ast.PredId {
        if (self.edb.get(pred)) |id| return id;
        const info = self.source.preds.items[pred];
        const id = try self.builder.predicate(self.sourceName(pred), info.arity);
        try self.edb.put(self.allocator, pred, id);
        for (self.source.facts.items) |fact| {
            if (fact.pred == pred) try self.builder.fact(id, fact.row);
        }
        return id;
    }

    /// Emits the rewritten rules of one adorned predicate: each source rule
    /// guarded by the magic predicate, plus a magic rule per derived body
    /// literal propagating demand sideways.
    fn emitAdorned(self: *State, pred: ast.PredId, mask: Mask) DemandError!void {
        const adorned_pred = self.adorned.get(key(pred, mask)).?;
        for (self.source.rules.items) |*rule| {
            if (rule.head.pred() != pred) continue;
            const head_terms = rule.head.plain.terms;

            // First pass: the adornment of every body literal, from the
            // bound head positions and each earlier positive literal.
            var bound = try std.DynamicBitSetUnmanaged.initEmpty(self.allocator, rule.var_count);
            defer bound.deinit(self.allocator);
            for (head_terms, 0..) |term, i| {
                if (term == .variable and mask & (@as(Mask, 1) << @intCast(i)) != 0) {
                    bound.set(term.variable);
                }
            }
            var lit_masks: std.ArrayListUnmanaged(Mask) = .empty;
            defer lit_masks.deinit(self.allocator);
            for (rule.body) |literal| {
                var lit_mask: Mask = 0;
                for (literal.atom.terms, 0..) |term, i| {
                    const is_bound = switch (term) {
                        .constant => true,
                        .variable => |var_id| bound.isSet(var_id),
                        .wildcard => unreachable, // Lowered by analysis.
                    };
                    if (is_bound) lit_mask |= @as(Mask, 1) << @intCast(i);
                }
                try lit_masks.append(self.allocator, lit_mask);
                for (literal.atom.terms) |term| {
                    if (term == .variable) bound.set(term.variable);
                }
            }

            // Second pass: the rewritten rule, and one magic rule per
            // derived body literal.
            var r = self.builder.rule(adorned_pred);
            try r.head(try self.mapTerms(&r, head_terms));
            if (try self.magicPred(pred, mask)) |magic_pred| {
                try r.pos(magic_pred, try self.boundTerms(&r, head_terms, mask));
            }
            for (rule.body, lit_masks.items, 0..) |literal, lit_mask, lit_index| {
                if (self.source.preds.items[literal.atom.pred].derived) {
                    const adorned_lit = try self.adornedPred(literal.atom.pred, lit_mask);
                    if (try self.magicPred(literal.atom.pred, lit_mask)) |magic_lit| {
                        try self.emitMagicRule(rule, mask, lit_masks.items, lit_index, magic_lit);
                    }
                    try r.pos(adorned_lit, try self.mapTerms(&r, literal.atom.terms));
                } else {
                    try r.pos(try self.edbPred(literal.atom.pred), try self.mapTerms(&r, literal.atom.terms));
                }
            }
            for (rule.assigns) |assign| {
                try r.assign(try self.mapVar(&r, assign.target), try self.mapExpr(&r, assign.expr));
            }
            for (rule.compares) |compare| {
                try r.cmpExpr(try self.mapExpr(&r, compare.lhs), compare.op, try self.mapExpr(&r, compare.rhs));
            }
            try r.finish();
        }
    }

    /// Emits the magic rule demanding body literal `lit_index` of `rule`:
    /// its bound arguments, given the head's demand and every earlier
    /// positive literal. With no body items at all, the demand is a ground
    /// fact.
    fn emitMagicRule(
        self: *State,
        rule: *const ast.Rule,
        head_mask: Mask,
        lit_masks: []const Mask,
        lit_index: usize,
        magic_lit: ast.PredId,
    ) DemandError!void {
        const literal = rule.body[lit_index];
        const lit_mask = lit_masks[lit_index];
        const head_terms = rule.head.plain.terms;
        const head_magic = try self.magicPred(rule.head.pred(), head_mask);

        if (head_magic == null and lit_index == 0) {
            // No guard and no earlier literals: the demanded binding is
            // ground (constants only, or lit_mask would be empty).
            var row: std.ArrayListUnmanaged(u64) = .empty;
            defer row.deinit(self.allocator);
            for (literal.atom.terms, 0..) |term, i| {
                if (lit_mask & (@as(Mask, 1) << @intCast(i)) == 0) continue;
                try row.append(self.allocator, term.constant);
            }
            try self.builder.fact(magic_lit, row.items);
            return;
        }

        var r = self.builder.rule(magic_lit);
        try r.head(try self.boundTerms(&r, literal.atom.terms, lit_mask));
        if (head_magic) |magic_pred| {
            try r.pos(magic_pred, try self.boundTerms(&r, head_terms, head_mask));
        }
        for (rule.body[0..lit_index], lit_masks[0..lit_index]) |earlier, earlier_mask| {
            const earlier_pred = if (self.source.preds.items[earlier.atom.pred].derived)
                try self.adornedPred(earlier.atom.pred, earlier_mask)
            else
                try self.edbPred(earlier.atom.pred);
            try r.pos(earlier_pred, try self.mapTerms(&r, earlier.atom.terms));
        }
        try r.finish();
    }

    /// Maps one source variable to the rule builder's variable id space by
    /// stable per-id names.
    fn mapVar(self: *State, r: *builder_mod.RuleBuilder, var_id: ast.VarId) DemandError!ast.Term {
        _ = self;
        var name_buf: [8]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "v{d}", .{var_id}) catch unreachable;
        return r.v(name);
    }

    fn mapTerm(self: *State, r: *builder_mod.RuleBuilder, term: ast.Term) DemandError!ast.Term {
        return switch (term) {
            .variable => |var_id| try self.mapVar(r, var_id),
            .constant => term,
            .wildcard => unreachable, // Lowered by analysis.
        };
    }

    fn mapTerms(self: *State, r: *builder_mod.RuleBuilder, terms: []const ast.Term) DemandError![]const ast.Term {
        const copy = try self.program.allocator().alloc(ast.Term, terms.len);
        for (terms, 0..) |term, i| copy[i] = try self.mapTerm(r, term);
        return copy;
    }

    /// The terms in bound positions of `mask`, mapped into `r`.
    fn boundTerms(self: *State, r: *builder_mod.RuleBuilder, terms: []const ast.Term, mask: Mask) DemandError![]const ast.Term {
        var list: std.ArrayListUnmanaged(ast.Term) = .empty;
        defer list.deinit(self.allocator);
        for (terms, 0..) |term, i| {
            if (mask & (@as(Mask, 1) << @intCast(i)) == 0) continue;
            try list.append(self.allocator, try self.mapTerm(r, term));
        }
        return try self.program.allocator().dupe(ast.Term, list.items);
    }

    fn mapExpr(self: *State, r: *builder_mod.RuleBuilder, expr: ast.Expr) DemandError!ast.Expr {
        return switch (expr) {
            .term => |term| ast.Expr{ .term = try self.mapTerm(r, term) },
            .binop => |binop| try r.binExpr(
                binop.op,
                try self.mapExpr(r, binop.lhs),
                try self.mapExpr(r, binop.rhs),
            ),
        };
    }
};

const dyntuple_max = @import("dyntuple.zig").MAX_ARITY;

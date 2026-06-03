//! # Rule Builder
//!
//! The module provides the programmatic construction path for Datalog
//! programs: embedders build the same IR the textual parser produces,
//! without going through source text.
//!
//! ## Usage
//!
//! ```zig
//! var builder = Builder{ .program = &program, .interner = &interner };
//! const edge = try builder.predicate("edge", 2);
//! const path = try builder.predicate("path", 2);
//!
//! try builder.fact(edge, &.{ 1, 2 });
//!
//! var r = builder.rule(path);
//! const x = try r.v("X");
//! const y = try r.v("Y");
//! const z = try r.v("Z");
//! try r.head(&.{ x, z });
//! try r.pos(path, &.{ x, y });
//! try r.pos(edge, &.{ y, z });
//! try r.finish();
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const dyntuple = @import("dyntuple.zig");
const interner_mod = @import("interner.zig");
const Interner = interner_mod.Interner;

/// Errors produced while building a program.
pub const BuildError = ast.ConstructError || interner_mod.EncodeError || error{
    InvalidAggregate,
    MissingHead,
    EmptyBody,
} || Allocator.Error;

pub const Builder = struct {
    program: *ast.Program,
    interner: *Interner,

    /// Declares or looks up a predicate by name and arity.
    pub fn predicate(self: *Builder, name: []const u8, arity: u16) BuildError!ast.PredId {
        const name_atom = try self.interner.intern(name);
        return self.program.predicate(name_atom, arity);
    }

    /// Creates an integer constant term.
    pub fn int(self: *Builder, value: u64) BuildError!ast.Term {
        _ = self;
        return ast.Term{ .constant = try interner_mod.encodeInt(value) };
    }

    /// Creates a string constant term, interning the string.
    pub fn str(self: *Builder, bytes: []const u8) BuildError!ast.Term {
        return ast.Term{ .constant = try self.interner.intern(bytes) };
    }

    /// An anonymous wildcard term.
    pub const wild = ast.Term.wildcard;

    /// Adds a ground fact from pre-encoded atoms.
    pub fn fact(self: *Builder, pred: ast.PredId, row: []const dyntuple.Atom) BuildError!void {
        const info = self.program.preds.items[pred];
        if (row.len != info.arity) return error.ArityMismatch;
        const copy = try self.program.allocator().dupe(dyntuple.Atom, row);
        try self.program.facts.append(self.program.allocator(), .{ .pred = pred, .row = copy });
    }

    /// Adds a ground fact from surface values, encoding ints and interning
    /// strings.
    pub fn factValues(self: *Builder, pred: ast.PredId, values: []const interner_mod.Value) BuildError!void {
        const info = self.program.preds.items[pred];
        if (values.len != info.arity) return error.ArityMismatch;
        const row = try self.program.allocator().alloc(dyntuple.Atom, values.len);
        for (values, 0..) |value, i| {
            row[i] = try self.interner.encode(value);
        }
        try self.program.facts.append(self.program.allocator(), .{ .pred = pred, .row = row });
    }

    /// Adds a stored query; null pattern columns are free.
    pub fn query(self: *Builder, pred: ast.PredId, pattern: []const ?dyntuple.Atom) BuildError!void {
        const info = self.program.preds.items[pred];
        if (pattern.len != info.arity) return error.ArityMismatch;
        const copy = try self.program.allocator().dupe(?dyntuple.Atom, pattern);
        try self.program.queries.append(self.program.allocator(), .{ .pred = pred, .pattern = copy });
    }

    /// Begins a rule with the given head predicate. Call `head` or `aggHead`
    /// plus at least one `pos`, then `finish`.
    pub fn rule(self: *Builder, head_pred: ast.PredId) RuleBuilder {
        return RuleBuilder{ .builder = self, .head_pred = head_pred };
    }
};

/// Accumulates one rule. Variables are interned per rule by name; the same
/// name yields the same `VarId` within the rule.
pub const RuleBuilder = struct {
    builder: *Builder,
    head_pred: ast.PredId,
    head_spec: ?ast.Head = null,
    body: std.ArrayListUnmanaged(ast.Literal) = .empty,
    var_names: std.ArrayListUnmanaged([]const u8) = .empty,
    span: ast.Span = .{},

    fn arena(self: *RuleBuilder) Allocator {
        return self.builder.program.allocator();
    }

    /// Returns the variable term for `name`, creating a fresh `VarId` on
    /// first use within this rule.
    pub fn v(self: *RuleBuilder, name: []const u8) BuildError!ast.Term {
        for (self.var_names.items, 0..) |existing, id| {
            if (std.mem.eql(u8, existing, name)) {
                return ast.Term{ .variable = @intCast(id) };
            }
        }
        const id: ast.VarId = @intCast(self.var_names.items.len);
        const copy = try self.arena().dupe(u8, name);
        try self.var_names.append(self.arena(), copy);
        return ast.Term{ .variable = id };
    }

    /// Sets a plain head with the given argument terms.
    pub fn head(self: *RuleBuilder, terms: []const ast.Term) BuildError!void {
        const info = self.builder.program.preds.items[self.head_pred];
        if (terms.len != info.arity) return error.ArityMismatch;
        const copy = try self.arena().dupe(ast.Term, terms);
        self.head_spec = ast.Head{ .plain = .{ .pred = self.head_pred, .terms = copy, .span = self.span } };
    }

    /// Sets an aggregate head: `group_terms` are the non-aggregate arguments
    /// in order, `agg_slot` is the argument position of the aggregate, and
    /// `arg` is the aggregated body variable.
    pub fn aggHead(
        self: *RuleBuilder,
        group_terms: []const ast.Term,
        agg_slot: u16,
        func: ast.AggFunc,
        arg: ast.Term,
    ) BuildError!void {
        const info = self.builder.program.preds.items[self.head_pred];
        if (group_terms.len + 1 != info.arity) return error.ArityMismatch;
        if (agg_slot >= info.arity) return error.InvalidAggregate;
        if (arg != .variable) return error.InvalidAggregate;
        const copy = try self.arena().dupe(ast.Term, group_terms);
        self.head_spec = ast.Head{ .aggregate = .{
            .pred = self.head_pred,
            .group_terms = copy,
            .agg_slot = agg_slot,
            .func = func,
            .arg = arg,
            .span = self.span,
        } };
    }

    /// Appends a positive body literal.
    pub fn pos(self: *RuleBuilder, pred: ast.PredId, terms: []const ast.Term) BuildError!void {
        try self.literal(pred, terms, false);
    }

    /// Appends a negated body literal.
    pub fn neg(self: *RuleBuilder, pred: ast.PredId, terms: []const ast.Term) BuildError!void {
        try self.literal(pred, terms, true);
    }

    fn literal(self: *RuleBuilder, pred: ast.PredId, terms: []const ast.Term, negated: bool) BuildError!void {
        const info = self.builder.program.preds.items[pred];
        if (terms.len != info.arity) return error.ArityMismatch;
        const copy = try self.arena().dupe(ast.Term, terms);
        try self.body.append(self.arena(), .{
            .atom = .{ .pred = pred, .terms = copy },
            .negated = negated,
        });
    }

    /// Validates and appends the rule to the program, marking the head
    /// predicate as derived.
    pub fn finish(self: *RuleBuilder) BuildError!void {
        const head_spec = self.head_spec orelse return error.MissingHead;
        if (self.body.items.len == 0) return error.EmptyBody;

        const program = self.builder.program;
        program.preds.items[self.head_pred].derived = true;
        try program.rules.append(program.allocator(), .{
            .head = head_spec,
            .body = self.body.items,
            .var_count = @intCast(self.var_names.items.len),
            .index = @intCast(program.rules.items.len),
            .span = self.span,
        });
    }
};

test "Builder: transitive closure program" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();

    var builder = Builder{ .program = &program, .interner = &interner };

    const edge = try builder.predicate("edge", 2);
    const path = try builder.predicate("path", 2);

    try builder.fact(edge, &.{ 1, 2 });
    try builder.fact(edge, &.{ 2, 3 });

    {
        var r = builder.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        try r.head(&.{ x, y });
        try r.pos(edge, &.{ x, y });
        try r.finish();
    }
    {
        var r = builder.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        const z = try r.v("Z");
        try r.head(&.{ x, z });
        try r.pos(path, &.{ x, y });
        try r.pos(edge, &.{ y, z });
        try r.finish();
    }

    try std.testing.expectEqual(@as(usize, 2), program.facts.items.len);
    try std.testing.expectEqual(@as(usize, 2), program.rules.items.len);
    try std.testing.expectEqual(@as(u16, 3), program.rules.items[1].var_count);
    try std.testing.expect(program.preds.items[path].derived);
    try std.testing.expect(!program.preds.items[edge].derived);
}

test "Builder: repeated variable names share an id" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();

    var builder = Builder{ .program = &program, .interner = &interner };
    const p = try builder.predicate("p", 2);

    var r = builder.rule(p);
    const x1 = try r.v("X");
    const x2 = try r.v("X");
    const y = try r.v("Y");

    try std.testing.expectEqual(x1.variable, x2.variable);
    try std.testing.expect(x1.variable != y.variable);
}

test "Builder: arity errors" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();

    var builder = Builder{ .program = &program, .interner = &interner };
    const edge = try builder.predicate("edge", 2);

    try std.testing.expectError(error.ArityMismatch, builder.fact(edge, &.{1}));
    try std.testing.expectError(error.ArityConflict, builder.predicate("edge", 3));

    var r = builder.rule(edge);
    const x = try r.v("X");
    try std.testing.expectError(error.ArityMismatch, r.head(&.{x}));
    try std.testing.expectError(error.ArityMismatch, r.pos(edge, &.{ x, x, x }));
    try std.testing.expectError(error.MissingHead, r.finish());
}

test "Builder: aggregate head validation" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();

    var builder = Builder{ .program = &program, .interner = &interner };
    const emp = try builder.predicate("emp", 2);
    const total = try builder.predicate("total", 2);

    var r = builder.rule(total);
    const d = try r.v("D");
    const s = try r.v("S");

    // Aggregate argument must be a variable.
    try std.testing.expectError(
        error.InvalidAggregate,
        r.aggHead(&.{d}, 1, .sum, try builder.int(3)),
    );
    try r.aggHead(&.{d}, 1, .sum, s);
    try r.pos(emp, &.{ d, s });
    try r.finish();

    try std.testing.expectEqual(@as(usize, 1), program.rules.items.len);
    try std.testing.expectEqual(ast.AggFunc.sum, program.rules.items[0].head.aggregate.func);
}

test "Builder: string facts go through the interner" {
    const allocator = std.testing.allocator;

    var program = ast.Program.init(allocator);
    defer program.deinit();
    var interner = Interner.init(allocator);
    defer interner.deinit();

    var builder = Builder{ .program = &program, .interner = &interner };
    const edge = try builder.predicate("edge", 2);

    try builder.factValues(edge, &.{ .{ .str = "a" }, .{ .str = "b" } });
    try builder.factValues(edge, &.{ .{ .str = "b" }, .{ .int = 7 } });

    const row0 = program.facts.items[0].row;
    try std.testing.expect(interner_mod.isStr(row0[0]));
    try std.testing.expectEqualStrings("a", interner.resolve(row0[0]).str);
    const row1 = program.facts.items[1].row;
    try std.testing.expectEqual(interner_mod.Value{ .int = 7 }, interner.resolve(row1[1]));
}

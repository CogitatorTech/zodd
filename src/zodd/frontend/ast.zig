//! # Datalog Program IR
//!
//! The module defines the intermediate representation shared by the
//! programmatic builder and the textual parser.
//!
//! Values are erased into the `u64` atom space managed by the interner.
//! Variables are interned per rule into dense `VarId`s, so semantic analysis
//! can use bitsets instead of name lookups. All variable-length data is
//! owned by the program's arena and freed in one shot by `Program.deinit`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const dyntuple = @import("dyntuple.zig");

/// Identifies a predicate; an index into `Program.preds`.
pub const PredId = u32;

/// Identifies a variable within a single rule. Dense, starting at zero.
pub const VarId = u16;

/// Byte range in the source text, for parser diagnostics. Builder-made IR
/// leaves it zeroed and uses the rule index for location instead.
pub const Span = struct {
    start: u32 = 0,
    end: u32 = 0,
};

/// A term is a rule-scoped variable, a ground constant in the atom space, or
/// an anonymous wildcard (each occurrence is distinct; lowered to a fresh
/// variable during analysis).
pub const Term = union(enum) {
    variable: VarId,
    constant: dyntuple.Atom,
    wildcard,
};

/// Aggregate functions usable in rule heads.
pub const AggFunc = enum { count, sum, min, max };

/// A relational atom: a predicate applied to terms. `terms.len` equals the
/// predicate's arity.
pub const Atom = struct {
    pred: PredId,
    terms: []Term,
    span: Span = .{},
};

/// A body literal: a positive or negated atom.
pub const Literal = struct {
    atom: Atom,
    negated: bool = false,
};

/// An aggregate rule head, like `total(D, sum(S))`. The head's arity is
/// `group_terms.len + 1`; `agg_slot` is the argument position the aggregate
/// occupies and `group_terms` are the remaining arguments in order.
pub const AggHead = struct {
    pred: PredId,
    group_terms: []Term,
    agg_slot: u16,
    func: AggFunc,
    /// The aggregated body variable. `count` parses one but ignores it.
    arg: Term,
    span: Span = .{},
};

/// Head of a rule: a plain atom or an aggregate head.
pub const Head = union(enum) {
    plain: Atom,
    aggregate: AggHead,

    /// Returns the head predicate.
    pub fn pred(self: Head) PredId {
        return switch (self) {
            .plain => |atom| atom.pred,
            .aggregate => |agg| agg.pred,
        };
    }
};

/// A rule with a head and a non-empty body.
pub const Rule = struct {
    head: Head,
    body: []Literal,
    /// Number of distinct rule-scoped variables, including lowered wildcards.
    var_count: u16,
    /// Display names indexed by `VarId`. Variables lowered from wildcards
    /// have ids at or past `var_names.len` and display as `_`.
    var_names: []const []const u8 = &.{},
    /// Index of this rule within the program, for diagnostics.
    index: u32,
    span: Span = .{},
};

/// A ground fact: all columns are constants.
pub const Fact = struct {
    pred: PredId,
    row: []dyntuple.Atom,
    span: Span = .{},
};

/// A stored query (`?- p(1, X).`): null pattern columns are free.
pub const Query = struct {
    pred: PredId,
    pattern: []?dyntuple.Atom,
    span: Span = .{},
};

/// Predicate metadata.
pub const PredInfo = struct {
    /// Interned atom of the predicate name, for display.
    name_atom: dyntuple.Atom,
    arity: u16,
    /// True if the predicate appears in some rule head.
    derived: bool = false,
    /// Assigned by stratification.
    stratum: u16 = 0,
};

/// Errors from IR construction shared by the builder and the parser.
pub const ConstructError = error{
    ArityMismatch,
    ArityConflict,
    ArityTooLarge,
};

/// The whole program IR. All slices held by rules, facts, and queries live
/// in `arena`.
pub const Program = struct {
    arena: std.heap.ArenaAllocator,
    preds: std.ArrayListUnmanaged(PredInfo),
    /// Predicate name atom to id.
    pred_ids: std.AutoHashMapUnmanaged(dyntuple.Atom, PredId),
    facts: std.ArrayListUnmanaged(Fact),
    rules: std.ArrayListUnmanaged(Rule),
    queries: std.ArrayListUnmanaged(Query),

    /// Initializes an empty program backed by a fresh arena.
    pub fn init(child_allocator: Allocator) Program {
        return Program{
            .arena = std.heap.ArenaAllocator.init(child_allocator),
            .preds = .empty,
            .pred_ids = .empty,
            .facts = .empty,
            .rules = .empty,
            .queries = .empty,
        };
    }

    /// Frees the arena and with it every slice the IR owns.
    pub fn deinit(self: *Program) void {
        self.arena.deinit();
    }

    /// The allocator IR nodes must be allocated with.
    pub fn allocator(self: *Program) Allocator {
        return self.arena.allocator();
    }

    /// Looks up or registers a predicate. Re-registering with a different
    /// arity returns `error.ArityConflict`.
    pub fn predicate(
        self: *Program,
        name_atom: dyntuple.Atom,
        arity: u16,
    ) (ConstructError || Allocator.Error)!PredId {
        if (arity > dyntuple.MAX_ARITY) return error.ArityTooLarge;
        if (self.pred_ids.get(name_atom)) |id| {
            if (self.preds.items[id].arity != arity) return error.ArityConflict;
            return id;
        }
        const id: PredId = @intCast(self.preds.items.len);
        try self.preds.append(self.allocator(), .{ .name_atom = name_atom, .arity = arity });
        try self.pred_ids.put(self.allocator(), name_atom, id);
        return id;
    }

    /// Looks up a predicate by name atom.
    pub fn findPredicate(self: *const Program, name_atom: dyntuple.Atom) ?PredId {
        return self.pred_ids.get(name_atom);
    }
};

test "Program: predicate registration and arity conflict" {
    var program = Program.init(std.testing.allocator);
    defer program.deinit();

    const p1 = try program.predicate(100, 2);
    const p2 = try program.predicate(100, 2);
    const q = try program.predicate(101, 1);

    try std.testing.expectEqual(p1, p2);
    try std.testing.expect(p1 != q);
    try std.testing.expectError(error.ArityConflict, program.predicate(100, 3));
    try std.testing.expectError(error.ArityTooLarge, program.predicate(102, dyntuple.MAX_ARITY + 1));
    try std.testing.expectEqual(@as(?PredId, p1), program.findPredicate(100));
    try std.testing.expectEqual(@as(?PredId, null), program.findPredicate(999));
}

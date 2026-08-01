//! # Datalog Database
//!
//! The module provides the user-facing entry point to the Datalog frontend:
//! build or parse a program, solve it, and query the results.
//!
//! ## Usage
//!
//! ```zig
//! var db = zodd.Database.init(allocator);
//! defer db.deinit();
//!
//! try db.run(
//!     \\edge(1, 2).
//!     \\edge(2, 3).
//!     \\path(X, Y) :- edge(X, Y).
//!     \\path(X, Z) :- path(X, Y), edge(Y, Z).
//! );
//! try db.solve();
//!
//! var it = try db.query("path", &.{ .{ .int = 1 }, null });
//! defer it.deinit();
//! while (it.next()) |row| {
//!     std.debug.print("{f}\n", .{row});
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const analyze_mod = @import("analyze.zig");
const builder_mod = @import("builder.zig");
const dyntuple = @import("dyntuple.zig");
const evaluator_mod = @import("evaluator.zig");
const explain_mod = @import("explain.zig");
const magic = @import("magic.zig");
const interner_mod = @import("interner.zig");
const parser_mod = @import("parser.zig");
const plan_mod = @import("plan.zig");
const DynTuple = dyntuple.DynTuple;

pub const Value = interner_mod.Value;
pub const Diagnostic = analyze_mod.Diagnostic;

/// All errors the frontend can produce.
pub const FrontendError = parser_mod.ParseError ||
    analyze_mod.AnalyzeError ||
    evaluator_mod.EvalError ||
    error{UnknownPredicate};

/// Errors produced by `explainPlan` and `explain`.
pub const ExplainError = FrontendError || std.Io.Writer.Error || error{
    /// `explain` requires `track_provenance` set before the solve.
    ProvenanceNotTracked,
    /// The tuple given to `explain` is not in the predicate's relation.
    TupleNotFound,
};

/// An embeddable Datalog database: facts and rules go in through `run`,
/// `addFact`, or the `builder`; `solve` computes the fixed point; `query`
/// reads the results.
///
/// The struct is self-referential through its builders and iterators; do
/// not copy or move it while in use.
pub const Database = struct {
    allocator: Allocator,
    program: ast.Program,
    interner: interner_mod.Interner,
    evaluator: ?evaluator_mod.Evaluator = null,
    /// State of the last `queryDemand`: the rewritten program and its
    /// evaluator, torn down on the next demand query, solve, or deinit.
    demand_program: ?ast.Program = null,
    demand_evaluator: ?evaluator_mod.Evaluator = null,
    diagnostic: Diagnostic = .{},
    /// Bounds the fixed-point rounds within each stratum;
    /// `error.MaxIterationsExceeded` when exceeded. Null means no limit.
    max_iterations: ?usize = null,
    /// When true, `solve` records how each derived tuple was first obtained,
    /// enabling `explain`. Costs one map entry per derived tuple.
    track_provenance: bool = false,

    pub fn init(allocator: Allocator) Database {
        return Database{
            .allocator = allocator,
            .program = ast.Program.init(allocator),
            .interner = interner_mod.Interner.init(allocator),
        };
    }

    pub fn deinit(self: *Database) void {
        self.clearDemand();
        if (self.evaluator) |*evaluator| evaluator.deinit();
        self.program.deinit();
        self.interner.deinit();
    }

    /// A builder for programmatic rule construction. The builder borrows
    /// this database; results become visible to `solve` immediately.
    pub fn builder(self: *Database) builder_mod.Builder {
        return builder_mod.Builder{ .program = &self.program, .interner = &self.interner };
    }

    /// Adds a ground fact by predicate name, declaring the predicate on
    /// first use with the row's arity.
    pub fn addFact(self: *Database, pred_name: []const u8, row: []const Value) FrontendError!void {
        var b = self.builder();
        const pred = try b.predicate(pred_name, @intCast(row.len));
        try b.factValues(pred, row);
    }

    /// Removes one occurrence of a previously added ground fact and
    /// invalidates computed results; the next solve or query recomputes
    /// from scratch. Returns true if a matching fact was removed. Only
    /// base facts can be retracted; derived tuples disappear when the
    /// facts deriving them do.
    pub fn retract(self: *Database, pred_name: []const u8, row: []const Value) FrontendError!bool {
        const name_atom = self.interner.find(pred_name) orelse return error.UnknownPredicate;
        const pred = self.program.findPredicate(name_atom) orelse return error.UnknownPredicate;
        const info = self.program.preds.items[pred];
        if (row.len != info.arity) return error.ArityMismatch;

        // A value the interner has never seen cannot match any stored fact.
        var encoded: [dyntuple.MAX_ARITY]u64 = undefined;
        for (row, 0..) |value, i| {
            encoded[i] = switch (value) {
                .int => |v| try interner_mod.encodeInt(v),
                .str => |s| self.interner.find(s) orelse return false,
            };
        }

        for (self.program.facts.items, 0..) |fact, i| {
            if (fact.pred != pred) continue;
            if (!std.mem.eql(u64, fact.row, encoded[0..row.len])) continue;
            _ = self.program.facts.swapRemove(i);
            self.clearDemand();
            if (self.evaluator) |*evaluator| evaluator.deinit();
            self.evaluator = null;
            return true;
        }
        return false;
    }

    /// Parses Datalog source, appending its facts, rules, and queries to
    /// the program. On error, `lastDiagnostic` has the details.
    pub fn run(self: *Database, source: []const u8) FrontendError!void {
        self.diagnostic = .{};
        try parser_mod.parse(&self.program, &self.interner, source, &self.diagnostic);
    }

    /// Analyzes the program (safety, arity, stratification) and computes
    /// all derived relations. Solving again after adding facts or rules
    /// recomputes from scratch.
    pub fn solve(self: *Database) FrontendError!void {
        self.diagnostic = .{};

        // Any failure below leaves the database without results, so a later
        // `query` or `explain` re-solves and reports the failure instead of
        // serving partial or stale relations.
        self.clearDemand();
        if (self.evaluator) |*old| old.deinit();
        self.evaluator = null;

        const analysis = try analyze_mod.analyze(&self.program, &self.diagnostic);

        // Recursive arithmetic can derive fresh values forever; refuse to
        // evaluate it without an explicit iteration bound.
        if (analysis.recursive_assignment and self.max_iterations == null) {
            return error.IterationLimitRequired;
        }

        self.evaluator = evaluator_mod.Evaluator.init(self.allocator, &self.program);
        errdefer {
            self.evaluator.?.deinit();
            self.evaluator = null;
        }
        self.evaluator.?.track_provenance = self.track_provenance;
        try self.evaluator.?.solve(analysis.stratum_count, self.max_iterations);
    }

    /// Queries a predicate with a partial binding: null columns are free.
    /// Solves first if needed. The iterator borrows the database; it is
    /// invalidated by the next `solve` or `deinit`.
    pub fn query(self: *Database, pred_name: []const u8, pattern: []const ?Value) FrontendError!RowIterator {
        if (self.evaluator == null) try self.solve();

        const name_atom = self.interner.find(pred_name) orelse return error.UnknownPredicate;
        const pred = self.program.findPredicate(name_atom) orelse return error.UnknownPredicate;
        const info = self.program.preds.items[pred];
        if (pattern.len != info.arity) return error.ArityMismatch;

        // Encode the pattern. A string constant the interner has never seen
        // cannot match anything.
        var encoded: [dyntuple.MAX_ARITY]?u64 = @splat(null);
        for (pattern, 0..) |slot, i| {
            const value = slot orelse continue;
            encoded[i] = switch (value) {
                .int => |v| try interner_mod.encodeInt(v),
                .str => |s| self.interner.find(s) orelse return RowIterator.empty(&self.interner, info.arity),
            };
        }

        return self.rowsOf(&self.evaluator.?, pred, encoded, info.arity);
    }

    /// Answers one query with a demand-driven (magic sets) evaluation:
    /// only tuples the bound arguments can reach are computed, instead of
    /// the whole program. Falls back to `query` (full evaluation) when the
    /// rewrite does not apply: no bound argument, a non-derived predicate,
    /// or negation or aggregation among the relevant rules. Results are
    /// identical to `query` either way. The iterator borrows the database;
    /// it is invalidated by the next query, solve, or deinit.
    pub fn queryDemand(self: *Database, pred_name: []const u8, pattern: []const ?Value) FrontendError!RowIterator {
        const name_atom = self.interner.find(pred_name) orelse return error.UnknownPredicate;
        const pred = self.program.findPredicate(name_atom) orelse return error.UnknownPredicate;
        const info = self.program.preds.items[pred];
        if (pattern.len != info.arity) return error.ArityMismatch;

        // Encode the pattern. A string constant the interner has never seen
        // cannot match anything.
        var encoded: [dyntuple.MAX_ARITY]?u64 = @splat(null);
        for (pattern, 0..) |slot, i| {
            const value = slot orelse continue;
            encoded[i] = switch (value) {
                .int => |v| try interner_mod.encodeInt(v),
                .str => |s| self.interner.find(s) orelse return RowIterator.empty(&self.interner, info.arity),
            };
        }

        // Analysis validates the source program and lowers wildcards, which
        // the rewrite relies on.
        self.diagnostic = .{};
        const analysis = try analyze_mod.analyze(&self.program, &self.diagnostic);
        if (analysis.recursive_assignment and self.max_iterations == null) {
            return error.IterationLimitRequired;
        }

        self.clearDemand();
        const demand = magic.transform(
            self.allocator,
            &self.program,
            &self.interner,
            pred,
            encoded[0..info.arity],
        ) catch |err| switch (err) {
            error.DemandUnsupported => return self.query(pred_name, pattern),
            else => |other| return other,
        };

        self.demand_program = demand.program;
        errdefer self.clearDemand();
        const demand_analysis = try analyze_mod.analyze(&self.demand_program.?, null);
        self.demand_evaluator = evaluator_mod.Evaluator.init(self.allocator, &self.demand_program.?);
        try self.demand_evaluator.?.solve(demand_analysis.stratum_count, self.max_iterations);

        return self.rowsOf(&self.demand_evaluator.?, demand.query_pred, encoded, info.arity);
    }

    fn clearDemand(self: *Database) void {
        if (self.demand_evaluator) |*demand_evaluator| demand_evaluator.deinit();
        self.demand_evaluator = null;
        if (self.demand_program) |*demand_program| demand_program.deinit();
        self.demand_program = null;
    }

    /// Rows of `pred` in `evaluator` matching an encoded pattern.
    fn rowsOf(
        self: *Database,
        evaluator: *const evaluator_mod.Evaluator,
        pred: ast.PredId,
        encoded: [dyntuple.MAX_ARITY]?u64,
        arity: u16,
    ) RowIterator {
        const relation = evaluator.relationOf(pred) orelse
            return RowIterator.empty(&self.interner, arity);

        // Gallop to the candidate range using the bound prefix columns.
        var prefix_len: usize = 0;
        var probe = dyntuple.zero_tuple;
        while (prefix_len < arity) : (prefix_len += 1) {
            const bound = encoded[prefix_len] orelse break;
            dyntuple.set(&probe, prefix_len, bound);
        }
        var elements: []const DynTuple = relation.elements;
        if (prefix_len > 0) {
            elements = dyntuple.gallopPrefix(elements, &probe, prefix_len);
        }

        return RowIterator{
            .elements = elements,
            .pattern = encoded,
            .prefix = probe,
            .prefix_len = prefix_len,
            .arity = arity,
            .interner = &self.interner,
        };
    }

    /// Writes the compiled join plan of every rule: steps, join keys,
    /// constant and equality checks, and the head projection. Analyzes the
    /// program first; analysis errors land in `lastDiagnostic`.
    pub fn explainPlan(self: *Database, writer: *std.Io.Writer) ExplainError!void {
        self.diagnostic = .{};
        _ = try analyze_mod.analyze(&self.program, &self.diagnostic);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        for (self.program.rules.items, 0..) |*rule, i| {
            const plan = try plan_mod.compile(arena.allocator(), rule);
            if (i > 0) try writer.writeAll("\n");
            try explain_mod.writePlan(writer, &self.program, &self.interner, rule, &plan);
        }
    }

    /// Writes the proof tree showing how a derived tuple was obtained:
    /// the rule deriving it and, recursively, the ground premises. Requires
    /// `track_provenance` set before the solve. `max_depth` bounds the
    /// expanded rule levels; null and values above
    /// `explain.MAX_PROOF_DEPTH` fall back to that cap.
    pub fn explain(
        self: *Database,
        writer: *std.Io.Writer,
        pred_name: []const u8,
        row: []const Value,
        max_depth: ?usize,
    ) ExplainError!void {
        if (self.evaluator == null) try self.solve();
        const evaluator = &self.evaluator.?;
        if (!evaluator.track_provenance) return error.ProvenanceNotTracked;

        const name_atom = self.interner.find(pred_name) orelse return error.UnknownPredicate;
        const pred = self.program.findPredicate(name_atom) orelse return error.UnknownPredicate;
        const info = self.program.preds.items[pred];
        if (row.len != info.arity) return error.ArityMismatch;

        var tuple = dyntuple.zero_tuple;
        for (row, 0..) |value, i| {
            const atom = switch (value) {
                .int => |v| try interner_mod.encodeInt(v),
                .str => |s| self.interner.find(s) orelse return error.TupleNotFound,
            };
            dyntuple.set(&tuple, i, atom);
        }

        const relation = evaluator.relationOf(pred) orelse return error.TupleNotFound;
        const range = dyntuple.gallopPrefix(relation.elements, &tuple, info.arity);
        if (range.len == 0 or dyntuple.cmpPrefix(&range[0], &tuple, info.arity) != .eq) {
            return error.TupleNotFound;
        }

        try explain_mod.writeProof(writer, &self.program, &self.interner, evaluator, pred, tuple, max_depth);
    }

    /// Details for the most recent parse, analysis, or solve error.
    pub fn lastDiagnostic(self: *const Database) ?Diagnostic {
        if (self.diagnostic.message.len == 0) return null;
        return self.diagnostic;
    }
};

/// One query result row. Columns decode back into integers or strings.
pub const Row = struct {
    tuple: *const DynTuple,
    arity: u16,
    interner: *const interner_mod.Interner,

    /// Number of columns.
    pub fn width(self: Row) usize {
        return self.arity;
    }

    /// Decodes one column.
    pub fn get(self: Row, col: usize) Value {
        std.debug.assert(col < self.arity);
        return self.interner.resolve(dyntuple.get(self.tuple, col));
    }

    /// Formats as `(1, "a")`; use with `{f}`.
    pub fn format(self: Row, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.writeAll("(");
        var col: usize = 0;
        while (col < self.arity) : (col += 1) {
            if (col > 0) try writer.writeAll(", ");
            try interner_mod.writeValueLiteral(writer, self.get(col));
        }
        try writer.writeAll(")");
    }
};

/// Iterates the rows matching a query pattern.
pub const RowIterator = struct {
    elements: []const DynTuple,
    pattern: [dyntuple.MAX_ARITY]?u64,
    prefix: DynTuple,
    prefix_len: usize,
    arity: u16,
    interner: *const interner_mod.Interner,
    index: usize = 0,

    fn empty(interner: *const interner_mod.Interner, arity: u16) RowIterator {
        return RowIterator{
            .elements = &.{},
            .pattern = @splat(null),
            .prefix = dyntuple.zero_tuple,
            .prefix_len = 0,
            .arity = arity,
            .interner = interner,
        };
    }

    /// Returns the next matching row, or null when exhausted.
    pub fn next(self: *RowIterator) ?Row {
        outer: while (self.index < self.elements.len) {
            const tuple = &self.elements[self.index];

            // The candidate range is sorted; once the bound prefix stops
            // matching, nothing further can match.
            if (self.prefix_len > 0 and
                dyntuple.cmpPrefix(tuple, &self.prefix, self.prefix_len) != .eq)
            {
                self.index = self.elements.len;
                return null;
            }

            self.index += 1;
            var col: usize = self.prefix_len;
            while (col < self.arity) : (col += 1) {
                if (self.pattern[col]) |bound| {
                    if (dyntuple.get(tuple, col) != bound) continue :outer;
                }
            }
            return Row{ .tuple = tuple, .arity = self.arity, .interner = self.interner };
        }
        return null;
    }

    /// Iterators borrow the database and own nothing; provided for
    /// call-site symmetry.
    pub fn deinit(self: *RowIterator) void {
        _ = self;
    }
};

// --- Tests -----------------------------------------------------------------

test "Database: parse, solve, and query" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2).
        \\edge(2, 3).
        \\edge(3, 4).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );
    try db.solve();

    // All pairs.
    var all = try db.query("path", &.{ null, null });
    defer all.deinit();
    var count: usize = 0;
    while (all.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, 6), count);

    // Bound first column uses the sorted fast path.
    var from_one = try db.query("path", &.{ Value{ .int = 1 }, null });
    defer from_one.deinit();
    var targets: [3]u64 = undefined;
    var n: usize = 0;
    while (from_one.next()) |row| : (n += 1) {
        targets[n] = row.get(1).int;
    }
    try std.testing.expectEqualSlices(u64, &.{ 2, 3, 4 }, targets[0..n]);

    // Fully bound.
    var exact = try db.query("path", &.{ Value{ .int = 2 }, Value{ .int = 4 } });
    defer exact.deinit();
    try std.testing.expect(exact.next() != null);
    try std.testing.expect(exact.next() == null);
}

test "Database: strings, negation, and aggregates end to end" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\node("a"). node("b"). node("c").
        \\blocked("b").
        \\safe(X) :- node(X), not blocked(X).
        \\edge("a", "b"). edge("a", "c"). edge("b", "c").
        \\deg(N, count(M)) :- edge(N, M).
    );
    try db.solve();

    var safe = try db.query("safe", &.{null});
    defer safe.deinit();
    var names: [2][]const u8 = undefined;
    var n: usize = 0;
    while (safe.next()) |row| : (n += 1) {
        names[n] = row.get(0).str;
    }
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqualStrings("a", names[0]);
    try std.testing.expectEqualStrings("c", names[1]);

    var deg_a = try db.query("deg", &.{ Value{ .str = "a" }, null });
    defer deg_a.deinit();
    try std.testing.expectEqual(@as(u64, 2), deg_a.next().?.get(1).int);
    try std.testing.expect(deg_a.next() == null);
}

test "Database: comparisons end to end" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();
    db.track_provenance = true;

    try db.run(
        \\person(1, 17). person(2, 30). person(3, 18).
        \\adult(X) :- person(X, Age), Age >= 18.
    );
    try db.solve();

    var it = try db.query("adult", &.{null});
    defer it.deinit();
    try std.testing.expectEqual(@as(u64, 2), it.next().?.get(0).int);
    try std.testing.expectEqual(@as(u64, 3), it.next().?.get(0).int);
    try std.testing.expect(it.next() == null);

    // The proof tree shows the grounded comparison.
    var buffer: [256]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try db.explain(&writer, "adult", &.{.{ .int = 2 }}, null);
    try std.testing.expectEqualStrings(
        \\adult(2)
        \\  via rule 0: adult(X) :- person(X, Age), Age >= 18.
        \\  person(2, 30) (fact)
        \\  30 >= 18 (holds)
        \\
    , writer.buffered());

    // Unsafe comparison variables are rejected at solve time.
    try db.run("bad(X) :- person(X, _), X < Unbound.");
    try std.testing.expectError(error.UnsafeComparisonVariable, db.solve());
    try std.testing.expect(db.lastDiagnostic() != null);
}

test "Database: addFact and builder interoperate with run" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.addFact("edge", &.{ .{ .int = 1 }, .{ .int = 2 } });
    try db.addFact("edge", &.{ .{ .int = 2 }, .{ .int = 3 } });
    try db.run("path(X, Y) :- edge(X, Y). path(X, Z) :- path(X, Y), edge(Y, Z).");

    // query() solves on demand.
    var it = try db.query("path", &.{ null, null });
    defer it.deinit();
    var count: usize = 0;
    while (it.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "Database: re-solve after adding facts" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run("path(X, Y) :- edge(X, Y). edge(1, 2).");
    try db.solve();

    try db.addFact("edge", &.{ .{ .int = 2 }, .{ .int = 3 } });
    try db.solve();

    var it = try db.query("path", &.{ null, null });
    defer it.deinit();
    var count: usize = 0;
    while (it.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "Database: diagnostics and error surface" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try std.testing.expectError(error.NonGroundFact, db.run("edge(1, X)."));
    try std.testing.expect(db.lastDiagnostic() != null);

    try db.run("p(X) :- q(X), not p(X). q(1).");
    try std.testing.expectError(error.NegationCycle, db.solve());
    try std.testing.expect(db.lastDiagnostic() != null);
}

test "Database: unknown predicates and unseen strings" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run("edge(\"a\", \"b\"). path(X, Y) :- edge(X, Y).");
    try db.solve();

    try std.testing.expectError(error.UnknownPredicate, db.query("nope", &.{null}));

    // A string the program never mentions matches nothing.
    var it = try db.query("path", &.{ Value{ .str = "zzz" }, null });
    defer it.deinit();
    try std.testing.expect(it.next() == null);
}

test "Database: row formatting" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run("pair(1, \"a\").");
    var it = try db.query("pair", &.{ null, null });
    defer it.deinit();

    var buffer: [64]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try writer.print("{f}", .{it.next().?});
    try std.testing.expectEqualStrings("(1, \"a\")", writer.buffered());
}

test "Database: row formatting escapes strings" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    const quoted = [_]u8{ 0x61, 0x22, 0x62 };
    const line = [_]u8{ 0x6c, 0x69, 0x6e, 0x65, 0x0a };
    const path = [_]u8{ 0x70, 0x61, 0x74, 0x68, 0x5c, 0x72, 0x6f, 0x6f, 0x74 };
    try db.addFact("msg", &.{ .{ .str = &quoted }, .{ .str = &line }, .{ .str = &path } });

    var it = try db.query("msg", &.{ null, null, null });
    defer it.deinit();

    var buffer: [96]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try writer.print("{f}", .{it.next().?});

    const expected = [_]u8{
        0x28, 0x22, 0x61, 0x5c, 0x22, 0x62, 0x22, 0x2c,
        0x20, 0x22, 0x6c, 0x69, 0x6e, 0x65, 0x5c, 0x6e,
        0x22, 0x2c, 0x20, 0x22, 0x70, 0x61, 0x74, 0x68,
        0x5c, 0x5c, 0x72, 0x6f, 0x6f, 0x74, 0x22, 0x29,
    };
    try std.testing.expectEqualSlices(u8, &expected, writer.buffered());
}

test "Database: explainPlan writes every rule's plan" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );

    var buffer: [512]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try db.explainPlan(&writer);

    try std.testing.expectEqualStrings(
        \\rule 0 (stratum 0): path(X, Y) :- edge(X, Y).
        \\  scan edge -> (X, Y)
        \\  head path(X, Y)
        \\
        \\rule 1 (stratum 0): path(X, Z) :- path(X, Y), edge(Y, Z).
        \\  scan path -> (X, Y)
        \\  join edge on (Y) -> (Y, X, Z)
        \\  head path(X, Z)
        \\
    , writer.buffered());
}

test "Database: explain writes a proof tree" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();
    db.track_provenance = true;

    try db.run(
        \\edge(1, 2).
        \\edge(2, 3).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );

    var buffer: [512]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    // Solves on demand, like query.
    try db.explain(&writer, "path", &.{ .{ .int = 1 }, .{ .int = 3 } }, null);

    try std.testing.expectEqualStrings(
        \\path(1, 3)
        \\  via rule 1: path(X, Z) :- path(X, Y), edge(Y, Z).
        \\  path(1, 2)
        \\    via rule 0: path(X, Y) :- edge(X, Y).
        \\    edge(1, 2) (fact)
        \\  edge(2, 3) (fact)
        \\
    , writer.buffered());

    // max_depth stops expansion.
    writer = std.Io.Writer.fixed(&buffer);
    try db.explain(&writer, "path", &.{ .{ .int = 1 }, .{ .int = 3 } }, 0);
    try std.testing.expectEqualStrings("path(1, 3) (depth limit)\n", writer.buffered());
}

test "Database: explain covers negation and aggregates" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();
    db.track_provenance = true;

    try db.run(
        \\node("a"). node("b").
        \\blocked("b").
        \\safe(X) :- node(X), not blocked(X).
        \\edge(1, 2). edge(1, 3).
        \\deg(N, count(M)) :- edge(N, M).
    );
    try db.solve();

    var buffer: [512]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try db.explain(&writer, "safe", &.{.{ .str = "a" }}, null);
    try std.testing.expectEqualStrings(
        \\safe("a")
        \\  via rule 0: safe(X) :- node(X), not blocked(X).
        \\  node("a") (fact)
        \\  not blocked("a") (absent)
        \\
    , writer.buffered());

    writer = std.Io.Writer.fixed(&buffer);
    try db.explain(&writer, "deg", &.{ .{ .int = 1 }, .{ .int = 2 } }, null);
    try std.testing.expectEqualStrings(
        \\deg(1, 2)
        \\  via rule 1 (aggregate): deg(N, count(M)) :- edge(N, M).
        \\
    , writer.buffered());
}

test "Database: explain error surface" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run("edge(1, 2). path(X, Y) :- edge(X, Y).");
    try db.solve();

    var buffer: [64]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);

    // Solved without tracking.
    try std.testing.expectError(
        error.ProvenanceNotTracked,
        db.explain(&writer, "path", &.{ .{ .int = 1 }, .{ .int = 2 } }, null),
    );

    db.track_provenance = true;
    try db.solve();
    try std.testing.expectError(
        error.TupleNotFound,
        db.explain(&writer, "path", &.{ .{ .int = 9 }, .{ .int = 9 } }, null),
    );
    try std.testing.expectError(
        error.TupleNotFound,
        db.explain(&writer, "path", &.{ .{ .str = "zzz" }, .{ .int = 2 } }, null),
    );
    try std.testing.expectError(
        error.UnknownPredicate,
        db.explain(&writer, "nope", &.{.{ .int = 1 }}, null),
    );
    try std.testing.expectError(
        error.ArityMismatch,
        db.explain(&writer, "path", &.{.{ .int = 1 }}, null),
    );
}

test "Database: max iterations setting" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    try db.run(
        \\edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5). edge(5, 6).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );
    db.max_iterations = 2;
    try std.testing.expectError(error.MaxIterationsExceeded, db.solve());

    db.max_iterations = null;
    try db.solve();
}

test "Database: queryDemand computes only the demanded slice" {
    const allocator = std.testing.allocator;

    var db = Database.init(allocator);
    defer db.deinit();

    // Two disconnected chains; the full closure holds 20 path tuples.
    try db.run(
        \\edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).
        \\edge(6, 7). edge(7, 8). edge(8, 9). edge(9, 10).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
    );

    var it = try db.queryDemand("path", &.{ Value{ .int = 9 }, null });
    defer it.deinit();
    try std.testing.expect(it.next() != null);
    try std.testing.expect(it.next() == null);

    // Demand for path(9, _) reaches one path tuple and two magic bindings;
    // everything else stays uncomputed.
    var derived_tuples: usize = 0;
    for (db.demand_program.?.preds.items, 0..) |info, pred| {
        if (!info.derived) continue;
        if (db.demand_evaluator.?.relationOf(@intCast(pred))) |relation| {
            derived_tuples += relation.len();
        }
    }
    try std.testing.expect(derived_tuples < 5);
}

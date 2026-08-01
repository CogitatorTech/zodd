//! # Stratified Evaluator
//!
//! The module executes an analyzed Datalog program: strata run in order,
//! each to a semi-naive fixed point driven by the engine's `Variable` and
//! `Iteration`, and completed strata freeze into immutable relations that
//! later strata read for joins, negation, and aggregation.
//!
//! Per round, each recursive rule is evaluated once per same-stratum body
//! atom, with that atom reading the delta (`recent`) and the others their
//! full contents. Cross-position duplicates are removed by the engine's own
//! dedup in `Variable.changed`. Non-recursive rules (including all aggregate
//! rules) read only frozen relations and run once per stratum.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Relation = @import("../relation.zig").Relation;
const Variable = @import("../variable.zig").Variable;
const Iteration = @import("../iteration.zig").Iteration;
const IterateError = @import("../iteration.zig").IterateError;
const ast = @import("ast.zig");
const dyntuple = @import("dyntuple.zig");
const interner_mod = @import("interner.zig");
const plan_mod = @import("plan.zig");
const join_runtime = @import("join_runtime.zig");
const DynTuple = dyntuple.DynTuple;

/// Errors produced by evaluation.
pub const EvalError = plan_mod.PlanError || IterateError;

const DynRelation = Relation(DynTuple);
const DynVariable = Variable(DynTuple);

/// How a derived tuple was first obtained. Recorded during `solve` when
/// provenance tracking is enabled.
pub const Derivation = union(enum) {
    /// Seeded directly as a fact.
    fact,
    /// Derived by a plain rule. `binding` holds the value of every rule
    /// variable, indexed by `VarId`; substituting it into the rule body
    /// reconstructs the ground premises.
    rule: struct { rule: u32, binding: DynTuple },
    /// Derived by an aggregate rule; the fold has no single premise binding.
    aggregate: u32,
};

/// Provenance map key: a tuple of a specific predicate.
const ProvKey = struct { pred: ast.PredId, tuple: DynTuple };

pub const Evaluator = struct {
    allocator: Allocator,
    program: *const ast.Program,
    /// Frozen relation per predicate: EDB facts up front, derived predicates
    /// as their stratum completes.
    results: std.AutoHashMapUnmanaged(ast.PredId, DynRelation),
    /// When true, `solve` records a `Derivation` per derived tuple.
    track_provenance: bool = false,
    /// Worker threads for rule evaluation within a round: 1 evaluates
    /// sequentially, 0 uses one thread per CPU. Values above 1 require a
    /// thread-safe allocator. Ignored (sequential) on single-threaded
    /// targets and while provenance is tracked.
    parallelism: usize = 1,
    /// First recorded derivation per derived tuple. First-wins keeps proofs
    /// well founded: a derivation only cites tuples from earlier rounds.
    provenance: std.AutoHashMapUnmanaged(ProvKey, Derivation) = .empty,

    /// Initializes an evaluator for an analyzed program. The program must
    /// have passed `analyze` (strata assigned, wildcards lowered).
    pub fn init(allocator: Allocator, program: *const ast.Program) Evaluator {
        return Evaluator{
            .allocator = allocator,
            .program = program,
            .results = .empty,
        };
    }

    /// Deinitializes the evaluator and frees all frozen relations.
    pub fn deinit(self: *Evaluator) void {
        var it = self.results.valueIterator();
        while (it.next()) |relation| {
            relation.deinit();
        }
        self.results.deinit(self.allocator);
        self.provenance.deinit(self.allocator);
    }

    /// Returns the computed relation for a predicate, if any.
    pub fn relationOf(self: *const Evaluator, pred: ast.PredId) ?*const DynRelation {
        return self.results.getPtr(pred);
    }

    /// Returns the recorded derivation of a derived tuple, if any.
    pub fn derivationOf(self: *const Evaluator, pred: ast.PredId, tuple: DynTuple) ?Derivation {
        return self.provenance.get(.{ .pred = pred, .tuple = tuple });
    }

    /// Records the first derivation seen for a tuple; later derivations of
    /// the same tuple are ignored.
    fn record(self: *Evaluator, pred: ast.PredId, tuple: DynTuple, derivation: Derivation) Allocator.Error!void {
        const entry = try self.provenance.getOrPut(self.allocator, .{ .pred = pred, .tuple = tuple });
        if (!entry.found_existing) entry.value_ptr.* = derivation;
    }

    /// Runs the program to a fixed point, stratum by stratum.
    /// `max_iterations` bounds the rounds within each stratum and surfaces
    /// as `error.MaxIterationsExceeded`, matching `Iteration`.
    pub fn solve(self: *Evaluator, stratum_count: u16, max_iterations: ?usize) EvalError!void {
        self.clearResults();

        // Scratch arenas: `solve_arena` lives for the whole call (plans,
        // fact grouping); `round_arena` is reset after every round
        // (projections, join buffers, merged full relations).
        var solve_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer solve_arena.deinit();
        var round_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer round_arena.deinit();

        const seeds = try self.loadFacts(solve_arena.allocator());

        const plans = try solve_arena.allocator().alloc(plan_mod.Plan, self.program.rules.items.len);
        for (self.program.rules.items, 0..) |*rule, i| {
            plans[i] = try plan_mod.compile(solve_arena.allocator(), rule);
        }

        var stratum: u16 = 0;
        while (stratum < stratum_count) : (stratum += 1) {
            try self.solveStratum(
                stratum,
                seeds,
                plans,
                max_iterations,
                solve_arena.allocator(),
                &round_arena,
            );
        }
    }

    fn clearResults(self: *Evaluator) void {
        var it = self.results.valueIterator();
        while (it.next()) |relation| {
            relation.deinit();
        }
        self.results.clearRetainingCapacity();
        self.provenance.clearRetainingCapacity();
    }

    /// Groups fact rows by predicate. Non-derived predicates freeze into
    /// `results` immediately; derived predicates keep their rows as seeds
    /// for their stratum's fixed point.
    fn loadFacts(self: *Evaluator, arena: Allocator) EvalError![]std.ArrayListUnmanaged(DynTuple) {
        const rows = try self.groupFactRows(arena);

        for (self.program.preds.items, 0..) |info, pred| {
            if (info.derived) continue;
            var relation = try DynRelation.fromSlice(self.allocator, rows[pred].items);
            errdefer relation.deinit();
            try self.results.put(self.allocator, @intCast(pred), relation);
        }

        // Facts seeding derived predicates need explicit provenance; rules
        // may rederive them, and the fact must win.
        if (self.track_provenance) {
            for (self.program.preds.items, 0..) |info, pred| {
                if (!info.derived) continue;
                for (rows[pred].items) |tuple| {
                    try self.record(@intCast(pred), tuple, .fact);
                }
            }
        }

        return rows;
    }

    /// Incrementally maintains a previous `solve` after base-fact changes.
    /// Additions propagate as semi-naive deltas: every stratum resumes from
    /// its frozen result, so unchanged tuples are never re-derived. A
    /// stratum whose inputs saw deletions, or whose rules read changed
    /// predicates through negation or aggregation, is recomputed from
    /// scratch instead; strata unaffected by any change are not touched at
    /// all. Results equal a full re-solve. Not compatible with provenance
    /// tracking (the caller falls back to `solve`).
    pub fn maintain(
        self: *Evaluator,
        stratum_count: u16,
        max_iterations: ?usize,
        added: []const ast.Fact,
        deleted: []const ast.Fact,
    ) EvalError!void {
        var solve_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer solve_arena.deinit();
        var round_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer round_arena.deinit();
        const arena = solve_arena.allocator();

        const pred_count = self.program.preds.items.len;
        const plans = try arena.alloc(plan_mod.Plan, self.program.rules.items.len);
        for (self.program.rules.items, 0..) |*rule, i| {
            plans[i] = try plan_mod.compile(arena, rule);
        }

        // Per-predicate changes: added tuples (read at delta positions of
        // downstream rules) and a deletion flag (forces recompute).
        var add_rows = try arena.alloc(std.ArrayListUnmanaged(DynTuple), pred_count);
        @memset(add_rows, .empty);
        for (added) |fact| try add_rows[fact.pred].append(arena, dyntuple.fromSlice(fact.row));
        var del_rows = try arena.alloc(std.ArrayListUnmanaged(DynTuple), pred_count);
        @memset(del_rows, .empty);
        for (deleted) |fact| try del_rows[fact.pred].append(arena, dyntuple.fromSlice(fact.row));

        const deltas = try arena.alloc([]const DynTuple, pred_count);
        @memset(deltas, &.{});
        var has_del = try std.DynamicBitSetUnmanaged.initEmpty(arena, pred_count);

        const seed_rows = try self.groupFactRows(arena);

        // Rebuild changed base relations from the fact list, which stays
        // the source of truth: retracting one of two identical facts must
        // leave the tuple present, so set arithmetic on the old relation
        // would be wrong.
        for (self.program.preds.items, 0..) |info, pred_usize| {
            const pred: ast.PredId = @intCast(pred_usize);
            if (info.derived) continue;
            if (add_rows[pred].items.len == 0 and del_rows[pred].items.len == 0) continue;

            var old = if (self.results.fetchRemove(pred)) |entry| entry.value else DynRelation.empty(self.allocator);
            var rebuilt = try DynRelation.fromSlice(self.allocator, seed_rows[pred].items);
            errdefer rebuilt.deinit();

            const diff = try diffRelations(arena, old.elements, rebuilt.elements);
            deltas[pred] = diff.added;
            if (diff.removed) has_del.set(pred);
            old.deinit();
            try self.results.put(self.allocator, pred, rebuilt);
        }

        var stratum: u16 = 0;
        while (stratum < stratum_count) : (stratum += 1) {
            var touched_add = false;
            var touched_del = false;
            var unsound_read = false;
            for (self.program.rules.items) |rule| {
                if (self.program.preds.items[rule.head.pred()].stratum != stratum) continue;
                const head_pred = rule.head.pred();
                if (add_rows[head_pred].items.len > 0) touched_add = true;
                if (del_rows[head_pred].items.len > 0) touched_del = true;
                for (rule.body) |literal| {
                    const changed_input = deltas[literal.atom.pred].len > 0;
                    if (changed_input) {
                        touched_add = true;
                        // Additions below a negation or an aggregate can
                        // remove tuples here; the delta path cannot express
                        // that.
                        if (literal.negated or rule.head == .aggregate) unsound_read = true;
                    }
                    if (has_del.isSet(literal.atom.pred)) touched_del = true;
                }
            }
            if (!touched_add and !touched_del) continue;

            if (touched_del or unsound_read) {
                try self.recomputeStratum(stratum, seed_rows, plans, max_iterations, arena, &round_arena, deltas, &has_del);
            } else {
                try self.extendStratum(stratum, plans, max_iterations, arena, &round_arena, add_rows, deltas);
            }
        }
    }

    /// Recomputes one stratum from scratch and records how its predicates
    /// changed for downstream strata.
    fn recomputeStratum(
        self: *Evaluator,
        stratum: u16,
        seed_rows: []std.ArrayListUnmanaged(DynTuple),
        plans: []const plan_mod.Plan,
        max_iterations: ?usize,
        arena: Allocator,
        round_arena: *std.heap.ArenaAllocator,
        deltas: [][]const DynTuple,
        has_del: *std.DynamicBitSetUnmanaged,
    ) EvalError!void {
        // Capture and drop the old relations of this stratum's predicates.
        var old = std.AutoHashMapUnmanaged(ast.PredId, DynRelation).empty;
        defer {
            var it = old.valueIterator();
            while (it.next()) |relation| relation.deinit();
            old.deinit(self.allocator);
        }
        for (self.program.preds.items, 0..) |info, pred| {
            if (!info.derived or info.stratum != stratum) continue;
            if (self.results.fetchRemove(@intCast(pred))) |entry| {
                try old.put(self.allocator, @intCast(pred), entry.value);
            }
        }

        try self.solveStratum(stratum, seed_rows, plans, max_iterations, arena, round_arena);

        for (self.program.preds.items, 0..) |info, pred_usize| {
            const pred: ast.PredId = @intCast(pred_usize);
            if (!info.derived or info.stratum != stratum) continue;
            const old_elements: []const DynTuple = if (old.getPtr(pred)) |relation| relation.elements else &.{};
            const new_elements: []const DynTuple = if (self.results.getPtr(pred)) |relation| relation.elements else &.{};
            const diff = try diffRelations(arena, old_elements, new_elements);
            deltas[pred] = diff.added;
            if (diff.removed) has_del.set(pred);
        }
    }

    /// Extends one stratum with pure additions: the frozen result seeds the
    /// stable set, delta rules fire once per changed frozen input, and the
    /// usual semi-naive loop propagates recursion. Only genuinely new
    /// tuples are derived and recorded for downstream strata.
    fn extendStratum(
        self: *Evaluator,
        stratum: u16,
        plans: []const plan_mod.Plan,
        max_iterations: ?usize,
        arena: Allocator,
        round_arena: *std.heap.ArenaAllocator,
        add_rows: []std.ArrayListUnmanaged(DynTuple),
        deltas: [][]const DynTuple,
    ) EvalError!void {
        var iter = Iteration(DynTuple).init(self.allocator, max_iterations);
        defer iter.deinit();

        var vars: std.AutoHashMapUnmanaged(ast.PredId, *DynVariable) = .empty;
        defer vars.deinit(self.allocator);

        // Old element copies for the post-freeze diff; the originals move
        // into the variables.
        var old_elements: std.AutoHashMapUnmanaged(ast.PredId, []const DynTuple) = .empty;
        defer old_elements.deinit(self.allocator);

        for (self.program.preds.items, 0..) |info, pred_usize| {
            const pred: ast.PredId = @intCast(pred_usize);
            if (!info.derived or info.stratum != stratum) continue;
            const variable = try iter.variable();
            if (self.results.fetchRemove(pred)) |entry| {
                var relation = entry.value;
                try old_elements.put(self.allocator, pred, try arena.dupe(DynTuple, relation.elements));
                variable.seedStable(relation) catch |err| {
                    relation.deinit();
                    return err;
                };
            } else {
                try old_elements.put(self.allocator, pred, &.{});
            }
            try variable.insertSlice(add_rows[pred].items);
            try vars.put(self.allocator, pred, variable);
        }
        if (vars.count() == 0) return;

        // Delta tasks against changed frozen inputs (lower strata and base
        // facts), then the recursive tasks for the fixed-point loop.
        var seed_tasks: std.ArrayListUnmanaged(Task) = .empty;
        var loop_tasks: std.ArrayListUnmanaged(Task) = .empty;
        for (self.program.rules.items, 0..) |rule, i| {
            if (self.program.preds.items[rule.head.pred()].stratum != stratum) continue;
            for (rule.body, 0..) |literal, lit_index| {
                if (self.isDeltaPosition(literal, stratum)) {
                    try loop_tasks.append(arena, .{ .rule_index = @intCast(i), .delta_lit = @intCast(lit_index) });
                } else if (!literal.negated and deltas[literal.atom.pred].len > 0) {
                    try seed_tasks.append(arena, .{ .rule_index = @intCast(i), .delta_lit = @intCast(lit_index) });
                }
            }
        }

        var ctx = Context{
            .evaluator = self,
            .vars = &vars,
            .stratum = stratum,
            .arena = round_arena.allocator(),
            .full_cache = .empty,
            .deltas = deltas,
        };

        try self.evalTasks(&ctx, seed_tasks.items, plans);
        _ = round_arena.reset(.retain_capacity);

        while (try iter.changed()) {
            ctx.full_cache = .empty;
            try self.evalTasks(&ctx, loop_tasks.items, plans);
            _ = round_arena.reset(.retain_capacity);
        }

        var var_it = vars.iterator();
        while (var_it.next()) |entry| {
            var relation = try entry.value_ptr.*.complete();
            errdefer relation.deinit();
            try self.results.put(self.allocator, entry.key_ptr.*, relation);
            const diff = try diffRelations(arena, old_elements.get(entry.key_ptr.*).?, relation.elements);
            deltas[entry.key_ptr.*] = diff.added;
        }
    }

    const Diff = struct { added: []const DynTuple, removed: bool };

    /// Sorted-set walk producing the tuples of `new` missing from `old` and
    /// whether `old` holds tuples missing from `new`.
    fn diffRelations(arena: Allocator, old: []const DynTuple, new: []const DynTuple) Allocator.Error!Diff {
        var added: std.ArrayListUnmanaged(DynTuple) = .empty;
        var removed = false;
        var i: usize = 0;
        var j: usize = 0;
        while (i < old.len and j < new.len) {
            switch (DynRelation.compareTuples(old[i], new[j])) {
                .lt => {
                    removed = true;
                    i += 1;
                },
                .gt => {
                    try added.append(arena, new[j]);
                    j += 1;
                },
                .eq => {
                    i += 1;
                    j += 1;
                },
            }
        }
        if (i < old.len) removed = true;
        try added.appendSlice(arena, new[j..]);
        return .{ .added = added.items, .removed = removed };
    }

    /// Groups the program's fact rows by predicate.
    fn groupFactRows(self: *Evaluator, arena: Allocator) EvalError![]std.ArrayListUnmanaged(DynTuple) {
        const rows = try arena.alloc(std.ArrayListUnmanaged(DynTuple), self.program.preds.items.len);
        for (rows) |*list| list.* = .empty;
        for (self.program.facts.items) |fact| {
            try rows[fact.pred].append(arena, dyntuple.fromSlice(fact.row));
        }
        return rows;
    }

    fn solveStratum(
        self: *Evaluator,
        stratum: u16,
        seeds: []std.ArrayListUnmanaged(DynTuple),
        plans: []const plan_mod.Plan,
        max_iterations: ?usize,
        solve_arena: Allocator,
        round_arena: *std.heap.ArenaAllocator,
    ) EvalError!void {
        // Variables for this stratum's derived predicates.
        var iter = Iteration(DynTuple).init(self.allocator, max_iterations);
        defer iter.deinit();

        var vars: std.AutoHashMapUnmanaged(ast.PredId, *DynVariable) = .empty;
        defer vars.deinit(self.allocator);

        for (self.program.preds.items, 0..) |info, pred| {
            if (!info.derived or info.stratum != stratum) continue;
            const variable = try iter.variable();
            try variable.insertSlice(seeds[pred].items);
            try vars.put(self.allocator, @intCast(pred), variable);
        }
        if (vars.count() == 0) return;

        // Split this stratum's rule evaluations into tasks: non-recursive
        // rules read only frozen relations and run once; recursive rules
        // run per round, once per same-stratum body atom holding the delta.
        var once_tasks: std.ArrayListUnmanaged(Task) = .empty;
        var delta_tasks: std.ArrayListUnmanaged(Task) = .empty;
        for (self.program.rules.items, 0..) |rule, i| {
            if (self.program.preds.items[rule.head.pred()].stratum != stratum) continue;
            if (self.countDeltaPositions(&rule, stratum) == 0) {
                try once_tasks.append(solve_arena, .{ .rule_index = @intCast(i), .delta_lit = null });
            } else {
                for (rule.body, 0..) |literal, lit_index| {
                    if (!self.isDeltaPosition(literal, stratum)) continue;
                    try delta_tasks.append(solve_arena, .{
                        .rule_index = @intCast(i),
                        .delta_lit = @intCast(lit_index),
                    });
                }
            }
        }

        var ctx = Context{
            .evaluator = self,
            .vars = &vars,
            .stratum = stratum,
            .arena = round_arena.allocator(),
            .full_cache = .empty,
        };

        try self.evalTasks(&ctx, once_tasks.items, plans);
        _ = round_arena.reset(.retain_capacity);

        while (try iter.changed()) {
            ctx.full_cache = .empty;
            try self.evalTasks(&ctx, delta_tasks.items, plans);
            _ = round_arena.reset(.retain_capacity);
        }

        // Freeze the stratum.
        var var_it = vars.iterator();
        while (var_it.next()) |entry| {
            var relation = try entry.value_ptr.*.complete();
            errdefer relation.deinit();
            try self.results.put(self.allocator, entry.key_ptr.*, relation);
        }
    }

    /// One rule evaluation within a round.
    const Task = struct { rule_index: u32, delta_lit: ?u16 };

    /// Threading is unavailable on single-threaded builds and freestanding
    /// targets like the Wasm module.
    const can_thread = !builtin.single_threaded and builtin.target.os.tag != .freestanding;

    /// Evaluates one round's tasks, in parallel when `parallelism` allows.
    /// Parallel workers only append to head variables (serialized by a
    /// mutex), so the derived tuple sets are identical to sequential
    /// evaluation; provenance stays sequential because first-wins recording
    /// depends on evaluation order.
    fn evalTasks(
        self: *Evaluator,
        ctx: *Context,
        tasks: []const Task,
        plans: []const plan_mod.Plan,
    ) EvalError!void {
        if (comptime can_thread) {
            const threads = if (self.parallelism == 0)
                (std.Thread.getCpuCount() catch 1)
            else
                self.parallelism;
            if (threads > 1 and tasks.len > 1 and !self.track_provenance) {
                return self.evalTasksParallel(ctx, tasks, plans, @min(threads, tasks.len));
            }
        }

        for (tasks) |task| {
            try self.evalRule(ctx, task.rule_index, &plans[task.rule_index], task.delta_lit);
        }
    }

    fn evalTasksParallel(
        self: *Evaluator,
        ctx: *Context,
        tasks: []const Task,
        plans: []const plan_mod.Plan,
        threads: usize,
    ) EvalError!void {
        // Merge every same-stratum variable up front so workers never write
        // the shared cache.
        var var_it = ctx.vars.iterator();
        while (var_it.next()) |entry| {
            _ = try ctx.full(entry.key_ptr.*, entry.value_ptr.*);
        }

        // Per-task output lists; their buffers live in the worker arenas,
        // which stay alive until the coordinator has inserted everything.
        const outputs = try ctx.arena.alloc(std.ArrayListUnmanaged(DynTuple), tasks.len);
        @memset(outputs, .empty);
        const worker_arenas = try ctx.arena.alloc(std.heap.ArenaAllocator, threads);
        for (worker_arenas) |*arena| arena.* = std.heap.ArenaAllocator.init(self.allocator);
        defer for (worker_arenas) |*arena| arena.deinit();

        const handles = try ctx.arena.alloc(std.Thread, threads);
        const failures = try ctx.arena.alloc(?EvalError, threads);
        @memset(failures, null);

        var spawned: usize = 0;
        for (0..threads) |i| {
            handles[i] = std.Thread.spawn(.{}, evalWorker, .{
                self, ctx, tasks, plans, outputs, &worker_arenas[i], i, threads, &failures[i],
            }) catch break;
            spawned += 1;
        }
        // Strides of threads that failed to spawn run on this one.
        for (spawned..threads) |i| {
            evalWorker(self, ctx, tasks, plans, outputs, &worker_arenas[i], i, threads, &failures[i]);
        }
        for (handles[0..spawned]) |handle| handle.join();

        for (failures) |failure| {
            if (failure) |err| return err;
        }

        // Insert in task order, on this thread, so evaluation matches the
        // sequential path exactly.
        for (tasks, outputs) |task, output| {
            if (output.items.len == 0) continue;
            const rule = &self.program.rules.items[task.rule_index];
            const head_var = ctx.vars.get(rule.head.pred()).?;
            try head_var.insertSlice(output.items);
        }
    }

    fn evalWorker(
        self: *Evaluator,
        shared: *const Context,
        tasks: []const Task,
        plans: []const plan_mod.Plan,
        outputs: []std.ArrayListUnmanaged(DynTuple),
        arena: *std.heap.ArenaAllocator,
        start: usize,
        stride: usize,
        failure: *?EvalError,
    ) void {
        var ctx = Context{
            .evaluator = self,
            .vars = shared.vars,
            .stratum = shared.stratum,
            .arena = arena.allocator(),
            // Shared and fully precomputed: reads only.
            .full_cache = shared.full_cache,
            .deltas = shared.deltas,
        };

        var i = start;
        while (i < tasks.len) : (i += stride) {
            const task = tasks[i];
            ctx.collect = &outputs[i];
            self.evalRule(&ctx, task.rule_index, &plans[task.rule_index], task.delta_lit) catch |err| {
                failure.* = err;
                return;
            };
        }
    }

    fn isDeltaPosition(self: *const Evaluator, literal: ast.Literal, stratum: u16) bool {
        if (literal.negated) return false;
        const info = self.program.preds.items[literal.atom.pred];
        return info.derived and info.stratum == stratum;
    }

    fn countDeltaPositions(self: *const Evaluator, rule: *const ast.Rule, stratum: u16) usize {
        var count: usize = 0;
        for (rule.body) |literal| {
            if (self.isDeltaPosition(literal, stratum)) count += 1;
        }
        return count;
    }

    const Context = struct {
        evaluator: *Evaluator,
        vars: *const std.AutoHashMapUnmanaged(ast.PredId, *DynVariable),
        stratum: u16,
        arena: Allocator,
        /// Per-round cache of merged stable+recent contents per predicate.
        full_cache: std.AutoHashMapUnmanaged(ast.PredId, []const DynTuple),
        /// When set, derived head tuples are collected here (allocated from
        /// `arena`) instead of inserted into the head variable; parallel
        /// workers use this so all inserts happen on the coordinating
        /// thread, in task order.
        collect: ?*std.ArrayListUnmanaged(DynTuple) = null,

        /// Per-predicate added tuples of frozen inputs (lower strata and
        /// base facts), read at delta positions during incremental
        /// maintenance; null during a full solve.
        deltas: ?[]const []const DynTuple = null,

        /// The tuples a body literal reads this round.
        fn source(ctx: *Context, literal: ast.Literal, delta_lit: ?u16, lit_index: u16) Allocator.Error![]const DynTuple {
            const pred = literal.atom.pred;
            const is_delta = delta_lit != null and delta_lit.? == lit_index;
            if (ctx.vars.get(pred)) |variable| {
                if (is_delta) {
                    return variable.recent.elements;
                }
                return ctx.full(pred, variable);
            }
            if (is_delta) {
                if (ctx.deltas) |deltas| return deltas[pred];
            }
            if (ctx.evaluator.results.getPtr(pred)) |relation| {
                return relation.elements;
            }
            return &.{};
        }

        /// Merged stable batches plus recent of a same-stratum variable.
        fn full(ctx: *Context, pred: ast.PredId, variable: *const DynVariable) Allocator.Error![]const DynTuple {
            if (ctx.full_cache.get(pred)) |slice| return slice;

            var total: usize = variable.recent.len();
            for (variable.stable.items) |batch| total += batch.len();

            const buffer = try ctx.arena.alloc(DynTuple, total);
            var offset: usize = 0;
            for (variable.stable.items) |batch| {
                @memcpy(buffer[offset .. offset + batch.len()], batch.elements);
                offset += batch.len();
            }
            @memcpy(buffer[offset..], variable.recent.elements);

            const relation = try DynRelation.fromSlice(ctx.arena, buffer);
            try ctx.full_cache.put(ctx.arena, pred, relation.elements);
            return relation.elements;
        }
    };

    /// Evaluates one rule with the given literal holding the delta (or all
    /// literals frozen when `delta_lit` is null) and inserts derived head
    /// tuples into the head predicate's variable.
    fn evalRule(
        self: *Evaluator,
        ctx: *Context,
        rule_index: u32,
        plan: *const plan_mod.Plan,
        delta_lit: ?u16,
    ) EvalError!void {
        const rule = &self.program.rules.items[rule_index];
        const arena = ctx.arena;

        // Run the step chain; `current` is the intermediate result.
        var current: []const DynTuple = &.{dyntuple.zero_tuple};
        for (plan.steps) |step| {
            switch (step) {
                .scan => |load| {
                    const src = try ctx.source(rule.body[load.lit], delta_lit, load.lit);
                    const relation = try loadAtom(arena, src, &load);
                    current = relation.elements;
                },
                .join => |join| {
                    const left = try dyntuple.relayout(arena, current, join.i_proj);
                    const src = try ctx.source(rule.body[join.load.lit], delta_lit, join.load.lit);
                    const right = try loadAtom(arena, src, &join.load);
                    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
                    try join_runtime.mergeJoinPrefix(
                        arena,
                        left.elements,
                        right.elements,
                        join.key_len,
                        join.out_spec,
                        &out,
                    );
                    current = out.items;
                },
                .anti => |anti| {
                    const src = try ctx.source(rule.body[anti.load.lit], delta_lit, anti.load.lit);
                    const filter = try loadAtom(arena, src, &anti.load);
                    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
                    try join_runtime.antiFilterPrefix(
                        arena,
                        current,
                        anti.i_key_cols,
                        filter.elements,
                        &out,
                    );
                    current = out.items;
                },
                .cmp => |cmp| {
                    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
                    for (current) |*tuple| {
                        if (satisfies(&cmp, tuple)) try out.append(arena, tuple.*);
                    }
                    current = out.items;
                },
                .assign => |assign| {
                    var out: std.ArrayListUnmanaged(DynTuple) = .empty;
                    for (current) |*tuple| {
                        const value = cmpValue(assign.expr, tuple) orelse continue;
                        var extended = tuple.*;
                        dyntuple.set(&extended, assign.dest, value);
                        try out.append(arena, extended);
                    }
                    current = out.items;
                },
            }
            if (current.len == 0) return;
        }

        // Project the head and insert into the head variable.
        var head_tuples: std.ArrayListUnmanaged(DynTuple) = .empty;
        switch (plan.head) {
            .plain => |head_cols| {
                for (current) |*tuple| {
                    const head_tuple = projectHead(tuple, head_cols);
                    try head_tuples.append(arena, head_tuple);
                    if (self.track_provenance) {
                        // Re-index the intermediate from layout columns to
                        // variable ids; the binding grounds the whole body.
                        var binding = dyntuple.zero_tuple;
                        for (plan.layout, 0..) |var_id, col| {
                            dyntuple.set(&binding, var_id, dyntuple.get(tuple, col));
                        }
                        try self.record(rule.head.pred(), head_tuple, .{
                            .rule = .{ .rule = rule_index, .binding = binding },
                        });
                    }
                }
            },
            .aggregate => |agg| {
                const grouped = try dyntuple.relayout(arena, current, agg.proj);
                var folded: std.ArrayListUnmanaged(DynTuple) = .empty;
                try join_runtime.aggregateDyn(
                    arena,
                    grouped.elements,
                    agg.group_len,
                    agg.val_col,
                    agg.func,
                    &folded,
                );
                for (folded.items) |*tuple| {
                    const head_tuple = projectHead(tuple, agg.head_cols);
                    try head_tuples.append(arena, head_tuple);
                    if (self.track_provenance) {
                        try self.record(rule.head.pred(), head_tuple, .{ .aggregate = rule_index });
                    }
                }
            },
        }

        if (head_tuples.items.len > 0) {
            if (ctx.collect) |out| {
                try out.appendSlice(ctx.arena, head_tuples.items);
            } else {
                const head_var = ctx.vars.get(rule.head.pred()).?;
                try head_var.insertSlice(head_tuples.items);
            }
        }
    }

    /// Evaluates a comparison filter on one intermediate tuple. Equality and
    /// inequality compare raw atoms (interning makes equal values identical);
    /// ordered operators compare integers, and a string operand fails the
    /// comparison.
    fn satisfies(cmp: *const plan_mod.CmpStep, tuple: *const DynTuple) bool {
        const lhs = cmpValue(cmp.lhs, tuple) orelse return false;
        const rhs = cmpValue(cmp.rhs, tuple) orelse return false;
        switch (cmp.op) {
            .eq => return lhs == rhs,
            .ne => return lhs != rhs,
            else => {},
        }
        if (interner_mod.isStr(lhs) or interner_mod.isStr(rhs)) return false;
        return switch (cmp.op) {
            .lt => lhs < rhs,
            .le => lhs <= rhs,
            .gt => lhs > rhs,
            .ge => lhs >= rhs,
            .eq, .ne => unreachable,
        };
    }

    /// Evaluates one comparison side. Null means the value does not exist:
    /// arithmetic on a string operand, overflow, underflow, division by
    /// zero, or a result past the 63-bit atom range. A null side fails the
    /// comparison for that tuple.
    fn cmpValue(arg: plan_mod.CmpArg, tuple: *const DynTuple) ?dyntuple.Atom {
        return switch (arg) {
            .col => |col| dyntuple.get(tuple, col),
            .constant => |constant| constant,
            .binop => |binop| blk: {
                const lhs = cmpValue(binop.lhs, tuple) orelse break :blk null;
                const rhs = cmpValue(binop.rhs, tuple) orelse break :blk null;
                if (interner_mod.isStr(lhs) or interner_mod.isStr(rhs)) break :blk null;
                const result = switch (binop.op) {
                    .add => std.math.add(u64, lhs, rhs) catch break :blk null,
                    .sub => std.math.sub(u64, lhs, rhs) catch break :blk null,
                    .mul => std.math.mul(u64, lhs, rhs) catch break :blk null,
                    .div => if (rhs == 0) break :blk null else lhs / rhs,
                };
                if (result > interner_mod.PAYLOAD_MASK) break :blk null;
                break :blk result;
            },
        };
    }

    fn projectHead(tuple: *const DynTuple, head_cols: []const plan_mod.HeadCol) DynTuple {
        var out = dyntuple.zero_tuple;
        for (head_cols, 0..) |head_col, i| {
            const value = switch (head_col) {
                .col => |col| dyntuple.get(tuple, col),
                .constant => |constant| constant,
            };
            dyntuple.set(&out, i, value);
        }
        return out;
    }

    /// Loads a body atom's tuples: applies constant and repeated-variable
    /// checks, projects per `src_cols`, and returns the sorted, deduplicated
    /// relation.
    fn loadAtom(
        arena: Allocator,
        src: []const DynTuple,
        load: *const plan_mod.AtomLoad,
    ) Allocator.Error!DynRelation {
        var list: std.ArrayListUnmanaged(DynTuple) = .empty;

        outer: for (src) |*tuple| {
            for (load.const_checks) |check| {
                if (dyntuple.get(tuple, check.col) != check.value) continue :outer;
            }
            for (load.eq_checks) |check| {
                if (dyntuple.get(tuple, check.col_a) != dyntuple.get(tuple, check.col_b)) continue :outer;
            }
            var out = dyntuple.zero_tuple;
            for (load.src_cols, 0..) |src_col, i| {
                dyntuple.set(&out, i, dyntuple.get(tuple, src_col));
            }
            try list.append(arena, out);
        }

        return DynRelation.fromSlice(arena, list.items);
    }
};

// --- Tests -----------------------------------------------------------------

const Builder = @import("builder.zig").Builder;
const Interner = @import("interner.zig").Interner;
const analyze_mod = @import("analyze.zig");

const TestSetup = struct {
    program: ast.Program,
    interner: Interner,

    fn init(allocator: Allocator) TestSetup {
        return .{
            .program = ast.Program.init(allocator),
            .interner = Interner.init(allocator),
        };
    }

    fn deinit(self: *TestSetup) void {
        self.program.deinit();
        self.interner.deinit();
    }

    fn builder(self: *TestSetup) Builder {
        return Builder{ .program = &self.program, .interner = &self.interner };
    }

    fn solve(self: *TestSetup, evaluator: *Evaluator, max_iterations: ?usize) !void {
        const analysis = try analyze_mod.analyze(&self.program, null);
        try evaluator.solve(analysis.stratum_count, max_iterations);
    }
};

test "Evaluator: transitive closure" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const edge = try b.predicate("edge", 2);
    const path = try b.predicate("path", 2);

    try b.fact(edge, &.{ 1, 2 });
    try b.fact(edge, &.{ 2, 3 });
    try b.fact(edge, &.{ 3, 4 });

    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        try r.head(&.{ x, y });
        try r.pos(edge, &.{ x, y });
        try r.finish();
    }
    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        const z = try r.v("Z");
        try r.head(&.{ x, z });
        try r.pos(path, &.{ x, y });
        try r.pos(edge, &.{ y, z });
        try r.finish();
    }

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    const result = evaluator.relationOf(path).?;
    // 1->2,3,4; 2->3,4; 3->4.
    try std.testing.expectEqual(@as(usize, 6), result.len());
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&result.elements[0], 0));
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&result.elements[0], 1));
    try std.testing.expectEqual(@as(u64, 3), dyntuple.get(&result.elements[5], 0));
    try std.testing.expectEqual(@as(u64, 4), dyntuple.get(&result.elements[5], 1));
}

test "Evaluator: stratified negation" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const node = try b.predicate("node", 1);
    const blocked = try b.predicate("blocked", 1);
    const safe = try b.predicate("safe", 1);

    try b.fact(node, &.{1});
    try b.fact(node, &.{2});
    try b.fact(node, &.{3});
    try b.fact(blocked, &.{2});

    // safe(X) :- node(X), not blocked(X).
    var r = b.rule(safe);
    const x = try r.v("X");
    try r.head(&.{x});
    try r.pos(node, &.{x});
    try r.neg(blocked, &.{x});
    try r.finish();

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    const result = evaluator.relationOf(safe).?;
    try std.testing.expectEqual(@as(usize, 2), result.len());
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&result.elements[0], 0));
    try std.testing.expectEqual(@as(u64, 3), dyntuple.get(&result.elements[1], 0));
}

test "Evaluator: aggregate over a derived relation" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const edge = try b.predicate("edge", 2);
    const path = try b.predicate("path", 2);
    const reach_count = try b.predicate("reach_count", 2);

    try b.fact(edge, &.{ 1, 2 });
    try b.fact(edge, &.{ 2, 3 });

    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        try r.head(&.{ x, y });
        try r.pos(edge, &.{ x, y });
        try r.finish();
    }
    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        const z = try r.v("Z");
        try r.head(&.{ x, z });
        try r.pos(path, &.{ x, y });
        try r.pos(edge, &.{ y, z });
        try r.finish();
    }
    {
        // reach_count(X, count(Y)) :- path(X, Y).
        var r = b.rule(reach_count);
        const x = try r.v("X");
        const y = try r.v("Y");
        try r.aggHead(&.{x}, 1, .count, y);
        try r.pos(path, &.{ x, y });
        try r.finish();
    }

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    const result = evaluator.relationOf(reach_count).?;
    // path: 1->{2,3}, 2->{3}; counts: (1,2), (2,1).
    try std.testing.expectEqual(@as(usize, 2), result.len());
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&result.elements[0], 0));
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&result.elements[0], 1));
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&result.elements[1], 0));
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&result.elements[1], 1));
}

test "Evaluator: comparison filters" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const person = try b.predicate("person", 2);
    const adult = try b.predicate("adult", 1);
    const pair = try b.predicate("pair", 2);

    try b.fact(person, &.{ 1, 17 });
    try b.fact(person, &.{ 2, 30 });
    try b.fact(person, &.{ 3, 18 });

    {
        // adult(X) :- person(X, Age), Age >= 18.
        var r = b.rule(adult);
        const x = try r.v("X");
        const age = try r.v("Age");
        try r.head(&.{x});
        try r.pos(person, &.{ x, age });
        try r.cmp(age, .ge, try b.int(18));
        try r.finish();
    }
    {
        // pair(X, Y) :- person(X, A), person(Y, B), X != Y, A < B.
        var r = b.rule(pair);
        const x = try r.v("X");
        const a = try r.v("A");
        const y = try r.v("Y");
        const bb = try r.v("B");
        try r.head(&.{ x, y });
        try r.pos(person, &.{ x, a });
        try r.pos(person, &.{ y, bb });
        try r.cmp(x, .ne, y);
        try r.cmp(a, .lt, bb);
        try r.finish();
    }

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    const adults = evaluator.relationOf(adult).?;
    try std.testing.expectEqual(@as(usize, 2), adults.len());
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&adults.elements[0], 0));
    try std.testing.expectEqual(@as(u64, 3), dyntuple.get(&adults.elements[1], 0));

    // Ages 17 < 30, 17 < 18, 18 < 30: pairs (1, 2), (1, 3), (3, 2).
    const pairs = evaluator.relationOf(pair).?;
    try std.testing.expectEqual(@as(usize, 3), pairs.len());
}

test "Evaluator: ordered comparisons fail on string operands" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const item = try b.predicate("item", 1);
    const small = try b.predicate("small", 1);
    const other = try b.predicate("other", 1);

    try b.fact(item, &.{try b.interner.encode(.{ .str = "a" })});
    try b.fact(item, &.{try b.interner.encode(.{ .int = 1 })});

    {
        // small(X) :- item(X), X < 5.  (the string never satisfies <)
        var r = b.rule(small);
        const x = try r.v("X");
        try r.head(&.{x});
        try r.pos(item, &.{x});
        try r.cmp(x, .lt, try b.int(5));
        try r.finish();
    }
    {
        // other(X) :- item(X), X != "a".  (inequality works on any values)
        var r = b.rule(other);
        const x = try r.v("X");
        try r.head(&.{x});
        try r.pos(item, &.{x});
        try r.cmp(x, .ne, try b.str("a"));
        try r.finish();
    }

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    const smalls = evaluator.relationOf(small).?;
    try std.testing.expectEqual(@as(usize, 1), smalls.len());
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&smalls.elements[0], 0));

    const others = evaluator.relationOf(other).?;
    try std.testing.expectEqual(@as(usize, 1), others.len());
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&others.elements[0], 0));
}

test "Evaluator: facts seed derived predicates" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const edge = try b.predicate("edge", 2);
    const path = try b.predicate("path", 2);

    try b.fact(edge, &.{ 1, 2 });
    try b.fact(path, &.{ 10, 20 }); // direct fact for a derived predicate

    var r = b.rule(path);
    const x = try r.v("X");
    const y = try r.v("Y");
    try r.head(&.{ x, y });
    try r.pos(edge, &.{ x, y });
    try r.finish();

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    const result = evaluator.relationOf(path).?;
    try std.testing.expectEqual(@as(usize, 2), result.len());
}

test "Evaluator: max iterations bounds the fixed point" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const edge = try b.predicate("edge", 2);
    const path = try b.predicate("path", 2);

    // A long chain needs many rounds.
    var i: u64 = 0;
    while (i < 10) : (i += 1) {
        try b.fact(edge, &.{ i, i + 1 });
    }

    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        try r.head(&.{ x, y });
        try r.pos(edge, &.{ x, y });
        try r.finish();
    }
    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        const z = try r.v("Z");
        try r.head(&.{ x, z });
        try r.pos(path, &.{ x, y });
        try r.pos(edge, &.{ y, z });
        try r.finish();
    }

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try std.testing.expectError(error.MaxIterationsExceeded, setup.solve(&evaluator, 2));

    // With no limit the same program converges.
    var evaluator2 = Evaluator.init(allocator, &setup.program);
    defer evaluator2.deinit();
    try setup.solve(&evaluator2, null);
    try std.testing.expectEqual(@as(usize, 55), evaluator2.relationOf(path).?.len());
}

test "Evaluator: provenance records facts and first derivations" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const edge = try b.predicate("edge", 2);
    const path = try b.predicate("path", 2);

    try b.fact(edge, &.{ 1, 2 });
    try b.fact(edge, &.{ 2, 3 });
    try b.fact(path, &.{ 7, 8 }); // direct fact for a derived predicate

    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        try r.head(&.{ x, y });
        try r.pos(edge, &.{ x, y });
        try r.finish();
    }
    {
        var r = b.rule(path);
        const x = try r.v("X");
        const y = try r.v("Y");
        const z = try r.v("Z");
        try r.head(&.{ x, z });
        try r.pos(path, &.{ x, y });
        try r.pos(edge, &.{ y, z });
        try r.finish();
    }

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    evaluator.track_provenance = true;
    try setup.solve(&evaluator, null);

    // The seeded fact wins over any rederivation.
    const seeded = evaluator.derivationOf(path, dyntuple.fromSlice(&.{ 7, 8 })).?;
    try std.testing.expect(seeded == .fact);

    // path(1, 2) comes from rule 0 with X=1, Y=2.
    const direct = evaluator.derivationOf(path, dyntuple.fromSlice(&.{ 1, 2 })).?;
    try std.testing.expectEqual(@as(u32, 0), direct.rule.rule);
    try std.testing.expectEqual(@as(u64, 1), dyntuple.get(&direct.rule.binding, 0));
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&direct.rule.binding, 1));

    // path(1, 3) comes from rule 1 with X=1, Y=2, Z=3.
    const step = evaluator.derivationOf(path, dyntuple.fromSlice(&.{ 1, 3 })).?;
    try std.testing.expectEqual(@as(u32, 1), step.rule.rule);
    try std.testing.expectEqual(@as(u64, 2), dyntuple.get(&step.rule.binding, 1));
    try std.testing.expectEqual(@as(u64, 3), dyntuple.get(&step.rule.binding, 2));

    // EDB tuples have no provenance entries.
    try std.testing.expect(evaluator.derivationOf(edge, dyntuple.fromSlice(&.{ 1, 2 })) == null);

    // Without tracking, nothing is recorded.
    var plain = Evaluator.init(allocator, &setup.program);
    defer plain.deinit();
    try setup.solve(&plain, null);
    try std.testing.expect(plain.derivationOf(path, dyntuple.fromSlice(&.{ 1, 2 })) == null);
}

test "Evaluator: rule with only negated body literals" {
    const allocator = std.testing.allocator;

    var setup = TestSetup.init(allocator);
    defer setup.deinit();
    var b = setup.builder();

    const q = try b.predicate("q", 1);
    const flag = try b.predicate("flag", 0);

    try b.fact(q, &.{5});

    // flag() :- not q(7).
    var r = b.rule(flag);
    try r.head(&.{});
    try r.neg(q, &.{try b.int(7)});
    try r.finish();

    var evaluator = Evaluator.init(allocator, &setup.program);
    defer evaluator.deinit();
    try setup.solve(&evaluator, null);

    // q(7) is absent, so flag() holds: one empty tuple.
    try std.testing.expectEqual(@as(usize, 1), evaluator.relationOf(flag).?.len());
}

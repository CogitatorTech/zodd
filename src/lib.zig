//! # Zodd
//!
//! Zodd is a small, embeddable Datalog engine in Zig.
//!
//! ## Quickstart
//!
//! ```zig
//! const std = @import("std");
//! const zodd = @import("zodd");
//!
//! pub fn main() !void {
//!     var gpa = std.heap.DebugAllocator(.{}){};
//!     defer _ = gpa.deinit();
//!     const allocator = gpa.allocator();
//!
//!     const Edge = struct { u32, u32 };
//!
//!     // Base relation.
//!     var edges = try zodd.Relation(Edge).fromSlice(allocator, &[_]Edge{
//!         .{ 1, 2 }, .{ 2, 3 }, .{ 3, 4 },
//!     });
//!     defer edges.deinit();
//!
//!     // Variable for the transitive closure.
//!     var reachable = zodd.Variable(Edge).init(allocator);
//!     defer reachable.deinit();
//!     try reachable.insertSlice(edges.elements);
//!
//!     // reachable(X, Z) :- reachable(X, Y), edge(Y, Z).
//!     while (try reachable.changed()) {
//!         var batch: std.ArrayList(Edge) = .empty;
//!         defer batch.deinit(allocator);
//!         for (reachable.recent.elements) |r| {
//!             for (edges.elements) |e| {
//!                 if (e[0] == r[1]) try batch.append(allocator, .{ r[0], e[1] });
//!             }
//!         }
//!         if (batch.items.len > 0) {
//!             const rel = try zodd.Relation(Edge).fromSlice(allocator, batch.items);
//!             try reachable.insert(rel);
//!         }
//!     }
//!
//!     var result = try reachable.complete();
//!     defer result.deinit();
//!     std.debug.print("Reachable pairs: {d}\n", .{result.len()});
//! }
//! ```

const relation = @import("zodd/relation.zig");
const variable = @import("zodd/variable.zig");
const iteration = @import("zodd/iteration.zig");
const join = @import("zodd/join.zig");
const extend = @import("zodd/extend.zig");
const index_mod = @import("zodd/index.zig");
const aggregate_mod = @import("zodd/aggregate.zig");

/// Immutable, sorted, deduplicated relation.
pub const Relation = relation.Relation;

/// Mutable relation used inside fixed-point loops (holds stable, recent, and
/// to-add batches for semi-naive evaluation).
pub const Variable = variable.Variable;

/// Exponential + binary search over a sorted slice.
pub const gallop = variable.gallop;

/// Fixed-point driver for a set of variables.
pub const Iteration = iteration.Iteration;

/// Error set returned by `Iteration.changed`.
pub const IterateError = iteration.IterateError;

/// Sort-merge join between two sorted relations on a common key.
pub const joinHelper = join.joinHelper;

/// Semi-naive join that inserts the projected result into an output variable.
pub const joinInto = join.joinInto;

/// Semi-naive anti-join; keeps tuples whose key is absent from `filter`.
pub const joinAnti = join.joinAnti;

/// Leaper vtable used by leapfrog-style extensions.
pub const Leaper = extend.Leaper;

/// Extends a tuple with every value that shares its key in a relation (semi-join).
pub const ExtendWith = extend.ExtendWith;

/// Drops tuples whose (key, val) pair is present in a relation (anti-join predicate).
pub const FilterAnti = extend.FilterAnti;

/// Extends a tuple with values for its key while excluding those present in another relation.
pub const ExtendAnti = extend.ExtendAnti;

/// Runs a leapfrog extension over `source.recent` and inserts results into `output`.
pub const extendInto = extend.extendInto;

/// B-tree secondary index keyed by an extracted attribute.
pub const SecondaryIndex = index_mod.SecondaryIndex;

/// Group-by aggregation with a user-supplied folder.
pub const aggregate = aggregate_mod.aggregate;

test {
    @import("std").testing.refAllDecls(@This());
    // Pull the modules themselves into test scope so their inline `test`
    // blocks compile; the module identifiers themselves stay private here.
    _ = relation;
    _ = variable;
    _ = iteration;
    _ = join;
    _ = extend;
    _ = index_mod;
    _ = aggregate_mod;
}

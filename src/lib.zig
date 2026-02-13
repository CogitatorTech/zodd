//! # Zodd
//!
//! Zodd is a Datalog engine in Zig.
//!
//! ## Quickstart
//!
//! ```zig
//! const std = @import("std");
//! const zodd = @import("zodd");
//!
//! pub fn main() !void {
//!     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//!     defer _ = gpa.deinit();
//!     const allocator = gpa.allocator();
//!
//!     // 1. Create execution context
//!     var ctx = zodd.ExecutionContext.init(allocator);
//!     defer ctx.deinit();
//!
//!     // 2. Define relation (e.g., edge(x, y))
//!     const Edge = struct { u32, u32 };
//!     var edge = try zodd.Relation(Edge).fromSlice(&ctx, &[_]Edge{
//!         .{ 1, 2 }, .{ 2, 3 }, .{ 3, 4 }
//!     });
//!     defer edge.deinit();
//!
//!     // 3. Define variable for transitive closure path(x, y)
//!     var path = try zodd.Variable(Edge).init(&ctx, &edge);
//!     defer path.deinit();
//!
//!     // 4. Run semi-naive evaluation
//!     while (try path.changed()) {
//!         // path(x, z) :- path(x, y), edge(y, z).
//!         const new_paths = try zodd.joinInto(Edge, Edge, u32, &path, &edge, 1, 0, struct {
//!             fn f(x: u32, y: u32, z: u32) Edge { return .{ x, z }; }
//!         }.f);
//!         try path.insert(new_paths);
//!     }
//!
//!     std.debug.print("Path count: {}\n", .{path.complete().len});
//! }
//! ```
//!
//! ## Components
//!
//! - `Relation`: The immutable data structure (sorted, deduplicated tuples).
//! - `Variable`: The mutable relation for fixed-point iterations.
//! - `join`: The merge-join algorithms.
//! - `extend`: The primitives for extending tuples (semi-joins, anti-joins).
//! - `index`: The indexes for lookups.
//! - `aggregate`: The group-by and aggregation operations.

/// Relation module.
pub const relation = @import("zodd/relation.zig");
/// Variable module.
pub const variable = @import("zodd/variable.zig");
/// Iteration module.
pub const iteration = @import("zodd/iteration.zig");
/// Join module.
pub const join = @import("zodd/join.zig");
/// Extend module.
pub const extend = @import("zodd/extend.zig");
/// Execution context module.
pub const context = @import("zodd/context.zig");

/// Index module.
pub const index = @import("zodd/index.zig");
/// Aggregation module.
pub const aggregate = @import("zodd/aggregate.zig");

/// Relation type.
pub const Relation = relation.Relation;
/// Variable type.
pub const Variable = variable.Variable;
/// Gallop search helper.
pub const gallop = variable.gallop;
/// Iteration type.
pub const Iteration = iteration.Iteration;
/// Join helper for sorted relations.
pub const joinHelper = join.joinHelper;
/// Join into a variable.
pub const joinInto = join.joinInto;
/// Anti-join into a variable.
pub const joinAnti = join.joinAnti;
/// Leaper interface for extend.
pub const Leaper = extend.Leaper;
/// Extend relation by key.
pub const ExtendWith = extend.ExtendWith;
/// Anti filter using a relation.
pub const FilterAnti = extend.FilterAnti;
/// Anti extend using a relation.
pub const ExtendAnti = extend.ExtendAnti;
/// Extend into a variable.
pub const extendInto = extend.extendInto;
/// Aggregate helper.
pub const aggregateFn = aggregate.aggregate;
/// Execution context type.
pub const ExecutionContext = context.ExecutionContext;

test {
    @import("std").testing.refAllDecls(@This());
}

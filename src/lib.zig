//! Zodd: datalog engine for Zig.


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

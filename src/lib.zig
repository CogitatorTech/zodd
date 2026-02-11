//! Zodd: A small embeddable Datalog engine for Zig.

pub const relation = @import("zodd/relation.zig");
pub const variable = @import("zodd/variable.zig");
pub const iteration = @import("zodd/iteration.zig");
pub const join = @import("zodd/join.zig");
pub const extend = @import("zodd/extend.zig");
pub const context = @import("zodd/context.zig");

pub const index = @import("zodd/index.zig");
pub const aggregate = @import("zodd/aggregate.zig");

pub const Relation = relation.Relation;
pub const Variable = variable.Variable;
pub const gallop = variable.gallop;
pub const Iteration = iteration.Iteration;
pub const joinHelper = join.joinHelper;
pub const joinInto = join.joinInto;
pub const joinAnti = join.joinAnti;
pub const Leaper = extend.Leaper;
pub const ExtendWith = extend.ExtendWith;
pub const FilterAnti = extend.FilterAnti;
pub const ExtendAnti = extend.ExtendAnti;
pub const extendInto = extend.extendInto;
pub const aggregateFn = aggregate.aggregate;
pub const ExecutionContext = context.ExecutionContext;

test {
    @import("std").testing.refAllDecls(@This());
}

//! # Datalog Parser
//!
//! Recursive-descent parser producing the same IR the programmatic builder
//! produces (it drives a `Builder` internally, so arity checks and variable
//! interning are shared).
//!
//! Grammar (v1):
//!
//! ```text
//! program   = { clause } eof
//! clause    = fact | rule | query
//! fact      = atom "."
//! rule      = head ":-" body "."
//! query     = "?-" atom "."
//! head      = atom with at most one aggregate argument `func(Var)`
//! body      = body_item { "," body_item }
//! body_item = literal | comparison
//! literal   = [ "not" ] atom
//! comparison = term cmp_op term
//! cmp_op    = "<" | "<=" | ">" | ">=" | "=" | "!="
//! atom      = pred_name "(" [ term { "," term } ] ")"
//! term      = Variable | "_" | integer | string
//! ```
//!
//! Constants are integers or quoted strings; bare lowercase identifiers are
//! not constants. `not`, `count`, `sum`, `min`, and `max` are reserved.
//! Comparisons are filters: every comparison variable must also occur in a
//! positive body literal, and wildcards are not allowed. Ordered operators
//! compare integers; a string operand fails the comparison.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const builder_mod = @import("builder.zig");
const interner_mod = @import("interner.zig");
const token_mod = @import("token.zig");
const Diagnostic = @import("analyze.zig").Diagnostic;
const Builder = builder_mod.Builder;
const Lexer = token_mod.Lexer;
const Token = token_mod.Token;

/// Errors produced while parsing. Structural details land in `Diagnostic`.
pub const ParseError = token_mod.LexError || builder_mod.BuildError || error{
    UnexpectedToken,
    InvalidEscape,
    IntegerOverflow,
    NonGroundFact,
    ReservedWord,
    MultipleAggregates,
};

const reserved_words = [_][]const u8{ "not", "count", "sum", "min", "max" };

/// Parses Datalog source, appending facts, rules, and queries to `program`.
/// On error, fills `diagnostic` (when provided) with a message and source
/// span.
pub fn parse(
    program: *ast.Program,
    interner: *interner_mod.Interner,
    source: []const u8,
    diagnostic: ?*Diagnostic,
) ParseError!void {
    var parser = Parser{
        .lexer = Lexer.init(source),
        .builder = Builder{ .program = program, .interner = interner },
        .diagnostic = diagnostic,
        .current = undefined,
    };
    try parser.advance();
    while (parser.current.kind != .eof) {
        try parser.clause();
    }
}

/// A parsed argument, before conversion into IR terms.
const Arg = union(enum) {
    variable: []const u8,
    constant: u64,
    wildcard,
    aggregate: struct {
        func: ast.AggFunc,
        arg_name: []const u8,
        slot: u16,
    },
};

const Parser = struct {
    lexer: Lexer,
    builder: Builder,
    diagnostic: ?*Diagnostic,
    current: Token,

    fn arena(self: *Parser) Allocator {
        return self.builder.program.allocator();
    }

    fn advance(self: *Parser) ParseError!void {
        self.current = self.lexer.next() catch |err| {
            const here = ast.Span{ .start = self.lexer.pos, .end = self.lexer.pos + 1 };
            return self.fail(err, here, "invalid token");
        };
    }

    fn fail(self: *Parser, err: ParseError, span: ast.Span, message: []const u8) ParseError {
        if (self.diagnostic) |diag| {
            diag.* = .{ .message = message, .span = span };
        }
        return err;
    }

    fn expect(self: *Parser, kind: token_mod.TokenKind, what: []const u8) ParseError!Token {
        if (self.current.kind != kind) {
            return self.fail(error.UnexpectedToken, self.current.span, what);
        }
        const token = self.current;
        try self.advance();
        return token;
    }

    fn clause(self: *Parser) ParseError!void {
        if (self.current.kind == .query_prefix) {
            try self.advance();
            return self.query();
        }

        const name_token = try self.predName();
        const args = try self.parseArgs(true);

        switch (self.current.kind) {
            .dot => {
                try self.advance();
                return self.fact(name_token, args);
            },
            .turnstile => {
                try self.advance();
                return self.rule(name_token, args);
            },
            else => return self.fail(error.UnexpectedToken, self.current.span, "expected '.' or ':-'"),
        }
    }

    fn predName(self: *Parser) ParseError!Token {
        const token = try self.expect(.ident_lower, "expected a predicate name");
        const name = self.lexer.text(token);
        for (reserved_words) |word| {
            if (std.mem.eql(u8, name, word)) {
                return self.fail(error.ReservedWord, token.span, "reserved word used as a predicate name");
            }
        }
        return token;
    }

    /// Parses `( [ term { "," term } ] )`. Aggregate arguments are only
    /// recognized when `allow_aggregate` is set (clause heads).
    fn parseArgs(self: *Parser, allow_aggregate: bool) ParseError![]Arg {
        _ = try self.expect(.lparen, "expected '('");

        var args: std.ArrayListUnmanaged(Arg) = .empty;
        if (self.current.kind == .rparen) {
            try self.advance();
            return args.items;
        }

        while (true) {
            const arg = try self.parseArg(allow_aggregate, @intCast(args.items.len));
            try args.append(self.arena(), arg);
            if (self.current.kind == .comma) {
                try self.advance();
                continue;
            }
            _ = try self.expect(.rparen, "expected ',' or ')'");
            return args.items;
        }
    }

    fn parseArg(self: *Parser, allow_aggregate: bool, slot: u16) ParseError!Arg {
        const token = self.current;
        switch (token.kind) {
            .ident_upper => {
                try self.advance();
                return Arg{ .variable = self.lexer.text(token) };
            },
            .wildcard => {
                try self.advance();
                return Arg{ .wildcard = {} };
            },
            .integer => {
                try self.advance();
                const value = std.fmt.parseInt(u64, self.lexer.text(token), 10) catch {
                    return self.fail(error.IntegerOverflow, token.span, "integer literal does not fit in 64 bits");
                };
                const atom = interner_mod.encodeInt(value) catch {
                    return self.fail(error.IntegerTooLarge, token.span, "integer literal does not fit in 63 bits");
                };
                return Arg{ .constant = atom };
            },
            .string => {
                try self.advance();
                const raw = self.lexer.text(token);
                const buffer = try self.arena().alloc(u8, raw.len);
                const unescaped = token_mod.unescapeString(raw, buffer) catch {
                    return self.fail(error.InvalidEscape, token.span, "invalid string escape");
                };
                return Arg{ .constant = try self.builder.interner.intern(unescaped) };
            },
            .ident_lower => {
                const name = self.lexer.text(token);
                if (allow_aggregate) {
                    if (aggFunc(name)) |func| {
                        try self.advance();
                        _ = try self.expect(.lparen, "expected '(' after aggregate function");
                        const var_token = try self.expect(.ident_upper, "expected a variable in aggregate");
                        _ = try self.expect(.rparen, "expected ')' after aggregate variable");
                        return Arg{ .aggregate = .{
                            .func = func,
                            .arg_name = self.lexer.text(var_token),
                            .slot = slot,
                        } };
                    }
                }
                return self.fail(error.UnexpectedToken, token.span, "bare identifiers are not constants; use an integer or a quoted string");
            },
            else => return self.fail(error.UnexpectedToken, token.span, "expected a term"),
        }
    }

    fn aggFunc(name: []const u8) ?ast.AggFunc {
        inline for (@typeInfo(ast.AggFunc).@"enum".fields) |field| {
            if (std.mem.eql(u8, name, field.name)) {
                return @field(ast.AggFunc, field.name);
            }
        }
        return null;
    }

    fn fact(self: *Parser, name_token: Token, args: []const Arg) ParseError!void {
        const pred = try self.declare(name_token, @intCast(args.len));
        const row = try self.arena().alloc(u64, args.len);
        for (args, 0..) |arg, i| {
            row[i] = switch (arg) {
                .constant => |value| value,
                else => return self.fail(error.NonGroundFact, name_token.span, "facts must be ground (constants only)"),
            };
        }
        try self.builder.fact(pred, row);
    }

    fn rule(self: *Parser, head_token: Token, head_args: []const Arg) ParseError!void {
        const head_pred = try self.declare(head_token, @intCast(head_args.len));
        var r = self.builder.rule(head_pred);
        r.span = head_token.span;

        // Head: plain, or aggregate with exactly one aggregate argument.
        var aggregate: ?Arg = null;
        var plain_count: usize = 0;
        for (head_args) |arg| {
            if (arg == .aggregate) {
                if (aggregate != null) {
                    return self.fail(error.MultipleAggregates, head_token.span, "at most one aggregate per head");
                }
                aggregate = arg;
            } else {
                plain_count += 1;
            }
        }

        if (aggregate) |agg_arg| {
            const group_terms = try self.arena().alloc(ast.Term, plain_count);
            var i: usize = 0;
            for (head_args) |arg| {
                if (arg == .aggregate) continue;
                group_terms[i] = try self.toTerm(&r, arg);
                i += 1;
            }
            const agg = agg_arg.aggregate;
            try r.aggHead(group_terms, agg.slot, agg.func, try r.v(agg.arg_name));
        } else {
            const terms = try self.arena().alloc(ast.Term, head_args.len);
            for (head_args, 0..) |arg, i| {
                terms[i] = try self.toTerm(&r, arg);
            }
            try r.head(terms);
        }

        // Body items: a term opens a comparison, anything else a literal.
        while (true) {
            switch (self.current.kind) {
                .ident_upper, .wildcard, .integer, .string => try self.comparison(&r),
                else => try self.bodyLiteral(&r),
            }

            if (self.current.kind == .comma) {
                try self.advance();
                continue;
            }
            _ = try self.expect(.dot, "expected ',' or '.'");
            break;
        }

        try r.finish();
    }

    fn bodyLiteral(self: *Parser, r: *builder_mod.RuleBuilder) ParseError!void {
        var negated = false;
        if (self.current.kind == .ident_lower and
            std.mem.eql(u8, self.lexer.text(self.current), "not"))
        {
            negated = true;
            try self.advance();
        }

        const name_token = try self.predName();
        const args = try self.parseArgs(false);
        const pred = try self.declare(name_token, @intCast(args.len));

        const terms = try self.arena().alloc(ast.Term, args.len);
        for (args, 0..) |arg, i| {
            terms[i] = try self.toTerm(r, arg);
        }
        if (negated) {
            try r.neg(pred, terms);
        } else {
            try r.pos(pred, terms);
        }
    }

    fn comparison(self: *Parser, r: *builder_mod.RuleBuilder) ParseError!void {
        const lhs_span = self.current.span;
        const lhs = try self.toTerm(r, try self.parseArg(false, 0));

        const op: ast.CmpOp = switch (self.current.kind) {
            .less_than => .lt,
            .less_equal => .le,
            .greater_than => .gt,
            .greater_equal => .ge,
            .equal => .eq,
            .not_equal => .ne,
            else => return self.fail(error.UnexpectedToken, self.current.span, "expected a comparison operator"),
        };
        try self.advance();

        const rhs_span = self.current.span;
        const rhs = try self.toTerm(r, try self.parseArg(false, 0));

        r.cmp(lhs, op, rhs) catch |err| switch (err) {
            error.InvalidComparison => return self.fail(
                err,
                .{ .start = lhs_span.start, .end = rhs_span.end },
                "wildcards are not allowed in comparisons",
            ),
            else => return err,
        };
    }

    fn query(self: *Parser) ParseError!void {
        const name_token = try self.predName();
        const args = try self.parseArgs(false);
        _ = try self.expect(.dot, "expected '.'");

        const pred = try self.declare(name_token, @intCast(args.len));
        const pattern = try self.arena().alloc(?u64, args.len);
        for (args, 0..) |arg, i| {
            pattern[i] = switch (arg) {
                .constant => |value| value,
                .variable, .wildcard => null,
                .aggregate => unreachable, // not parsed in query position
            };
        }
        try self.builder.query(pred, pattern);
    }

    fn declare(self: *Parser, name_token: Token, arity: u16) ParseError!ast.PredId {
        return self.builder.predicate(self.lexer.text(name_token), arity) catch |err| {
            return self.fail(err, name_token.span, "predicate used with conflicting arity");
        };
    }

    fn toTerm(self: *Parser, r: *builder_mod.RuleBuilder, arg: Arg) ParseError!ast.Term {
        return switch (arg) {
            .variable => |name| try r.v(name),
            .constant => |value| ast.Term{ .constant = value },
            .wildcard => Builder.wild,
            .aggregate => self.fail(error.UnexpectedToken, self.current.span, "aggregates are only allowed in rule heads"),
        };
    }
};

// --- Tests -----------------------------------------------------------------

const Interner = interner_mod.Interner;

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
};

test "parse: facts, rules, and queries" {
    var setup = TestSetup.init(std.testing.allocator);
    defer setup.deinit();

    try parse(&setup.program, &setup.interner,
        \\% A graph and its closure.
        \\edge(1, 2).
        \\edge(2, 3).
        \\path(X, Y) :- edge(X, Y).
        \\path(X, Z) :- path(X, Y), edge(Y, Z).
        \\?- path(1, X).
    , null);

    try std.testing.expectEqual(@as(usize, 2), setup.program.facts.items.len);
    try std.testing.expectEqual(@as(usize, 2), setup.program.rules.items.len);
    try std.testing.expectEqual(@as(usize, 1), setup.program.queries.items.len);
    try std.testing.expectEqual(@as(usize, 2), setup.program.preds.items.len);

    const q = setup.program.queries.items[0];
    try std.testing.expectEqual(@as(?u64, 1), q.pattern[0]);
    try std.testing.expectEqual(@as(?u64, null), q.pattern[1]);
}

test "parse: negation, wildcards, and strings" {
    var setup = TestSetup.init(std.testing.allocator);
    defer setup.deinit();

    try parse(&setup.program, &setup.interner,
        \\node("a"). node("b").
        \\blocked("b").
        \\has_edge(X) :- edge(X, _).
        \\safe(X) :- node(X), not blocked(X).
        \\edge("a", "b").
    , null);

    const rules = setup.program.rules.items;
    try std.testing.expectEqual(@as(usize, 2), rules.len);
    try std.testing.expect(rules[1].body[1].negated);
    // The wildcard is still a wildcard pre-analysis.
    try std.testing.expect(rules[0].body[0].atom.terms[1] == .wildcard);

    // "b" interned identically across facts.
    const node_b = setup.program.facts.items[1].row[0];
    const blocked_b = setup.program.facts.items[2].row[0];
    try std.testing.expectEqual(node_b, blocked_b);
}

test "parse: comparisons" {
    var setup = TestSetup.init(std.testing.allocator);
    defer setup.deinit();

    try parse(&setup.program, &setup.interner,
        \\person(1, 17). person(2, 30).
        \\adult(X) :- person(X, Age), Age >= 18.
        \\pair(X, Y) :- person(X, A), person(Y, B), X != Y, A < B.
    , null);

    const rules = setup.program.rules.items;
    try std.testing.expectEqual(@as(usize, 2), rules.len);

    try std.testing.expectEqual(@as(usize, 1), rules[0].compares.len);
    try std.testing.expectEqual(ast.CmpOp.ge, rules[0].compares[0].op);
    try std.testing.expectEqual(@as(u64, 18), rules[0].compares[0].rhs.constant);

    try std.testing.expectEqual(@as(usize, 2), rules[1].compares.len);
    try std.testing.expectEqual(ast.CmpOp.ne, rules[1].compares[0].op);
    try std.testing.expectEqual(ast.CmpOp.lt, rules[1].compares[1].op);
    // Comparison variables share ids with the literal occurrences.
    try std.testing.expectEqual(
        rules[1].body[0].atom.terms[0].variable,
        rules[1].compares[0].lhs.variable,
    );
}

test "parse: aggregate heads" {
    var setup = TestSetup.init(std.testing.allocator);
    defer setup.deinit();

    try parse(&setup.program, &setup.interner,
        \\deg(N, count(M)) :- edge(N, M).
        \\edge(1, 2).
    , null);

    const head = setup.program.rules.items[0].head.aggregate;
    try std.testing.expectEqual(ast.AggFunc.count, head.func);
    try std.testing.expectEqual(@as(u16, 1), head.agg_slot);
    try std.testing.expectEqual(@as(usize, 1), head.group_terms.len);
}

test "parse: diagnostics carry spans" {
    var setup = TestSetup.init(std.testing.allocator);
    defer setup.deinit();

    var diag = Diagnostic{};
    const source = "edge(1, X).";
    try std.testing.expectError(
        error.NonGroundFact,
        parse(&setup.program, &setup.interner, source, &diag),
    );
    try std.testing.expect(diag.span != null);
    try std.testing.expect(diag.message.len > 0);
}

test "parse: error cases" {
    const cases = [_]struct { source: []const u8, err: anyerror }{
        .{ .source = "p(a).", .err = error.UnexpectedToken },
        .{ .source = "p(1) :- q(1)", .err = error.UnexpectedToken },
        .{ .source = "not(1).", .err = error.ReservedWord },
        .{ .source = "p(-1).", .err = error.NegativeInteger },
        .{ .source = "p(99999999999999999999).", .err = error.IntegerOverflow },
        .{ .source = "p(X) :- q(X), r(count(X)).", .err = error.UnexpectedToken },
        .{ .source = "t(count(X), sum(X)) :- q(X).", .err = error.MultipleAggregates },
        .{ .source = "p(1). p(1, 2).", .err = error.ArityConflict },
        .{ .source = "p(X) :- q(X), X < _.", .err = error.InvalidComparison },
        .{ .source = "p(X) :- q(X), X q(X).", .err = error.UnexpectedToken },
        .{ .source = "p(X) :- q(X), X <.", .err = error.UnexpectedToken },
        .{ .source = "p(X) :- q(X), not X < 1.", .err = error.UnexpectedToken },
    };

    for (cases) |case| {
        var setup = TestSetup.init(std.testing.allocator);
        defer setup.deinit();
        const result = parse(&setup.program, &setup.interner, case.source, null);
        try std.testing.expectError(case.err, result);
    }
}

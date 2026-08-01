//! # Datalog Lexer
//!
//! The module tokenizes Datalog source text.
//!
//! Lexical conventions (Prolog style): predicate names are lowercase-initial
//! identifiers, variables are uppercase-initial (or underscore-led)
//! identifiers, `_` alone is the anonymous wildcard, integers are
//! non-negative `u64` literals, strings are double-quoted with `\"`, `\\`,
//! `\n`, and `\t` escapes, comparison operators are `<`, `<=`, `>`, `>=`,
//! `=`, and `!=`, arithmetic operators are `+`, `-`, `*`, and `/`, and `%`
//! starts a line comment.

const std = @import("std");
const Span = @import("ast.zig").Span;

pub const TokenKind = enum {
    /// Lowercase-initial identifier: predicate names and soft keywords.
    ident_lower,
    /// Uppercase- or underscore-initial identifier: a variable.
    ident_upper,
    /// `_` alone: the anonymous wildcard.
    wildcard,
    integer,
    /// A quoted string; the span includes the quotes.
    string,
    lparen,
    rparen,
    comma,
    dot,
    /// `:-`
    turnstile,
    /// `?-`
    query_prefix,
    less_than,
    /// `<=`
    less_equal,
    greater_than,
    /// `>=`
    greater_equal,
    equal,
    /// `!=`
    not_equal,
    plus,
    minus,
    star,
    slash,
    eof,
};

pub const Token = struct {
    kind: TokenKind,
    span: Span,
};

pub const LexError = error{
    InvalidCharacter,
    UnterminatedString,
    NegativeInteger,
};

pub const Lexer = struct {
    source: []const u8,
    pos: u32 = 0,

    pub fn init(source: []const u8) Lexer {
        return Lexer{ .source = source };
    }

    /// The source text of a token.
    pub fn text(self: *const Lexer, token: Token) []const u8 {
        return self.source[token.span.start..token.span.end];
    }

    /// Returns the next token, or a `LexError` whose location is the current
    /// position.
    pub fn next(self: *Lexer) LexError!Token {
        self.skipTrivia();

        const start = self.pos;
        if (self.pos >= self.source.len) {
            return Token{ .kind = .eof, .span = .{ .start = start, .end = start } };
        }

        const c = self.source[self.pos];
        switch (c) {
            '(' => return self.single(.lparen),
            ')' => return self.single(.rparen),
            ',' => return self.single(.comma),
            '.' => return self.single(.dot),
            ':' => {
                if (self.peekAt(1) == '-') {
                    self.pos += 2;
                    return Token{ .kind = .turnstile, .span = .{ .start = start, .end = self.pos } };
                }
                return error.InvalidCharacter;
            },
            '?' => {
                if (self.peekAt(1) == '-') {
                    self.pos += 2;
                    return Token{ .kind = .query_prefix, .span = .{ .start = start, .end = self.pos } };
                }
                return error.InvalidCharacter;
            },
            '+' => return self.single(.plus),
            '-' => return self.single(.minus),
            '*' => return self.single(.star),
            '/' => return self.single(.slash),
            '<' => return self.maybeEqual(.less_than, .less_equal),
            '>' => return self.maybeEqual(.greater_than, .greater_equal),
            '=' => return self.single(.equal),
            '!' => {
                if (self.peekAt(1) == '=') {
                    self.pos += 2;
                    return Token{ .kind = .not_equal, .span = .{ .start = start, .end = self.pos } };
                }
                return error.InvalidCharacter;
            },
            '"' => return self.lexString(start),
            '0'...'9' => {
                while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
                    self.pos += 1;
                }
                return Token{ .kind = .integer, .span = .{ .start = start, .end = self.pos } };
            },
            'a'...'z', 'A'...'Z', '_' => {
                while (self.pos < self.source.len and isIdentChar(self.source[self.pos])) {
                    self.pos += 1;
                }
                const ident = self.source[start..self.pos];
                const kind: TokenKind = if (std.mem.eql(u8, ident, "_"))
                    .wildcard
                else if (std.ascii.isLower(ident[0]))
                    .ident_lower
                else
                    .ident_upper;
                return Token{ .kind = kind, .span = .{ .start = start, .end = self.pos } };
            },
            else => return error.InvalidCharacter,
        }
    }

    fn single(self: *Lexer, kind: TokenKind) Token {
        const start = self.pos;
        self.pos += 1;
        return Token{ .kind = kind, .span = .{ .start = start, .end = self.pos } };
    }

    /// Lexes a one-character operator, or its two-character form when the
    /// next character is `=`.
    fn maybeEqual(self: *Lexer, bare: TokenKind, with_equal: TokenKind) Token {
        const start = self.pos;
        if (self.peekAt(1) == '=') {
            self.pos += 2;
            return Token{ .kind = with_equal, .span = .{ .start = start, .end = self.pos } };
        }
        self.pos += 1;
        return Token{ .kind = bare, .span = .{ .start = start, .end = self.pos } };
    }

    fn lexString(self: *Lexer, start: u32) LexError!Token {
        self.pos += 1; // opening quote
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == '\\') {
                self.pos += 2; // skip the escaped character
                continue;
            }
            if (c == '"') {
                self.pos += 1;
                return Token{ .kind = .string, .span = .{ .start = start, .end = self.pos } };
            }
            if (c == '\n') break;
            self.pos += 1;
        }
        self.pos = start;
        return error.UnterminatedString;
    }

    fn skipTrivia(self: *Lexer) void {
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (std.ascii.isWhitespace(c)) {
                self.pos += 1;
            } else if (c == '%') {
                while (self.pos < self.source.len and self.source[self.pos] != '\n') {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn peekAt(self: *const Lexer, offset: u32) ?u8 {
        const index = self.pos + offset;
        if (index >= self.source.len) return null;
        return self.source[index];
    }

    fn isIdentChar(c: u8) bool {
        return std.ascii.isAlphanumeric(c) or c == '_';
    }
};

/// Unescapes the contents of a string token (span includes the quotes) into
/// `buffer`, which must be at least `span` long. Returns the unescaped
/// slice or `error.InvalidEscape`.
pub fn unescapeString(raw: []const u8, buffer: []u8) error{InvalidEscape}![]const u8 {
    std.debug.assert(raw.len >= 2);
    const inner = raw[1 .. raw.len - 1];
    var out: usize = 0;
    var i: usize = 0;
    while (i < inner.len) : (i += 1) {
        if (inner[i] != '\\') {
            buffer[out] = inner[i];
            out += 1;
            continue;
        }
        i += 1;
        if (i >= inner.len) return error.InvalidEscape;
        buffer[out] = switch (inner[i]) {
            '"' => '"',
            '\\' => '\\',
            'n' => '\n',
            't' => '\t',
            else => return error.InvalidEscape,
        };
        out += 1;
    }
    return buffer[0..out];
}

test "Lexer: tokenizes a rule" {
    var lexer = Lexer.init("path(X, Z) :- path(X, Y), edge(Y, Z). % comment");

    const expected = [_]TokenKind{
        .ident_lower, .lparen,      .ident_upper, .comma,       .ident_upper, .rparen,
        .turnstile,   .ident_lower, .lparen,      .ident_upper, .comma,       .ident_upper,
        .rparen,      .comma,       .ident_lower, .lparen,      .ident_upper, .comma,
        .ident_upper, .rparen,      .dot,         .eof,
    };
    for (expected) |kind| {
        const token = try lexer.next();
        try std.testing.expectEqual(kind, token.kind);
    }
}

test "Lexer: literals, wildcard, and query prefix" {
    var lexer = Lexer.init("?- p(42, \"hi\\n\", _, _Tail).");

    try std.testing.expectEqual(TokenKind.query_prefix, (try lexer.next()).kind);
    try std.testing.expectEqual(TokenKind.ident_lower, (try lexer.next()).kind);
    try std.testing.expectEqual(TokenKind.lparen, (try lexer.next()).kind);

    const int_token = try lexer.next();
    try std.testing.expectEqual(TokenKind.integer, int_token.kind);
    try std.testing.expectEqualStrings("42", lexer.text(int_token));
    try std.testing.expectEqual(TokenKind.comma, (try lexer.next()).kind);

    const str_token = try lexer.next();
    try std.testing.expectEqual(TokenKind.string, str_token.kind);
    var buffer: [16]u8 = undefined;
    const unescaped = try unescapeString(lexer.text(str_token), &buffer);
    try std.testing.expectEqualStrings("hi\n", unescaped);

    try std.testing.expectEqual(TokenKind.comma, (try lexer.next()).kind);
    try std.testing.expectEqual(TokenKind.wildcard, (try lexer.next()).kind);
    try std.testing.expectEqual(TokenKind.comma, (try lexer.next()).kind);
    // `_Tail` is a variable, not a wildcard.
    try std.testing.expectEqual(TokenKind.ident_upper, (try lexer.next()).kind);
}

test "Lexer: comparison operators" {
    var lexer = Lexer.init("X < Y <= Z > 1 >= 2 = 3 != 4");

    const expected = [_]TokenKind{
        .ident_upper, .less_than,     .ident_upper, .less_equal, .ident_upper, .greater_than,
        .integer,     .greater_equal, .integer,     .equal,      .integer,     .not_equal,
        .integer,     .eof,
    };
    for (expected) |kind| {
        const token = try lexer.next();
        try std.testing.expectEqual(kind, token.kind);
    }
}

test "Lexer: error cases" {
    // `-` lexes as the minus operator; the parser rejects it in term
    // position with error.NegativeInteger.
    var negative = Lexer.init("p(-1).");
    _ = try negative.next();
    _ = try negative.next();
    try std.testing.expectEqual(TokenKind.minus, (try negative.next()).kind);

    var unterminated = Lexer.init("p(\"abc");
    _ = try unterminated.next();
    _ = try unterminated.next();
    try std.testing.expectError(error.UnterminatedString, unterminated.next());

    var bad = Lexer.init("p @ q");
    _ = try bad.next();
    try std.testing.expectError(error.InvalidCharacter, bad.next());

    // `!` is only valid as part of `!=`.
    var bang = Lexer.init("p ! q");
    _ = try bang.next();
    try std.testing.expectError(error.InvalidCharacter, bang.next());
}

test "unescapeString: rejects bad escapes" {
    var buffer: [8]u8 = undefined;
    try std.testing.expectError(error.InvalidEscape, unescapeString("\"\\q\"", &buffer));
}

//! # Symbol Interner
//!
//! The module maps string constants to atoms in the frontend's `u64` value
//! space and resolves them back for display.
//!
//! Encoding: bit 63 is a tag. When clear, the low 63 bits are a raw integer.
//! When set, the low 63 bits are a dense string id assigned in insertion
//! order. The engine core never inspects the tag; it sorts and joins raw
//! `u64` values.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Atom = @import("dyntuple.zig").Atom;

/// Tag bit marking an atom as an interned-string id.
pub const STR_TAG: Atom = 1 << 63;

/// Mask extracting the payload (integer value or string id) from an atom.
pub const PAYLOAD_MASK: Atom = STR_TAG - 1;

/// A decoded frontend value: a raw integer or an interned string.
pub const Value = union(enum) {
    int: u64,
    str: []const u8,
};

/// Errors produced when encoding values into the atom space.
pub const EncodeError = error{IntegerTooLarge};

/// Encodes a raw integer as an atom. Integers must fit in 63 bits.
pub fn encodeInt(value: u64) EncodeError!Atom {
    if (value > PAYLOAD_MASK) return error.IntegerTooLarge;
    return value;
}

/// Returns true if the atom is an interned-string id.
pub fn isStr(atom: Atom) bool {
    return (atom & STR_TAG) != 0;
}

pub const Interner = struct {
    allocator: Allocator,
    /// String id to owned bytes.
    strings: std.ArrayListUnmanaged([]const u8),
    /// Bytes to tagged atom. Keys alias the owned copies in `strings`.
    map: std.StringHashMapUnmanaged(Atom),

    /// Initializes an empty interner.
    pub fn init(allocator: Allocator) Interner {
        return Interner{
            .allocator = allocator,
            .strings = .empty,
            .map = .empty,
        };
    }

    /// Deinitializes the interner and frees all owned strings.
    pub fn deinit(self: *Interner) void {
        for (self.strings.items) |s| {
            self.allocator.free(s);
        }
        self.strings.deinit(self.allocator);
        self.map.deinit(self.allocator);
    }

    /// Interns a string and returns its tagged atom. Idempotent: equal
    /// strings always produce the same atom.
    pub fn intern(self: *Interner, bytes: []const u8) Allocator.Error!Atom {
        if (self.map.get(bytes)) |atom| return atom;

        const copy = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(copy);

        const id: Atom = @intCast(self.strings.items.len);
        const atom = STR_TAG | id;

        try self.strings.append(self.allocator, copy);
        errdefer _ = self.strings.pop();

        try self.map.put(self.allocator, copy, atom);
        return atom;
    }

    /// Looks up a string's atom without interning it.
    pub fn find(self: *const Interner, bytes: []const u8) ?Atom {
        return self.map.get(bytes);
    }

    /// Encodes a surface value as an atom, interning strings as needed.
    pub fn encode(self: *Interner, value: Value) (Allocator.Error || EncodeError)!Atom {
        return switch (value) {
            .int => |v| try encodeInt(v),
            .str => |s| try self.intern(s),
        };
    }

    /// Decodes an atom back into a surface value. Unknown string ids decode
    /// to an empty string (cannot happen for atoms produced by this interner).
    pub fn resolve(self: *const Interner, atom: Atom) Value {
        if (!isStr(atom)) return Value{ .int = atom };
        const id = atom & PAYLOAD_MASK;
        if (id >= self.strings.items.len) return Value{ .str = "" };
        return Value{ .str = self.strings.items[@intCast(id)] };
    }
};

test "Interner: intern is idempotent" {
    const allocator = std.testing.allocator;

    var interner = Interner.init(allocator);
    defer interner.deinit();

    const a1 = try interner.intern("alice");
    const a2 = try interner.intern("alice");
    const b = try interner.intern("bob");

    try std.testing.expectEqual(a1, a2);
    try std.testing.expect(a1 != b);
    try std.testing.expect(isStr(a1));
}

test "Interner: round-trip ints and strings" {
    const allocator = std.testing.allocator;

    var interner = Interner.init(allocator);
    defer interner.deinit();

    const int_atom = try interner.encode(.{ .int = 42 });
    const str_atom = try interner.encode(.{ .str = "x" });

    try std.testing.expectEqual(Value{ .int = 42 }, interner.resolve(int_atom));
    try std.testing.expectEqualStrings("x", interner.resolve(str_atom).str);
}

test "Interner: integers must fit in 63 bits" {
    try std.testing.expectEqual(@as(Atom, PAYLOAD_MASK), try encodeInt(PAYLOAD_MASK));
    try std.testing.expectError(error.IntegerTooLarge, encodeInt(PAYLOAD_MASK + 1));
    try std.testing.expectError(error.IntegerTooLarge, encodeInt(std.math.maxInt(u64)));
}

test "Interner: ints and strings never collide" {
    const allocator = std.testing.allocator;

    var interner = Interner.init(allocator);
    defer interner.deinit();

    const str_atom = try interner.intern("0");
    const int_atom = try encodeInt(0);

    try std.testing.expect(str_atom != int_atom);
    try std.testing.expect(!isStr(int_atom));
}

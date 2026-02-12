const std = @import("std");
const zodd = @import("zodd");

// Role-Based Access Control (RBAC) Authorization Engine
//
// Computes effective user permissions through role hierarchy inheritance,
// permission grants, and explicit denials using Datalog rules:
//
//   has_role(U, R)   :- user_role(U, R).
//   has_role(U, R2)  :- has_role(U, R1), role_hier(R1, R2).
//   can_access(U, P) :- has_role(U, R), role_perm(R, P).
//   effective(U, P)  :- can_access(U, P), NOT denied(U, P).

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - RBAC Authorization Example\n", .{});
    std.debug.print("=================================================\n\n", .{});

    // Identifiers (using u32 for simplicity):
    //   Users:       alice=1, bob=2, charlie=3
    //   Roles:       viewer=10, editor=20, admin=30, superadmin=40
    //   Permissions: read=100, write=110, delete=120, manage_users=130, audit=140

    const Pair = struct { u32, u32 };

    // user_role(User, Role)
    const user_role_data = [_]Pair{
        .{ 1, 10 }, // alice -> viewer
        .{ 2, 20 }, // bob -> editor
        .{ 3, 40 }, // charlie -> superadmin
    };

    // role_hier(SubRole, SuperRole) -- SubRole inherits from SuperRole
    const role_hier_data = [_]Pair{
        .{ 20, 10 }, // editor inherits viewer
        .{ 30, 20 }, // admin inherits editor
        .{ 40, 30 }, // superadmin inherits admin
    };

    // role_perm(Role, Permission)
    const role_perm_data = [_]Pair{
        .{ 10, 100 }, // viewer -> read
        .{ 20, 110 }, // editor -> write
        .{ 30, 120 }, // admin -> delete
        .{ 30, 130 }, // admin -> manage_users
        .{ 40, 140 }, // superadmin -> audit
    };

    // denied(User, Permission) -- explicit denials override grants
    const denied_data = [_]Pair{
        .{ 3, 120 }, // charlie denied delete (despite being superadmin)
    };

    const user_names = [_][]const u8{ "", "alice", "bob", "charlie" };

    const roleName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                10 => "viewer",
                20 => "editor",
                30 => "admin",
                40 => "superadmin",
                else => "unknown",
            };
        }
    }.get;

    const permName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                100 => "read",
                110 => "write",
                120 => "delete",
                130 => "manage_users",
                140 => "audit",
                else => "unknown",
            };
        }
    }.get;

    std.debug.print("User-Role assignments:\n", .{});
    for (user_role_data) |ur| {
        std.debug.print("  {s} -> {s}\n", .{ user_names[ur[0]], roleName(ur[1]) });
    }

    std.debug.print("\nRole hierarchy (child inherits parent):\n", .{});
    for (role_hier_data) |rh| {
        std.debug.print("  {s} inherits {s}\n", .{ roleName(rh[0]), roleName(rh[1]) });
    }

    std.debug.print("\nRole-Permission grants:\n", .{});
    for (role_perm_data) |rp| {
        std.debug.print("  {s} -> {s}\n", .{ roleName(rp[0]), permName(rp[1]) });
    }

    std.debug.print("\nExplicit denials:\n", .{});
    for (denied_data) |d| {
        std.debug.print("  {s} denied {s}\n", .{ user_names[d[0]], permName(d[1]) });
    }

    // -- Build relations --

    var user_role = try zodd.Relation(Pair).fromSlice(&ctx, &user_role_data);
    defer user_role.deinit();

    var role_hier = try zodd.Relation(Pair).fromSlice(&ctx, &role_hier_data);
    defer role_hier.deinit();

    var role_perm = try zodd.Relation(Pair).fromSlice(&ctx, &role_perm_data);
    defer role_perm.deinit();

    var denied = try zodd.Relation(Pair).fromSlice(&ctx, &denied_data);
    defer denied.deinit();

    // -- Step 1: Compute has_role(User, Role) via transitive role inheritance --
    //   has_role(U, R) :- user_role(U, R).
    //   has_role(U, R2) :- has_role(U, R1), role_hier(R1, R2).

    var has_role = zodd.Variable(Pair).init(&ctx);
    defer has_role.deinit();

    try has_role.insertSlice(&ctx, user_role.elements);

    std.debug.print("\nComputing effective roles via hierarchy...\n", .{});

    const PairList = std.ArrayListUnmanaged(Pair);
    var iter: usize = 0;
    while (try has_role.changed()) : (iter += 1) {
        var results = PairList{};
        defer results.deinit(allocator);

        for (has_role.recent.elements) |hr| {
            const user = hr[0];
            const role = hr[1];

            // role_hier(role, parent_role) -> has_role(user, parent_role)
            for (role_hier.elements) |rh| {
                if (rh[0] == role) {
                    try results.append(allocator, .{ user, rh[1] });
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Pair).fromSlice(&ctx, results.items);
            try has_role.insert(rel);
        }

        if (iter > 50) break;
    }

    var has_role_result = try has_role.complete();
    defer has_role_result.deinit();

    std.debug.print("\nEffective roles:\n", .{});
    for (has_role_result.elements) |hr| {
        std.debug.print("  {s} has role {s}\n", .{ user_names[hr[0]], roleName(hr[1]) });
    }

    // -- Step 2: Compute can_access(User, Perm) via joinInto --
    //   can_access(U, P) :- has_role(U, R), role_perm(R, P).
    //
    // has_role is keyed by (User, Role) and role_perm is keyed by (Role, Perm).
    // We join on the Role field. To use joinInto, we need both inputs keyed by the
    // join key as the first field.
    // has_role is (User, Role), so we need to re-key it as (Role, User).
    // role_perm is already (Role, Perm).

    var has_role_by_role = zodd.Variable(Pair).init(&ctx);
    defer has_role_by_role.deinit();
    {
        var flipped = PairList{};
        defer flipped.deinit(allocator);
        for (has_role_result.elements) |hr| {
            try flipped.append(allocator, .{ hr[1], hr[0] }); // (Role, User)
        }
        try has_role_by_role.insertSlice(&ctx, flipped.items);
        _ = try has_role_by_role.changed();
    }

    var role_perm_var = zodd.Variable(Pair).init(&ctx);
    defer role_perm_var.deinit();
    try role_perm_var.insertSlice(&ctx, role_perm.elements);
    _ = try role_perm_var.changed();

    const Triple = struct { u32, u32, u32 };
    var can_access_triple = zodd.Variable(Triple).init(&ctx);
    defer can_access_triple.deinit();

    // joinInto: key=Role, val1=User, val2=Perm -> (Role, User, Perm)
    try zodd.joinInto(u32, u32, u32, Triple, &ctx, &has_role_by_role, &role_perm_var, &can_access_triple, struct {
        fn logic(role: *const u32, user: *const u32, perm: *const u32) Triple {
            _ = role;
            return .{ user.*, perm.*, 0 };
        }
    }.logic);

    _ = try can_access_triple.changed();

    // Extract (User, Perm) pairs
    var can_access = zodd.Variable(Pair).init(&ctx);
    defer can_access.deinit();
    {
        var pairs = PairList{};
        defer pairs.deinit(allocator);
        for (can_access_triple.recent.elements) |t| {
            try pairs.append(allocator, .{ t[0], t[1] });
        }
        try can_access.insertSlice(&ctx, pairs.items);
        _ = try can_access.changed();
    }

    std.debug.print("\nAll granted permissions (before denials):\n", .{});
    for (can_access.recent.elements) |ca| {
        std.debug.print("  {s} can {s}\n", .{ user_names[ca[0]], permName(ca[1]) });
    }

    // -- Step 3: Apply denials --
    //   effective(U, P) :- can_access(U, P), NOT denied(U, P).
    //
    // We need an anti-join on the full (User, Perm) pair. Since joinAnti keys on
    // the first tuple field only, we use a manual filter against the denied relation.

    var effective = zodd.Variable(Pair).init(&ctx);
    defer effective.deinit();
    {
        var eff_list = PairList{};
        defer eff_list.deinit(allocator);
        for (can_access.recent.elements) |ca| {
            var is_denied = false;
            for (denied.elements) |d| {
                if (d[0] == ca[0] and d[1] == ca[1]) {
                    is_denied = true;
                    break;
                }
            }
            if (!is_denied) {
                try eff_list.append(allocator, ca);
            }
        }
        try effective.insertSlice(&ctx, eff_list.items);
        _ = try effective.changed();
    }

    std.debug.print("\nEffective permissions (after denials):\n", .{});
    for (effective.recent.elements) |e| {
        std.debug.print("  {s} can {s}\n", .{ user_names[e[0]], permName(e[1]) });
    }

    std.debug.print("\nTotal: {} effective permissions\n", .{effective.recent.len()});
}

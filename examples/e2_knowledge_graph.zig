const std = @import("std");
const zodd = @import("zodd");

// Knowledge Graph Reasoning (Medical Ontology)
//
// Infers new biomedical facts from a medical ontology through type hierarchy
// and property inheritance. This is a common pattern in healthcare, pharma,
// and biotech for drug repurposing, adverse effect prediction, and clinical
// decision support.
//
// Datalog rules:
//   is_a(X, Z)          :- is_a(X, Y), is_a(Y, Z).
//   has_symptom(D, S)   :- is_a(D, D2), has_symptom(D2, S).
//   treats(Drug, D)     :- targets(Drug, P), associated_with(P, D).
//   side_effect(Drug, S):- treats(Drug, D), has_symptom(D, S).

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var ctx = zodd.ExecutionContext.init(allocator);

    std.debug.print("Zodd Datalog Engine - Knowledge Graph Reasoning\n", .{});
    std.debug.print("================================================\n\n", .{});

    // Ontology IDs:
    //   Diseases:  cardiovascular=10, heart_disease=11, arrhythmia=12,
    //              hypertension=13, respiratory=20, asthma=21
    //   Symptoms:  chest_pain=30, shortness_of_breath=31, fatigue=32,
    //              irregular_heartbeat=33, wheezing=34, high_bp=35
    //   Proteins:  ace_enzyme=40, beta_receptor=41, calcium_channel=42
    //   Drugs:     lisinopril=50, metoprolol=51, amlodipine=52

    const Pair = struct { u32, u32 };

    // Type hierarchy: is_a(subtype, supertype)
    const is_a_data = [_]Pair{
        .{ 11, 10 }, // heart_disease is_a cardiovascular
        .{ 12, 11 }, // arrhythmia is_a heart_disease
        .{ 13, 10 }, // hypertension is_a cardiovascular
        .{ 21, 20 }, // asthma is_a respiratory
    };

    // Direct symptom associations: has_symptom(disease, symptom)
    const symptom_data = [_]Pair{
        .{ 10, 32 }, // cardiovascular -> fatigue
        .{ 11, 30 }, // heart_disease -> chest_pain
        .{ 11, 31 }, // heart_disease -> shortness_of_breath
        .{ 12, 33 }, // arrhythmia -> irregular_heartbeat
        .{ 13, 35 }, // hypertension -> high_bp
        .{ 20, 31 }, // respiratory -> shortness_of_breath
        .{ 21, 34 }, // asthma -> wheezing
    };

    // Drug-protein targeting: targets(drug, protein)
    const targets_data = [_]Pair{
        .{ 50, 40 }, // lisinopril targets ace_enzyme
        .{ 51, 41 }, // metoprolol targets beta_receptor
        .{ 52, 42 }, // amlodipine targets calcium_channel
    };

    // Protein-disease associations: associated_with(protein, disease)
    const assoc_data = [_]Pair{
        .{ 40, 13 }, // ace_enzyme associated_with hypertension
        .{ 40, 11 }, // ace_enzyme associated_with heart_disease
        .{ 41, 12 }, // beta_receptor associated_with arrhythmia
        .{ 41, 11 }, // beta_receptor associated_with heart_disease
        .{ 42, 13 }, // calcium_channel associated_with hypertension
        .{ 42, 12 }, // calcium_channel associated_with arrhythmia
    };

    const entityName = struct {
        fn get(id: u32) []const u8 {
            return switch (id) {
                10 => "cardiovascular",
                11 => "heart_disease",
                12 => "arrhythmia",
                13 => "hypertension",
                20 => "respiratory",
                21 => "asthma",
                30 => "chest_pain",
                31 => "shortness_of_breath",
                32 => "fatigue",
                33 => "irregular_heartbeat",
                34 => "wheezing",
                35 => "high_bp",
                40 => "ace_enzyme",
                41 => "beta_receptor",
                42 => "calcium_channel",
                50 => "lisinopril",
                51 => "metoprolol",
                52 => "amlodipine",
                else => "unknown",
            };
        }
    }.get;

    std.debug.print("Type hierarchy:\n", .{});
    for (is_a_data) |r| {
        std.debug.print("  {s} is_a {s}\n", .{ entityName(r[0]), entityName(r[1]) });
    }

    std.debug.print("\nDirect symptom associations:\n", .{});
    for (symptom_data) |s| {
        std.debug.print("  {s} has_symptom {s}\n", .{ entityName(s[0]), entityName(s[1]) });
    }

    std.debug.print("\nDrug targets:\n", .{});
    for (targets_data) |t| {
        std.debug.print("  {s} targets {s}\n", .{ entityName(t[0]), entityName(t[1]) });
    }

    std.debug.print("\nProtein-disease associations:\n", .{});
    for (assoc_data) |a| {
        std.debug.print("  {s} associated_with {s}\n", .{ entityName(a[0]), entityName(a[1]) });
    }

    // -- Build relations --

    var is_a_rel = try zodd.Relation(Pair).fromSlice(&ctx, &is_a_data);
    defer is_a_rel.deinit();

    var symptom_rel = try zodd.Relation(Pair).fromSlice(&ctx, &symptom_data);
    defer symptom_rel.deinit();

    var targets_rel = try zodd.Relation(Pair).fromSlice(&ctx, &targets_data);
    defer targets_rel.deinit();

    var assoc_rel = try zodd.Relation(Pair).fromSlice(&ctx, &assoc_data);
    defer assoc_rel.deinit();

    // -- Step 1: Compute transitive type hierarchy --
    //   is_a(X, Z) :- is_a(X, Y), is_a(Y, Z).

    var is_a = zodd.Variable(Pair).init(&ctx);
    defer is_a.deinit();
    try is_a.insertSlice(&ctx, is_a_rel.elements);

    std.debug.print("\nComputing transitive type hierarchy...\n", .{});

    const PairList = std.ArrayListUnmanaged(Pair);
    var iter: usize = 0;
    while (try is_a.changed()) : (iter += 1) {
        var results = PairList{};
        defer results.deinit(allocator);

        for (is_a.recent.elements) |r| {
            for (is_a_rel.elements) |base| {
                if (base[0] == r[1]) {
                    try results.append(allocator, .{ r[0], base[1] });
                }
            }
        }

        if (results.items.len > 0) {
            const rel = try zodd.Relation(Pair).fromSlice(&ctx, results.items);
            try is_a.insert(rel);
        }
        if (iter > 50) break;
    }

    var is_a_result = try is_a.complete();
    defer is_a_result.deinit();

    std.debug.print("\nFull type hierarchy (including inferred):\n", .{});
    for (is_a_result.elements) |r| {
        std.debug.print("  {s} is_a {s}\n", .{ entityName(r[0]), entityName(r[1]) });
    }

    // -- Step 2: Inherit symptoms through type hierarchy --
    //   has_symptom(D, S) :- is_a(D, D2), has_symptom(D2, S).

    var has_symptom = zodd.Variable(Pair).init(&ctx);
    defer has_symptom.deinit();
    try has_symptom.insertSlice(&ctx, symptom_rel.elements);

    // For each is_a(D, D2), propagate symptoms from D2 to D
    {
        var inherited = PairList{};
        defer inherited.deinit(allocator);

        for (is_a_result.elements) |r| {
            for (symptom_rel.elements) |s| {
                if (s[0] == r[1]) {
                    try inherited.append(allocator, .{ r[0], s[1] });
                }
            }
        }

        // Also propagate through the full transitive hierarchy
        // (symptoms inherited by parent are inherited by grandchild)
        for (is_a_result.elements) |r| {
            for (inherited.items) |s| {
                if (s[0] == r[1]) {
                    try inherited.append(allocator, .{ r[0], s[1] });
                }
            }
        }

        if (inherited.items.len > 0) {
            try has_symptom.insertSlice(&ctx, inherited.items);
        }
    }
    _ = try has_symptom.changed();

    var symptom_result = try has_symptom.complete();
    defer symptom_result.deinit();

    std.debug.print("\nAll symptoms (direct + inherited):\n", .{});
    for (symptom_result.elements) |s| {
        std.debug.print("  {s} has_symptom {s}\n", .{ entityName(s[0]), entityName(s[1]) });
    }

    // -- Step 3: Infer drug-disease relationships via joinInto --
    //   treats(Drug, D) :- targets(Drug, P), associated_with(P, D).
    //
    // Join key = Protein. targets is (Drug, Protein), assoc is (Protein, Disease).
    // Rekey targets as (Protein, Drug) to align the join key.

    var targets_by_protein = zodd.Variable(Pair).init(&ctx);
    defer targets_by_protein.deinit();
    {
        var flipped = PairList{};
        defer flipped.deinit(allocator);
        for (targets_rel.elements) |t| {
            try flipped.append(allocator, .{ t[1], t[0] }); // (Protein, Drug)
        }
        try targets_by_protein.insertSlice(&ctx, flipped.items);
        _ = try targets_by_protein.changed();
    }

    var assoc_var = zodd.Variable(Pair).init(&ctx);
    defer assoc_var.deinit();
    try assoc_var.insertSlice(&ctx, assoc_rel.elements);
    _ = try assoc_var.changed();

    const Triple = struct { u32, u32, u32 };
    var treats_triple = zodd.Variable(Triple).init(&ctx);
    defer treats_triple.deinit();

    // joinInto: key=Protein, val1=Drug, val2=Disease
    try zodd.joinInto(u32, u32, u32, Triple, &ctx, &targets_by_protein, &assoc_var, &treats_triple, struct {
        fn logic(_: *const u32, drug: *const u32, disease: *const u32) Triple {
            return .{ drug.*, disease.*, 0 };
        }
    }.logic);

    _ = try treats_triple.changed();

    // Extract (Drug, Disease) pairs
    var treats = PairList{};
    defer treats.deinit(allocator);

    for (treats_triple.recent.elements) |t| {
        try treats.append(allocator, .{ t[0], t[1] });
    }

    std.debug.print("\nInferred drug-disease relationships:\n", .{});
    for (treats.items) |t| {
        std.debug.print("  {s} treats {s}\n", .{ entityName(t[0]), entityName(t[1]) });
    }

    // -- Step 4: Predict potential side effects --
    //   side_effect(Drug, S) :- treats(Drug, D), has_symptom(D, S).

    std.debug.print("\nPotential side effects (drug treats disease that has symptom):\n", .{});
    var se_count: usize = 0;
    for (treats.items) |t| {
        for (symptom_result.elements) |s| {
            if (s[0] == t[1]) {
                std.debug.print("  {s} -> {s} (via {s})\n", .{
                    entityName(t[0]),
                    entityName(s[1]),
                    entityName(t[1]),
                });
                se_count += 1;
            }
        }
    }

    std.debug.print("\nSummary: {} type relations, {} symptom associations, {} drug-disease links, {} potential side effects\n", .{
        is_a_result.len(),
        symptom_result.len(),
        treats.items.len,
        se_count,
    });
}

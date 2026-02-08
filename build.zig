const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the zodd module
    const zodd_mod = b.addModule("zodd", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add Ordered dependency to zodd module
    const ordered_dep = b.dependency("ordered", .{
        .target = target,
        .optimize = optimize,
    });
    zodd_mod.addImport("ordered", ordered_dep.module("ordered"));

    // Static library artifact
    const lib = b.addLibrary(.{
        .name = "zodd",
        .linkage = .static,
        .root_module = zodd_mod,
    });
    b.installArtifact(lib);

    // Unit tests (embedded in src/lib.zig)
    const lib_tests = b.addTest(.{
        .root_module = zodd_mod,
        .name = "unit-tests",
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_lib_tests.step);

    // Discover and add tests from tests/ directory
    // (only available when developing zodd, not when used as a dependency)
    if (std.fs.cwd().openDir("tests", .{ .iterate = true })) |tests_dir| {
        // Lazy-load Minish dependency (only needed for property tests)
        const minish_dep = b.dependency("minish", .{
            .target = target,
            .optimize = optimize,
        });

        var dir = tests_dir;
        var it = dir.iterate();
        while (it.next() catch null) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".zig")) continue;

            const stem = entry.name[0 .. entry.name.len - 4];
            const src_rel = b.fmt("tests/{s}", .{entry.name});

            const test_mod = b.createModule(.{
                .root_source_file = b.path(src_rel),
                .target = target,
                .optimize = optimize,
            });
            test_mod.addImport("zodd", zodd_mod);

            // Property tests need Minish
            if (std.mem.eql(u8, stem, "property_tests")) {
                test_mod.addImport("minish", minish_dep.module("minish"));
            }

            const test_exe = b.addTest(.{
                .root_module = test_mod,
                .name = stem,
            });
            const run_test = b.addRunArtifact(test_exe);
            test_step.dependOn(&run_test.step);
        }
    } else |_| {}

    // Discover and add examples from examples/ directory
    if (std.fs.cwd().openDir("examples", .{ .iterate = true })) |examples_dir| {
        var dir = examples_dir;
        const run_all_step = b.step("run-all", "Run all examples");

        var it = dir.iterate();
        while (it.next() catch null) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".zig")) continue;

            const stem = entry.name[0 .. entry.name.len - 4];
            const src_rel = b.fmt("examples/{s}", .{entry.name});

            const example_mod = b.addModule(stem, .{
                .root_source_file = b.path(src_rel),
                .target = target,
                .optimize = optimize,
            });
            example_mod.addImport("zodd", zodd_mod);

            const exe = b.addExecutable(.{
                .name = stem,
                .root_module = example_mod,
            });
            b.installArtifact(exe);

            const run_cmd = b.addRunArtifact(exe);
            const step_name = b.fmt("run-{s}", .{stem});
            const run_example_step = b.step(step_name, b.fmt("Run example {s}", .{stem}));
            run_example_step.dependOn(&run_cmd.step);

            run_all_step.dependOn(run_example_step);
        }
    } else |_| {}

    // API Documentation
    const docs_step = b.step("docs", "Generate API documentation");

    const doc_obj = b.addObject(.{
        .name = "zodd",
        .root_module = zodd_mod,
    });

    const install_docs = b.addInstallDirectory(.{
        .source_dir = doc_obj.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });

    docs_step.dependOn(&install_docs.step);
}

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

    const io = b.graph.io;

    // Discover and add tests from tests/ directory
    // (only available when developing zodd, not when used as a dependency)
    if (b.build_root.handle.openDir(io, "tests", .{ .iterate = true })) |tests_dir| {
        // Lazy-load Minish dependency (only needed for property tests)
        const minish_dep = b.dependency("minish", .{
            .target = target,
            .optimize = optimize,
        });

        var dir = tests_dir;
        var it = dir.iterate();
        while (it.next(io) catch null) |entry| {
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
    if (b.build_root.handle.openDir(io, "examples", .{ .iterate = true })) |examples_dir| {
        var dir = examples_dir;
        const run_all_step = b.step("run-all", "Run all examples");

        var it = dir.iterate();
        while (it.next(io) catch null) |entry| {
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

    // Web frontend (see web/)
    {
        const wasm_target = b.resolveTargetQuery(.{
            .cpu_arch = .wasm32,
            .os_tag = .freestanding,
        });
        const wasm_optimize: std.builtin.OptimizeMode = .ReleaseSmall;

        // A second zodd module instance bound to the wasm target.
        const zodd_wasm_mod = b.createModule(.{
            .root_source_file = b.path("src/lib.zig"),
            .target = wasm_target,
            .optimize = wasm_optimize,
        });
        const ordered_wasm_dep = b.dependency("ordered", .{
            .target = wasm_target,
            .optimize = wasm_optimize,
        });
        zodd_wasm_mod.addImport("ordered", ordered_wasm_dep.module("ordered"));

        const build_options = b.addOptions();
        const version = getVersion(b);
        build_options.addOption([]const u8, "version", version);
        const commit = getGitInfo(b);
        build_options.addOption([]const u8, "commit", commit);

        const wasm_mod = b.createModule(.{
            .root_source_file = b.path("web/zodd_wasm.zig"),
            .target = wasm_target,
            .optimize = wasm_optimize,
        });
        wasm_mod.addImport("zodd", zodd_wasm_mod);
        wasm_mod.addImport("build_options", build_options.createModule());

        const wasm_exe = b.addExecutable(.{
            .name = "zodd",
            .root_module = wasm_mod,
        });
        wasm_exe.entry = .disabled; // No _start; the module is a library.
        wasm_exe.rdynamic = true; // Export the `export fn` symbols.

        const install_wasm = b.addInstallFile(wasm_exe.getEmittedBin(), "web/zodd.wasm");
        const wasm_step = b.step("wasm", "Build the web frontend Wasm module");
        wasm_step.dependOn(&install_wasm.step);
    }

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

fn getVersion(b: *std.Build) []const u8 {
    const zon_content = b.build_root.handle.readFileAlloc(b.graph.io, "build.zig.zon", b.allocator, .unlimited) catch return "unknown";
    if (std.mem.indexOf(u8, zon_content, ".version = \"")) |index| {
        const start = index + ".version = \"".len;
        if (std.mem.indexOfScalarPos(u8, zon_content, start, '"')) |end| {
            return zon_content[start..end];
        }
    }
    return "unknown";
}

fn getGitInfo(b: *std.Build) []const u8 {
    var code1: u8 = 0;
    const branch = b.runAllowFail(&[_][]const u8{ "git", "branch", "--show-current" }, &code1, .ignore) catch "";
    var code2: u8 = 0;
    const hash = b.runAllowFail(&[_][]const u8{ "git", "rev-parse", "--short", "HEAD" }, &code2, .ignore) catch "";

    const clean_branch = std.mem.trim(u8, branch, " \t\r\n");
    const clean_hash = std.mem.trim(u8, hash, " \t\r\n");

    if (clean_branch.len > 0 and clean_hash.len > 0) {
        return b.fmt("{s}@{s}", .{ clean_branch, clean_hash });
    }
    return "unknown";
}

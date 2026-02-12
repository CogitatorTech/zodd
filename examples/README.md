### Examples

#### List of Examples

| # | File                                                   | Description                                                              |
|---|--------------------------------------------------------|--------------------------------------------------------------------------|
| 1 | [e1_transitive_closure.zig](e1_transitive_closure.zig) | Computes the transitive closure of a directed graph using Datalog rules. |
| 2 | [e2_same_generation.zig](e2_same_generation.zig)       | Finds all pairs of nodes at the same depth in a hierarchy.               |
| 3 | [e3_points_to_analysis.zig](e3_points_to_analysis.zig) | Performs points-to analysis for a program.                               |
| 4 | [e4_rbac_authorization.zig](e4_rbac_authorization.zig) | RBAC authorization with role hierarchy, joins, and denial filtering.     |
| 5 | [e5_taint_analysis.zig](e5_taint_analysis.zig)         | Security taint analysis using leapfrog trie join for taint propagation.  |
| 6 | [e6_dependency_resolution.zig](e6_dependency_resolution.zig) | Package dependency resolution with aggregation and reverse-dep index. |

#### Running Examples

To execute an example, run the following command from the root of the repository:

```sh
zig build run-{FILE_NAME_WITHOUT_EXTENSION}
```

For example:

```sh
zig build run-e1_transitive_closure
zig build run-e2_same_generation
zig build run-e3_points_to_analysis
zig build run-e4_rbac_authorization
zig build run-e5_taint_analysis
zig build run-e6_dependency_resolution
```


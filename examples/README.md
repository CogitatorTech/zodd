### Examples

#### List of Examples

| # | File                                                   | Description                                                                                   |
|---|--------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| 1 | [e1_transitive_closure.zig](e1_transitive_closure.zig) | Computes the transitive closure of a directed graph using Datalog rules.                      |
| 2 | [e2_same_generation.zig](e2_same_generation.zig)       | Finds all pairs of nodes at the same depth in a hierarchy (siblings, cousins).                |
| 3 | [e3_points_to_analysis.zig](e3_points_to_analysis.zig) | Performs points-to analysis for program analysis (alloc, assign, load, and store operations). |

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
```

### Examples

#### List of Examples

| # | File                                                         | Description                                                              |
|---|--------------------------------------------------------------|--------------------------------------------------------------------------|
| 1 | [e1_network_reachability.zig](e1_network_reachability.zig)   | Network zone reachability based on routing and firewall rules.           |
| 2 | [e2_knowledge_graph.zig](e2_knowledge_graph.zig)             | Ontology reasoning with type hierarchies and drug-disease relationships. |
| 3 | [e3_data_lineage.zig](e3_data_lineage.zig)                   | Data lineage tracking through transformations and anonymization.         |
| 4 | [e4_rbac_authorization.zig](e4_rbac_authorization.zig)       | RBAC authorization with role hierarchies and explicit denials.           |
| 5 | [e5_taint_analysis.zig](e5_taint_analysis.zig)               | Taint analysis tracing untrusted inputs to program sinks.                |
| 6 | [e6_dependency_resolution.zig](e6_dependency_resolution.zig) | Package dependency resolution with size aggregation and indexes.         |
| 7 | [e7_datalog_frontend.zig](e7_datalog_frontend.zig)           | Datalog parser, evaluator, and query frontend.                           |
| 8 | [e8_comparison_filters.zig](e8_comparison_filters.zig)       | Comparison filters to monitor latencies against SLA limits.              |
| 9 | [e9_arithmetic_hops.zig](e9_arithmetic_hops.zig)             | Arithmetic assignments counting network hops under an iteration limit.   |

#### Running Examples

To execute an example, run the following command from the root of the repository:

```sh
zig build run-{FILE_NAME_WITHOUT_EXTENSION}
```

For example:

```sh
zig build run-e1_network_reachability
zig build run-e2_knowledge_graph
zig build run-e3_data_lineage
zig build run-e4_rbac_authorization
zig build run-e5_taint_analysis
zig build run-e6_dependency_resolution
zig build run-e7_datalog_frontend
zig build run-e8_comparison_filters
zig build run-e9_arithmetic_hops
```

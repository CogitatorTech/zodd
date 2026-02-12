### Examples

#### List of Examples

| # | File                                                         | Description                                                                |
|---|--------------------------------------------------------------|----------------------------------------------------------------------------|
| 1 | [e1_network_reachability.zig](e1_network_reachability.zig)   | Network zone reachability through routing and firewall rule analysis.      |
| 2 | [e2_knowledge_graph.zig](e2_knowledge_graph.zig)             | Medical ontology reasoning with type hierarchy and drug-disease inference. |
| 3 | [e3_data_lineage.zig](e3_data_lineage.zig)                   | Data lineage tracking for GDPR compliance with PII propagation.            |
| 4 | [e4_rbac_authorization.zig](e4_rbac_authorization.zig)       | RBAC authorization with role hierarchy, joins, and denial filtering.       |
| 5 | [e5_taint_analysis.zig](e5_taint_analysis.zig)               | Security taint analysis using leapfrog trie join for taint propagation.    |
| 6 | [e6_dependency_resolution.zig](e6_dependency_resolution.zig) | Package dependency resolution with aggregation and reverse-dep index.      |

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
```

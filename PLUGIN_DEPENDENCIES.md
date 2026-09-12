# Plugin dependencies

Catalyst requires the Plugins-owned `dataset.preparation.v1` capability for
inspect, prepare, and transform operations. Platform may install/activate the
package and return an opaque `connection_ref`; Catalyst then invokes it directly
with the Plugins-owned `cyrene-plugin-runtime` SDK.

The Product owns Dataset/Preparation state, policy, lineage, publication, and
handoffs. The Plugin owns the payload contract and concrete parsing,
normalization, deduplication, grouping, transformation, and split algorithms.
Missing bindings fail closed; there is no local DuckDB implementation fallback.

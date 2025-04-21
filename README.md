# Aggregation Index Tree

A specialized balanced binary tree-like data structure that stores pre-aggregated values at nodes to accelerate aggregation operations.

## Overview

The Aggregation Index Tree (AIT) is designed to optimize aggregation operations by pre-computing and caching aggregations at each node in the tree. This approach significantly accelerates both global and filtered aggregation queries compared to traditional columnar storage methods.

## Structure

The AIT consists of three main components:

```rust
struct AggregationIndexTree {
    nodes: Vec<AggregationTreeNode>,  // Tree nodes storing the hierarchy
    doc_id_map: HashMap<u32, usize>,  // Maps original doc_id to position in sorted values
    position_map: Vec<(usize, usize)>, // Maps position to (node_idx, offset_in_node)
}
```

Each node in the tree is either:

1. **Internal Node** - Contains:
   - A split value
   - References to left and right children
   - Pre-computed aggregations

2. **Leaf Node** - Contains:
   - Document IDs
   - Actual values
   - Pre-computed aggregations

```rust
enum AggregationTreeNode {
    Internal {
        split_value: f64,
        left: usize,
        right: usize,
        aggregations: NodeAggregations,
    },
    Leaf {
        doc_ids: Vec<u32>,
        values: Vec<f64>,
        aggregations: NodeAggregations,
    },
}
```

Each node maintains aggregation statistics:

```rust
struct NodeAggregations {
    min_value: f64,
    max_value: f64,
    sum: f64,
    count: u32,
}
```

## Performance Benchmarks

Performance was evaluated with the following configuration:
- **Documents**: 10,000,000
- **Filter percentage**: 1%
- **Leaf size**: 64
- **Iterations**: 5

### Results

| Metric | AIT | Traditional Columnar | Improvement |
|--------|-----|----------------------|-------------|
| Build time | 290.06ms | 71.77ms | - |
| Memory usage | 326.44 MB | 76.29 MB | 4.28x higher |
| Global aggregation time | 150ns | 9.39ms | 62,570x faster |
| Filtered aggregation time (1%) | 8.16ms | 89.96ms | 11.02x faster |

### Memory-Performance Tradeoff

The AIT uses approximately 4.28x more memory than traditional columnar storage but delivers:
- **62,570x** speedup for global aggregations
- **11.02x** speedup for filtered aggregations (1% of data)

## Key Features

- **Pre-computed aggregations**: Each node stores min, max, sum, and count
- **Fast lookup**: Efficient mapping from document IDs to tree positions
- **Balanced structure**: Similar to a balanced binary tree for consistent performance
- **Efficient filtering**: Quickly prunes branches that don't match filter criteria

## Use Cases

The AIT is particularly effective for:
- Real-time analytics dashboards requiring sub-millisecond response times
- Applications with frequent aggregation queries on the same dataset
- Systems that prioritize query speed over memory usage
- Scenarios with both global and filtered aggregation requirements


## Summary

The Aggregation Index Tree provides exceptional query performance for aggregation operations with a reasonable memory overhead. It's ideal for scenarios where query speed is critical and the additional memory usage is acceptable.

- **Build time**: 290.06ms
- **Memory overhead**: 4.28x compared to columnar storage
- **Global query speedup**: 62,570x
- **Filtered query speedup**: 11.02x
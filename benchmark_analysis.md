# Benchmark Analysis: Replacing `collect()` + `isin()` with `join()` in NB_Load_Bronze_From_SERPRO

## Overview
This document analyzes the performance optimization of replacing a `collect()` + `isin()` pattern with a Spark `join()` operation in `VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_From_SERPRO.Notebook/notebook-content.py`.

## The Issue
The original code uses the following pattern:
1. Filters a large DataFrame (`serpro_biddings_df` or `serpro_contracts_df`).
2. Collects the distinct IDs (`Número Licitação` or `Número Contrato`) to the driver as a Python list using `.collect()`.
3. Filters subsequent DataFrames using `.filter(col(...).isin(list))`.

### Performance Bottlenecks:
1.  **Driver Memory Pressure**: `collect()` brings all data to the driver node. If the number of distinct IDs is large, this can cause an OutOfMemoryError on the driver.
2.  **Serialization Overhead**: Transferring data from executors to the driver and back (for broadcasting the list in `isin()`) incurs significant serialization/deserialization costs.
3.  **Scalability Limit**: The approach is not scalable. As the dataset grows, the driver becomes a single point of failure.
4.  **Inefficient Filtering**: `isin()` with a large list is generally less efficient than a optimized join, as it may be implemented as a broadcast of the list to all tasks, which has limits.

## The Solution
The optimization replaces the `collect()` + `isin()` pattern with a `left_semi` join.

### Benefits:
1.  **Distributed Execution**: The join operation is fully distributed and executed on the cluster. Data remains on executors.
2.  **Scalability**: Handles arbitrarily large datasets without overwhelming the driver.
3.  **Optimizer Leverage**: Spark's Catalyst optimizer can choose the most efficient join strategy (e.g., BroadcastHashJoin if the filtered IDs are small enough, or SortMergeJoin if they are large).
4.  **Reduced Network I/O**: Eliminates the round-trip of data to the driver.

## Verification
Due to the lack of a Spark environment in the current CI/CD pipeline, direct benchmarking (measuring execution time) is not possible. However, this is a standard Spark optimization pattern. Correctness will be verified via unit tests mocking the Spark API to ensure the logical transformation is correct.

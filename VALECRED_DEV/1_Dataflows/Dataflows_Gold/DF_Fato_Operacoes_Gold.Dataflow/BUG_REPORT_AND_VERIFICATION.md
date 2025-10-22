# Bug Report and Verification for DF_Fato_Operacoes_Gold

## 1. Bug Identification

*   **File**: `mashup.pq`
*   **Query**: `fato_operacoes_recompra`
*   **Issue**: A division-by-zero error occurs during the calculation of the `tarifas_de_recompra` column.
*   **Failing Code**: `Table.AddColumn(..., "tarifas_de_recompra", each [tot_tar] / [n_docs_recompra])`
*   **Impact**: The error causes the entire `DF_Fato_Operacoes_Gold` dataflow refresh to fail.

## 2. Test Case Definition

This test case is designed to fail before the fix and pass after its implementation. Due to the limitations of the development environment (inability to execute dataflows directly), this test is documented here as a manual verification step.

### Test Data

A sample table would be used as the source, containing at least one row where `n_docs_recompra` is `0`.

| cod_operacao | tot_tar | n_docs_recompra |
|--------------|---------|-----------------|
| 1            | 100     | 2               |
| 2            | 200     | 0               |
| 3            | 150     | 5               |

### Verification Steps

#### Before the Fix (Expected Failure)

1.  Create a new test query in Power Query.
2.  Use the sample data above as the source.
3.  Add a custom column using the original, buggy formula:
    ```m
    = Table.AddColumn(Source, "tarifas_de_recompra", each [tot_tar] / [n_docs_recompra])
    ```
4.  **Expected Result**: The query refresh will fail with a `Expression.Error: There was an error in the report. Details: [DataSource.Error] An error occurred while processing the report.` (or similar division-by-zero error) when it processes the row where `cod_operacao` is 2.

#### After the Fix (Expected Success)

1.  Create a new test query in Power Query.
2.  Use the sample data above as the source.
3.  Add a custom column using the corrected, safe formula:
    ```m
    = Table.AddColumn(Source, "tarifas_de_recompra", each if [n_docs_recompra] > 0 then [tot_tar] / [n_docs_recompra] else 0)
    ```
4.  **Expected Result**: The query will refresh successfully. The resulting table will be:

| cod_operacao | tot_tar | n_docs_recompra | tarifas_de_recompra |
|--------------|---------|-----------------|---------------------|
| 1            | 100     | 2               | 50                  |
| 2            | 200     | 0               | 0                   |
| 3            | 150     | 5               | 30                  |

## 3. Implemented Fix

The formula in the `fato_operacoes_recompra` query within `mashup.pq` has been updated to prevent the division-by-zero error.

*   **Corrected Code**: `Table.AddColumn(..., "tarifas_de_recompra", each if [n_docs_recompra] > 0 then [tot_tar] / [n_docs_recompra] else 0)`

This change ensures the dataflow's stability and resilience to invalid data.

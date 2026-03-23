# Scribe's Daily Documentation Sync

## Change Log

### Changed

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| Cluster Missing Diagnosis Script | `tests/diagnose_cluster_missing.py` | Updated the script to use a `full_outer` join when generating features from `df_metrics_pagos` and `df_metrics_risco` to diagnose cluster missing clients. | Changed |
| `DF_Preparacao_Silver.Dataflow` | `VALECRED_DEV/1_Dataflows/Dataflows_Silver/DF_Preparacao_Silver.Dataflow/mashup.pq` | Removed `FLOATING` column extraction. | Removed |
| `VAI_Inferencia_Online.Notebook` | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Added functions `create_progress_bar` and `display_terminal_dashboard`. | Added |

1. **Remove `FLOATING` from removal lists in Dataflows:**
   Already done via python script for `DF_Preparacao_Silver.Dataflow/mashup.pq`. Wait, I need to check `VALECRED_DEV/1_Dataflows/Dataflows_Silver/DF_Fato_Operacoes_Silver.Dataflow/mashup.pq` too just in case.

2. **Add `floating` to Silver Notebook Schema:**
   Modify `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py`:
   - Add `col("FLOATING").alias("floating")` inside `get_operacoes_schema`.

3. **Add `floating` to Gold Notebook Schema:**
   Modify `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py`:
   - Add `col("floating")` inside `select_fato_operacoes_columns`.

4. **Update `NB_Gold_Relatorio_Produtos_Mensal.Notebook`:**
   - Instead of grouping by `cod_cliente` and showing client details (nome_cliente, grupo_economico, nome_gerente), remove them completely? The user said "remover medidas agrupadas por clientes... preciso medir apenas as operações".
   - Replace `df_clientes` load with just using operation data.
   - Remove `nome_cliente`, `grupo_economico`, `nome_gerente`, `cod_cliente` from `df_final.select`? Yes, "remover medidas agrupadas por clientes". Let's remove them and `cod_cliente` from `groupBy` everywhere. Wait! Operations are unique by `cod_operacao`. Even if we group by `cod_cliente, cod_operacao` it's the exact same result! BUT, maybe the user wants to group by `mes_ref` and `nome_plataforma` and not have it split by client! "está cheio de medidas de clientes que não serve para nada, preciso medir apenas as operações... remover medidas agrupadas por clientes." - If the user wants to aggregate all operations into one report without client level, then grouping by client would cause one row per client per operation! But wait, `cod_operacao` is unique! If I group by `cod_operacao`, each row is exactly one operation. `cod_cliente` is also unique per operation. So removing `cod_cliente` from the `groupBy` alongside `cod_operacao` changes literally nothing to the row count! It just removes the column.
   Wait, if it's "remover medidas agrupadas por clientes", maybe it means removing `cod_cliente` and the associated columns from the report entirely.
   - For `process_operacoes_stream`: Calculate `prazo_medio` and `prazo_medio_original` using `data_aceite` instead of `data_deferimento`. "não consigo achar o prazo médio verdadeiro dos titulos, datediff(dataaceite da operação, vencimento do título, dias)."
   - Include the `floating` field from `fato_operacoes`.
   - Update `df_map_ops` to also map `floating` and `data_aceite` (or just add `floating` and `data_aceite` to `df_ops`). Wait, `df_ops` uses `data_deferimento` for `mes_ref`. Should it use `data_aceite` everywhere? The user only specified `datediff(dataaceite da operação, vencimento do título, dias)`. So just for the `prazo_medio` calculation.

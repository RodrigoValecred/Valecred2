import json

old_code = """
let
    // 1. CARGA DE DADOS
    // Define a tabela de origem com o histórico de alterações de status
    Fonte = stg_pareceres_alteracao_status,

    // 2. CRIAÇÃO DA CHAVE DE RELACIONAMENTO (Próxima Linha)
    // Cria um novo índice somando 1 para preparar o cruzamento com a linha seguinte
    #"Adição Inserida" = Table.AddColumn(Fonte, "Adição", each [Índice] + 1, type number),

    // Gera uma chave única combinando o novo índice e o código do cliente
    #"Personalização Adicionada3" = Table.AddColumn(#"Adição Inserida", "chave_secundaria", each [Adição]*1000000000+[CODCLIENTE]),

    // Garante que a chave secundária seja tratada como número inteiro
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Personalização Adicionada3",{{"chave_secundaria", Int64.Type}}),

    // 3. CRUZAMENTO DE DADOS (PROCV / JOIN)
    // Faz um Left Join com a própria tabela (usando a tabela original mapeada em outra consulta) para buscar os dados do status seguinte
    #"Consultas Mescladas" = Table.NestedJoin(#"Tipo Alterado", {"chave_secundaria"}, stg_pareceres_alteracao_status, {"chave_original"}, "esteira de propostas", JoinKind.LeftOuter),

    // Expande os dados retornados do Join, trazendo as informações do "próximo" status
    // Nota: Apesar do nome da coluna gerada conter "ANTERIOR", a lógica do índice + 1 na verdade busca o evento futuro/posterior
    #"esteira de propostas Expandido1" = Table.ExpandTableColumn(#"Consultas Mescladas", "esteira de propostas", {"STATUS DO CLIENTE", "DATALOG",  "MACROPROCESSO", "FASE",  "chave_original"}, {"STATUS DO CLIENTE ANTERIOR", "DATALOG ANTERIOR", "MACROPROCESSO ANTERIOR", "FASE ANTERIOR", "chave_anterior"}),

    // 4. FILTRAGEM DE ALTERAÇÕES REAIS
    // Cria um validador booleano (True/False) para verificar se o status atual é igual ao próximo status
    #"Personalização Adicionada" = Table.AddColumn(#"esteira de propostas Expandido1", "VALIDADOR", each [STATUS DO CLIENTE]=[STATUS DO CLIENTE ANTERIOR]),

    // Mantém apenas as linhas onde o validador é Falso, ou seja, onde houve de fato uma MUDANÇA de status
    #"Linhas Filtradas1" = Table.SelectRows(#"Personalização Adicionada", each ([VALIDADOR] = false)),

    // Remove colunas técnicas e intermediárias que perderam a utilidade após o filtro
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas1",{"chave_original", "Adição", "chave_secundaria", "VALIDADOR"}),

    // 5. ANÁLISE DE FLUXO (DEVOLUÇÃO VS RECEBIDA)
    // Identifica se o processo voltou da área de Crédito para a Comercial (Devolução)
    #"Personalização Adicionada1" = Table.AddColumn(#"Colunas Removidas", "Devolução", each [MACROPROCESSO]="CREDITO" and  [MACROPROCESSO ANTERIOR]="COMERCIAL"),

    // Identifica se o processo avançou ou foi recebido da Comercial pela área de Crédito
    #"Personalização Adicionada2" = Table.AddColumn(#"Personalização Adicionada1", "Recebida", each [MACROPROCESSO]="COMERCIAL" and [MACROPROCESSO ANTERIOR]="CREDITO"),

    // 6. FORMATAÇÃO E LIMPEZA FINAL
    // Reorganiza as colunas em uma ordem lógica de leitura
    #"Colunas Reordenadas" = Table.ReorderColumns(#"Personalização Adicionada2",{"Índice", "CODCLIENTE", "BASE", "DATALOG ANTERIOR", "DATALOG", "chave_base_cliente", "STATUS DO CLIENTE ANTERIOR", "STATUS DO CLIENTE", "MACROPROCESSO ANTERIOR", "MACROPROCESSO", "FASE ANTERIOR", "FASE", "Usuário", "CODSTATUSCLIENTE", "Devolução", "Recebida"}),

    // Remove o código de status que não é mais necessário
    #"Colunas Removidas1" = Table.RemoveColumns(#"Colunas Reordenadas",{"CODSTATUSCLIENTE"}),

    // Renomeia as colunas para o português padrão, corrigindo os sufixos para refletir a realidade dos dados ("posterior")
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas1",{{"STATUS DO CLIENTE ANTERIOR", "Status do cliente posterior"}, {"STATUS DO CLIENTE", "Status do cliente"}, {"MACROPROCESSO ANTERIOR", "Macroprocesso posterior"}, {"MACROPROCESSO", "Macroprocesso"}, {"FASE ANTERIOR", "Fase posterior"}, {"FASE", "Fase"}, {"DATALOG ANTERIOR", "Data posterior"}, {"DATALOG", "Data"}}),
    #"Tipo Alterado1" = Table.TransformColumnTypes(#"Colunas Renomeadas",{{"Devolução", type logical}, {"Recebida", type logical}})
in
    #"Tipo Alterado1"
"""

import re

# Extract final columns from old code
match = re.search(r'#"Colunas Reordenadas"\s*=\s*Table\.ReorderColumns\([^,]+,\{(.*?)\}\)', old_code)
if match:
    cols = match.group(1).replace('"', '').split(', ')
    cols = [c.strip() for c in cols]
    print(f"Colunas originais antes do renomear: {cols}")

match_rename = re.search(r'#"Colunas Renomeadas"\s*=\s*Table\.RenameColumns\([^,]+,\{(.*?)\}\)', old_code)
if match_rename:
    renames_str = match_rename.group(1)
    renames = re.findall(r'\{"([^"]+)",\s*"([^"]+)"\}', renames_str)
    rename_map = dict(renames)
    print(f"Mapa de renomeacao: {rename_map}")

    final_cols = []
    for c in cols:
        if c == 'CODSTATUSCLIENTE':
            continue
        final_cols.append(rename_map.get(c, c))
    print(f"Colunas finais esperadas: {final_cols}")

# Guia de Migração: Modelo de Previsão de Inadimplência 2025

**Status:** Arquivado (Aguardando Infraestrutura Python no Servidor)
**Script Original:** `VALECRED_DEV/6_Machine_Learning/ML_Previsao_Inadimplencia_2025.Notebook/notebook-content.py`
**Data de Desativação:** [Preencher Data]
**Motivo:** Migração do Microsoft Fabric (Lakehouse) para infraestrutura on-premise/Power BI. A linguagem nativa Power Query (M) não suporta a execução de modelos `.joblib` treinados em Python sem um Gateway Pessoal configurado com ambiente Python.

---

## 1. Visão Geral da Lógica Original (PySpark)

O script original realizava as seguintes etapas:
1. **Leitura e Filtragem de Dados (Predicate Pushdown):**
   - Lida da tabela `staging_titulos_limpa` filtrando apenas títulos em aberto (`LIQUIDACAO` nula) e removendo tipos de documentos (ex: 'BL').
   - Lida da tabela `staging_operacoes_limpa` para obter apenas operações deferidas e aceitas (`STATUSANALISE = 'D'` e `STATUSACEITE = 'A'`).
   - Lida da tabela `dim_cliente` (Cedentes).
   - Lida da tabela `staging_cad_geral_limpa` para informações geográficas (`CIDADE`, `UF`).

2. **Criação da Tabela Mestra:**
   - Realizava um `INNER JOIN` entre Títulos e Operações.
   - Realizava `LEFT JOIN` com Cedentes e Cadastro Geral.
   - Renomeava as colunas vindas do Cedente adicionando o sufixo `_CEDENTE`.

3. **Carga do Modelo e Features:**
   - Carregava o modelo `credit_risk_model_v2.joblib` e a lista de features `model_features_v2.joblib` do sistema de arquivos.

4. **Inferência Distribuída (Pandas UDF):**
   - Usava uma função `@pandas_udf` otimizada com Scalar Iterators.
   - Reconstruía o Pandas DataFrame, aplicava tipagem categórica e executava o `predict_proba`.

5. **Exportação:**
   - Salvava os resultados na tabela `LH_Silver.previsao_inadimplencia_2025`.

---

## 2. Preparação de Dados em Power Query M (Para Futura Reativação)

Abaixo está o script Power Query M que recria a "Tabela Mestra" de dados (Passos 1 e 2). Quando o ambiente Python estiver disponível no Gateway, este código poderá ser usado para gerar o dataset base.

```powerquery
let
    // ------------------------------------------------------------------
    // 1. CARGA DAS FONTES (Substitua pelos nomes reais no PBI)
    // ------------------------------------------------------------------
    Fonte_Titulos = staging_titulos_limpa,
    Fonte_Operacoes = staging_operacoes_limpa,
    Fonte_Cedentes = dim_cliente,
    Fonte_CadGeral = staging_cad_geral_limpa,

    // ------------------------------------------------------------------
    // 2. FILTROS INICIAIS (Equivalente ao Predicate Pushdown)
    // ------------------------------------------------------------------
    // Títulos: Liquidacao nula e TDOC diferente de 'BL' e outros
    #"Filtro Titulos 1" = Table.SelectRows(Fonte_Titulos, each [LIQUIDACAO] = null),
    #"Filtro Titulos Final" = Table.SelectRows(#"Filtro Titulos 1", each not List.Contains({"BL"}, [TDOC])),

    // Operações: Deferidas, Aceitas, e excluindo certos TTO
    TiposExcluir = {"RE","RC","PR","AB","AM","LB","PB"},
    #"Filtro Ops 1" = Table.SelectRows(Fonte_Operacoes, each [STATUSANALISE] = "D" and [STATUSACEITE] = "A" and [ACEITO] = "S"),
    #"Filtro Ops Final" = Table.SelectRows(#"Filtro Ops 1", each not List.Contains(TiposExcluir, [TTO])),

    // ------------------------------------------------------------------
    // 3. RENOMEANDO COLUNAS DO CEDENTE (Sufixo _CEDENTE)
    // ------------------------------------------------------------------
    #"Cedentes Renomeados" = Table.RenameColumns(Fonte_Cedentes,{
        {"DATAINCLUSAO", "DATAINCLUSAO_CEDENTE"},
        {"USUAINCLUSAO", "USUAINCLUSAO_CEDENTE"},
        {"DATAALTERACAO", "DATAALTERACAO_CEDENTE"},
        {"USUAALTERACAO", "USUAALTERACAO_CEDENTE"},
        {"CODRATING", "CODRATING_CEDENTE"},
        {"PEFIN", "PEFIN_CEDENTE"},
        {"BAIXADOPEFIN", "BAIXADOPEFIN_CEDENTE"},
        {"PREIMPRESSO", "PREIMPRESSO_CEDENTE"},
        {"BOLETOESPECIAL", "BOLETOESPECIAL_CEDENTE"},
        {"TARIFARECOMPRA", "TARIFARECOMPRA_CEDENTE"},
        {"RECEBEBOLETO", "RECEBEBOLETO_CEDENTE"}
    }, MissingField.Ignore),

    // ------------------------------------------------------------------
    // 4. TABELA MESTRA (JOINS)
    // ------------------------------------------------------------------
    // Inner Join: Titulos e Operacoes (Usa NestedJoin + remoção de nulos para simular Inner)
    #"Merge Titulos_Ops" = Table.NestedJoin(#"Filtro Titulos Final", {"CODOPERACAO"}, #"Filtro Ops Final", {"CODOPERACAO"}, "Ops", JoinKind.Inner),
    // Expanda TODAS as colunas que vieram da tabela de operações e que o modelo precisa
    #"Expande Ops" = Table.ExpandTableColumn(#"Merge Titulos_Ops", "Ops", {"CODCLIENTE", "STATUSANALISE", "TTO", "STTO", "FATOR"}), // Ajuste as colunas conforme necessário

    // Left Join com Cedentes
    #"Merge Cedentes" = Table.NestedJoin(#"Expande Ops", {"CODCLIENTE"}, #"Cedentes Renomeados", {"CODCLIENTE"}, "Cedentes", JoinKind.LeftOuter),
    // Expanda as colunas de Cedentes
    #"Expande Cedentes" = Table.ExpandTableColumn(#"Merge Cedentes", "Cedentes", {"DATAINCLUSAO_CEDENTE", "CODRATING_CEDENTE"}), // Ajuste as colunas

    // Left Join com Cadastro Geral (Para Cidade e UF)
    #"Merge CadGeral" = Table.NestedJoin(#"Expande Cedentes", {"CPFCNPJ"}, Fonte_CadGeral, {"CPFCNPJ"}, "CadGeral", JoinKind.LeftOuter),
    #"Expande CadGeral" = Table.ExpandTableColumn(#"Merge CadGeral", "CadGeral", {"CIDADE", "UF"})
in
    #"Expande CadGeral"
```

---

## 3. Instruções para o Futuro (Como ativar o Modelo no Power BI)

Quando o Python for instalado na máquina/servidor onde o Gateway Pessoal do Power BI está configurado:

1. Assegure-se de instalar as dependências via pip (`pandas`, `scikit-learn`, `joblib`).
2. Coleque os arquivos `credit_risk_model_v2.joblib` e `model_features_v2.joblib` em uma pasta acessível pelo Gateway (Ex: `C:\Modelos_ML\`).
3. No Power Query, após a etapa `"Expande CadGeral"` do código M acima, clique em **"Executar Script Python"** na aba Transformar.
4. Utilize o seguinte script base (adaptado do Pandas UDF antigo) para chamar o modelo no dataframe `dataset` (variável nativa do Power Query):

```python
import pandas as pd
import joblib

# 1. Carregar artefatos do disco local
model = joblib.load('C:/Modelos_ML/credit_risk_model_v2.joblib')
features = joblib.load('C:/Modelos_ML/model_features_v2.joblib')

# O Power Query automaticamente passa os dados anteriores na variável 'dataset'
X = dataset.copy()

# 2. Assegurar as features e ordem correta
X_model = X[features].copy()

# 3. Tratamento categórico necessário pelo Pipeline Scikit-Learn
categorical_cols = ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']
for col in categorical_cols:
    if col in X_model.columns:
        X_model[col] = X_model[col].astype('category')

# 4. Executar inferência (Probabilidade Classe 1)
dataset['PROBABILIDADE_INADIMPLENCIA'] = model.predict_proba(X_model)[:, 1]
```

5. O resultado final será o Dataset com a nova coluna `PROBABILIDADE_INADIMPLENCIA`, exatamente como na tabela `LH_Silver.previsao_inadimplencia_2025`.

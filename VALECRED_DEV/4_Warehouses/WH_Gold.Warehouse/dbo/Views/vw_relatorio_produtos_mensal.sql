CREATE VIEW [dbo].[vw_relatorio_produtos_mensal] AS
WITH
    -- 1. Dimensão Clientes
    BaseClientes AS (
        SELECT cod_cliente, nome, grupo_economico, nome_gerente
        FROM (
            SELECT cod_cliente, nome, grupo_economico, nome_gerente,
                   ROW_NUMBER() OVER(PARTITION BY cod_cliente ORDER BY cod_cliente) as rn
            FROM [dbo].[dim_clientes]
        ) t
        WHERE rn = 1
    ),

    -- Contingência de Plataforma (Cadastre-se -> Bridge -> Gerente -> Plataforma)
    PlataformaClientes AS (
        SELECT b.cod_cliente, p.nome_plataforma AS nome_plataforma_cli
        FROM (
            SELECT cod_cliente, cod_gerente,
                   ROW_NUMBER() OVER(PARTITION BY cod_cliente ORDER BY cod_cliente) as rn
            FROM [dbo].[bridge_cliente_gerente]
            WHERE data_fim_vigencia = '9999-12-31'
        ) b
        LEFT JOIN [dbo].[staging_gerentes] g ON b.cod_gerente = g.cod_broker
        LEFT JOIN [dbo].[staging_plataformas] p ON g.cod_agencia = p.cod_agencia
        WHERE b.rn = 1 AND p.nome_plataforma IS NOT NULL
    ),

    -- Tabela de Mapeamento para Enriquecer Prorrogações e Mora
    MapOps AS (
        SELECT cod_operacao, nbordero AS nbordero_op, nome_plataforma AS nome_plataforma_op,
               chave_produto AS chave_produto_op, data_deferimento AS data_deferimento_op,
               cod_cliente AS cod_cliente_op, floating AS floating_op,
               prazo_medio_ponderado_dias AS prazo_medio_ponderado_dias_op
        FROM (
            SELECT cod_operacao, nbordero, nome_plataforma, chave_produto, data_deferimento,
                   cod_cliente, floating, prazo_medio_ponderado_dias,
                   ROW_NUMBER() OVER(PARTITION BY cod_operacao ORDER BY cod_operacao) as rn
            FROM [dbo].[fato_operacoes]
        ) t
        WHERE rn = 1
    ),

    -- STREAM 1: OPERAÇÕES
    OpsFiltradas AS (
        SELECT *
        FROM [dbo].[fato_operacoes]
        WHERE status_aceite = 'A' AND status_analise = 'D' AND YEAR(data_deferimento) >= 2025
    ),
    TitulosAggOp AS (
        SELECT
            t.cod_operacao,
            SUM(t.valor * t.prazo) AS soma_valor_prazo_op,
            SUM(t.valor) AS soma_valor_titulos_op,
            SUM(t.valor * DATEDIFF(day, o.data_deferimento, t.vencimento)) AS soma_valor_prazo_original_op,
            MIN(t.vencimento) AS menor_vencimento_titulos,
            MAX(t.vencimento) AS maior_vencimento_titulos
        FROM [dbo].[fato_titulos] t
        INNER JOIN OpsFiltradas o ON t.cod_operacao = o.cod_operacao
        WHERE t.aceito = 'S'
        GROUP BY t.cod_operacao
    ),
    StreamOperacoes AS (
        SELECT
            DATEFROMPARTS(YEAR(o.data_deferimento), MONTH(o.data_deferimento), 1) AS mes_ref,
            o.cod_operacao,
            o.cod_cliente,
            o.nbordero,
            o.chave_produto AS sub_tipo_produto,
            o.nome_plataforma,
            o.data_deferimento,
            o.floating,
            o.prazo_medio_ponderado_dias AS prazo_medio_total,
            SUM(o.valor_de_face) AS volume,
            SUM(t.soma_valor_prazo_op) AS total_valor_prazo_mes,
            SUM(t.soma_valor_prazo_original_op) AS total_valor_prazo_original_mes,
            SUM(COALESCE(o.desagio, 0) + COALESCE(o.total_de_tarifas, 0)) AS receita,
            COUNT(o.cod_operacao) AS qtd_eventos,
            MIN(COALESCE(t.menor_vencimento_titulos, o.menor_vencimento)) AS menor_vencimento,
            MAX(COALESCE(t.maior_vencimento_titulos, o.maior_vencimento)) AS maior_vencimento,
            'OPERACOES' AS tipo_produto
        FROM OpsFiltradas o
        LEFT JOIN TitulosAggOp t ON o.cod_operacao = t.cod_operacao
        GROUP BY DATEFROMPARTS(YEAR(o.data_deferimento), MONTH(o.data_deferimento), 1),
                 o.cod_operacao, o.cod_cliente, o.nbordero, o.chave_produto, o.nome_plataforma,
                 o.data_deferimento, o.floating, o.prazo_medio_ponderado_dias
    ),
    StreamOperacoesCalc AS (
        SELECT
            mes_ref, cod_operacao, cod_cliente, nbordero, sub_tipo_produto, nome_plataforma,
            data_deferimento, volume, receita, qtd_eventos, menor_vencimento, maior_vencimento, tipo_produto,
            CASE WHEN volume > 0 THEN total_valor_prazo_mes / volume ELSE 0 END AS prazo_medio,
            CASE WHEN volume > 0 THEN total_valor_prazo_original_mes / volume ELSE 0 END AS prazo_medio_original,
            prazo_medio_total,
            prazo_medio_total AS prazo_medio_ponderado_dias,
            prazo_medio_total - CASE WHEN volume > 0 THEN total_valor_prazo_mes / volume ELSE 0 END AS floating,
            CASE WHEN total_valor_prazo_mes > 0 THEN (receita / (total_valor_prazo_mes / 30.0)) * 100.0 ELSE 0 END AS taxa_media
        FROM StreamOperacoes
    ),

    -- STREAM 2: PRORROGAÇÕES
    ProrrogacoesEnrich AS (
        SELECT
            p.cod_operacao,
            COALESCE(NULLIF(TRIM(p.nbordero), ''), m.nbordero_op) AS nbordero,
            COALESCE(NULLIF(TRIM(p.chave_produto), ''), m.chave_produto_op) AS chave_produto,
            COALESCE(NULLIF(TRIM(p.nome_plataforma), ''), m.nome_plataforma_op) AS nome_plataforma,
            COALESCE(p.data_deferimento, m.data_deferimento_op) AS data_deferimento,
            COALESCE(NULLIF(TRIM(CAST(p.cod_cliente AS VARCHAR(50))), ''), CAST(m.cod_cliente_op AS VARCHAR(50))) AS cod_cliente,
            COALESCE(p.floating, m.floating_op) AS floating,
            COALESCE(p.prazo_medio_ponderado_dias, m.prazo_medio_ponderado_dias_op) AS prazo_medio_ponderado_dias,
            p.data_inclusao,
            p.valor,
            p.dias_prorrogados,
            p.juros,
            p.cod_titulo
        FROM [dbo].[fato_prorrogacoes_de_titulos] p
        LEFT JOIN MapOps m ON p.cod_operacao = m.cod_operacao
        WHERE YEAR(p.data_inclusao) >= 2025
    ),
    StreamProrrogacoes AS (
        SELECT
            DATEFROMPARTS(YEAR(p.data_inclusao), MONTH(p.data_inclusao), 1) AS mes_ref,
            p.cod_operacao,
            CAST(p.cod_cliente AS INT) AS cod_cliente,
            p.nbordero,
            'PR' AS sub_tipo_produto,
            COALESCE(p.nome_plataforma, pc.nome_plataforma_cli, 'N/D') AS nome_plataforma,
            COALESCE(p.data_deferimento, CAST(p.data_inclusao AS DATE)) AS data_deferimento,
            p.floating,
            p.prazo_medio_ponderado_dias,
            SUM(p.valor) AS volume,
            SUM(p.valor * p.dias_prorrogados) AS total_valor_dias_mes,
            SUM(p.juros) AS receita,
            COUNT(p.cod_titulo) AS qtd_eventos,
            'PRORROGACOES' AS tipo_produto
        FROM ProrrogacoesEnrich p
        LEFT JOIN PlataformaClientes pc ON CAST(p.cod_cliente AS INT) = pc.cod_cliente
        GROUP BY DATEFROMPARTS(YEAR(p.data_inclusao), MONTH(p.data_inclusao), 1),
                 p.cod_operacao, p.cod_cliente, p.nbordero,
                 COALESCE(p.nome_plataforma, pc.nome_plataforma_cli, 'N/D'),
                 COALESCE(p.data_deferimento, CAST(p.data_inclusao AS DATE)),
                 p.floating, p.prazo_medio_ponderado_dias
    ),
    StreamProrrogacoesCalc AS (
        SELECT
            mes_ref, cod_operacao, cod_cliente, nbordero, sub_tipo_produto, nome_plataforma,
            data_deferimento, volume, receita, qtd_eventos, CAST(NULL AS DATE) AS menor_vencimento, CAST(NULL AS DATE) AS maior_vencimento, tipo_produto,
            CASE WHEN volume > 0 THEN total_valor_dias_mes / volume ELSE 0 END AS prazo_medio,
            CAST(NULL AS FLOAT) AS prazo_medio_original,
            (CASE WHEN volume > 0 THEN total_valor_dias_mes / volume ELSE 0 END) + COALESCE(floating, 0) AS prazo_medio_total,
            prazo_medio_ponderado_dias,
            floating,
            CASE WHEN total_valor_dias_mes > 0 THEN (receita / (total_valor_dias_mes / 30.0)) * 100.0 ELSE 0 END AS taxa_media
        FROM StreamProrrogacoes
    ),

    -- STREAM 3: MORA
    MoraEnrich AS (
        SELECT
            b.cod_operacao,
            COALESCE(NULLIF(TRIM(b.nbordero), ''), m.nbordero_op) AS nbordero,
            COALESCE(NULLIF(TRIM(b.chave_produto), ''), m.chave_produto_op) AS chave_produto,
            COALESCE(NULLIF(TRIM(b.nome_plataforma), ''), m.nome_plataforma_op) AS nome_plataforma,
            COALESCE(b.data_deferimento, m.data_deferimento_op) AS data_deferimento,
            COALESCE(NULLIF(TRIM(CAST(b.cod_cliente AS VARCHAR(50))), ''), CAST(m.cod_cliente_op AS VARCHAR(50))) AS cod_cliente,
            COALESCE(b.floating, m.floating_op) AS floating,
            COALESCE(b.prazo_medio_ponderado_dias, m.prazo_medio_ponderado_dias_op) AS prazo_medio_ponderado_dias,
            b.data_baixa,
            b.data_vencimento,
            b.valor_pago,
            b.juros,
            b.cod_titulo
        FROM [dbo].[fato_baixas] b
        LEFT JOIN MapOps m ON b.cod_operacao = m.cod_operacao
        WHERE YEAR(b.data_baixa) >= 2025 AND b.juros > 0
    ),
    TitulosDates AS (
        SELECT cod_titulo, venc_prorrogado
        FROM [dbo].[fato_titulos]
        GROUP BY cod_titulo, venc_prorrogado
    ),
    MoraDates AS (
        SELECT
            m.*,
            COALESCE(m.nome_plataforma, pc.nome_plataforma_cli, 'N/D') AS nome_plataforma_final,
            COALESCE(m.data_deferimento, m.data_baixa) AS data_deferimento_final,
            CASE WHEN YEAR(t.venc_prorrogado) > 1900 THEN t.venc_prorrogado ELSE m.data_vencimento END AS data_referencia_mora
        FROM MoraEnrich m
        LEFT JOIN PlataformaClientes pc ON CAST(m.cod_cliente AS INT) = pc.cod_cliente
        LEFT JOIN TitulosDates t ON m.cod_titulo = t.cod_titulo
    ),
    MoraCalc AS (
        SELECT
            *,
            CASE
                WHEN data_baixa IS NULL OR data_referencia_mora IS NULL THEN 0
                WHEN YEAR(data_baixa) <= 1900 THEN 0
                WHEN YEAR(data_referencia_mora) <= 1900 THEN 0
                ELSE DATEDIFF(day, data_referencia_mora, data_baixa)
            END AS dias_atraso
        FROM MoraDates
    ),
    StreamMora AS (
        SELECT
            DATEFROMPARTS(YEAR(data_baixa), MONTH(data_baixa), 1) AS mes_ref,
            cod_operacao,
            CAST(cod_cliente AS INT) AS cod_cliente,
            nbordero,
            chave_produto AS sub_tipo_produto,
            nome_plataforma_final AS nome_plataforma,
            data_deferimento_final AS data_deferimento,
            floating,
            prazo_medio_ponderado_dias,
            SUM(valor_pago) AS volume,
            SUM(valor_pago * dias_atraso) AS total_valor_atraso_mes,
            SUM(juros) AS receita,
            COUNT(cod_titulo) AS qtd_eventos,
            'MORA' AS tipo_produto
        FROM MoraCalc
        GROUP BY DATEFROMPARTS(YEAR(data_baixa), MONTH(data_baixa), 1),
                 cod_operacao, cod_cliente, nbordero, chave_produto,
                 nome_plataforma_final, data_deferimento_final, floating, prazo_medio_ponderado_dias
    ),
    StreamMoraCalc AS (
        SELECT
            mes_ref, cod_operacao, cod_cliente, nbordero, sub_tipo_produto, nome_plataforma,
            data_deferimento, volume, receita, qtd_eventos, CAST(NULL AS DATE) AS menor_vencimento, CAST(NULL AS DATE) AS maior_vencimento, tipo_produto,
            CASE WHEN volume > 0 THEN total_valor_atraso_mes / volume ELSE 0 END AS prazo_medio,
            CAST(NULL AS FLOAT) AS prazo_medio_original,
            (CASE WHEN volume > 0 THEN total_valor_atraso_mes / volume ELSE 0 END) + COALESCE(floating, 0) AS prazo_medio_total,
            prazo_medio_ponderado_dias,
            floating,
            CASE WHEN total_valor_atraso_mes > 0 THEN (receita / (total_valor_atraso_mes / 30.0)) * 100.0 ELSE 0 END AS taxa_media
        FROM StreamMora
    ),

    -- UNIFICAR STREAMS
    UnionStreams AS (
        SELECT * FROM StreamOperacoesCalc
        UNION ALL
        SELECT * FROM StreamProrrogacoesCalc
        UNION ALL
        SELECT * FROM StreamMoraCalc
    )

-- FINAL SELECT COM ENRIQUECIMENTO DE CLIENTE
SELECT
    u.mes_ref AS mes_referencia,
    u.cod_cliente,
    c.nome AS nome_cliente,
    c.grupo_economico,
    u.cod_operacao,
    u.nbordero,
    u.sub_tipo_produto,
    u.nome_plataforma,
    u.data_deferimento,
    u.tipo_produto,
    ROUND(u.volume, 2) AS volume,
    ROUND(u.prazo_medio, 2) AS prazo_medio_dias,
    ROUND(u.prazo_medio_original, 2) AS prazo_medio_original_dias,
    u.floating,
    ROUND(u.prazo_medio_total, 2) AS prazo_medio_total_dias,
    CAST(u.prazo_medio_ponderado_dias AS FLOAT) AS prazo_medio_ponderado_dias,
    u.menor_vencimento AS menor_vencimento_op,
    u.maior_vencimento AS maior_vencimento_op,
    ROUND(u.taxa_media, 4) AS taxa_media_mensal_pct,
    ROUND(u.receita, 2) AS receita,
    u.qtd_eventos
FROM UnionStreams u
LEFT JOIN BaseClientes c ON u.cod_cliente = c.cod_cliente;
GO

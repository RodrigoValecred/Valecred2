CREATE VIEW [dbo].[vw_fato_pareceres_de_alteracao_de_status] AS
WITH cte_clientes AS (
    -- Equivalent to staging_clientes_limpa resolution using bronze cad_clientes
    SELECT
        CPFCNPJ AS cpf_cnpj,
        CODCLIENTE AS cod_cliente,
        ROW_NUMBER() OVER(PARTITION BY CPFCNPJ ORDER BY CODCLIENTE ASC) as rn
    FROM [LH_Bronze].[dbo].[cad_clientes]
    WHERE CPFCNPJ IS NOT NULL AND CPFCNPJ <> ''
),
cte_pareceres AS (
    SELECT
        p.CODPARECER,
        p.CPFCNPJ,
        p.CODOPERACAO,
        p.DATAINCLUSAO AS DATALOG,
        p.USUAINCLUSAO,
        TRIM(SUBSTRING(p.OBS, 22, 100)) AS STATUS_DO_CLIENTE,
        40 AS BASE
    FROM [LH_Bronze].[dbo].[cad_geral_pareceres] p
    WHERE
        YEAR(p.DATAINCLUSAO) >= 2024
        AND CAST(p.CODTIPOPARECER AS BIGINT) = 1
        AND p.CPFCNPJ IS NOT NULL AND p.CPFCNPJ <> ''
        AND p.OBS IS NOT NULL AND p.OBS <> ''
        AND p.USUAINCLUSAO IS NOT NULL
        AND p.DATAINCLUSAO IS NOT NULL
        AND p.OBS LIKE 'STATUS ALTERADO PARA %'
)
SELECT
    p.CODPARECER,
    c.cod_cliente AS CODCLIENTE,
    p.STATUS_DO_CLIENTE,
    p.DATALOG,
    p.BASE,
    u.NOME AS USUARIO,
    CONCAT(CAST(p.BASE AS VARCHAR), '-', CAST(c.cod_cliente AS VARCHAR)) AS chave_base_cliente,
    ROW_NUMBER() OVER (PARTITION BY c.cod_cliente ORDER BY p.DATALOG ASC) AS INDICE,
    CAST((ROW_NUMBER() OVER (PARTITION BY c.cod_cliente ORDER BY p.DATALOG ASC)) * 1000000000 + c.cod_cliente AS BIGINT) AS chave_original
FROM cte_pareceres p
LEFT JOIN cte_clientes c ON p.CPFCNPJ = c.cpf_cnpj AND c.rn = 1
LEFT JOIN [LH_Bronze].[dbo].[cad_usuarios] u ON p.USUAINCLUSAO = u.CODUSUARIO
WHERE c.cod_cliente IS NOT NULL;

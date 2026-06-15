CREATE VIEW [dbo].[vw_relatorio_saidas_risco] AS
SELECT
    ps.CODCLIENTE,
    c.cpf_cnpj,
    ps.STATUS_DO_CLIENTE,
    ps.DATALOG,
    pb.OBS AS PARECER_COMPLETO,
    ps.USUARIO
FROM
    [LH_Silver].[dbo].[pareceres_de_alteracao_de_status] ps
LEFT JOIN
    (SELECT cod_cliente, cpf_cnpj, ROW_NUMBER() OVER(PARTITION BY cod_cliente ORDER BY cod_cliente) as rn FROM [LH_Silver].[dbo].[staging_clientes_limpa]) c ON ps.CODCLIENTE = c.cod_cliente AND c.rn = 1
LEFT JOIN
    [LH_Bronze].[dbo].[cad_geral_pareceres] pb ON ps.CODPARECER = pb.CODPARECER
WHERE
    UPPER(ps.STATUS_DO_CLIENTE) LIKE '%SAIDA DE RISCO%' OR
    UPPER(ps.STATUS_DO_CLIENTE) LIKE '%SAÍDA DE RISCO%';

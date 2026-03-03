CREATE TABLE [dbo].[fato_operacoes] (

	[cod_operacao] int NULL, 
	[cod_cliente] int NULL, 
	[data_analise] date NULL, 
	[total_tarifas] decimal(18,2) NULL, 
	[pmp] float NULL, 
	[cod_gerente] int NULL, 
	[vop] decimal(18,2) NULL, 
	[desagio] decimal(18,2) NULL, 
	[valor_x_prazo] float NULL, 
	[chave_produto] varchar(4) NULL, 
	[tac_mesa] decimal(18,2) NULL, 
	[tarifas_de_operacao] decimal(18,2) NULL
,
	[floating] float NULL,
	[prazo_medio] float NULL,
	[prazo_medio_total] float NULL,
	[menor_vencimento] date NULL,
	[maior_vencimento] date NULL
);
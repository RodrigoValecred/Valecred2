CREATE TABLE [dbo].[fato_operacoes] (

	[cod_operacao] int NULL, 
	[data_inclusao] date NULL, 
	[tarifas_de_operacao] decimal(38,6) NULL, 
	[vop] float NULL, 
	[chave_produto] varchar(8000) NULL, 
	[tac_mesa] decimal(38,6) NULL, 
	[desagio] decimal(10,2) NULL, 
	[total_tarifas] decimal(10,2) NULL, 
	[pmp] float NULL, 
	[valor_x_prazo] float NULL
);
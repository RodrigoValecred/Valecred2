CREATE TABLE [dbo].[dim_clientes] (

	[sk_cliente] int NULL, 
	[cod_cliente] int NULL, 
	[cpf_cnpj] varchar(14) NULL, 
	[nome_cliente] varchar(255) NULL, 
	[cnae] varchar(7) NULL, 
	[data_status] datetime2(6) NULL, 
	[status_cliente_esteira] varchar(50) NULL, 
	[macroprocesso_esteira] varchar(30) NULL, 
	[fase_esteira] varchar(20) NULL, 
	[recuperacao_judicial] varchar(1) NULL, 
	[endereco_rua] varchar(255) NULL, 
	[endereco_numero] varchar(20) NULL, 
	[endereco_bairro] varchar(100) NULL, 
	[endereco_cidade] varchar(100) NULL, 
	[endereco_uf] varchar(2) NULL, 
	[endereco_cep] varchar(8) NULL, 
	[cod_contrato] int NULL
);
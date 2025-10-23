CREATE TABLE [dbo].[dim_gerentes] (

	[sk_gerente] int NULL, 
	[cod_gerente] int NULL, 
	[cpf_cnpj] varchar(14) NULL, 
	[nome_gerente] varchar(255) NULL, 
	[nome_plataforma] varchar(50) NULL, 
	[gestor_da_plataforma] varchar(30) NULL, 
	[cod_plataforma] int NULL, 
	[cod_usuario] int NULL
);
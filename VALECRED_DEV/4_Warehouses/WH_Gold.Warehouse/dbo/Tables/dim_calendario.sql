CREATE TABLE [dbo].[dim_calendario] (

	[sk_data] bigint NULL, 
	[data] date NULL, 
	[ano] bigint NULL, 
	[trimestre] bigint NULL, 
	[mes] bigint NULL, 
	[dia] bigint NULL, 
	[dia_semana] varchar(8000) NULL, 
	[numero_dia_semana] bigint NULL, 
	[nome_mes] varchar(8000) NULL, 
	[mes_ano_abrev] varchar(8000) NULL, 
	[fim_de_semana] varchar(8000) NULL
);
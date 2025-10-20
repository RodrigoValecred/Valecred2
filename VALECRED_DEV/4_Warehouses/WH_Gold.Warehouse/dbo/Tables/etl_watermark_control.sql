CREATE TABLE [dbo].[etl_watermark_control] (

	[PipelineName] varchar(255) NULL, 
	[LastWatermarkValue] date NULL
);
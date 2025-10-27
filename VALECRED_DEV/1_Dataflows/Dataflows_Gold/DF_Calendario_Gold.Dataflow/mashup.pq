section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_calendario_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sk_data", DestinationColumnName = "sk_data"], [SourceColumnName = "data", DestinationColumnName = "data"], [SourceColumnName = "ano", DestinationColumnName = "ano"], [SourceColumnName = "trimestre", DestinationColumnName = "trimestre"], [SourceColumnName = "mes", DestinationColumnName = "mes"], [SourceColumnName = "dia", DestinationColumnName = "dia"], [SourceColumnName = "dia_semana", DestinationColumnName = "dia_semana"], [SourceColumnName = "numero_dia_semana", DestinationColumnName = "numero_dia_semana"], [SourceColumnName = "nome_mes", DestinationColumnName = "nome_mes"], [SourceColumnName = "mes_ano_abrev", DestinationColumnName = "mes_ano_abrev"], [SourceColumnName = "fim_de_semana", DestinationColumnName = "fim_de_semana"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_calendario = let
    DataInicial = #date(2017, 1, 1),
    DataFinal = #date(2025, 12, 31),
    NumeroDeDias = Duration.Days(DataFinal - DataInicial) + 1,
    ListaDeDatas = List.Dates(DataInicial, NumeroDeDias, #duration(1, 0, 0, 0)),
    TabelaDeDatas = Table.FromList(ListaDeDatas, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    RenomearColuna = Table.RenameColumns(TabelaDeDatas,{{"Column1", "data"}}),
    AlterarTipo = Table.TransformColumnTypes(RenomearColuna,{{"data", type date}}),

    InserirAno = Table.AddColumn(AlterarTipo, "ano", each Date.Year([data]), Int64.Type),
    InserirMes = Table.AddColumn(InserirAno, "mes", each Date.Month([data]), Int64.Type),
    InserirDia = Table.AddColumn(InserirMes, "dia", each Date.Day([data]), Int64.Type),
    InserirTrimestre = Table.AddColumn(InserirDia, "trimestre", each Date.QuarterOfYear([data]), Int64.Type),

    InserirNomeMes = Table.AddColumn(InserirTrimestre, "nome_mes", each Date.ToText([data], "MMMM", "pt-BR"), type text),
    InserirMesAbreviado = Table.AddColumn(InserirNomeMes, "mes_ano_abrev", each Date.ToText([data], "MMM/yy", "pt-BR"), type text),
    InserirNomeDiaSemana = Table.AddColumn(InserirMesAbreviado, "dia_semana", each Date.ToText([data], "dddd", "pt-BR"), type text),
    InserirNumeroDiaSemana = Table.AddColumn(InserirNomeDiaSemana, "numero_dia_semana", each Date.DayOfWeek([data], Day.Monday) + 1, Int64.Type),

    InserirChaveData = Table.AddColumn(InserirNumeroDiaSemana, "sk_data", each Number.FromText(Date.ToText([data], "yyyyMMdd")), Int64.Type),

    OrdenarColunas = Table.ReorderColumns(InserirChaveData, {"sk_data", "data", "ano", "trimestre", "mes", "dia", "dia_semana", "numero_dia_semana", "nome_mes", "mes_ano_abrev"}),

    FiltrarFimDeSemana = Table.AddColumn(OrdenarColunas, "fim_de_semana", each if [numero_dia_semana] = 6 or [numero_dia_semana] = 7 then "Sim" else "Não", type text)
in
    FiltrarFimDeSemana;
shared dim_calendario_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_calendario", Schema = "dbo"]}?[Data]?
in
  TableNavigation;

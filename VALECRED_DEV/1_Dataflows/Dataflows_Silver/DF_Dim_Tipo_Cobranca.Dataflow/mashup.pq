[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared source_cad_conta_cobranca_ocorrencia = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_conta_cobranca_ocorrencia", ItemKind = "Table"]}[Data]
in
    tabela_origem;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_tipo_cobranca_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_tipo_cobranca = let
  Origem = source_cad_conta_cobranca_ocorrencia,
  #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"DESCRICAO", "descricao"}}),
    
    // Cria 'chave_ocorrencia' concatenando TOCORRENCIA, CODINSTRUCAO e CODBANCO.
    chaveOcorrenciaInserida = Table.AddColumn(#"Colunas renomeadas", "chave_ocorrencia", each Text.Combine(
    {Text.From([TOCORRENCIA], "pt-BR"), "-", Text.From([CODINSTRUCAO], "pt-BR"), "-", Text.From([CODBANCO], "pt-BR")}), type text),
    
    // Remove colunas originais usadas na chave e outras como VALOR.
    colunasRemovidas = Table.RemoveColumns(chaveOcorrenciaInserida, {"CODOCORCOBRBANCO", "CODINSTRUCAO", "CODBANCO", "TOCORRENCIA", "VALOR", "DATAINCLUSAO", "USUAINCLUSAO", "DATALTERACAO", "USUAALTERACAO"}),
    
    // Remove linhas duplicadas com base na 'chave_ocorrencia'.
    duplicatasRemovidas = Table.Distinct(colunasRemovidas, {"chave_ocorrencia"})
in
    duplicatasRemovidas;
shared dim_tipo_cobranca_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_tipo_cobranca", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

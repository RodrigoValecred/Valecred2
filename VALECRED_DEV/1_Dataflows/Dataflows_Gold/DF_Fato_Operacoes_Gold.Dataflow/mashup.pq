section Section1;
shared staging_titulos = let
  Origem = Lakehouse.Contents([]),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_titulos", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_tac_m = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_tac_m", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_operacoes = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navegação = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 1" = Navegação{[Id = "staging_operacoes", ItemKind = "Table"]}[Data]
in
    #"Navegação 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fato_operacoes_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "cod_operacao", DestinationColumnName = "cod_operacao"], [SourceColumnName = "data_analise", DestinationColumnName = "data_inclusao"], [SourceColumnName = "tarifas_de_operacao", DestinationColumnName = "tarifas_de_operacao"], [SourceColumnName = "vop", DestinationColumnName = "vop"], [SourceColumnName = "chave_produto", DestinationColumnName = "chave_produto"], [SourceColumnName = "tac_mesa", DestinationColumnName = "tac_mesa"], [SourceColumnName = "desagio", DestinationColumnName = "desagio"], [SourceColumnName = "total_tarifas", DestinationColumnName = "total_tarifas"], [SourceColumnName = "valor_x_prazo", DestinationColumnName = "valor_x_prazo"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared fato_operacoes = let
    // 1. Pré-agregar Títulos para evitar fan-out
    FonteTitulos = staging_titulos,
    FiltroTitulos = Table.SelectRows(FonteTitulos, each [aceito] = "S" and [t_doc] <> "BL"),
  #"Personalização adicionada 1" = Table.TransformColumnTypes(Table.AddColumn(FiltroTitulos, "valor_x_prazo", each [valor] * [prazo]), {{"valor_x_prazo", type number}}),
    GrupoTitulos = Table.Group(#"Personalização adicionada 1", {"cod_operacao"}, {
    {"vop", each List.Sum([valor]), type number},
    {"desagio", each List.Sum([desagio]), type number},
    {"valor_x_prazo", each List.Sum([valor_x_prazo]), type number }}),

    // 2. Filtrar Operações
    FonteOperacoes = staging_operacoes,
  #"Linhas filtradas" = Table.SelectRows(FonteOperacoes, each ([tto] = "CM" or [tto] = "FC" or [tto] = "GR" or [tto] = "NO")),
    FiltroOperacoes = Table.SelectRows(#"Linhas filtradas", each [status_aceite] = "A" and [status_analise] = "D" ),
    ColunasOperacoes = Table.SelectColumns(FiltroOperacoes, {"cod_operacao", "data_analise", "tto", "stto","tot_tar","pmp"}),

    // 3. Juntar Operações com Títulos Agregados
    JuntarOperacoesTitulos = Table.NestedJoin(ColunasOperacoes, {"cod_operacao"}, GrupoTitulos, {"cod_operacao"}, "TitulosAgregados", JoinKind.Inner),
    ExpandirTitulos = Table.ExpandTableColumn(JuntarOperacoesTitulos, "TitulosAgregados", {"vop","desagio","valor_x_prazo"}, {"vop","desagio","valor_x_prazo"}),

    // 4. Adicionar Dimensões
    AdicionarChaveProduto = Table.AddColumn(ExpandirTitulos, "chave_produto", each Text.Combine({[tto], [stto]}), type text),
    FiltroProdutos = Table.SelectRows(AdicionarChaveProduto, each [chave_produto] <> null and not List.Contains({"RN", "CS", "NOTR"}, Text.Upper([chave_produto]))),

    AlterarTipoData = Table.TransformColumnTypes(FiltroProdutos,{{"data_analise", type date}}),
  #"Colunas removidas" = Table.RemoveColumns(AlterarTipoData, {"tto", "stto"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Colunas removidas", {{"tot_tar", "tarifas_de_operacao"}}),
  #"Consultas mescladas" = Table.NestedJoin(#"Colunas renomeadas", {"cod_operacao"}, staging_tac_m, {"cod_operacao"}, "staging_tac_m", JoinKind.LeftOuter),
  #"Expandido staging_tac_m" = Table.ExpandTableColumn(#"Consultas mescladas", "staging_tac_m", {"total"}, {"tac_mesa"}),
  #"Valor substituído" = Table.ReplaceValue(#"Expandido staging_tac_m", null, 0, Replacer.ReplaceValue, {"tac_mesa"}),
  #"Linhas classificadas" = Table.Sort(#"Valor substituído", {{"data_analise", Order.Descending}}),
  #"Personalização adicionada" = Table.AddColumn(#"Linhas classificadas", "tarifas_da_operacao", each [tarifas_de_operacao] - [tac_mesa]),
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Personalização adicionada", {{"tarifas_da_operacao", type number}}),
  #"Colunas renomeadas 1" = Table.RenameColumns(#"Tipo de coluna alterado", {{"tarifas_de_operacao", "total_tarifas"}, {"tarifas_da_operacao", "tarifas_de_operacao"}})
in
    #"Colunas renomeadas 1";
shared staging_operacoes_recompras = let
  Origem = staging_operacoes,
  #"Linhas filtradas" = Table.SelectRows(Origem, each ([tto] = "RC" or [tto] = "RE") and ([status_analise] = "D")),
  #"Linhas filtradas 1" = Table.SelectRows(#"Linhas filtradas", each [status_aceite] = "A"),
  #"Colunas removidas" = Table.RemoveColumns(#"Linhas filtradas 1", {"tot_taxa_adm", "aprovacao_1", "tac", "cod_indeferimento"})
in
  #"Colunas removidas";
shared staging_titulos_boletos = let
  Origem = staging_titulos,
  #"Linhas filtradas" = Table.SelectRows(Origem, each [t_doc] = "BL"),
  #"Colunas removidas" = Table.RemoveColumns(#"Linhas filtradas", { "chave_danfe", "praca", "doc_confirmado", "usua_conf", "data_conf", "seu_numero_bancario", "nosso_numero", "prazo", "desagio", "liquido", "cod_banco_cobr"})
in
  #"Colunas removidas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fato_operacoes_recompra_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "cod_operacao", DestinationColumnName = "cod_operacao"], [SourceColumnName = "cod_empresa", DestinationColumnName = "cod_empresa"], [SourceColumnName = "cod_cliente", DestinationColumnName = "cod_cliente"], [SourceColumnName = "cpf_cnpj_cliente", DestinationColumnName = "cpf_cnpj_cliente"], [SourceColumnName = "usua_st_analise", DestinationColumnName = "usua_st_analise"], [SourceColumnName = "data_analise", DestinationColumnName = "data_analise"], [SourceColumnName = "data_aceite", DestinationColumnName = "data_aceite"], [SourceColumnName = "stto", DestinationColumnName = "stto"], [SourceColumnName = "fator", DestinationColumnName = "fator"], [SourceColumnName = "tarifa", DestinationColumnName = "tarifa"], [SourceColumnName = "tarifa_recompra", DestinationColumnName = "tarifa_recompra"], [SourceColumnName = "n_docs", DestinationColumnName = "n_docs"], [SourceColumnName = "n_docs_recompra", DestinationColumnName = "n_docs_recompra"], [SourceColumnName = "pmp", DestinationColumnName = "pmp"], [SourceColumnName = "tot_adval", DestinationColumnName = "tot_adval"], [SourceColumnName = "tot_tar", DestinationColumnName = "tot_tar"], [SourceColumnName = "nota_fiscal", DestinationColumnName = "nota_fiscal"], [SourceColumnName = "usua_trava", DestinationColumnName = "usua_trava"], [SourceColumnName = "data_inclusao", DestinationColumnName = "data_inclusao"], [SourceColumnName = "usua_inclusao", DestinationColumnName = "usua_inclusao"], [SourceColumnName = "data_alteracao", DestinationColumnName = "data_alteracao"], [SourceColumnName = "nota_servico", DestinationColumnName = "nota_servico"], [SourceColumnName = "contrato_fisico", DestinationColumnName = "contrato_fisico"], [SourceColumnName = "n_bordero", DestinationColumnName = "n_bordero"], [SourceColumnName = "tto", DestinationColumnName = "tto"], [SourceColumnName = "operacao_informal", DestinationColumnName = "operacao_informal"], [SourceColumnName = "cod_gerente", DestinationColumnName = "cod_gerente"], [SourceColumnName = "cod_titulo", DestinationColumnName = "cod_titulo"], [SourceColumnName = "n_doc", DestinationColumnName = "n_doc"], [SourceColumnName = "cpf_cnpj_sacado", DestinationColumnName = "cpf_cnpj_sacado"], [SourceColumnName = "cpf_cnpj_cedente", DestinationColumnName = "cpf_cnpj_cedente"], [SourceColumnName = "valor", DestinationColumnName = "valor"], [SourceColumnName = "amortizacoes", DestinationColumnName = "amortizacoes"], [SourceColumnName = "liquidacao", DestinationColumnName = "liquidacao"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared fato_operacoes_recompra = let
  Origem = Table.NestedJoin(staging_operacoes_recompras, {"cod_operacao"}, staging_titulos_boletos, {"cod_operacao"}, "fato_boletos", JoinKind.LeftOuter),
  #"Expandido fato_boletos" = Table.ExpandTableColumn(Origem, "fato_boletos", {"cod_titulo", "n_doc", "cpf_cnpj_sacado", "cpf_cnpj_cedente", "valor", "amortizacoes", "liquidacao"}, {"cod_titulo", "n_doc", "cpf_cnpj_sacado", "cpf_cnpj_cedente", "valor", "amortizacoes", "liquidacao"}),
  #"Colunas removidas" = Table.RemoveColumns(#"Expandido fato_boletos", {"status_analise", "status_aceite", "data_envio_email"}),
  #"Personalização adicionada" = Table.AddColumn(#"Colunas removidas", "tarifas_de_recompra", each [tot_tar] / [n_docs_recompra])
in
  #"Personalização adicionada";
shared fato_operacoes_recompra_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "fato_operacoes_recompra", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared fato_operacoes_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "fato_operacoes"]}[Data]
in
  TableNavigation;

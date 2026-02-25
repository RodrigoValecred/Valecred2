[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared staging_operacoes = let
    // Conecta ao banco de dados SQL.
    conexaoBancoDados = staging_tratamento_de_operacoes,
    // Classifica as linhas pela data de análise em ordem decrescente.
    linhasOrdenadasPorDataAnalise = Table.Sort(conexaoBancoDados,{{"DATAANALISE", Order.Descending}}),
    // Altera o tipo da coluna 'base' para texto.
    tipoColunaBaseAlterado = Table.TransformColumnTypes(linhasOrdenadasPorDataAnalise,{{"BASE", type text}}),

    // Mescla com estudo de operações para obter TAXACADASTRO.
    estudoOperacoesMesclado = Table.NestedJoin(tipoColunaBaseAlterado, {"CODOPERACAO"}, source_tab_estudo_op, {"CODOPERACAO"}, "estudoOpTabela", JoinKind.LeftOuter),
    // Expande a coluna TAXACADASTRO.
    taxaCadastroExpandida = Table.ExpandTableColumn(estudoOperacoesMesclado, "estudoOpTabela", {"FATOR"}, {"TAXACADASTRO"}),

    // Mescla com usuários enriquecidos para dados de inclusão.
    usuariosInclusaoMesclado = Table.NestedJoin(taxaCadastroExpandida, {"USUAINCLUSAO"}, dim_usuarios, {"cod_usuario"}, "usuariosInclusaoTabela", JoinKind.LeftOuter),
    // Expande nome, função e nível do usuário de inclusão.
    usuariosInclusaoExpandido = Table.ExpandTableColumn(usuariosInclusaoMesclado, "usuariosInclusaoTabela", {"nome_do_usuario", "funcao", "nivel"}, {"USUARIO_INCLUSAO", "INCLUIDO_POR", "NIVEL"}),

    // Mescla com usuários enriquecidos para dados do analista.
    usuariosAnaliseMesclado = Table.NestedJoin(usuariosInclusaoExpandido, {"USUASTANALISE"}, dim_usuarios, {"cod_usuario"}, "usuariosAnaliseTabela", JoinKind.LeftOuter),
    // Expande o nome do analista.
    analistaExpandido = Table.ExpandTableColumn(usuariosAnaliseMesclado, "usuariosAnaliseTabela", {"nome_do_usuario"}, {"ANALISTA"}),

    // Mescla com usuários enriquecidos para dados do analista que travou.
    usuariosTravaMesclado = Table.NestedJoin(analistaExpandido, {"USUATRAVA"}, dim_usuarios, {"cod_usuario"}, "usuariosTravaTabela", JoinKind.LeftOuter),
    // Expande o nome do analista que travou.
    analistaTravaExpandido = Table.ExpandTableColumn(usuariosTravaMesclado, "usuariosTravaTabela", {"nome_do_usuario"}, {"ANALISTA_TRAVA"}),

    // Converte colunas de texto para maiúsculas.
    textoParaMaiusculas = Table.TransformColumns(analistaTravaExpandido, {{"USUARIO_INCLUSAO", Text.Upper, type text}, {"INCLUIDO_POR", Text.Upper, type text}}),

    // Mescla com grupos de indeferimento para obter motivos.
    gruposIndeferimentoMesclado = Table.NestedJoin(textoParaMaiusculas, {"CODINDEFERIMENTO"}, sup_motivos_de_indeferimento, {"codindeferimento"}, "sup_motivos_de_indeferimento", JoinKind.LeftOuter),
    // Expande colunas de motivo e grupo de indeferimento.
    motivosIndeferimentoExpandido = Table.ExpandTableColumn(gruposIndeferimentoMesclado, "sup_motivos_de_indeferimento", {"motivo_indeferimento", "grupo_motivo_indeferimento"}, {"MOTIVO INDEFERIMENTO", "GRUPO MOTIVO INDEFERIMENTO"}),
    // Adiciona a chave para o produto (TTO+STTO).
    chaveProdutoAdicionada = Table.AddColumn(motivosIndeferimentoExpandido, "chave_produto", each Text.Combine({[TTO], [STTO]}), type text),

    // Extrai a data do campo DATAANALISE.
    dataDeferimentoExtraida = Table.AddColumn(chaveProdutoAdicionada, "Data deferimento", each Date.From([DATAANALISE]), type date),
    // Classifica operações em "VALE S" ou "VALE N".
    eraClassificada = Table.AddColumn(dataDeferimentoExtraida, "ERA", each if [Data deferimento] <> null and [Data deferimento] > #date(2023, 8, 31) then "VALE S" else "VALE N"),

    // Substitui valores nulos por 0 nas colunas numéricas.
    nulosNumericosSubstituidos = Table.ReplaceValue(eraClassificada, null, 0, Replacer.ReplaceValue, {"TAC", "TOTTAXAADM", "TOTADVAL", "TOTTAR", "NDOCSRECOMPRA"}),
  #"Colunas renomeadas" = Table.RenameColumns(nulosNumericosSubstituidos, {{"NBORDERO", "n_bordero"}, {"CODBROKER", "cod_broker"}, {"CODOPERACAO", "cod_operacao"}, {"CODEMPRESA", "cod_empresa"}, {"DATAINCLUSAO", "data_inclusao"}, {"CODCLIENTE", "cod_cliente"}, {"CPFCNPJCLIENTE", "cpf_cnpj_cliente"}, {"STATUSANALISE", "status_analise"}, {"DATAANALISE", "data_analise"}, {"STATUSACEITE", "status_aceite"}, {"DATAACEITE", "data_aceite"}, {"DATAENVIOEMAIL", "data_envio_email"}, {"STTO", "stto"}, {"FATOR", "fator"}, {"TARIFA", "tarifa"}, {"TARIFARECOMPRA", "tarifa_recompra"}, {"TAC", "tac"}, {"NDOCS", "n_doc"}, {"NDOCSRECOMPRA", "n_doc_recompra"}, {"TOTADVAL", "tot_adval"}, {"TOTTAXAADM", "tot_taxa_adm"}, {"TOTTAR", "tot_tar"}, {"APROVACAO1", "aprovacao_1"}, {"DATAALTERACAO", "data_alteracao"}, {"CODINDEFERIMENTO", "cod_indeferimento"}, {"CONTRATOFISICO", "contrato_fisico"}, {"USUAINCLUSAO", "cod_usuario_inclusao"}, {"USUATRAVA", "cod_usuario_trava"}, {"USUASTANALISE", "cod_usuario_status_analise"}, {"NOTASERVICO", "nota_servico"}, {"BASE", "base"}, {"TTO", "tto"}, {"TAXACADASTRO", "taxa_cadastro"}, {"USUARIO_INCLUSAO", "nome_do_usuario_inclusao"}, {"NIVEL", "nivel_do_usuario"}, {"ANALISTA", "nome_do_analista"}, {"ANALISTA_TRAVA", "nome_do_analista_trava"}, {"MOTIVO INDEFERIMENTO", "motivo_indeferimento"}, {"GRUPO MOTIVO INDEFERIMENTO", "grupo_motivo_indeferimento"}, {"ERA", "gestao"}}),
  #"Colunas removidas" = Table.RemoveColumns(#"Colunas renomeadas", {"INCLUIDO_POR"})
in
    #"Colunas removidas";
shared dim_gerentes = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "dim_gerentes", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared dim_plataformas = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "dim_plataformas", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared dim_usuarios = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "dim_usuarios", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared #"gerente da operacao" = let
    // 1. Conecta-se à fonte de dados SQL do Azure Synapse Analytics (ou Fabric Data Warehouse).
    Fonte = Sql.Database("gjdb6aepx2pujchian7qjs46o4-3mm24qi564pupgwhzs6cy5ooce.datawarehouse.fabric.microsoft.com", "LH_Silver"),
    // 1. Conecta-se à fonte de dados SQL do Azure Synapse Analytics (ou Fabric Data Warehouse).
    dbo_fato_titulos_antecipados = Fonte{[Schema="dbo",Item="fato_titulos_antecipados"]}[Data],
    #"Outras Colunas Removidas" = Table.SelectColumns(dbo_fato_titulos_antecipados,{"CODOPERACAO", "GerenteID", "NOMEPLATAFORMA", "NOMEGERENTE"}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Outras Colunas Removidas", {"NOMEPLATAFORMA"}, dim_plataformas, {"nome_plataforma"}, "plataformas", JoinKind.LeftOuter),
    #"plataformas Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "plataformas", {"gestor_da_plataforma"}, {"GESTOR DA OPERACAO"}),
    #"Duplicatas Removidas" = Table.Distinct(#"plataformas Expandido", {"CODOPERACAO"})
in
    #"Duplicatas Removidas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_operacoes_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_operacoes = let
    // Define a origem dos dados como a tabela Silver_Operacoes_Padronizado.
    origem = staging_operacoes,

    // Filtra linhas com tipos de operação específicos (CM, FC, NO, RN).
    // Nota: O input 'Fonte' foi alterado para 'ttoColunaRenomeada' para seguir o fluxo da query.
    linhasFiltradasTipoOperacao = Table.SelectRows(origem, each ([tto] = "CM" or [tto] = "FC" or [tto] = "NO" or [tto] = "RN" or [tto] = "GR" or [tto] = "CS")),
    
    // Insere a coluna 'chave_ano_mes' extraindo o ano e mês da 'Data deferimento'.
    chaveAnoMesInserida = Table.AddColumn(linhasFiltradasTipoOperacao, "chave_ano_mes", each Text.BetweenDelimiters(Text.From([data_analise], "pt-BR"), "/", " "), type text),
    
    // Insere a coluna 'chave_ano_mes_base_empresa' combinando chave_ano_mes e CODEMPRESA.
    chaveAnoMesBaseEmpresaInserida = Table.AddColumn(chaveAnoMesInserida, "chave_ano_mes_base_empresa", each Text.Combine({[chave_ano_mes], "-", Text.From([cod_empresa], "pt-BR")}), type text),
    
    // Classifica as linhas pela 'Data deferimento' em ordem decrescente.
    linhasClassificadas = Table.Sort(chaveAnoMesBaseEmpresaInserida,{{"data_analise", Order.Descending}}),
    #"Consultas Mescladas" = Table.NestedJoin(linhasClassificadas, {"cod_operacao"}, #"gerente da operacao", {"CODOPERACAO"}, "gerente da operacao", JoinKind.LeftOuter),
    #"gerente da operacao Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "gerente da operacao", {"GerenteID"}, {"gerente da operacao.GerenteID"}),
    #"Personalização Adicionada" = Table.AddColumn(#"gerente da operacao Expandido", "Personalizar", each [cod_broker] ?? [gerente da operacao.GerenteID]),
    #"Colunas Removidas" = Table.RemoveColumns(#"Personalização Adicionada",{"cod_broker", "gerente da operacao.GerenteID"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas",{{"Personalizar", "cod_broker"}}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Renomeadas",{{"cod_broker", Int64.Type}, {"gestao", type text}}),
    #"Consultas Mescladas1" = Table.NestedJoin(#"Tipo Alterado", {"cod_broker"}, dim_gerentes, {"cod_broker"}, "gerentes", JoinKind.LeftOuter),
    #"gerentes Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas1", "gerentes", {"nome_gerente", "plataforma", "gestor"}, {"nome_do_gerente", "plataforma_da_operacao", "gestor_da_operacao"}),
  #"Linhas classificadas" = Table.Sort(#"gerentes Expandido", {{"data_inclusao", Order.Descending}})
in
    #"Linhas classificadas";
shared source_operacoes = let
  // Data de corte para análise
  DataCorteAnalise = #datetime(2020, 12, 31, 0, 0, 0),
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_operacoes_limpa", ItemKind = "Table"]}[Data],
  selecionarColunas = Table.SelectColumns(#"Navegação 2", {"NBORDERO","CODBROKER","CODOPERACAO","CODEMPRESA","DATAINCLUSAO","CODCLIENTE","CPFCNPJCLIENTE","STATUSANALISE","DATAANALISE","STATUSACEITE","DATAACEITE","DATAENVIOEMAIL","STTO","TTO","FATOR","TARIFA","TARIFARECOMPRA","TAC","NDOCS","NDOCSRECOMPRA","TOTADVAL","TOTTAXAADM","TOTTAR","APROVACAO1","DATAALTERACAO","CODINDEFERIMENTO","CONTRATOFISICO","USUAINCLUSAO","USUATRAVA","USUASTANALISE","NOTASERVICO"}),
    // Filtra linhas com DATAANALISE posterior a DataCorteAnalise.
  linhasDataAnaliseFiltradas = Table.SelectRows(selecionarColunas, each [DATAANALISE] > DataCorteAnalise),
  // Adiciona a coluna "BASE" com o valor constante 40.
  baseAdicionada = Table.AddColumn(linhasDataAnaliseFiltradas, "BASE", each 40),
      // Adiciona uma coluna temporária 'TTO_Corrigido_Temp' com lógica de correção.
  ttoCorrigidoAdicionado = Table.AddColumn(baseAdicionada, "TTO_Corrigido_Temp",
      each if [CODOPERACAO] = 3042074 then "CS" else [TTO], type text),
  // Remove a coluna 'TTO' original.
  ttoOriginalRemovido = Table.RemoveColumns(ttoCorrigidoAdicionado, {"TTO"}),
  // Renomeia a coluna temporária 'TTO_Corrigido_Temp' para 'TTO'.
  ttoColunaRenomeada = Table.RenameColumns(ttoOriginalRemovido, {{"TTO_Corrigido_Temp", "TTO"}})
in
    ttoColunaRenomeada;
shared source_tab_estudo_op = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_estudo_op", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared sup_motivos_de_indeferimento = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "sup_motivos_de_indeferimento", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared fact_operacoes_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_operacoes", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared staging_tratamento_de_operacoes = source_operacoes;

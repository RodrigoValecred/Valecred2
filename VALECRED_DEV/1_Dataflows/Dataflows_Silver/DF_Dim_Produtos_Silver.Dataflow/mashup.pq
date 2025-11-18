[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_produtos_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_produtos = let
  // Origem dos dados: Silver_Operacoes_Padronizado (tabela de operações padronizadas)
  fonte = fact_operacoes,

  // Seleciona as colunas STTO e TTO, que representam o subtipo e o tipo do produto, respectivamente.
  colunasSelecionadas = Table.SelectColumns(fonte, {"stto", "tto"}),

  // Remove as linhas duplicadas, mantendo apenas as combinações únicas de STTO e TTO.
  duplicatasRemovidas = Table.Distinct(colunasSelecionadas),

  // Mescla com a tabela lb_produtos para obter a descrição do tipo de produto. Left Outer Join baseado em TTO = CODTTO.
  mescladoComProdutos = Table.NestedJoin(duplicatasRemovidas, {"tto"}, source_tab_tipooperacao, {"codtto"}, "lb_produtos", JoinKind.LeftOuter),
  expandidoProdutos = Table.ExpandTableColumn(mescladoComProdutos, "lb_produtos", {"descricao"}, {"tipo"}),

  // Mescla com a tabela lb_produtos_subtipos para obter a descrição do subtipo do produto. Left Outer Join baseado em STTO = CODSTTO.
  mescladoComSubtipos = Table.NestedJoin(expandidoProdutos, {"stto"}, source_tab_subtipooperacao, {"cod_stto"}, "lb_produtos_subtipos", JoinKind.LeftOuter),
  expandidoSubtipos = Table.ExpandTableColumn(mescladoComSubtipos, "lb_produtos_subtipos", {"descricao"}, {"subtipo"}),

  // Adiciona uma coluna que combina o tipo e o subtipo do produto para criar uma chave única.
  colunaChaveProdutoAdicionada = Table.AddColumn(expandidoSubtipos, "chave_produto", each Text.Combine({[tto], [stto]}), type text),

  // Classifica as linhas com base na chave do produto em ordem crescente.
  linhasClassificadas = Table.Sort(colunaChaveProdutoAdicionada, {{"chave_produto", Order.Ascending}}),

  // Cria uma coluna "Produto" que combina o subtipo e o tipo do produto. Se o subtipo for nulo, usa apenas o tipo.
  colunaProdutoAdicionada = Table.AddColumn(linhasClassificadas, "produto", each if [subtipo] = null then [tipo] else Text.Combine({[subtipo], " - ", [tipo]}), type text),
    // devido o subtipo CM, o nome do produto poderia aparecer Comissária 2 vezes, ex CMCM
    corrigidoNomeProdutoComissarias = Table.ReplaceValue(colunaProdutoAdicionada," - COMISSÁRIA","",Replacer.ReplaceText,{"produto"}),
    // A antiga Comissária foi alterada para Comissaria Simples, o nome do novo produto
    alterarNomeComissaria = Table.ReplaceValue(corrigidoNomeProdutoComissarias,"COMISSÁRIA","COMISSARIA SIMPLES",Replacer.ReplaceText,{"produto"}),

  // Remove as colunas TTO, STTO, "Tipo do produto" e "Subtipo do produto", que não são mais necessárias.
  colunasRemovidas = Table.RemoveColumns(alterarNomeComissaria, {"tto", "stto", "tipo", "subtipo"}),

  // Duplica a coluna "Produto" para criar uma cópia que será usada para informações de mercado.
  colunaProdutoDuplicada = Table.DuplicateColumn(colunasRemovidas, "produto", "Produto - Copiar"),

  // Renomeia a coluna duplicada para "Produto - Informação de Mercado".
  colunaInformacaoRenomeada = Table.RenameColumns(colunaProdutoDuplicada, {{"Produto - Copiar", "produto_informacao_de_mercado"}}),

  // Substitui valores específicos na coluna "Produto - Informação de Mercado" por outros termos para padronizar a terminologia.
  valorSubstituidoNormal = Table.ReplaceValue(colunaInformacaoRenomeada, "NORMAL", "Desconto", Replacer.ReplaceText, {"produto_informacao_de_mercado"}),
  valorSubstituidoCGP = Table.ReplaceValue(valorSubstituidoNormal, "CGP - FLUXO DE CAIXA SECURITIZADORA", "Giro Parcelado", Replacer.ReplaceText, {"produto_informacao_de_mercado"}),
  valorSubstituidoMateriaPrima = Table.ReplaceValue(valorSubstituidoCGP, "MATERIA PRIMA - FLUXO DE CAIXA SECURITIZADORA", "Fomento", Replacer.ReplaceText, {"produto_informacao_de_mercado"}),

  // Converte o texto da coluna "Produto - Informação de Mercado" para maiúsculas.
  informacaoEmMaiusculas = Table.TransformColumns(valorSubstituidoMateriaPrima, {{"produto_informacao_de_mercado", Text.Upper, type text}}),
  #"Duplicatas removidas" = Table.Distinct(informacaoEmMaiusculas, {"chave_produto"})

in
  #"Duplicatas removidas" // Retorna a tabela final com todos os produtos únicos e suas informações formatadas.
;
shared fact_operacoes = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "fact_operacoes", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared source_tab_tipooperacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_tipooperacao", ItemKind = "Table"]}[Data],
  #"Colunas renomeadas" = Table.RenameColumns(tabela_origem, {{"CODTTO", "codtto"}, {"DESCRICAO", "descricao"}, {"COR", "cor"}, {"CUSTO", "custo"}, {"STATUS", "status"}, {"COBRANCA", "cobranca"}, {"ANEXOCC", "anexocc"}, {"TIPOMESA", "tipomesa"}}),
  #"Consulta acrescentada" = Table.Combine({#"Colunas renomeadas", sup_produtos_ausentes}),
    
    // Remove as colunas "COR" e "CUSTO".
    colunasRemovidas = Table.RemoveColumns(#"Consulta acrescentada", {"cor", "custo", "status", "cobranca", "anexocc", "tipomesa"})

in
    colunasRemovidas;
shared sup_produtos_ausentes = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "sup_produtos_ausentes", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared source_tab_subtipooperacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_subtipooperacao", ItemKind = "Table"]}[Data],
  #"Colunas removidas" = Table.RemoveColumns(tabela_origem, {"CODTTO", "COR"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Colunas removidas", {{"CODSTTO", "cod_stto"}, {"DESCRICAO", "descricao"}})
in
    #"Colunas renomeadas";
shared dim_produtos_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_produtos", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

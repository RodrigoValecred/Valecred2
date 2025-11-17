[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared produtos = let
  // Origem dos dados: Silver_Operacoes_Padronizado (tabela de operações padronizadas)
  fonte = fact_operacoes,

  // Seleciona as colunas STTO e TTO, que representam o subtipo e o tipo do produto, respectivamente.
  colunasSelecionadas = Table.SelectColumns(fonte, {"stto", "tto"}),

  // Remove as linhas duplicadas, mantendo apenas as combinações únicas de STTO e TTO.
  duplicatasRemovidas = Table.Distinct(colunasSelecionadas),

  // Mescla com a tabela lb_produtos para obter a descrição do tipo de produto. Left Outer Join baseado em TTO = CODTTO.
  mescladoComProdutos = Table.NestedJoin(duplicatasRemovidas, {"tto"}, replica_tab_tipooperacao, {"CODTTO"}, "lb_produtos", JoinKind.LeftOuter),
  expandidoProdutos = Table.ExpandTableColumn(mescladoComProdutos, "lb_produtos", {"DESCRICAO"}, {"Tipo do produto"}),

  // Mescla com a tabela lb_produtos_subtipos para obter a descrição do subtipo do produto. Left Outer Join baseado em STTO = CODSTTO.
  mescladoComSubtipos = Table.NestedJoin(expandidoProdutos, {"STTO"}, replica_tab_subtipooperacao, {"CODSTTO"}, "lb_produtos_subtipos", JoinKind.LeftOuter),
  expandidoSubtipos = Table.ExpandTableColumn(mescladoComSubtipos, "lb_produtos_subtipos", {"DESCRICAO"}, {"Subtipo do produto"}),

  // Adiciona uma coluna que combina o tipo e o subtipo do produto para criar uma chave única.
  colunaChaveProdutoAdicionada = Table.AddColumn(expandidoSubtipos, "chave_produto", each Text.Combine({[TTO], [STTO]}), type text),

  // Classifica as linhas com base na chave do produto em ordem crescente.
  linhasClassificadas = Table.Sort(colunaChaveProdutoAdicionada, {{"chave_produto", Order.Ascending}}),

  // Cria uma coluna "Produto" que combina o subtipo e o tipo do produto. Se o subtipo for nulo, usa apenas o tipo.
  colunaProdutoAdicionada = Table.AddColumn(linhasClassificadas, "Produto", each if [Subtipo do produto] = null then [Tipo do produto] else Text.Combine({[Subtipo do produto], " - ", [Tipo do produto]}), type text),
    // devido o subtipo CM, o nome do produto poderia aparecer Comissária 2 vezes, ex CMCM
    corrigidoNomeProdutoComissarias = Table.ReplaceValue(colunaProdutoAdicionada," - COMISSÁRIA","",Replacer.ReplaceText,{"Produto"}),
    // A antiga Comissária foi alterada para Comissaria Simples, o nome do novo produto
    alterarNomeComissaria = Table.ReplaceValue(corrigidoNomeProdutoComissarias,"COMISSÁRIA","COMISSARIA SIMPLES",Replacer.ReplaceText,{"Produto"}),

  // Remove as colunas TTO, STTO, "Tipo do produto" e "Subtipo do produto", que não são mais necessárias.
  colunasRemovidas = Table.RemoveColumns(alterarNomeComissaria, {"TTO", "STTO", "Tipo do produto", "Subtipo do produto"}),

  // Duplica a coluna "Produto" para criar uma cópia que será usada para informações de mercado.
  colunaProdutoDuplicada = Table.DuplicateColumn(colunasRemovidas, "Produto", "Produto - Copiar"),

  // Renomeia a coluna duplicada para "Produto - Informação de Mercado".
  colunaInformacaoRenomeada = Table.RenameColumns(colunaProdutoDuplicada, {{"Produto - Copiar", "Produto - Informação de Mercado"}}),

  // Substitui valores específicos na coluna "Produto - Informação de Mercado" por outros termos para padronizar a terminologia.
  valorSubstituidoNormal = Table.ReplaceValue(colunaInformacaoRenomeada, "NORMAL", "Desconto", Replacer.ReplaceText, {"Produto - Informação de Mercado"}),
  valorSubstituidoCGP = Table.ReplaceValue(valorSubstituidoNormal, "CGP - FLUXO DE CAIXA SECURITIZADORA", "Giro Parcelado", Replacer.ReplaceText, {"Produto - Informação de Mercado"}),
  valorSubstituidoMateriaPrima = Table.ReplaceValue(valorSubstituidoCGP, "MATERIA PRIMA - FLUXO DE CAIXA SECURITIZADORA", "Fomento", Replacer.ReplaceText, {"Produto - Informação de Mercado"}),

  // Converte o texto da coluna "Produto - Informação de Mercado" para maiúsculas.
  informacaoEmMaiusculas = Table.TransformColumns(valorSubstituidoMateriaPrima, {{"Produto - Informação de Mercado", Text.Upper, type text}}),

  // Remove as linhas duplicadas com base na coluna "chave_produto", garantindo que cada produto tenha apenas uma entrada.
  duplicatasRemovidasChave = Table.Distinct(informacaoEmMaiusculas, {"chave_produto"})

in
  duplicatasRemovidasChave // Retorna a tabela final com todos os produtos únicos e suas informações formatadas.
;
shared replica_tab_tipooperacao = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi' e executa a query.
    mysqlOrigem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM tab_tipooperacao", ReturnSingleDatabase=true]),
    
    // Cria a tabela de produtos ausentes a partir de dados locais.
    produtosAusentesLocal = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45Wcg5W0lFydg0OPtzsD2QpG4ABkGWgFBsLAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [CODTTO = _t, DESCRICAO = _t, COR = _t, CUSTO = _t]),
    
    // Altera os tipos de coluna da tabela de produtos ausentes.
    produtosAusentesTipoAlterado = Table.TransformColumnTypes(produtosAusentesLocal, {{"CODTTO", type text}, {"DESCRICAO", type text}, {"COR", type text}, {"CUSTO", Int64.Type}}),

    // Combina a tabela do MySQL com a tabela de produtos ausentes.
    tabelasCombinadas = Table.Combine({mysqlOrigem, produtosAusentesTipoAlterado}),
    
    // Remove as colunas "COR" e "CUSTO".
    colunasRemovidas = Table.RemoveColumns(tabelasCombinadas,{"COR", "CUSTO"})

in
    colunasRemovidas;
shared replica_tab_subtipooperacao = let
    // Conecta-se ao banco de dados MySQL.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM tab_subtipooperacao", ReturnSingleDatabase=true]),

    // Remove as colunas COD TTO e COR.
    // Nota: Assumimos que o input para esta etapa deve ser 'origem', pois 'dbo_factvale40bi_tab_subtipooperacao' não está definido.
    colunasRemovidas = Table.RemoveColumns(origem,{"CODTTO", "COR"})
in
    colunasRemovidas;
shared fact_operacoes = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "fact_operacoes", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";

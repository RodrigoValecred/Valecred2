[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared staging_fato_titulos = let
    // Define a tabela 'replicas_tab_titulos' como a fonte inicial de dados
    fonteDadosTitulos = staging_titulos,
    // Cria a chave "chave_cliente_sacado" concatenando CODCLIENTE e RaizCNPJ.
    chaveClienteSacadoInserida = Table.AddColumn(fonteDadosTitulos, "chave_cliente_sacado", each Text.Combine({Text.From([CODCLIENTE], "pt-BR"), "-", [RaizCNPJ]}), type text),
    // Ordenar no mais novo para o mais antigo
    classificarPorDataInclusao = Table.Sort(chaveClienteSacadoInserida,{{"DATAINCLUSAO", Order.Descending}}),
    // Mescla dados dos títulos com informações de limites dos clientes sacados (relacionamento intercompany)
    mesclarTitulosComLimites = Table.NestedJoin(classificarPorDataInclusao, {"chave_cliente_sacado"}, staging_rlc_clientes_sacados_limites, {"chave_cliente_sacado"}, "relacionamento_intercompany", JoinKind.LeftOuter),
    // Filtra os títulos, removendo os do tipo "BL" e "RC" (boletos e recibos)
    filtrarTitulosPorTipo = Table.SelectRows(mesclarTitulosComLimites, each ([TDOC] <> "BL" and [TDOC] <> "RC")),
    // Expande a coluna de relacionamento intercompany para trazer o tipo (Intercompany ou não)
    expandirRelacionamentoIntercompany = Table.ExpandTableColumn(filtrarTitulosPorTipo, "relacionamento_intercompany", {"TIPO"}, {"intercompany"}),
    // Calcula o valor total do título multiplicado pelo prazo (VALOR X PRAZO)
    calcularValorTotalPorPrazo = Table.AddColumn(expandirRelacionamentoIntercompany, "valor_vezes_prazo", each [PRAZO]*[VALOR]),
    // Define o tipo da coluna 'valorXPrazo' como número para cálculos futuros
    converterValorTotalPrazoParaNumero = Table.TransformColumnTypes(calcularValorTotalPorPrazo,{{"valor_vezes_prazo", type number}}),
    // Padroniza a coluna Intercompany, substituindo nulos por "NÃO"
    padronizarColunaIntercompanyNulo = Table.ReplaceValue(converterValorTotalPrazoParaNumero,null,"NÃO",Replacer.ReplaceValue,{"intercompany"}),
    // Padroniza a coluna Intercompany, substituindo "INTERCIA" por "SIM"
    padronizarColunaIntercompanyIntercia = Table.ReplaceValue(padronizarColunaIntercompanyNulo,"INTERCIA","SIM",Replacer.ReplaceText,{"intercompany"}),
    // Mescla os dados dos títulos com informações dos produtos (chave_produto)
    mesclarTitulosComProdutos = Table.NestedJoin(padronizarColunaIntercompanyIntercia, {"chave_produto"}, dim_produtos, {"chave_produto"}, "produtos", JoinKind.LeftOuter),
    // Expande os dados dos produtos para trazer a informação de mercado
    expandirDadosProdutos = Table.ExpandTableColumn(mesclarTitulosComProdutos, "produtos", {"Produto - Informação de Mercado"}, {"produto_temp"}),
    // Personaliza o nome do produto se for Intercompany e o produto for "NO" ou "CM"
    personalizarNomeProdutoIntercompany = Table.AddColumn(expandirDadosProdutos, "produto_com_intercia_temp", each if [intercompany] = "SIM" and List.Contains({"NO","CM"},[chave_produto]) then   "INTERCOMPANY"  else [produto_temp], type text),
    // Remove a coluna original do Produto (já temos a personalizada)
    removerColunaProdutoOriginal = Table.RemoveColumns(personalizarNomeProdutoIntercompany,{"produto_temp"}),
    // Renomeia a coluna personalizada para "Produto com intercia"
    renomearColunaProdutoPersonalizado = Table.RenameColumns(removerColunaProdutoOriginal,{{"produto_com_intercia_temp", "produto_com_intercia"}}),
    // Adiciona o dia da semana do vencimento prorrogado
    adicionarDiaDaSemanaVencimento = Table.AddColumn(renomearColunaProdutoPersonalizado, "dia_da_semana", each Date.DayOfWeek([VENCPRORROGADO]), Int64.Type),
    // Ajusta a data de vencimento para o próximo dia útil, considerando finais de semana
    ajustarDataVencimentoParaDiaUtil = Table.AddColumn(adicionarDiaDaSemanaVencimento, "data_vencimento_util_temp", each if [dia_da_semana] = 0 then Date.AddDays([VENCPRORROGADO],1) else if [dia_da_semana] = 6 then Date.AddDays([VENCPRORROGADO],2) else [VENCPRORROGADO], type date),
    // Mescla os dados com a tabela de feriados para identificar feriados na data de vencimento
    mesclarComTabelaFeriados = Table.NestedJoin(ajustarDataVencimentoParaDiaUtil, {"data_vencimento_util_temp"}, source_tab_feriados, {"DATAFERIADO"}, "feriados", JoinKind.LeftOuter),
    // Expande os dados dos feriados para trazer o tipo do feriado
    expandirDadosFeriados = Table.ExpandTableColumn(mesclarComTabelaFeriados, "feriados", {"TFERIADO"}, {"tipo_feriado"}),
    // Calcula a data prevista de liquidação, considerando feriados e finais de semana
    calcularDataPrevisaoLiquidacao = Table.AddColumn(expandirDadosFeriados, "data_previsao_liquidacao",
each if [tipo_feriado] = "L" then  [data_vencimento_util_temp]
else if [tipo_feriado] = "N" and [dia_da_semana] = 5 then Date.AddDays([data_vencimento_util_temp],3)
else if [tipo_feriado] = "N" and [dia_da_semana] <> 5 then Date.AddDays([data_vencimento_util_temp],1)
else [data_vencimento_util_temp], type date),
    // Remove colunas auxiliares que não são mais necessárias
    removerColunasAuxiliaresData = Table.RemoveColumns(calcularDataPrevisaoLiquidacao,{"data_vencimento_util_temp", "tipo_feriado", "dia_da_semana"}),
    // Renomeia a coluna da data de previsão de liquidação para data de vencimento útil
    renomearColunaDataPrevisaoLiquidacao = Table.RenameColumns(removerColunasAuxiliaresData,{{"data_previsao_liquidacao", "data_vencimento_util"}}),
    // Calcula o status de deferimento com base nas colunas ACEITO, STATUSACEITE e STATUSANALISE
    calcularStatusDeferimento = Table.AddColumn(renomearColunaDataPrevisaoLiquidacao, "status_deferimento", each if [ACEITO] = "S" and [STATUSACEITE] = "A" and [STATUSANALISE] = "D" then "Sim" else "Não", type text),
    // Define o status "CLEAN" para títulos que não são de desconto
    definirStatusCleanTitulos = Table.AddColumn(calcularStatusDeferimento, "status_clean",
    each if [produto_com_intercia] = "DESCONTO" then "NORMAL"
    else "CLEAN", type text),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): N -> Atenção
    padronizarConfirmacaoDocumentosN = Table.ReplaceValue(definirStatusCleanTitulos,"N","Atenção",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): S -> null
    padronizarConfirmacaoDocumentosS = Table.ReplaceValue(padronizarConfirmacaoDocumentosN,"S",null,Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): C -> Positivo
    padronizarConfirmacaoDocumentosC = Table.ReplaceValue(padronizarConfirmacaoDocumentosS,"C","Positivo",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): P -> Problema
    padronizarConfirmacaoDocumentosP = Table.ReplaceValue(padronizarConfirmacaoDocumentosC,"P","Problema",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): A -> Alerta
    padronizarConfirmacaoDocumentosA = Table.ReplaceValue(padronizarConfirmacaoDocumentosP,"A","Alerta",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): null -> Não Contatado
    padronizarConfirmacaoDocumentosNulo = Table.ReplaceValue(padronizarConfirmacaoDocumentosA,null,"Não Contatado",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): E -> Eletrônico
    padronizarConfirmacaoDocumentosE = Table.ReplaceValue(padronizarConfirmacaoDocumentosNulo,"E","Eletrônico",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Padroniza a coluna de confirmação de documentos (DOCCONFIRMADO): AZ -> Eletrônico
    padronizarConfirmacaoDocumentosAZ = Table.ReplaceValue(padronizarConfirmacaoDocumentosE,"AZ","Eletrônico",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    // Renomeia a coluna DOCCONFIRMADO para CONFIRMAÇÃO para melhor entendimento
    renomearColunaDocConfirmado = Table.RenameColumns(padronizarConfirmacaoDocumentosAZ,{{"DOCCONFIRMADO", "confirmacao"}}),
    // Cria uma coluna para ordenar a confirmação (Não Contatado = 5, Atenção = 2, Eletrônico = 0, etc.)
    criarColunaOrdemConfirmacao = Table.AddColumn(renomearColunaDocConfirmado, "ordem_confirmacao", each if [confirmacao] = "Não Contatado" then 5 else if [confirmacao] = "Atenção" then 2 else if [confirmacao] = "Eletrônico" then 0
    else if [confirmacao] = "Positivo" then 1
    else if [confirmacao] = "Alerta" then 3
    else if [confirmacao] = "Problema" then 4
    else null, type number),
    // Mescla os dados com informações de recompra (relacionamento_recompras)
    mesclarComRelacionamentoRecompras = Table.NestedJoin(criarColunaOrdemConfirmacao, {"CODTITULO"}, limpeza_tab_operacoes_devolucoes, {"CODTITULO"}, "relacionamento_recompras", JoinKind.LeftOuter),
    // Expande os dados de recompra para trazer a chave da operação de recompra
    expandirRelacionamentoRecompras = Table.ExpandTableColumn(mesclarComRelacionamentoRecompras, "relacionamento_recompras", {"chave_base_operacao"}, {"chave_base_operacao_recompra"}),
    // Mescla os dados com informações da última checagem (ultima checagem)
    mesclarComUltimaChecagem = Table.NestedJoin(expandirRelacionamentoRecompras, {"CODTITULO"}, fact_ultima_confirmacao, {"cod_titulo"}, "ultima_checagem", JoinKind.LeftOuter),
    // Expande os dados da última checagem para trazer a confirmação
    expandirUltimaChecagem = Table.ExpandTableColumn(mesclarComUltimaChecagem, "ultima_checagem", {"confirmacao"}, {"confirmado_por"}),
    // Padroniza a coluna AMORTIZACOES, substituindo nulos por 0
    padronizarAmortizacoesNulo = Table.ReplaceValue(expandirUltimaChecagem,null,0,Replacer.ReplaceValue,{"AMORTIZACOES"}),
    // Renomeia todas as colunas para o padrão snake_case
    renomearColunasFinal = Table.RenameColumns(padronizarAmortizacoesNulo, {
        {"CODTITULO", "cod_titulo"}, {"CODOPERACAO", "cod_operacao"}, {"TDOC", "tipo_documento"}, {"NDOC", "numero_documento"},
        {"CPFCNPJSACADO", "cpf_cnpj_sacado"}, {"DATADOC", "data_documento"}, {"VENCIMENTO", "vencimento"}, {"VENCPRORROGADO", "vencimento_prorrogado"},
        {"VALOR", "valor"}, {"PRAZO", "prazo"}, {"ACEITO", "aceito"}, {"DATAINCLUSAO", "data_inclusao"}, {"USUAINCLUSAO", "usuario_inclusao"},
        {"DATAALTERACAO", "data_alteracao"}, {"USUAALTERACAO", "usuario_alteracao"}, {"AMORTIZACOES", "amortizacoes"}, {"CODCLIENTE", "cod_cliente"},
        {"DATAANALISE", "data_analise"}, {"STATUSACEITE", "status_aceite"}, {"STATUSANALISE", "status_analise"}, {"chave_produto", "chave_produto"},
        {"chave_base_empresa", "chave_base_empresa"}, {"status_protesto", "status_protesto"}, {"TipoDocumentoSacado", "tipo_documento_sacado"},
        {"RaizCNPJ", "raiz_cnpj"}
    })
in
    // Retorna a tabela final com todas as transformações
    renomearColunasFinal;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fato_titulos_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fato_titulos = let
    Origem = staging_fato_titulos
in
    Origem;

shared fact_operacoes = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "fact_operacoes", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared dim_produtos = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "dim_produtos", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared source_tab_titulos = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "tab_titulos", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared source_tab_feriados = let
    origem = Lakehouse.Contents(null),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_feriados", ItemKind = "Table"]}[Data],

    // Remove as colunas de metadados e ID de feriados.
    colunasRemovidas = Table.RemoveColumns(tabela_origem,{"CODFERIADO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO"}),

    // Adiciona a coluna "ANO/MES" extraindo o ano da coluna "DATAFERIADO".
    anoMesInserido = Table.AddColumn(colunasRemovidas, "ANO/MES", each Text.AfterDelimiter(Text.From([DATAFERIADO], "pt-BR"), "/"), type text),
    #"Linhas Classificadas" = Table.Sort(anoMesInserido,{{"DATAFERIADO", Order.Ascending}})
in
    #"Linhas Classificadas";
shared fact_protestos = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "fact_protestos", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared staging_titulos = let
  Origem = staging_limpeza_de_titulos,

    // Realiza um Left Outer Join com a tabela "replica_tab_operacoes" usando "CODOPERACAO".
    consultasMescladas = Table.NestedJoin(Origem, {"CODOPERACAO"}, fact_operacoes, {"cod_operacao"}, "operacoes", JoinKind.LeftOuter),

    // Expande a coluna "operacoes" para trazer dados específicos da operação.
    operacoesExpandido = Table.ExpandTableColumn(consultasMescladas, "operacoes", {"CODCLIENTE", "DATAANALISE", "STATUSACEITE", "STATUSANALISE","chave_produto","chave_base_empresa"}, {"CODCLIENTE", "DATAANALISE", "STATUSACEITE", "STATUSANALISE","chave_produto","chave_base_empresa"}),

    // Realiza um Left Outer Join com a tabela "replica_rlc_titulos_ocorrencias_cobranca2protestos" usando "CODTITULO".
    consultasMescladasProtestos = Table.NestedJoin(operacoesExpandido, {"CODTITULO"}, fact_protestos, {"cod_titulo"}, "lb_protestos", JoinKind.LeftOuter),

    // Expande a coluna "lb_protestos" para trazer "TIPO" como "Status Protesto".
    lbProtestosExpandido = Table.ExpandTableColumn(consultasMescladasProtestos, "lb_protestos", {"tipo"}, {"status_protesto"}),

    // Substitui valores nulos na coluna "Status Protesto" por "NÃO PROTESTADO".
    statusProtestoCorrigido = Table.ReplaceValue(lbProtestosExpandido,null,"NÃO PROTESTADO",Replacer.ReplaceValue,{"status_protesto"}),

    // Adiciona a coluna "TipoDocumentoSacado" (CPF, CNPJ ou Inválido).
    tipoDocumentoSacadoAdicionado = Table.AddColumn(statusProtestoCorrigido, "TipoDocumentoSacado", each if Text.Length([CPFCNPJSACADO])=11 then "CPF"
else if Text.Length([CPFCNPJSACADO])=14 then "CNPJ"
else "Inválido"),

    // Adiciona a coluna "RaizCNPJ" (8 primeiros dígitos do CNPJ ou o próprio CPF/Inválido).
    raizCnpjAdicionado = Table.AddColumn(tipoDocumentoSacadoAdicionado, "RaizCNPJ", each if [TipoDocumentoSacado]="CNPJ" then Text.Start([CPFCNPJSACADO],8) else [CPFCNPJSACADO]),

    // Altera o tipo das colunas "TipoDocumentoSacado" e "RaizCNPJ" para texto.
    tipoAlteradoFinal = Table.TransformColumnTypes(raizCnpjAdicionado,{{"TipoDocumentoSacado", type text}, {"RaizCNPJ", type text}})
in
    tipoAlteradoFinal;
shared staging_limpeza_de_titulos = let
  Origem = source_tab_titulos
in
  Origem;
shared fact_ultima_confirmacao = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "fact_ultima_confirmacao", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared source_tab_operacoes_devolucoes = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "tab_operacoes_devolucoes", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared limpeza_tab_operacoes_devolucoes = let
  Origem = source_tab_operacoes_devolucoes,
    
    // Remove colunas desnecessárias.
    colunasRemovidas = Table.RemoveColumns(Origem,{"USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA"}),
  #"Duplicatas removidas" = Table.Distinct(colunasRemovidas, {"CODTITULO"})
in
    #"Duplicatas removidas";
shared source_rlc_clientes_sacados_limites = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "rlc_clientes_sacados_limites", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_rlc_clientes_sacados_limites = let
  Origem = source_rlc_clientes_sacados_limites,

    // Substitui o valor "I" por "INTERCIA" na coluna "TIPO".
    valorSubstituido = Table.ReplaceValue(Origem,"I","INTERCIA",Replacer.ReplaceText,{"TIPO"}),

    // Adiciona a coluna "TipoDocumentoSacado" (CPF, CNPJ ou Inválido).
    tipoDocumentoSacadoAdicionado = Table.AddColumn(valorSubstituido, "TipoDocumentoSacado", each if Text.Length([CPFCNPJ])=11 then "CPF"
else if Text.Length([CPFCNPJ])=14 then "CNPJ"
else "Inválido"),

    // Adiciona a coluna "RaizCNPJ" (8 primeiros dígitos do CNPJ ou o próprio CPF/Inválido).
    raizCnpjAdicionado = Table.AddColumn(tipoDocumentoSacadoAdicionado, "RaizCNPJ", each if [TipoDocumentoSacado]="CNPJ" then Text.Start([CPFCNPJ],8) else [CPFCNPJ]),

    // Altera o tipo das colunas "TipoDocumentoSacado" e "RaizCNPJ" para texto.
    tipoAlterado = Table.TransformColumnTypes(raizCnpjAdicionado,{{"TipoDocumentoSacado", type text}, {"RaizCNPJ", type text}}),

    // Cria a chave "chave_cliente_sacado" concatenando CODCLIENTE e RaizCNPJ.
    chaveClienteSacadoInserida = Table.AddColumn(tipoAlterado, "chave_cliente_sacado", each Text.Combine({Text.From([CODCLIENTE], "pt-BR"), "-", [RaizCNPJ]}), type text),

    // Remove linhas duplicadas com base na coluna "chave_cliente_sacado".
    duplicatasRemovidas = Table.Distinct(chaveClienteSacadoInserida, {"chave_cliente_sacado"})
in
    duplicatasRemovidas;
shared fato_titulos_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "fato_titulos", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

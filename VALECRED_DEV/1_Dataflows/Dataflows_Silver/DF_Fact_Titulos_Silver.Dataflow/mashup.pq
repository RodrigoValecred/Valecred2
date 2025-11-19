section Section1;
shared staging_fato_titulos_materialized = let
    fonteDadosTitulos = staging_titulos,
    filtrarTitulosPorTipo = Table.SelectRows(fonteDadosTitulos, each ([TDOC] <> "BL" and [TDOC] <> "RC")),
    chaveClienteSacadoInserida = Table.AddColumn(filtrarTitulosPorTipo, "chave_cliente_sacado", each Text.Combine({Text.From([cod_cliente], "pt-BR"), "-", [RaizCNPJ]}), type text),
    mesclarTitulosComLimites = Table.NestedJoin(chaveClienteSacadoInserida, {"chave_cliente_sacado"}, staging_rlc_clientes_sacados_limites, {"chave_cliente_sacado"}, "relacionamento_intercompany", JoinKind.LeftOuter),
    expandirRelacionamentoIntercompany = Table.ExpandTableColumn(mesclarTitulosComLimites, "relacionamento_intercompany", {"TIPO"}, {"intercompany"}),
    padronizarColunaIntercompanyNulo = Table.ReplaceValue(expandirRelacionamentoIntercompany,null,"NÃO",Replacer.ReplaceValue,{"intercompany"}),
    padronizarColunaIntercompanyIntercia = Table.ReplaceValue(padronizarColunaIntercompanyNulo,"INTERCIA","SIM",Replacer.ReplaceText,{"intercompany"}),
    mesclarTitulosComProdutos = Table.NestedJoin(padronizarColunaIntercompanyIntercia, {"chave_produto"}, dim_produtos, {"chave_produto"}, "produtos", JoinKind.LeftOuter),
    expandirDadosProdutos = Table.ExpandTableColumn(mesclarTitulosComProdutos, "produtos", {"produto_informacao_de_mercado"}, {"produto_temp"}),
    mesclarComRelacionamentoRecompras = Table.NestedJoin(expandirDadosProdutos, {"CODTITULO"}, limpeza_tab_operacoes_devolucoes, {"CODTITULO"}, "relacionamento_recompras", JoinKind.LeftOuter),
    expandirRelacionamentoRecompras = Table.ExpandTableColumn(mesclarComRelacionamentoRecompras, "relacionamento_recompras", {"CODOPERACAO"}, {"cod_operacao_recompra"}),
    mesclarComUltimaChecagem = Table.NestedJoin(expandirRelacionamentoRecompras, {"CODTITULO"}, fact_ultima_confirmacao, {"cod_titulo"}, "ultima_checagem", JoinKind.LeftOuter),
    expandirUltimaChecagem = Table.ExpandTableColumn(mesclarComUltimaChecagem, "ultima_checagem", {"confirmacao"}, {"confirmado_por"}),
    padronizarAmortizacoesNulo = Table.ReplaceValue(expandirUltimaChecagem,null,0,Replacer.ReplaceValue,{"AMORTIZACOES"})
 in
    padronizarAmortizacoesNulo;
shared fato_titulos = let
    fonteDeDados = staging_fato_titulos_materialized,
    calcularValorTotalPorPrazo = Table.AddColumn(fonteDeDados, "valor_vezes_prazo", each [PRAZO]*[VALOR]),
    converterValorTotalPrazoParaNumero = Table.TransformColumnTypes(calcularValorTotalPorPrazo,{{"valor_vezes_prazo", type number}}),
    personalizarNomeProdutoIntercompany = Table.AddColumn(converterValorTotalPrazoParaNumero, "produto_com_intercia_temp", each if [intercompany] = "SIM" and List.Contains({"NO","CM"},[chave_produto]) then   "INTERCOMPANY"  else [produto_temp], type text),
    removerColunaProdutoOriginal = Table.RemoveColumns(personalizarNomeProdutoIntercompany,{"produto_temp"}),
    renomearColunaProdutoPersonalizado = Table.RenameColumns(removerColunaProdutoOriginal,{{"produto_com_intercia_temp", "produto_com_intercia"}}),
    adicionarDiaDaSemanaVencimento = Table.AddColumn(renomearColunaProdutoPersonalizado, "dia_da_semana", each Date.DayOfWeek([VENCPRORROGADO]), Int64.Type),
    ajustarDataVencimentoParaDiaUtil = Table.AddColumn(adicionarDiaDaSemanaVencimento, "data_vencimento_util_temp", each DateTime.Date(if [dia_da_semana] = 0 then Date.AddDays([VENCPRORROGADO],1) else if [dia_da_semana] = 6 then Date.AddDays([VENCPRORROGADO],2) else [VENCPRORROGADO]), type date),
    mesclarComTabelaFeriados = Table.NestedJoin(ajustarDataVencimentoParaDiaUtil, {"data_vencimento_util_temp"}, source_tab_feriados, {"DATAFERIADO"}, "feriados", JoinKind.LeftOuter),
    expandirDadosFeriados = Table.ExpandTableColumn(mesclarComTabelaFeriados, "feriados", {"TFERIADO"}, {"tipo_feriado"}),
    calcularDataPrevisaoLiquidacao = Table.AddColumn(expandirDadosFeriados, "data_previsao_liquidacao",
each DateTime.Date(if [tipo_feriado] = "L" then  [data_vencimento_util_temp]
else if [tipo_feriado] = "N" and [dia_da_semana] = 5 then Date.AddDays([data_vencimento_util_temp],3)
else if [tipo_feriado] = "N" and [dia_da_semana] <> 5 then Date.AddDays([data_vencimento_util_temp],1)
else [data_vencimento_util_temp]), type date),
    removerColunasAuxiliaresData = Table.RemoveColumns(calcularDataPrevisaoLiquidacao,{"data_vencimento_util_temp", "tipo_feriado", "dia_da_semana"}),
    renomearColunaDataPrevisaoLiquidacao = Table.RenameColumns(removerColunasAuxiliaresData,{{"data_previsao_liquidacao", "data_vencimento_util"}}),
    calcularStatusDeferimento = Table.AddColumn(renomearColunaDataPrevisaoLiquidacao, "status_deferimento", each if [ACEITO] = "S" and [STATUSACEITE] = "A" and [STATUSANALISE] = "D" then "Sim" else "Não", type text),
    definirStatusCleanTitulos = Table.AddColumn(calcularStatusDeferimento, "status_clean",
    each if [produto_com_intercia] = "DESCONTO" then "NORMAL"
    else "CLEAN", type text),
    padronizarConfirmacaoDocumentosN = Table.ReplaceValue(definirStatusCleanTitulos,"N","Atenção",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosS = Table.ReplaceValue(padronizarConfirmacaoDocumentosN,"S",null,Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosC = Table.ReplaceValue(padronizarConfirmacaoDocumentosS,"C","Positivo",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosP = Table.ReplaceValue(padronizarConfirmacaoDocumentosC,"P","Problema",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosA = Table.ReplaceValue(padronizarConfirmacaoDocumentosP,"A","Alerta",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosNulo = Table.ReplaceValue(padronizarConfirmacaoDocumentosA,null,"Não Contatado",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosE = Table.ReplaceValue(padronizarConfirmacaoDocumentosNulo,"E","Eletrônico",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    padronizarConfirmacaoDocumentosAZ = Table.ReplaceValue(padronizarConfirmacaoDocumentosE,"AZ","Eletrônico",Replacer.ReplaceValue,{"DOCCONFIRMADO"}),
    renomearColunaDocConfirmado = Table.RenameColumns(padronizarConfirmacaoDocumentosAZ,{{"DOCCONFIRMADO", "confirmacao"}}),
    criarColunaOrdemConfirmacao = Table.AddColumn(renomearColunaDocConfirmado, "ordem_confirmacao", each if [confirmacao] = "Não Contatado" then 5 else if [confirmacao] = "Atenção" then 2 else if [confirmacao] = "Eletrônico" then 0
    else if [confirmacao] = "Positivo" then 1
    else if [confirmacao] = "Alerta" then 3
    else if [confirmacao] = "Problema" then 4
    else null, type number),
    renomearColunasFinal = Table.RenameColumns(criarColunaOrdemConfirmacao, {
        {"CODTITULO", "cod_titulo"}, {"CODOPERACAO", "cod_operacao"}, {"TDOC", "tipo_documento"}, {"NDOC", "numero_documento"},
        {"CPFCNPJSACADO", "cpf_cnpj_sacado"},  {"VENCIMENTO", "vencimento"}, {"VENCPRORROGADO", "vencimento_prorrogado"},
        {"VALOR", "valor"}, {"PRAZO", "prazo"}, {"ACEITO", "aceito"}, {"DATAINCLUSAO", "data_inclusao"}, {"USUAINCLUSAO", "usuario_inclusao"},
        {"DATAALTERACAO", "data_alteracao"}, {"USUAALTERACAO", "usuario_alteracao"}, {"AMORTIZACOES", "amortizacoes"}, {"chave_produto", "chave_produto"},
        {"status_protesto", "status_protesto"}, {"TipoDocumentoSacado", "tipo_documento_sacado"}, {"RaizCNPJ", "raiz_cnpj"}})
in
    renomearColunasFinal;
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
  Origem = source_tab_titulos,

    // Realiza um Left Outer Join com a tabela "replica_tab_operacoes" usando "CODOPERACAO".
    consultasMescladas = Table.NestedJoin(Origem, {"CODOPERACAO"}, fact_operacoes, {"cod_operacao"}, "operacoes", JoinKind.LeftOuter),

    // Expande a coluna "operacoes" para trazer dados específicos da operação.
    operacoesExpandido = Table.ExpandTableColumn(consultasMescladas, "operacoes", {"cod_cliente", "data_analise", "status_aceite", "status_analise","chave_produto","cod_empresa"}, {"cod_cliente", "data_analise", "status_aceite", "status_analise","chave_produto","cod_empresa"}),

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

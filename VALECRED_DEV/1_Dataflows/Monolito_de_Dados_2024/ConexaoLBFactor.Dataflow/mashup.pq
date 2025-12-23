[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared replica_tab_titulos = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODTITULO, CODOPERACAO, NDOC,TDOC,VENCIMENTO,VENCPRORROGADO,PRAZO,CPFCNPJSACADO,CPFCNPJCEDENTE,VALOR,DESAGIO,LIQUIDO,AMORTIZACOES,VALORDEVIDO,LIQUIDACAO,#(lf)ACEITO,CODBANCOCOBR,DATACONF,USUACONF,DATAINCLUSAO,DOCCONFIRMADO,MOTIVO,PRACA,CHAVEDANFE,NOSSONUMERO FROM tab_titulos WHERE YEAR(DATAINCLUSAO) >= 2021", ReturnSingleDatabase=true]),

    // Altera o tipo das colunas para os tipos de dados corretos.
    tipoAlterado = Table.TransformColumnTypes(origem,{{"CODTITULO", Int64.Type}, {"CODOPERACAO", Int64.Type}, {"NDOC", type text}, {"TDOC", type text}, {"VENCIMENTO", type date}, {"VENCPRORROGADO", type date}, {"PRAZO", Int64.Type},
     {"CPFCNPJSACADO", type text}, {"CPFCNPJCEDENTE", type text}, {"VALOR", type number}, {"DESAGIO", type number}, {"LIQUIDO", type number}, {"AMORTIZACOES", type number}, {"VALORDEVIDO", type number}, {"LIQUIDACAO", type date}, {"ACEITO", type text},
      {"CODBANCOCOBR", Int64.Type}, {"DATACONF", type datetime}, {"USUACONF", type number}, {"DATAINCLUSAO", type datetime}, {"DOCCONFIRMADO", type text}, {"MOTIVO", type text}, {"PRACA", type text}, {"CHAVEDANFE", type text}}),

    // Realiza um Left Outer Join com a tabela "replica_tab_operacoes" usando "CODOPERACAO".
    consultasMescladas = Table.NestedJoin(tipoAlterado, {"CODOPERACAO"}, replica_tab_operacoes, {"CODOPERACAO"}, "operacoes", JoinKind.LeftOuter),

    // Expande a coluna "operacoes" para trazer dados específicos da operação.
    operacoesExpandido = Table.ExpandTableColumn(consultasMescladas, "operacoes", {"CODCLIENTE", "DATAANALISE", "STATUSACEITE", "STATUSANALISE"}, {"CODCLIENTE", "DATAANALISE", "STATUSACEITE", "STATUSANALISE"}),

    // Filtra linhas removendo aquelas com valor nulo em DATAANALISE.
    linhasDataAnaliseNaoNulas = Table.SelectRows(operacoesExpandido, each ([DATAANALISE] <> null)),

    // Adiciona a coluna "BASE" com o valor constante 40.
    personalizacaoAdicionada = Table.AddColumn(linhasDataAnaliseNaoNulas, "BASE", each 40),

    // Cria a chave combinada "chave_base_titulo" concatenando BASE e CODTITULO.
    chaveBaseTituloInserida = Table.AddColumn(personalizacaoAdicionada, "chave_base_titulo", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODTITULO], "pt-BR")}), type text),

    // Realiza um Left Outer Join com a tabela "replica_rlc_titulos_ocorrencias_cobranca2protestos" usando "CODTITULO".
    consultasMescladasProtestos = Table.NestedJoin(chaveBaseTituloInserida, {"CODTITULO"}, stg_protestos, {"CODTITULO"}, "lb_protestos", JoinKind.LeftOuter),

    // Expande a coluna "lb_protestos" para trazer "TIPO" como "Status Protesto".
    lbProtestosExpandido = Table.ExpandTableColumn(consultasMescladasProtestos, "lb_protestos", {"TIPO"}, {"Status Protesto"}),

    // Substitui valores nulos na coluna "Status Protesto" por "NÃO PROTESTADO".
    statusProtestoCorrigido = Table.ReplaceValue(lbProtestosExpandido,null,"NÃO PROTESTADO",Replacer.ReplaceValue,{"Status Protesto"}),

    // Adiciona a coluna "TipoDocumentoSacado" (CPF, CNPJ ou Inválido).
    tipoDocumentoSacadoAdicionado = Table.AddColumn(statusProtestoCorrigido, "TipoDocumentoSacado", each if Text.Length([CPFCNPJSACADO])=11 then "CPF"
else if Text.Length([CPFCNPJSACADO])=14 then "CNPJ"
else "Inválido"),

    // Adiciona a coluna "RaizCNPJ" (8 primeiros dígitos do CNPJ ou o próprio CPF/Inválido).
    raizCnpjAdicionado = Table.AddColumn(tipoDocumentoSacadoAdicionado, "RaizCNPJ", each if [TipoDocumentoSacado]="CNPJ" then Text.Start([CPFCNPJSACADO],8) else [CPFCNPJSACADO]),

    // Altera o tipo das colunas "TipoDocumentoSacado" e "RaizCNPJ" para texto.
    tipoAlteradoFinal = Table.TransformColumnTypes(raizCnpjAdicionado,{{"TipoDocumentoSacado", type text}, {"RaizCNPJ", type text}}),

    // Cria a chave "chave_cliente_sacado" concatenando CODCLIENTE e RaizCNPJ.
    chaveClienteSacadoInserida = Table.AddColumn(tipoAlteradoFinal, "chave_cliente_sacado", each Text.Combine({Text.From([CODCLIENTE], "pt-BR"), "-", [RaizCNPJ]}), type text)
in
    chaveClienteSacadoInserida;
shared replica_tab_operacoes = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT NBORDERO, CODBROKER, CODOPERACAO, CODEMPRESA, DATAINCLUSAO, CODCLIENTE, CPFCNPJCLIENTE, STATUSANALISE, DATAANALISE, STATUSACEITE, DATAACEITE, DATAENVIOEMAIL, STTO, TTO, FATOR, TARIFA, TARIFARECOMPRA, TAC, NDOCS, NDOCSRECOMPRA, TOTFAC, TOTADVAL, TOTTAXAADM, TOTTAR, TOTPENDENCIAS, TOTRETENCAO, TOTRECOMPRA, TOTDES,TOTDCP, APROVACAO1, DATAALTERACAO, CODINDEFERIMENTO, CONTRATOFISICO, USUAINCLUSAO, USUATRAVA, USUASTANALISE, NOTASERVICO FROM tab_operacoes", ReturnSingleDatabase=true]),
    // Filtra linhas com DATAANALISE posterior a 31/12/2020.
    linhasDataAnaliseFiltradas = Table.SelectRows(origem, each [DATAANALISE] > #datetime(2020, 12, 31, 0, 0, 0)),

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
shared replica_rlc_titulos_ocorrencias_cobranca = let
    // Conecta-se a um banco de dados MySQL.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM rlc_titulos_ocorrencias_cobranca", ReturnSingleDatabase=true]),
    #"Tipo Alterado" = Table.TransformColumnTypes(origem,{{"DATAOCORRENCIA", type date}, {"DATAVENCIMENTO", type date}, {"DATACREDITO", type date}}),
    // Classifica as linhas pela 'DATAOCORRENCIA' em ordem decrescente.
    linhasClassificadas = Table.Sort(#"Tipo Alterado",{{"DATAOCORRENCIA", Order.Descending}}),
    
    // Filtra as linhas com data de ocorrência posterior a 2024-01-01.
    linhasFiltradas = Table.SelectRows(linhasClassificadas, each [DATAOCORRENCIA] > #date(2024, 1, 1)),
    
    // Cria uma coluna mesclada 'Mesclado' a partir de TOCORRENCIA, CODOCORINTERNA e CODBANCO.
    colunaMescladaInserida = Table.AddColumn(linhasFiltradas, "Mesclado", each Text.Combine({Text.From([TOCORRENCIA], "pt-BR"), "-", Text.From([CODOCORINTERNA], "pt-BR"), "-", Text.From([CODBANCO], "pt-BR")}), type text),
    
    // Remove colunas desnecessárias.
    colunasRemovidas = Table.RemoveColumns(colunaMescladaInserida,{"MOTIVOCODOCORCOBRBANCO", "MOTIVOCONFINSTRUCAO", "CODOCORCOBR", "DATAGRAVACAO"}),
    
    // Mescla com 'lb_tipo_cobranca' usando 'Mesclado' e 'chave_ocorrencia'.
    consultasMescladas = Table.NestedJoin(colunasRemovidas, {"Mesclado"}, replica_cad_conta_cobranca_ocorrencia, {"chave_ocorrencia"}, "lb_tipo_cobranca", JoinKind.LeftOuter),
    
    // Expande a coluna 'lb_tipo_cobranca' para incluir 'DESCRICAO'.
    tipoCobrancaExpandido = Table.ExpandTableColumn(consultasMescladas, "lb_tipo_cobranca", {"DESCRICAO"}, {"DESCRICAO"})
in
    tipoCobrancaExpandido;
shared replica_cad_conta_cobranca_ocorrencia = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [ReturnSingleDatabase=true]),
    
    // Obtém dados da tabela 'cad_conta_cobranca_ocorrencia'.
    navegacao = origem{[Schema = "lbfactor", Item = "cad_conta_cobranca_ocorrencia"]}[Data],
    
    // Cria 'chave_ocorrencia' concatenando TOCORRENCIA, CODINSTRUCAO e CODBANCO.
    chaveOcorrenciaInserida = Table.AddColumn(navegacao, "chave_ocorrencia", each Text.Combine(
    {Text.From([TOCORRENCIA], "pt-BR"), "-", Text.From([CODINSTRUCAO], "pt-BR"), "-", Text.From([CODBANCO], "pt-BR")}), type text),
    
    // Remove colunas originais usadas na chave e outras como VALOR.
    colunasRemovidas = Table.RemoveColumns(chaveOcorrenciaInserida,{"CODOCORCOBRBANCO", "CODINSTRUCAO", "CODBANCO", "TOCORRENCIA", "VALOR"}),
    
    // Classifica as linhas pela 'chave_ocorrencia' em ordem ascendente.
    linhasClassificadas = Table.Sort(colunasRemovidas,{{"chave_ocorrencia", Order.Ascending}}),
    
    // Remove linhas duplicadas com base na 'chave_ocorrencia'.
    duplicatasRemovidas = Table.Distinct(linhasClassificadas, {"chave_ocorrencia"})
in
    duplicatasRemovidas;
shared stg_protestos = let
    // Conecta-se a um banco de dados MySQL para obter ocorrências de cobrança de títulos a partir de 2023.
    origem = replica_rlc_titulos_ocorrencias_cobranca,
    
    // Classifica as linhas pela 'DATAINCLUSAO' em ordem decrescente.
    linhasClassificadas = Table.Sort(origem,{{"DATAINCLUSAO", Order.Descending}}),
    
    // Filtra ocorrências com 'CODOCORINTERNA' <> 2, 'TOCORRENCIA' = 2 e 'CODOCORCOBRBANCO' 19 ou 23.
    linhasFiltradas = Table.SelectRows(linhasClassificadas, each ([CODOCORINTERNA] <> 2) and [TOCORRENCIA] = 2 and ([CODOCORCOBRBANCO] = 19 or [CODOCORCOBRBANCO] = 23)),
    
    // Remove colunas desnecessárias.
    // Nota: Este passo foi inferido do contexto anterior, pois estava faltando.
    colunasRemovidas = Table.RemoveColumns(linhasFiltradas,{ "VALORABATIM", "VALORDESCONTO", "CODTITULOOCORCOB", "CODOPERACAO", "CODBANCO", "CODAGENCIA",
     "CODCC", "CARTEIRA", "NOSSONUMERO", "SEUNUMERO", "DATAVENCIMENTO", "DATAOCORRENCIA", "DATACREDITO", "VALORDOCUMENTO", "VALORMORAJUROS", "VALORDESPCOBR", "VALORIOF", "VALOROUTRADESP", "VALOROUTROCRED", "VALORCREDITO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO",
      "USUAALTERACAO", "AGENCIARECEBEDORA", "BANCORECEBEDOR", "MD5"}),

    // Adiciona a coluna 'TIPO' como "PROTESTADO" ou "NÃO PROTESTADO".
    colunaCondicionalAdicionada = Table.AddColumn(colunasRemovidas, "TIPO", each if [CODOCORINTERNA] >= 8 then "PROTESTADO" else "NÃO PROTESTADO", type text),
    
    // Remove colunas auxiliares de ocorrência.
    colunasRemovidasFinais = Table.RemoveColumns(colunaCondicionalAdicionada,{"CODOCORINTERNA", "CODOCORCOBRBANCO", "TOCORRENCIA"}),
    
    // Remove linhas duplicadas com base na coluna 'CODTITULO'.
    duplicatasRemovidas = Table.Distinct(colunasRemovidasFinais, {"CODTITULO"})
in
    duplicatasRemovidas;
shared titulos = let
    // Define a tabela 'replicas_tab_titulos' como a fonte inicial de dados
    fonteDadosTitulos = replica_tab_titulos,
    // Ordenar no mais novo para o mais antigo
    classificarPorDataInclusao = Table.Sort(fonteDadosTitulos,{{"DATAINCLUSAO", Order.Descending}}),
    // Mescla dados dos títulos com informações de limites dos clientes sacados (relacionamento intercompany)
    mesclarTitulosComLimites = Table.NestedJoin(classificarPorDataInclusao, {"chave_cliente_sacado"}, replica_rlc_clientes_sacados_limites, {"chave_cliente_sacado"}, "relacionamento_intercompany", JoinKind.LeftOuter),
    // Filtra os títulos, removendo os do tipo "BL" e "RC" (boletos e recibos)
    filtrarTitulosPorTipo = Table.SelectRows(mesclarTitulosComLimites, each ([TDOC] <> "BL" and [TDOC] <> "RC")),
    // Expande a coluna de relacionamento intercompany para trazer o tipo (Intercompany ou não)
    expandirRelacionamentoIntercompany = Table.ExpandTableColumn(filtrarTitulosPorTipo, "relacionamento_intercompany", {"TIPO"}, {"Intercompany"}),
    // Calcula o valor total do título multiplicado pelo prazo (VALOR X PRAZO)
    calcularValorTotalPorPrazo = Table.AddColumn(expandirRelacionamentoIntercompany, "VALOR X PRAZO", each [PRAZO]*[VALOR]),
    // Define o tipo da coluna 'valorXPrazo' como número para cálculos futuros
    converterValorTotalPrazoParaNumero = Table.TransformColumnTypes(calcularValorTotalPorPrazo,{{"VALOR X PRAZO", type number}}),
    // Cria uma chave única para o sacado (BASE-CPFCNPJSACADO)
    criarChaveBaseSacado = Table.AddColumn(converterValorTotalPrazoParaNumero, "chaveBaseSacado", each Text.Combine({Text.From([BASE], "pt-BR"), "-", [CPFCNPJSACADO]}), type text),
    // Cria uma chave única para a operação (BASE-CODOPERACAO)
    criarChaveBaseOperacao = Table.AddColumn(criarChaveBaseSacado, "chaveBaseOperacao", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODOPERACAO], "pt-BR")}), type text),
    // Mescla os dados dos títulos com informações das operações (chave_base_operacao)
    mesclarTitulosComOperacoes = Table.NestedJoin(criarChaveBaseOperacao, {"chaveBaseOperacao"}, operacoes, {"chave_base_operacao"}, "operacoes", JoinKind.LeftOuter),
    // Expande os dados das operações para trazer informações sobre o produto e a empresa
    expandirDadosOperacoes = Table.ExpandTableColumn(mesclarTitulosComOperacoes, "operacoes", {"chave_produto","chave_base_empresa"}, {"chave_produto","chave_base_empresa"}),
    // Padroniza a coluna Intercompany, substituindo nulos por "NÃO"
    padronizarColunaIntercompanyNulo = Table.ReplaceValue(expandirDadosOperacoes,null,"NÃO",Replacer.ReplaceValue,{"Intercompany"}),
    // Padroniza a coluna Intercompany, substituindo "INTERCIA" por "SIM"
    padronizarColunaIntercompanyIntercia = Table.ReplaceValue(padronizarColunaIntercompanyNulo,"INTERCIA","SIM",Replacer.ReplaceText,{"Intercompany"}),
    // Mescla os dados dos títulos com informações dos produtos (chave_produto)
    mesclarTitulosComProdutos = Table.NestedJoin(padronizarColunaIntercompanyIntercia, {"chave_produto"}, produtos, {"chave_produto"}, "produtos", JoinKind.LeftOuter),
    // Expande os dados dos produtos para trazer a informação de mercado
    expandirDadosProdutos = Table.ExpandTableColumn(mesclarTitulosComProdutos, "produtos", {"Produto - Informação de Mercado"}, {"Produto"}),
    // Personaliza o nome do produto se for Intercompany e o produto for "NO" ou "CM"
    personalizarNomeProdutoIntercompany = Table.AddColumn(expandirDadosProdutos, "Personalizar", each if [Intercompany] = "SIM" and List.Contains({"NO","CM"},[chave_produto]) then   "INTERCOMPANY"  else [Produto], type text),
    // Remove a coluna original do Produto (já temos a personalizada)
    removerColunaProdutoOriginal = Table.RemoveColumns(personalizarNomeProdutoIntercompany,{"Produto"}),
    // Renomeia a coluna personalizada para "Produto com intercia"
    renomearColunaProdutoPersonalizado = Table.RenameColumns(removerColunaProdutoOriginal,{{"Personalizar", "Produto com intercia"}}),
    // Adiciona o dia da semana do vencimento prorrogado
    adicionarDiaDaSemanaVencimento = Table.AddColumn(renomearColunaProdutoPersonalizado, "DiaDaSemana", each Date.DayOfWeek([VENCPRORROGADO]), Int64.Type),
    // Ajusta a data de vencimento para o próximo dia útil, considerando finais de semana
    ajustarDataVencimentoParaDiaUtil = Table.AddColumn(adicionarDiaDaSemanaVencimento, "dataVencimentoUtil", each if [DiaDaSemana] = 0 then Date.AddDays([VENCPRORROGADO],1) else if [DiaDaSemana] = 6 then Date.AddDays([VENCPRORROGADO],2) else [VENCPRORROGADO], type date),
    // Mescla os dados com a tabela de feriados para identificar feriados na data de vencimento
    mesclarComTabelaFeriados = Table.NestedJoin(ajustarDataVencimentoParaDiaUtil, {"dataVencimentoUtil"}, replica_tab_feriados, {"DATAFERIADO"}, "feriados", JoinKind.LeftOuter),
    // Expande os dados dos feriados para trazer o tipo do feriado
    expandirDadosFeriados = Table.ExpandTableColumn(mesclarComTabelaFeriados, "feriados", {"TFERIADO"}, {"feriados.TFERIADO"}),
    // Calcula a data prevista de liquidação, considerando feriados e finais de semana
    calcularDataPrevisaoLiquidacao = Table.AddColumn(expandirDadosFeriados, "dataPrevisaoLiquidacao", 
each if [feriados.TFERIADO] = "L" then  [dataVencimentoUtil] 
else if [feriados.TFERIADO] = "N" and [DiaDaSemana] = 5 then Date.AddDays([dataVencimentoUtil],3) 
else if [feriados.TFERIADO] = "N" and [DiaDaSemana] <> 5 then Date.AddDays([dataVencimentoUtil],1) 
else [dataVencimentoUtil], type date),
    // Remove as linhas duplicadas com base na chave do título
    removerDuplicatasPorChaveTitulo = Table.Distinct(calcularDataPrevisaoLiquidacao, {"chave_base_titulo"}),
    // Remove colunas auxiliares que não são mais necessárias
    removerColunasAuxiliaresData = Table.RemoveColumns(removerDuplicatasPorChaveTitulo,{"dataVencimentoUtil", "feriados.TFERIADO", "DiaDaSemana"}),
    // Renomeia a coluna da data de previsão de liquidação para data de vencimento útil
    renomearColunaDataPrevisaoLiquidacao = Table.RenameColumns(removerColunasAuxiliaresData,{{"dataPrevisaoLiquidacao", "DATA VENCIMENTO UTIL"}}),
    // Calcula o status de deferimento com base nas colunas ACEITO, STATUSACEITE e STATUSANALISE
    calcularStatusDeferimento = Table.AddColumn(renomearColunaDataPrevisaoLiquidacao, "Status deferimento", each if [ACEITO] = "S" and [STATUSACEITE] = "A" and [STATUSANALISE] = "D" then "Sim" else "Não", type text),
    // Define o status "CLEAN" para títulos que não são de desconto
    definirStatusCleanTitulos = Table.AddColumn(calcularStatusDeferimento, "Status Clean", 
    each if [Produto com intercia] = "DESCONTO" then "NORMAL"
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
    renomearColunaDocConfirmado = Table.RenameColumns(padronizarConfirmacaoDocumentosAZ,{{"DOCCONFIRMADO", "CONFIRMAÇÃO"}}),
    // Cria uma coluna para ordenar a confirmação (Não Contatado = 5, Atenção = 2, Eletrônico = 0, etc.)
    criarColunaOrdemConfirmacao = Table.AddColumn(renomearColunaDocConfirmado, "ordemConfirmacao", each if [CONFIRMAÇÃO] = "Não Contatado" then 5 else if [CONFIRMAÇÃO] = "Atenção" then 2 else if [CONFIRMAÇÃO] = "Eletrônico" then 0 
    else if [CONFIRMAÇÃO] = "Positivo" then 1 
    else if [CONFIRMAÇÃO] = "Alerta" then 3 
    else if [CONFIRMAÇÃO] = "Problema" then 4 
    else null, type number),
    // Mescla os dados com informações de recompra (relacionamento_recompras)
    mesclarComRelacionamentoRecompras = Table.NestedJoin(criarColunaOrdemConfirmacao, {"chave_base_titulo"}, relacionamento_recompras, {"chave_base_titulo"}, "relacionamento_recompras", JoinKind.LeftOuter),
    // Expande os dados de recompra para trazer a chave da operação de recompra
    expandirRelacionamentoRecompras = Table.ExpandTableColumn(mesclarComRelacionamentoRecompras, "relacionamento_recompras", {"chave_base_operacao"}, {"chave_base_operacao_recompra"}),
    // Mescla os dados com informações da última checagem (ultima checagem)
    mesclarComUltimaChecagem = Table.NestedJoin(expandirRelacionamentoRecompras, {"CODTITULO"}, #"ultima checagem", {"CODTITULO"}, "ultima checagem", JoinKind.LeftOuter),
    // Expande os dados da última checagem para trazer a confirmação
    expandirUltimaChecagem = Table.ExpandTableColumn(mesclarComUltimaChecagem, "ultima checagem", {"CONFAO"}, {"CONFAO"}),
    // Renomeia a coluna de confirmação da última checagem
    renomearColunaConfirmacaoChecagem = Table.RenameColumns(expandirUltimaChecagem,{"CONFAO", "Confirmado por"}),
    // Padroniza a coluna AMORTIZACOES, substituindo nulos por 0
    padronizarAmortizacoesNulo = Table.ReplaceValue(renomearColunaConfirmacaoChecagem,null,0,Replacer.ReplaceValue,{"AMORTIZACOES"}),
    // Define o tipo da coluna BASE como texto
    converterBaseParaTexto = Table.TransformColumnTypes(padronizarAmortizacoesNulo,{{"BASE", type text}})
in
    // Retorna a tabela final com todas as transformações
    converterBaseParaTexto;
shared replica_rlc_clientes_sacados_limites = let
    // Conecta-se a um banco de dados MySQL para obter limites de clientes sacados.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODCLIENTE, CPFCNPJ, TIPO FROM rlc_clientes_sacados_limites #(lf)WHERE TIPO = 'I'", ReturnSingleDatabase=true]),

    // Substitui o valor "I" por "INTERCIA" na coluna "TIPO".
    valorSubstituido = Table.ReplaceValue(origem,"I","INTERCIA",Replacer.ReplaceText,{"TIPO"}),

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
shared stg_operacoes = let
    // Conecta ao banco de dados SQL.
    conexaoBancoDados = replica_tab_operacoes,
    // Classifica as linhas pela data de análise em ordem decrescente.
    linhasOrdenadasPorDataAnalise = Table.Sort(conexaoBancoDados,{{"DATAANALISE", Order.Descending}}),
    // Altera o tipo da coluna 'base' para texto.
    tipoColunaBaseAlterado = Table.TransformColumnTypes(linhasOrdenadasPorDataAnalise,{{"BASE", type text}}),

    // Mescla com estudo de operações para obter TAXACADASTRO.
    estudoOperacoesMesclado = Table.NestedJoin(tipoColunaBaseAlterado, {"CODOPERACAO"}, replica_tab_estudo_op, {"CODOPERACAO"}, "estudoOpTabela", JoinKind.LeftOuter),
    // Expande a coluna TAXACADASTRO.
    taxaCadastroExpandida = Table.ExpandTableColumn(estudoOperacoesMesclado, "estudoOpTabela", {"FATOR"}, {"TAXACADASTRO"}),

    // Mescla com usuários enriquecidos para dados de inclusão.
    usuariosInclusaoMesclado = Table.NestedJoin(taxaCadastroExpandida, {"USUAINCLUSAO"}, replica_cad_usuarios, {"CODUSUARIO"}, "usuariosInclusaoTabela", JoinKind.LeftOuter),
    // Expande nome, função e nível do usuário de inclusão.
    usuariosInclusaoExpandido = Table.ExpandTableColumn(usuariosInclusaoMesclado, "usuariosInclusaoTabela", {"NOME", "FUNCAO", "NIVEL"}, {"USUARIO_INCLUSAO", "INCLUIDO_POR", "NIVEL"}),

    // Mescla com usuários enriquecidos para dados do analista.
    usuariosAnaliseMesclado = Table.NestedJoin(usuariosInclusaoExpandido, {"USUASTANALISE"}, replica_cad_usuarios, {"CODUSUARIO"}, "usuariosAnaliseTabela", JoinKind.LeftOuter),
    // Expande o nome do analista.
    analistaExpandido = Table.ExpandTableColumn(usuariosAnaliseMesclado, "usuariosAnaliseTabela", {"NOME"}, {"ANALISTA"}),

    // Mescla com usuários enriquecidos para dados do analista que travou.
    usuariosTravaMesclado = Table.NestedJoin(analistaExpandido, {"USUATRAVA"}, replica_cad_usuarios, {"CODUSUARIO"}, "usuariosTravaTabela", JoinKind.LeftOuter),
    // Expande o nome do analista que travou.
    analistaTravaExpandido = Table.ExpandTableColumn(usuariosTravaMesclado, "usuariosTravaTabela", {"NOME"}, {"ANALISTA_TRAVA"}),

    // Converte colunas de texto para maiúsculas.
    textoParaMaiusculas = Table.TransformColumns(analistaTravaExpandido, {{"USUARIO_INCLUSAO", Text.Upper, type text}, {"INCLUIDO_POR", Text.Upper, type text}}),

    // Mescla com 'nivel_usuario' para informações de nível.
    nivelUsuarioMesclado = Table.NestedJoin(textoParaMaiusculas, {"NIVEL"}, nivel_usuario, {"NIVEL"}, "nivelUsuarioTabela", JoinKind.LeftOuter),

    // Mescla com grupos de indeferimento para obter motivos.
    gruposIndeferimentoMesclado = Table.NestedJoin(nivelUsuarioMesclado, {"CODINDEFERIMENTO"}, #"grupos indeferimento", {"CODINDEFERIMENTO"}, "motivosIndeferimentoTabela", JoinKind.LeftOuter),
    // Expande colunas de motivo e grupo de indeferimento.
    motivosIndeferimentoExpandido = Table.ExpandTableColumn(gruposIndeferimentoMesclado, "motivosIndeferimentoTabela", {"MOTIVO INDEFERIMENTO", "GRUPO MOTIVO INDEFERIMENTO"}, {"MOTIVO INDEFERIMENTO", "GRUPO MOTIVO INDEFERIMENTO"}),

    // Adiciona a chave composta para cliente (BASE-CODCLIENTE).
    chaveClienteAdicionada = Table.AddColumn(motivosIndeferimentoExpandido, "chave_base_cliente", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODCLIENTE], "pt-BR")}), type text),
    // Adiciona a chave composta para operação (BASE-CODOPERACAO).
    chaveOperacaoAdicionada = Table.AddColumn(chaveClienteAdicionada, "chave_base_operacao", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODOPERACAO], "pt-BR")}), type text),
    // Adiciona a chave para o produto (TTO+STTO).
    chaveProdutoAdicionada = Table.AddColumn(chaveOperacaoAdicionada, "chave_produto", each Text.Combine({[TTO], [STTO]}), type text),
    // Adiciona a chave composta para empresa (BASE-CODEMPRESA).
    chaveEmpresaAdicionada = Table.AddColumn(chaveProdutoAdicionada, "chave_base_empresa", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODEMPRESA], "pt-BR")}), type text),

    // Extrai a data do campo DATAANALISE.
    dataDeferimentoExtraida = Table.AddColumn(chaveEmpresaAdicionada, "Data deferimento", each Date.From([DATAANALISE]), type date),
    // Classifica operações em "VALE S" ou "VALE N".
    eraClassificada = Table.AddColumn(dataDeferimentoExtraida, "ERA", each if [Data deferimento] <> null and [Data deferimento] > #date(2023, 8, 31) then "VALE S" else "VALE N"),

    // Substitui valores nulos por 0 nas colunas numéricas.
    nulosNumericosSubstituidos = Table.ReplaceValue(eraClassificada, null, 0, Replacer.ReplaceValue, {"TAC", "TOTTAXAADM", "TOTADVAL", "TOTTAR", "NDOCSRECOMPRA"})
in
    nulosNumericosSubstituidos;
shared replica_tab_estudo_op = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    conexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM tab_estudo_op", ReturnSingleDatabase=true])
in
    conexaoMySQL;
shared replica_cad_usuarios = let
  // Origem dos dados: Conecta-se ao banco de dados MySQL na AWS
  origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM cad_usuarios", ReturnSingleDatabase=true]),

  // Remove colunas desnecessárias da tabela de usuários.
    colunasRemovidas = Table.RemoveColumns(
        origem,
        {
            "LOGIN",
            "SENHA",
            "STATUS",
            "EXPIRADA",
            "DATAEXPIRACAO",
            "TIPO",
            "CCUSTO",
            "WALLPAPER",
            "DATAINCLUSAO",
            "USUAINCLUSAO",
            "DATAALTERACAO",
            "USUAALTERACAO",
            "APELIDO"
        }
    ),

    // Converte a coluna NOME para maiúsculas.
    nomeEmMaiusculas = Table.TransformColumns(colunasRemovidas, {{"NOME", Text.Upper, type text}}),

    // Converte a coluna FUNCAO para maiúsculas.
    funcaoEmMaiusculas = Table.TransformColumns(nomeEmMaiusculas, {{"FUNCAO", Text.Upper, type nullable text}}),

    // Mescla com Bronze_NivelUsuario_Manual para obter a descrição do nível.
    nivelUsuarioMesclado = Table.NestedJoin(funcaoEmMaiusculas, {"NIVEL"}, nivel_usuario, {"NIVEL"}, "Bronze_NivelUsuario_Manual", JoinKind.LeftOuter),
    
    // Expande a coluna 'descricao' do nível do usuário.
    nivelUsuarioExpandido = Table.ExpandTableColumn(nivelUsuarioMesclado, "Bronze_NivelUsuario_Manual", {"descricao"}, {"nivel"})
in
    nivelUsuarioExpandido;
shared nivel_usuario = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("VY5JCsMwDEWvYrzOovOwNLYSBIkUZCUUTO5/jajGpHT5/tPwS/En3/mQJiTMKiGx+K0r/mxpQgFtfDEeQIAUKl+N8zKDrJjbyM2ifqGITEGQa3b/rbkEjmDg2NTjX81j0NCzTKHap9k44vHv9WUmPQq+a+2VB0scfBSE7O62Aw==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [NIVEL = _t, DESCRICAO = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"NIVEL", Int64.Type}, {"DESCRICAO", type text}})
in
    #"Tipo Alterado";
shared #"grupos indeferimento" = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("fVVLbtswEL0K4XUCWJ/E7XJCUe4AEkchKSNNkLu06KJAgayKnkg36Uk6QyqVZEkBvLDNx/m8N/P48nLIDjcHDVabBlpjA6nKKOqMg+Hn8IOuDg+vNy+HnP80T9p4T8qSarDFYJSmFr0fvjuE6Tzii018JR+vSVJaci00V9fKzWtog3GcqwP79erC3V6emlLxS/g9//agoWI0d6o6OIMCJeENOvL/jyP6dIWWEsgFWKI+TaiOvO9RXYzVWNES9jnSWoEPjpSH4a0CqdMh8zE7itjsyP+MvXjAj5DZJgHGd0YPbzVq1lbSxTKWXGSiKWuhmadJ+VnFmYhoazUOQwXxZzpaCMUEOsZ8MdcZ7lKGGlnqmEDkN/axh8qleHxaYTqKA6iRLE9FvH0/pqc2MRoF9arCi3Fn/m78rKDTgt6e+7d+zZZoFaUE1juA0g1KIPX3228V4Am2j9NdEXAlcE2OxQFWvhneAmpI2yL6tVRBw9qxdClm5+gydj0vKs8msafAS0g+7w4tBLysQSLXuF4yjKZV1Au+gzTdHzSXl9MQPzT02Bu4nt/8LsnRksVAIp/qTEOKW+onHXIRjYWssRFaFqayw9UpxZ3JqjzXroE3TeZek3PmPcZ7GhEyYOi5AB4LcopLVtnxeHM87meKK1hTt2hxjFgcd+jZJ63IVv4QR536JXPFtnlya7J5hh2o7kPvVhYqek4Eskn5WM7Km4tyCeRyHJI6806K4co6NLukFNsOmpwd2Ng51hlX5lHczxxqZ9uK08623caRxOj+u+TureqtckaM2H20rMXnSRrxjwfAJ+DeHnuszPNSnjKLI2ur4RcvH6THQ94mfJ69h7O+ynwZ3LEwDjcMtCyWQGhCEukPzzh38e5h8ytl2gYeC+SosmW34xrXvZ0PbLn39I3vK9p69cK+/gM=", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [CODINDEFERIMENTO = _t, #"MOTIVO INDEFERIMENTO" = _t, #"GRUPO MOTIVO INDEFERIMENTO" = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"CODINDEFERIMENTO", Int64.Type}, {"MOTIVO INDEFERIMENTO", type text}, {"GRUPO MOTIVO INDEFERIMENTO", type text}})
in
    #"Tipo Alterado";
shared operacoes = let
    // Define a origem dos dados como a tabela Silver_Operacoes_Padronizado.
    origem = stg_operacoes,

    // Filtra linhas com tipos de operação específicos (CM, FC, NO, RN).
    // Nota: O input 'Fonte' foi alterado para 'ttoColunaRenomeada' para seguir o fluxo da query.
    linhasFiltradasTipoOperacao = Table.SelectRows(origem, each ([TTO] = "CM" or [TTO] = "FC" or [TTO] = "NO" or [TTO] = "RN" or [TTO] = "GR" or [TTO] = "CS" or  [TTO] = "NC" )),
    
    // Insere a coluna 'chave_ano_mes' extraindo o ano e mês da 'Data deferimento'.
    chaveAnoMesInserida = Table.AddColumn(linhasFiltradasTipoOperacao, "chave_ano_mes", each Text.BetweenDelimiters(Text.From([DATAANALISE], "pt-BR"), "/", " "), type text),
    
    // Insere a coluna 'chave_ano_mes_base_empresa' combinando chave_ano_mes e CODEMPRESA.
    chaveAnoMesBaseEmpresaInserida = Table.AddColumn(chaveAnoMesInserida, "chave_ano_mes_base_empresa", each Text.Combine({[chave_ano_mes], "-", Text.From([CODEMPRESA], "pt-BR")}), type text),
    
    // Classifica as linhas pela 'Data deferimento' em ordem decrescente.
    linhasClassificadas = Table.Sort(chaveAnoMesBaseEmpresaInserida,{{"DATAANALISE", Order.Descending}}),
    #"Duplicatas Removidas" = Table.Distinct(linhasClassificadas, {"CODOPERACAO"}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Duplicatas Removidas", {"CODOPERACAO"}, #"gerente da operacao", {"CODOPERACAO"}, "gerente da operacao", JoinKind.LeftOuter),
    #"gerente da operacao Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "gerente da operacao", {"GerenteID"}, {"gerente da operacao.GerenteID"}),
    #"Personalização Adicionada" = Table.AddColumn(#"gerente da operacao Expandido", "Personalizar", each [CODBROKER] ?? [gerente da operacao.GerenteID]),
    #"Colunas Removidas" = Table.RemoveColumns(#"Personalização Adicionada",{"nivelUsuarioTabela","CODBROKER", "gerente da operacao.GerenteID"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas",{{"Personalizar", "CODBROKER"}}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Renomeadas",{{"CODBROKER", Int64.Type}, {"ERA", type text}}),
    #"Consultas Mescladas1" = Table.NestedJoin(#"Tipo Alterado", {"CODBROKER"}, gerentes, {"CODBROKER"}, "gerentes", JoinKind.LeftOuter),
    #"gerentes Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas1", "gerentes", {"nomeGerente", "plataforma", "gestor"}, {"gerentes.nomeGerente", "gerentes.plataforma", "gerentes.gestor"}),
    #"Colunas Renomeadas1" = Table.RenameColumns(#"gerentes Expandido",{{"gerentes.gestor", "Gestor da operação"}, {"gerentes.nomeGerente", "Nome do gerente"}, {"gerentes.plataforma", "Plataforma da operação"}})
in
    #"Colunas Renomeadas1";
shared #"gerente da operacao" = let
    // 1. Conecta-se à fonte de dados SQL do Azure Synapse Analytics (ou Fabric Data Warehouse).
    Fonte = Sql.Database("gjdb6aepx2pujchian7qjs46o4-3mm24qi564pupgwhzs6cy5ooce.datawarehouse.fabric.microsoft.com", "LH_Silver"),
    // 1. Conecta-se à fonte de dados SQL do Azure Synapse Analytics (ou Fabric Data Warehouse).
    dbo_fato_titulos_antecipados = Fonte{[Schema="dbo",Item="fato_titulos_antecipados"]}[Data],
    #"Outras Colunas Removidas" = Table.SelectColumns(dbo_fato_titulos_antecipados,{"CODOPERACAO", "GerenteID", "NOMEPLATAFORMA", "NOMEGERENTE"}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Outras Colunas Removidas", {"NOMEPLATAFORMA"}, plataformas, {"Nome da Plataforma"}, "plataformas", JoinKind.LeftOuter),
    #"plataformas Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "plataformas", {"Gestor"}, {"plataformas.Gestor"}),
    #"Duplicatas Removidas" = Table.Distinct(#"plataformas Expandido", {"CODOPERACAO"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Duplicatas Removidas",{{"plataformas.Gestor", "GESTOR DA OPERACAO"}})
in
    #"Colunas Renomeadas";
shared replica_cad_agencias = let
    // Conectar ao banco de dados MySQL.
    ConexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODAGENCIA, NOME FROM cad_agencias", ReturnSingleDatabase=true]),

    // Adicionar a coluna 'plataforma' (nome reduzido).
    AdicionarNomeReduzido = Table.AddColumn(
        ConexaoMySQL,
        "plataforma",
        each Text.AfterDelimiter([NOME], " "),
        type text
    ),

    // Adicionar a coluna 'gestor' com base no nome da plataforma.
    AdicionarGestor = Table.AddColumn(
        AdicionarNomeReduzido,
        "gestor",
        each
            if [NOME] = "PLATAFORMA CONTENCIOSO" then "VINICIUS"
            else if [NOME] = "PLATAFORMA VALECRED 2.0" then "RICARDO"
            else if [NOME] = "PLATAFORMA BROKER" then "RICARDO"
            else if [NOME] = "PLATAFORMA VALECRED 4.0" then "DANIEL"
            else if [NOME] = "PLATAFORMA VALECRED 5.0" then "WILLIAN"
            else null,
        type text
    ),

    // Tratar valores nulos na coluna 'gestor' (substituir por "").
    TratarNulosGestor = Table.ReplaceValue(
        AdicionarGestor,
        null,           // Valor a ser substituído (null).
        "NÃO ATRIBUÍDO",             // Novo valor (string vazia).
        Replacer.ReplaceValue, // Tipo de substituição.
        {"gestor"}      // Coluna a ser modificada.
    ),

    // Renomear colunas para CamelCase.
    RenomearColunas = Table.RenameColumns(
        TratarNulosGestor,
        {
            {"CODAGENCIA", "codAgencia"},
            {"NOME", "nomePlataforma"}, // Renomeando NOME para nomePlataforma
            {"gestor", "nomeGestorPlataforma"} // Renomeando NOME para nomePlataforma
        }
    ),

    // Definir tipos de dados.
    DefinirTipos = Table.TransformColumnTypes(
        RenomearColunas,
        {
            {"codAgencia", Int64.Type},  // Ou type text se for alfanumérico.
            {"nomePlataforma", type text},
            {"plataforma", type text},
            {"nomeGestorPlataforma", type text}
        }
    ),

    // 6. Renomeia as colunas da tabela para nomes mais amigáveis e padronizados.
    #"Colunas Renomeadas" = Table.RenameColumns(DefinirTipos,{{"nomePlataforma", "Nome da Plataforma"}, {"nomeGestorPlataforma", "Gestor"}, {"plataforma", "Plataforma"}, {"codAgencia", "CODAGENCIA"}})

// Retorna a tabela com as colunas renomeadas.
in
    #"Colunas Renomeadas";
shared plataformas = let
    Fonte = replica_cad_agencias
in
    Fonte;
shared replica_cad_brokers = let
    // Conecta-se a um banco de dados MySQL para buscar dados de brokers.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODBROKER, CPFCNPJ, CODAGENCIA, STATUS FROM cad_brokers", ReturnSingleDatabase=true]),

    // Define tipos de dados para as colunas.
tiposDefinidos = Table.TransformColumnTypes(
    origem,
    {
        {"CODBROKER", Int64.Type},
        {"CPFCNPJ",  type text},
        {"CODAGENCIA", Int64.Type},
        {"STATUS", type text}
    }
)
in
    tiposDefinidos;
shared stg_gerentes = let
    Fonte = replica_cad_brokers,

// Mescla com a tabela geral de PF/PJ (replica_cad_geral_pf_pj) usando cpfCnpj.
cadastrosMesclados = Table.NestedJoin(
    Fonte, {"CPFCNPJ"}, // Chave de mesclagem em tiposDefinidos
    replica_cad_geral_pf_pj, {"CPFCNPJ"}, // Chave de mesclagem em replica_cad_geral_pf_pj
    "Cadastros", JoinKind.LeftOuter
),

// Expande a coluna 'NOME' da tabela mesclada 'Cadastros' para 'nomeGerente'.
nomeGerenteExpandido = Table.ExpandTableColumn(
    cadastrosMesclados, "Cadastros", {"NOME"}, {"nomeGerente"}
),

// Mescla com Silver_Plataformas_Padronizacao usando codAgencia.
plataformasMescladas = Table.NestedJoin(
    nomeGerenteExpandido, {"CODAGENCIA"},
    replica_cad_agencias, {"CODAGENCIA"},
    "Plataformas", JoinKind.LeftOuter
),
    // Expande as colunas 'plataforma' e 'nomeGestorPlataforma' da tabela 'Plataformas'.
plataformasExpandidas = Table.ExpandTableColumn(plataformasMescladas, "Plataformas", {"Plataforma", "Gestor"}, {"plataforma", "gestor"}),

// Adiciona a coluna 'BASE' com o valor constante 40.
baseAdicionada = Table.AddColumn(plataformasExpandidas, "BASE", each 40)
in
    baseAdicionada;
shared replica_cad_geral_pf_pj = let
    // Conecta-se ao banco de dados MySQL.
    conexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CPFCNPJ, NOME, DATAFUNDACAO, RJ   FROM cad_geral_pf_pj", ReturnSingleDatabase=true]),

    // Adiciona a coluna 'BASE' com o valor constante 40.
    baseAdicionada = Table.AddColumn(conexaoMySQL, "BASE", each 40), // Note: Assumed previous step 'conexaoMySQL' is the correct input here, as 'Colunas Removidas' was not provided.

    // Corrige a grafia de 'METALASER SERVICOS E COMERCIO DE METAIS LTDA ' na coluna 'NOME'.
    metalaserCorrigido = Table.ReplaceValue(baseAdicionada,"METALASER SERVICOS E COMERCIO DE METAIS LTDA ","METALASER SERVICOS E COMERCIO DE METAIS LTDA",Replacer.ReplaceText,{"NOME"}),

    // Remove espaços extras (leading/trailing) da coluna 'NOME'.
    nomeEspacosRemovidos = Table.TransformColumns(metalaserCorrigido,{{"NOME", Text.Trim, type text}}),

    // Substitui '&' por '&' na coluna 'NOME'.
    nomeEComercialCorrigido = Table.ReplaceValue(nomeEspacosRemovidos,"&","&",Replacer.ReplaceText,{"NOME"})

// Retorna a tabela final transformada.
in
    nomeEComercialCorrigido;
shared gerentes = let
    Fonte = stg_gerentes,
    #"Coluna Mesclada Inserida" = Table.AddColumn(Fonte, "chave_base_gerente", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODBROKER], "pt-BR")}), type text),
    #"Coluna Mesclada Inserida1" = Table.AddColumn(#"Coluna Mesclada Inserida", "chave_base_plataforma", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODAGENCIA], "pt-BR")}), type text),
    #"Coluna Mesclada Inserida2" = Table.AddColumn(#"Coluna Mesclada Inserida1", "chave_base_cadastro", each Text.Combine({Text.From([BASE], "pt-BR"), "-", [CPFCNPJ]}), type text),
    #"Consultas Mescladas" = Table.NestedJoin(#"Coluna Mesclada Inserida2", {"chave_base_cadastro"}, cadastros, {"chave_base_cadastro"}, "cadastros", JoinKind.LeftOuter),
    #"cadastros Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "cadastros", {"NOME"}, {"Nome do Gerente"}),
    #"Texto em Maiúscula" = Table.TransformColumns(#"cadastros Expandido",{{"Nome do Gerente", Text.Upper, type text}}),
    #"Linhas Classificadas1" = Table.Sort(#"Texto em Maiúscula",{{"chave_base_gerente", Order.Ascending}}),
    #"Consultas Mescladas1" = Table.NestedJoin(#"Linhas Classificadas1", {"CODAGENCIA"}, plataformas, {"CODAGENCIA"}, "plataformas", JoinKind.LeftOuter),
    #"plataformas Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas1", "plataformas", {"Gestor"}, {"Gestor"})
in
    #"plataformas Expandido";
shared cadastros = let
    Fonte = replica_cad_geral_pf_pj,
    #"Coluna Mesclada Inserida" = Table.AddColumn(Fonte, "chave_base_cadastro", each Text.Combine({Text.From([BASE], "pt-BR"), "-", [CPFCNPJ]}), type text),
    #"Duplicatas Removidas" = Table.Distinct(#"Coluna Mesclada Inserida", {"chave_base_cadastro"}),
    #"Texto Limpo" = Table.TransformColumns(#"Duplicatas Removidas",{{"NOME", Text.Clean, type text}}),
    #"Colunas Reordenadas" = Table.ReorderColumns(#"Texto Limpo",{"chave_base_cadastro", "BASE", "CPFCNPJ", "NOME"})
in
    #"Colunas Reordenadas";
shared produtos = let
  // Origem dos dados: Silver_Operacoes_Padronizado (tabela de operações padronizadas)
  fonte = stg_operacoes,

  // Seleciona as colunas STTO e TTO, que representam o subtipo e o tipo do produto, respectivamente.
  colunasSelecionadas = Table.SelectColumns(fonte, {"STTO", "TTO"}),

  // Remove as linhas duplicadas, mantendo apenas as combinações únicas de STTO e TTO.
  duplicatasRemovidas = Table.Distinct(colunasSelecionadas),

  // Reordena as colunas para que TTO venha antes de STTO, facilitando a leitura.
  colunasReordenadas = Table.ReorderColumns(duplicatasRemovidas, {"TTO", "STTO"}),

  // Mescla com a tabela lb_produtos para obter a descrição do tipo de produto. Left Outer Join baseado em TTO = CODTTO.
  mescladoComProdutos = Table.NestedJoin(colunasReordenadas, {"TTO"}, replica_tab_tipooperacao, {"CODTTO"}, "lb_produtos", JoinKind.LeftOuter),
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
  duplicatasRemovidasChave = Table.Distinct(informacaoEmMaiusculas, {"chave_produto"}),
    #"Valor Substituído" = Table.ReplaceValue(duplicatasRemovidasChave,"NOTA DE SERVIÇO - CTE - DESCONTO","DESCONTO - NOTA DE SERVIÇO - CTE",Replacer.ReplaceText,{"Produto - Informação de Mercado"}),
    #"Valor Substituído1" = Table.ReplaceValue(#"Valor Substituído","NOTA DE SERVIÇO - DESCONTO","DESCONTO - NOTA DE SERVIÇO",Replacer.ReplaceText,{"Produto - Informação de Mercado"})

in
  #"Valor Substituído1" // Retorna a tabela final com todos os produtos únicos e suas informações formatadas.
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
shared replica_tab_feriados = let
    // Conecta-se a um banco de dados MySQL para buscar dados de feriados.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM tab_feriados", ReturnSingleDatabase=true]),

    // Remove as colunas de metadados e ID de feriados.
    // Nota: Assumimos que o input para esta etapa deve ser 'origem', pois 'dbo_factvale40bi_tab_feriados' não está definido.
    colunasRemovidas = Table.RemoveColumns(origem,{"CODFERIADO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO"}),

    // Adiciona a coluna "ANO/MES" extraindo o ano da coluna "DATAFERIADO".
    anoMesInserido = Table.AddColumn(colunasRemovidas, "ANO/MES", each Text.AfterDelimiter(Text.From([DATAFERIADO], "pt-BR"), "/"), type text),
    #"Linhas Classificadas" = Table.Sort(anoMesInserido,{{"DATAFERIADO", Order.Ascending}})
in
    #"Linhas Classificadas";
shared replica_tab_operacoes_devolucoes = let
    // Conecta-se a um banco de dados MySQL para obter dados de devoluções de operações.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM tab_operacoes_devolucoes", ReturnSingleDatabase=true]),
    
    // Remove colunas desnecessárias.
    // Nota: Assumimos que o input para esta etapa deve ser 'origem', pois 'dbo_factvale40bi_tab_operacoes_devolucoes' não está definido.
    colunasRemovidas = Table.RemoveColumns(origem,{"USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA"}),
    
    // Adiciona a coluna 'BASE' com o valor constante 40.
    personalizacaoAdicionada = Table.AddColumn(colunasRemovidas, "BASE", each 40),
    
    // Cria 'chave_base_titulo' concatenando 'BASE' e 'CODTITULO'.
    chaveBaseTituloInserida = Table.AddColumn(personalizacaoAdicionada, "chave_base_titulo", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODTITULO], "pt-BR")}), type text),
    
    // Cria 'chave_base_operacao' concatenando 'BASE' e 'CODOPERACAO'.
    chaveBaseOperacaoInserida = Table.AddColumn(chaveBaseTituloInserida, "chave_base_operacao", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODOPERACAO], "pt-BR")}), type text),
    
    // Cria 'chave_base_operacaooriginal' concatenando 'BASE' e 'CODOPERACAOORI'.
    chaveBaseOperacaoOriginalInserida = Table.AddColumn(chaveBaseOperacaoInserida, "chave_base_operacaooriginal", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODOPERACAOORI], "pt-BR")}), type text),
    
    // Remove as colunas originais de código de operação e título após a criação das chaves.
    colunasRemovidasFinal = Table.RemoveColumns(chaveBaseOperacaoOriginalInserida,{"CODOPERACAO", "CODOPERACAOORI", "CODTITULO"})
in
    colunasRemovidasFinal;
shared relacionamento_recompras = let
    Fonte = replica_tab_operacoes_devolucoes,
    #"Duplicatas Removidas" = Table.Distinct(Fonte, {"chave_base_titulo"})
in
    #"Duplicatas Removidas";
shared replica_tab_titulos_confirmacao = let
    // Conecta-se a um banco de dados MySQL para buscar dados de confirmação de títulos.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT#(lf)#(tab)CODOPERACAO ,#(lf)#(tab)CODTITULO ,#(lf)#(tab)CONFAO,#(lf)#(tab)CAST(OBSERVACAO AS CHAR) AS OBS,#(lf)#(tab)CODOCORCONFIRMACAO ,#(lf)#(tab)cast(DATAINCLUSAO as date) as DATAINCLUSAO,#(lf)#(tab)""40"" AS BASE,#(lf)#(tab)CONCAT(""40-"", CODTITULO)AS chave_base_titulo#(lf)FROM#(lf)#(tab)tab_titulos_confirmacao ttc#(lf)order by#(lf)#(tab)DATAINCLUSAO DESC", ReturnSingleDatabase=true]),
    
    // Insere uma nova coluna 'Data' extraindo apenas a data de 'DATAINCLUSAO'.
    // Nota: Assumimos que o input para esta etapa deve ser 'origem', pois 'dbo_factvale40bi_tab_titulos_confirmacao' não está definido.
    dataInserida = Table.AddColumn(origem, "Data", each DateTime.Date([DATAINCLUSAO]), type date),
    
    // Remove a coluna original 'DATAINCLUSAO'.
    colunasRemovidas = Table.RemoveColumns(dataInserida,{"DATAINCLUSAO"}),
    
    // Converte o texto da coluna 'OBS' para maiúsculas.
    textoEmMaiuscula = Table.TransformColumns(colunasRemovidas,{{"OBS", Text.Upper, type text}})
in
    textoEmMaiuscula;
shared #"ultima checagem" = let
    Fonte = replica_tab_titulos_confirmacao,
    #"Linhas Classificadas" = Table.Sort(Fonte,{{"Data", Order.Descending}}),
    #"Duplicatas Removidas" = Table.Distinct(#"Linhas Classificadas", {"CODTITULO"})
in
    #"Duplicatas Removidas";
shared #"notificações" = let
    // Conecta-se a um banco de dados MySQL para obter dados de títulos de cobrança.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODOPERACAO, CODTITULO, COBRADOAO, CODOCORCOBRANCA/*,OBSERVACAO*/, DATAINCLUSAO ,USUAINCLUSAO #(lf)FROM tab_titulos_cobranca#(lf)WHERE YEAR(DATAINCLUSAO) >= 2024", ReturnSingleDatabase=true]),

    // Filtra linhas onde CODOCORCOBRANCA é 12.
    linhasFiltradasCobranca = Table.SelectRows(origem, each ([CODOCORCOBRANCA] = 12)),
    
    // Cria a chave chaveBaseTitulo concatenando '40-' e CODTITULO.
    chaveBaseTituloInserida = Table.AddColumn(linhasFiltradasCobranca, "chave_base_titulo", each Text.Combine({"40-", Text.From([CODTITULO], "pt-BR")}), type text)
in
    chaveBaseTituloInserida;
shared lb_pareceres_de_operacoes = let
    // Conecta-se ao banco de dados MySQL para buscar pareceres de operações.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT#(lf)#(tab)#(tab)cgp.CODOPERACAO,#(lf)#(tab)#(tab)cgp.DATAINCLUSAO,#(lf)#(tab)#(tab)cu.APELIDO,#(lf)#(tab)#(tab)cgp.obs,#(lf)#(tab)#(tab)case#(lf)#(tab)#(tab)#(tab)when cgp.OBS LIKE '%#ESCROW%' THEN 1#(lf)#(tab)#(tab)ELSE 0#(lf)#(tab)END AS ESCROW,#(lf)#(tab)case#(lf)#(tab)#(tab)when cgp.OBS LIKE '%SPENCER%' THEN ""sim""#(lf)#(tab)#(tab)ELSE ""não""#(lf)#(tab)END AS ALCADA_SPENCER,#(lf)#(tab)case#(lf)#(tab)#(tab)when cgp.OBS LIKE '%CAIO%' THEN ""sim""#(lf)#(tab)#(tab)ELSE ""não""#(lf)#(tab)END AS ALCADA_CAIO,#(lf)#(tab)case#(lf)#(tab)#(tab)when cgp.OBS LIKE '%DAIANE%' THEN ""sim""#(lf)#(tab)#(tab)ELSE ""não""#(lf)#(tab)END AS ALCADA_DAIANE#(lf)FROM#(lf)#(tab)#(tab)cad_geral_pareceres cgp#(lf)join cad_usuarios cu on#(lf)#(tab)#(tab)cu.CODUSUARIO = cgp.USUAINCLUSAO#(lf)join tab_operacoes to2 on#(lf)#(tab)#(tab)to2.CODOPERACAO = cgp.CODOPERACAO#(lf)WHERE#(lf)#(tab)cgp.OBS IS NOT NULL#(lf)#(tab)and cgp.CODOPERACAO IS NOT NULL#(lf)#(tab)and cgp.CODTIPOPARECER = 10#(lf)#(tab)and YEAR (cgp.DATAINCLUSAO) >= 2024#(lf)#(tab)and cgp.OBS NOT LIKE '%<img alt=%'#(lf)order by#(lf)#(tab)#(tab)cgp.DATAINCLUSAO desc", ReturnSingleDatabase=true]),
    
    // Extrai o texto da coluna 'obs' (que é um BLOB) e cria 'TextoHTML'.
    textoHtmlExtraido = Table.AddColumn(origem, "TextoHTML", each Text.FromBinary([obs], 1252)),
    
    // Remove a coluna original 'obs'.
    colunasRemovidas = Table.RemoveColumns(textoHtmlExtraido, {"obs"}),
    
    // Altera o tipo da coluna 'TextoHTML' para texto.
    tipoColunaAlterado = Table.TransformColumnTypes(colunasRemovidas, {{"TextoHTML", type text}}),
    
    // Renomeia 'TextoHTML' de volta para 'obs'.
    colunasRenomeadasObs = Table.RenameColumns(tipoColunaAlterado, {{"TextoHTML", "obs"}}),
    
    // Altera o tipo da coluna 'ESCROW' para lógico.
    tipoColunaEscrowAlterado = Table.TransformColumnTypes(colunasRenomeadasObs, {{"ESCROW", type logical}}),
    
    // Renomeia a coluna 'obs' para 'Parecer'.
    // Nota: Assumimos que o input para esta etapa deve ser 'tipoColunaEscrowAlterado', pois 'dbo_factvale40bi_pareceres_de_operacoes' não está definido.
    colunasRenomeadasFinal = Table.RenameColumns(tipoColunaEscrowAlterado, {{"obs", "Parecer"}})
in
    colunasRenomeadasFinal;
shared operacoes_alcadas = let
    Fonte = lb_pareceres_de_operacoes,
    #"Colunas Removidas" = Table.RemoveColumns(Fonte,{"ESCROW"}),
    #"Personalização Adicionada" = Table.AddColumn(#"Colunas Removidas", "Personalizar", each [ALCADA_SPENCER]= "sim" or [ALCADA_CAIO] ="sim" or [ALCADA_DAIANE] = "sim"),
    #"Linhas Filtradas" = Table.SelectRows(#"Personalização Adicionada", each ([Personalizar] = true)),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Linhas Filtradas",{"Personalizar"}),
    #"Valor Substituído1" = Table.ReplaceValue(#"Colunas Removidas1","&Ccedil;","Ç",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído2" = Table.ReplaceValue(#"Valor Substituído1","&Atilde;","Ã",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído5" = Table.ReplaceValue(#"Valor Substituído2","&Aacute;","Á",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído8" = Table.ReplaceValue(#"Valor Substituído5","&Eacute;","É",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído4" = Table.ReplaceValue(#"Valor Substituído8","&Iacute;","Í",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído13" = Table.ReplaceValue(#"Valor Substituído4","&Otilde;","Õ",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído7" = Table.ReplaceValue(#"Valor Substituído13","&Uacute;","Ú",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído17" = Table.ReplaceValue(#"Valor Substituído7","&Oacute;","Ó",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído" = Table.ReplaceValue(#"Valor Substituído17","<div id=""cke_pastebin"">#(cr)#(lf)#(tab)","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído3" = Table.ReplaceValue(#"Valor Substituído","&nbsp;</div>#(cr)"," ",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído6" = Table.ReplaceValue(#"Valor Substituído3","&nbsp;"," ",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído9" = Table.ReplaceValue(#"Valor Substituído6","</div>#(cr)#(lf)","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído10" = Table.ReplaceValue(#"Valor Substituído9","#(lf) #(lf)","#(lf)",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído11" = Table.ReplaceValue(#"Valor Substituído10","<span style=""background-color:#ffff00;"">","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído12" = Table.ReplaceValue(#"Valor Substituído11","</span>"," ",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído14" = Table.ReplaceValue(#"Valor Substituído12","<div>#(cr)","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído15" = Table.ReplaceValue(#"Valor Substituído14","<p>#(cr)#(lf)","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído16" = Table.ReplaceValue(#"Valor Substituído15","</p>#(cr)","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído18" = Table.ReplaceValue(#"Valor Substituído16","&#39;","'",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído19" = Table.ReplaceValue(#"Valor Substituído18","#(tab)","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído20" = Table.ReplaceValue(#"Valor Substituído19","<span style=""background-color:#fff;"">","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído21" = Table.ReplaceValue(#"Valor Substituído20","<span style=""color: rgb(0, 0, 0); font-family: Arial, Helvetica, sans-serif; font-weight: 700;"">","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído22" = Table.ReplaceValue(#"Valor Substituído21","<span style=""background-color: rgb(255, 255, 0);"">","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído23" = Table.ReplaceValue(#"Valor Substituído22","<u>","",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído24" = Table.ReplaceValue(#"Valor Substituído23","&quot;","""",Replacer.ReplaceText,{"Parecer"}),
    #"Valor Substituído25" = Table.ReplaceValue(#"Valor Substituído24","<span style=""color: rgb(0, 0, 0); font-family: Arial, Helvetica, sans-serif; font-weight: 700; background-color: rgb(255, 255, 0);"">","",Replacer.ReplaceText,{"Parecer"})
in
    #"Valor Substituído25";
shared operacoes_escrow = let
    // Carrega a tabela de operações como fonte de dados inicial.
    fonteDeDados = #"replica_tab_operacoes",

    // Filtra a tabela para manter apenas as operações do tipo "CM".
    filtraOperacoesCM = Table.SelectRows(fonteDeDados, each ([TTO] = "CM")),

    // Seleciona apenas as colunas essenciais para as próximas etapas.
    selecionaColunasPrincipais = Table.SelectColumns(filtraOperacoesCM, {"CODOPERACAO", "TTO", "STTO"}),

    // Exclui uma operação específica (código 6031344) que não deve ser processada.
    removeOperacaoExcecao = Table.SelectRows(selecionaColunasPrincipais, each ([CODOPERACAO] <> 6031344)),

    // Adiciona uma coluna "produtoEscrow" (1 ou 0) se o STTO indicar um produto escrow.
    adicionaFlagProdutoEscrow = Table.AddColumn(removeOperacaoExcecao, "produtoEscrow", each if List.Contains({"EB", "ED", "ET"}, [STTO]) then 1 else 0, Int64.Type),

    // Junta os dados das operações com a tabela de pareceres usando o CODOPERACAO.
    mesclaComPareceres = Table.NestedJoin(adicionaFlagProdutoEscrow, {"CODOPERACAO"}, lb_pareceres_de_operacoes, {"CODOPERACAO"}, "DadosPareceres", JoinKind.LeftOuter),

    // Expande as colunas da tabela de pareceres para a tabela principal.
    expandeDadosDosPareceres = Table.ExpandTableColumn(mesclaComPareceres, "DadosPareceres", {"DATAINCLUSAO", "USUAINCLUSAO", "ESCROW", "ALCADA_SPENCER", "ALCADA_CAIO", "ALCADA_DAIANE"}, {"DATAINCLUSAO", "USUAINCLUSAO", "ESCROW", "ALCADA_SPENCER", "ALCADA_CAIO", "ALCADA_DAIANE"}),

    // Cria uma flag consolidada pegando o maior valor entre a flag do parecer e a do produto.
    criaFlagEscrowConsolidada = Table.AddColumn(expandeDadosDosPareceres, "escrowConsolidado", each List.Max({[ESCROW], [produtoEscrow]}), type number),

    // Remove as colunas de flag originais que já foram consolidadas.
    removeColunasTemporarias = Table.RemoveColumns(criaFlagEscrowConsolidada, {"ESCROW", "produtoEscrow"}),

    // Renomeia a coluna consolidada de volta para o nome padrão "ESCROW".
    renomeiaFlagEscrowFinal = Table.RenameColumns(removeColunasTemporarias, {{"escrowConsolidado", "ESCROW"}}),

    // Converte a coluna "ESCROW" (que contém 0 ou 1) para o tipo lógico (falso/verdadeiro).
    ajustaTipoColunaEscrow = Table.TransformColumnTypes(renomeiaFlagEscrowFinal, {{"ESCROW", type logical}}),

    // Ordena os dados por operação e data para facilitar a análise ou uso posterior.
    ordenaDadosFinais = Table.Sort(ajustaTipoColunaEscrow, {{"CODOPERACAO", Order.Descending}, {"DATAINCLUSAO", Order.Descending}})

in
    ordenaDadosFinais;
shared operacoes_prorrogacoes = let
    Fonte = #"stg_operacoes",
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([TTO] = "PR")),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas",{"DATAACEITE", "DATAENVIOEMAIL", "STTO", "FATOR", "TARIFA", "TARIFARECOMPRA", "TAC", "NDOCS", "NDOCSRECOMPRA", "TOTADVAL", "TOTTAXAADM", "TOTTAR", "APROVACAO1", "DATAALTERACAO", "CODINDEFERIMENTO",
    "CONTRATOFISICO", "TAXACADASTRO", "MOTIVO INDEFERIMENTO", "GRUPO MOTIVO INDEFERIMENTO"}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Colunas Removidas", {"CODOPERACAO"}, boletos_titulos, {"CODOPERACAO"}, "boletos_titulos", JoinKind.LeftOuter),
    #"boletos_titulos Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "boletos_titulos", {"CODTITULO", "NDOC", "CPFCNPJSACADO", "CPFCNPJCEDENTE", "VALOR",  "AMORTIZACOES", "LIQUIDACAO"}, {"CODTITULO", "NDOC", "CPFCNPJSACADO", "CPFCNPJCEDENTE", "VALOR",
    "AMORTIZACOES", "LIQUIDACAO"}),
    #"Colunas Removidas1" = Table.RemoveColumns(#"boletos_titulos Expandido",{"STATUSANALISE", "STATUSACEITE", "TTO"})
in
    #"Colunas Removidas1";
shared boletos_titulos = let
    Fonte = #"replica_tab_titulos",
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([TDOC] = "BL")),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas",{"VENCPRORROGADO", "PRAZO", "DESAGIO", "DATACONF", "USUACONF", "DOCCONFIRMADO", "CHAVEDANFE", "Status Protesto"})
in
    #"Colunas Removidas";
shared replica_tab_geral_pendencias = let
    // Conecta-se a um banco de dados MySQL para obter dados de pendências gerais.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT#(lf)#(tab)tgp.CODPENDENCIA ,#(lf)#(tab)tgp.CODCLIENTE ,#(lf)#(tab)tgp.CODEMPRESA ,#(lf)#(tab)tgp.STATUS ,#(lf)#(tab)tgp.TIPO ,#(lf)#(tab)tgp.LIQUIDACAO ,#(lf)#(tab)tgp.VALOR ,#(lf)#(tab)tgp.AMORTIZACOES ,#(lf)#(tab)tgp.SALDODEVEDOR ,#(lf)#(tab)cast( tgp.OBSERVACAO as char) as OBSERVACAO ,#(lf)#(tab)tgp.CODOPERACAO ,#(lf)#(tab)tgp.CODTITULO ,#(lf)#(tab)tgp.CODTITULOPROR ,#(lf)#(tab)tgp.CODPAGAMENTOPARCIAL ,#(lf)#(tab)tgp.DATAINCLUSAO ,#(lf)#(tab)tgp.USUAINCLUSAO ,#(lf)#(tab)tgp.TOTISS ,#(lf)#(tab)tgp.OP#(lf)FROM#(lf)#(tab)tab_geral_pendencias tgp", ReturnSingleDatabase=true])
in
    // Retorna a tabela de pendências.
    // Nota: Assumimos que o output final deve ser 'origem', pois 'dbo_factvale40bi_pendencias' não está definido.
    origem;
shared pendencias = let
    Fonte = replica_tab_geral_pendencias,
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([CODCLIENTE] = 15247293))
in
    #"Linhas Filtradas";
shared replica_tab_titulos_abatimento = let
    // Conecta-se a um banco de dados MySQL para obter dados de abatimentos de títulos.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODTITULOABAT, CODOPERACAO, CODTITULO, VALORDEVIDO, ABATIMENTO, DATAINCLUSAO FROM tab_titulos_abatimento", ReturnSingleDatabase=true]),
    
    // Extrai a parte da data da coluna 'DATAINCLUSAO'.
    // Nota: Assumimos que o input para esta etapa deve ser 'origem', pois 'Colunas Removidas' não está definido.
    dataInclusaoTextoExtraido = Table.TransformColumns(origem, {{"DATAINCLUSAO", each Text.BeforeDelimiter(Text.From(_, "pt-BR"), " "), type text}}),
    
    // Altera o tipo da coluna 'DATAINCLUSAO' para data.
    dataInclusaoTipoAlterado = Table.TransformColumnTypes(dataInclusaoTextoExtraido,{{"DATAINCLUSAO", type date}}),
    
    // Adiciona a coluna 'BASE' com o valor constante 40.
    personalizacaoAdicionada = Table.AddColumn(dataInclusaoTipoAlterado, "BASE", each 40),
    
    // Cria a coluna 'chave_base_titulo' combinando 'BASE' e 'CODTITULO'.
    chaveBaseTituloInserida = Table.AddColumn(personalizacaoAdicionada, "chave_base_titulo", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODTITULO], "pt-BR")}), type text)
in
    chaveBaseTituloInserida;
shared abatimentos = let
    Fonte = replica_tab_titulos_abatimento
in
    Fonte;
shared replica_tab_titulos_baixas = let
    // Conecta-se ao segundo banco de dados MySQL para buscar baixas históricas (a partir de 2023).
    origemDataHistorica = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODTITULO,DATABAIXA    , DATABAIXASIST, VLPAGO, DESCONTO, JUROS, TARIFARECOMPRA, DATAVENCIMENTO, CODOPERACAO, MOTIVO, PAGOPELO, FORMA, TIPOBAIXA#(lf)     FROM tab_titulos_baixas WHERE YEAR(DATABAIXASIST) >= 2023", ReturnSingleDatabase=true]),
    // Remove colunas de tipos complexos da tabela de baixas históricas.
    dataHistoricaColunasLimpas = Table.RemoveColumns(
        origemDataHistorica,
        Table.ColumnsOfType(
            origemDataHistorica,
            {type table, type record, type list, type nullable binary, type binary, type function}
        )
    ),
    // Corrige um valor específico na coluna JUROS.
    jurosHistoricoCorrigido1 = Table.ReplaceValue(dataHistoricaColunasLimpas, -858005.8, 3912.5, Replacer.ReplaceValue, {"JUROS"}),
    // Corrige outro valor na coluna JUROS.
    jurosHistoricoCorrigido2 = Table.ReplaceValue(jurosHistoricoCorrigido1, -4948525.71, -56747.24, Replacer.ReplaceValue, {"JUROS"}),
    // Substitui uma data inválida por nulo na coluna DATAVENCIMENTO.
    dataVencimentoHistoricaCorrigida = Table.ReplaceValue(jurosHistoricoCorrigido2, #datetime(2001, 1, 1, 0, 0, 0), null, Replacer.ReplaceValue, {"DATAVENCIMENTO"}),
    // Corrige outro valor na coluna JUROS.
    jurosHistoricoCorrigido3 = Table.ReplaceValue(dataVencimentoHistoricaCorrigida, -4140.75, 0, Replacer.ReplaceValue, {"JUROS"}),
    // Corrige outro valor na coluna JUROS.
    jurosHistoricoCorrigido4 = Table.ReplaceValue(jurosHistoricoCorrigido3, -1447.5, 52.5, Replacer.ReplaceValue, {"JUROS"}),
    // Corrige outro valor na coluna JUROS.
    jurosHistoricoCorrigido5 = Table.ReplaceValue(jurosHistoricoCorrigido4, -1825.72, 66.28, Replacer.ReplaceValue, {"JUROS"}),
    // Corrige outro valor na coluna JUROS.
    jurosHistoricoCorrigido6 = Table.ReplaceValue(jurosHistoricoCorrigido5, -965, 35, Replacer.ReplaceValue, {"JUROS"}),
    // Corrige outro valor na coluna JUROS.
    jurosHistoricoCorrigido7 = Table.ReplaceValue(jurosHistoricoCorrigido6, -26000, 0, Replacer.ReplaceValue, {"JUROS"}),

    // Realiza um Left Outer Join com a tabela "replica_tab_operacoes" usando CODOPERACAO.
    consultasMescladas = Table.NestedJoin(jurosHistoricoCorrigido7, {"CODOPERACAO"}, #"replica_tab_operacoes", {"CODOPERACAO"}, "lb_operacoes", JoinKind.LeftOuter),

    // Expande a coluna "lb_operacoes" para incluir CODEMPRESA.
    lbOperacoesExpandido = Table.ExpandTableColumn(consultasMescladas, "lb_operacoes", {"CODEMPRESA"}, {"CODEMPRESA"}),

    // Remove diversas colunas que não são mais necessárias.
    colunasRemovidas = Table.RemoveColumns(lbOperacoesExpandido,{ "CODOPERACAO","DATAVENCIMENTO"}),

    // Adiciona a coluna "BASE" com o valor constante 40.
    baseAdicionada1 = Table.AddColumn(colunasRemovidas, "BASE", each 40),

    // Cria a chave "chave_base_titulo" concatenando BASE e CODTITULO.
    chaveBaseTituloInserida = Table.AddColumn(baseAdicionada1, "chave_base_titulo", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODTITULO], "pt-BR")}), type text),

    // Cria a chave "chave_base_empresa" concatenando BASE e CODEMPRESA.
    chaveBaseEmpresaInserida = Table.AddColumn(chaveBaseTituloInserida, "chave_base_empresa", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODEMPRESA], "pt-BR")}), type text),

    // Substitui valores nulos por 0 na coluna DESCONTO.
    descontoNuloZero = Table.ReplaceValue(chaveBaseEmpresaInserida,null,0,Replacer.ReplaceValue,{"DESCONTO"}),

    // Substitui valores nulos por 0 na coluna TARIFARECOMPRA.
    tarifaRecompraNuloZero = Table.ReplaceValue(descontoNuloZero,null,0,Replacer.ReplaceValue,{"TARIFARECOMPRA"}),

    // Substitui valores nulos por 0 na coluna JUROS.
    jurosNuloZero = Table.ReplaceValue(tarifaRecompraNuloZero,null,0,Replacer.ReplaceValue,{"JUROS"}),

    // Calcula o "VALOR PAGO LIQUIDO".
    valorPagoLiquidoAdicionado = Table.AddColumn(jurosNuloZero, "VALOR PAGO LIQUIDO", each [VLPAGO]-[JUROS]+[DESCONTO]-[TARIFARECOMPRA]),

    // Altera o tipo das colunas "VALOR PAGO LIQUIDO" e "BASE".
    tiposAlterados = Table.TransformColumnTypes(valorPagoLiquidoAdicionado,{{"VALOR PAGO LIQUIDO", type number}, {"BASE", Int64.Type}}),

    // Substitui valores nulos por "PG" na coluna MOTIVO.
    motivoNuloPg = Table.ReplaceValue(tiposAlterados,null,"PG",Replacer.ReplaceValue,{"MOTIVO"})
in
    motivoNuloPg;
shared baixas = let
    Fonte = replica_tab_titulos_baixas,
    #"Linhas Classificadas" = Table.Sort(Fonte,{{"DATABAIXASIST", Order.Descending}}),
    #"Valor Substituído4" = Table.ReplaceValue(#"Linhas Classificadas",-26000,0,Replacer.ReplaceValue,{"JUROS"}),
    #"Valor Substituído3" = Table.ReplaceValue(#"Valor Substituído4",-307208.46,0,Replacer.ReplaceValue,{"JUROS"}),
    #"Valor Substituído" = Table.ReplaceValue(#"Valor Substituído3","C","CEDENTE",Replacer.ReplaceText,{"PAGOPELO"}),
    #"Valor Substituído1" = Table.ReplaceValue(#"Valor Substituído","S","SACADO",Replacer.ReplaceText,{"PAGOPELO"}),
    #"Valor Substituído2" = Table.ReplaceValue(#"Valor Substituído1","O","OPERAÇÃO",Replacer.ReplaceText,{"FORMA"}),
    #"Valor Substituído5" = Table.ReplaceValue(#"Valor Substituído2","B","BOLETO",Replacer.ReplaceText,{"FORMA"}),
    #"Valor Substituído6" = Table.ReplaceValue(#"Valor Substituído5","B","BANCO",Replacer.ReplaceText,{"TIPOBAIXA"}),
    #"Valor Substituído7" = Table.ReplaceValue(#"Valor Substituído6","PG","PAGAMENTO",Replacer.ReplaceText,{"MOTIVO"}),
    #"Valor Substituído8" = Table.ReplaceValue(#"Valor Substituído7","RC","RECOMPRA",Replacer.ReplaceText,{"MOTIVO"}),
    #"Valor Substituído9" = Table.ReplaceValue(#"Valor Substituído8","R","MANUAL",Replacer.ReplaceText,{"TIPOBAIXA"}),
    #"Valor Substituído10" = Table.ReplaceValue(#"Valor Substituído9","AM","AMORTIZAÇÃO",Replacer.ReplaceValue,{"MOTIVO"}),
    #"Valor Substituído11" = Table.ReplaceValue(#"Valor Substituído10","CR","CRÉDITO",Replacer.ReplaceText,{"FORMA"}),
    #"Valor Substituído12" = Table.ReplaceValue(#"Valor Substituído11",null,"NÃO INFORMADO",Replacer.ReplaceValue,{"TIPOBAIXA"}),
    #"Valor Substituído13" = Table.ReplaceValue(#"Valor Substituído12",null,"NÃO INFORMADO",Replacer.ReplaceValue,{"PAGOPELO"}),
    #"Valor Substituído14" = Table.ReplaceValue(#"Valor Substituído13",null,"NÃO INFORMADO",Replacer.ReplaceValue,{"FORMA"})
in
    #"Valor Substituído14";
shared chave_danfe = let
    Fonte = #"replica_tab_titulos",
    #"Outras Colunas Removidas" = Table.SelectColumns(Fonte,{"CHAVEDANFE"}),
    #"Personalização Adicionada" = Table.AddColumn(#"Outras Colunas Removidas", "TextLenght", each Text.Length([CHAVEDANFE])),
    #"Linhas Filtradas" = Table.SelectRows(#"Personalização Adicionada", each ([TextLenght] = 44)),
    #"Linhas Classificadas1" = Table.Sort(#"Linhas Filtradas",{{"CHAVEDANFE", Order.Descending}}),
    #"Linhas Filtradas1" = Table.SelectRows(#"Linhas Classificadas1", each not Text.Contains([CHAVEDANFE], "XML NF-E 495 MOMENTUM OP. 149717.XML de NF-E")),
    #"Valor Substituído" = Table.ReplaceValue(#"Linhas Filtradas1"," ","0",Replacer.ReplaceText,{"CHAVEDANFE"}),
    #"Duplicatas Removidas" = Table.Distinct(#"Valor Substituído", {"CHAVEDANFE"}),
    #"Personalização Adicionada1" = Table.AddColumn(#"Duplicatas Removidas", "UF", each Text.Start([CHAVEDANFE], 2)),
    #"Personalização Adicionada2" = Table.AddColumn(#"Personalização Adicionada1", "AAMM", each Text.Middle([CHAVEDANFE], 2, 4)),
    #"Personalização Adicionada3" = Table.AddColumn(#"Personalização Adicionada2", "CNPJ", each Text.Middle([CHAVEDANFE], 6, 14)),
    #"Personalização Adicionada4" = Table.AddColumn(#"Personalização Adicionada3", "Modelo", each Text.Middle([CHAVEDANFE], 20, 2)),
    #"Personalização Adicionada5" = Table.AddColumn(#"Personalização Adicionada4", "Serie", each Text.Middle([CHAVEDANFE], 22, 3)),
    #"Personalização Adicionada6" = Table.AddColumn(#"Personalização Adicionada5", "NumeroNF", each Text.Middle([CHAVEDANFE], 25, 9)),
    #"Personalização Adicionada7" = Table.AddColumn(#"Personalização Adicionada6", "CodigoNF", each Text.Middle([CHAVEDANFE], 34, 9)),
    #"Personalização Adicionada8" = Table.AddColumn(#"Personalização Adicionada7", "DV", each Text.End([CHAVEDANFE], 1))
in
    #"Personalização Adicionada8";
shared checagem = let
    Fonte = #"replica_tab_titulos_confirmacao"
in
    Fonte;
shared checagens_por_telefone = let
    Fonte = #"replica_tab_titulos_confirmacao",
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([CONFAO] = "TELEFONE")),
    #"Linhas Filtradas1" = Table.SelectRows(#"Linhas Filtradas", each [OBS] <> null and [OBS] <> ""),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas1",{"CODOCORCONFIRMACAO"}),
    #"Coluna Condicional Adicionada" = Table.AddColumn(#"Colunas Removidas", "AUTENTICIDADE_DA_NOTA", each if Text.Contains([OBS], "AUTENTICIDADE DE NOTA") then "sim" else if Text.Contains([OBS], "AUTENTICIDADE OK") then "sim"  else "não", type text),
    #"Coluna Condicional Adicionada1" = Table.AddColumn(#"Coluna Condicional Adicionada", "CT_E_CONFERIDA", each if Text.Contains([OBS], "CT-E CONFERIDA") then "sim" else "não", type text),
    #"Coluna Condicional Adicionada2" = Table.AddColumn(#"Coluna Condicional Adicionada1", "SEM_RASTREIO", each if Text.Contains([OBS], "NÃO CONSEGUI RASTREIO") then "sim" else if Text.Contains([OBS], "FORA DE SERVIÇO") then "sim" else if Text.Contains([OBS], "TENTATIVA") then "sim" else if Text.Contains([OBS], "NÚMERO DISCADO NÃO ESTÁ EM SERVIÇO") then "sim" else "não", type text),
    #"Coluna Condicional Adicionada3" = Table.AddColumn(#"Coluna Condicional Adicionada2", "NF_CONFERIDA_NO_PORTAL", each if Text.Contains([OBS], "NF CONFERIDA NO PORTAL") then "sim" else "não", type text),
    #"Coluna Condicional Adicionada4" = Table.AddColumn(#"Coluna Condicional Adicionada3", "CONTATADO", each if Text.Contains([OBS], "EM CONTATO") then "sim" else "não", type text),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Coluna Condicional Adicionada4",{"CONFAO", "CODOPERACAO", "CODTITULO", "BASE"})
in
    #"Colunas Removidas1";
shared replica_cad_clientes = let
    // Conecta-se ao banco de dados MySQL.
    conexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODCLIENTE, CPFCNPJ, CODATIVIDADE,DATASTATUS,CODSTATUSCLIENTE, CAPITALSOCIAL ,CODRATINGSUG FROM cad_clientes", ReturnSingleDatabase=true]),

    // Adiciona a coluna 'BASE' com o valor constante 40.
    // NOTE: Assumed 'conexaoMySQL' as input instead of 'Outras Colunas Removidas' which is not defined.
    baseAdicionada = Table.AddColumn(conexaoMySQL, "BASE", each 40),

    // Realiza um Left Outer Join com 'status de clientes da esteira' usando 'CODSTATUSCLIENTE'.
    statusClientesMesclado = Table.NestedJoin(baseAdicionada, {"CODSTATUSCLIENTE"}, #"status de clientes da esteira", {"CODSTATUSCLIENTE"}, "status de clientes da esteira", JoinKind.LeftOuter),

    // Expande 'status de clientes da esteira' para incluir 'Status do cliente', 'MACROPROCESSO' e 'FASE'.
    statusClientesExpandido = Table.ExpandTableColumn(statusClientesMesclado, "status de clientes da esteira", {"Status do cliente", "MACROPROCESSO", "FASE"}, {"Status do Cliente", "MACROPROCESSO", "FASE"}),

    // Realiza um Left Outer Join com 'lb_contratos' usando 'CODCLIENTE'.
    lbContratosMesclado = Table.NestedJoin(statusClientesExpandido, {"CODCLIENTE"}, replica_cad_contratos_clientes, {"CODCLIENTE"}, "lb_contratos", JoinKind.LeftOuter),

    // Expande 'lb_contratos' para incluir colunas de contrato.
    lbContratosExpandido = Table.ExpandTableColumn(lbContratosMesclado, "lb_contratos", {"CODCONTRATO","VALIDADELIMITE", "LIMITEFOMENTO","FATOR","TRANCHE","LIMITECOMISSARIA","PERCCONFIRMACAO","STATUSDIRETORIA"}, {"CODCONTRATO","Vencimento limite", "Limite","Taxa cadastro","Tranche","Limite comissaria","Percentual exigido de confirmação","STATUSDIRETORIA"}),

    // Substitui valores nulos em 'CODRATINGSUG' por 'B'.
    codRatingSugCorrigido = Table.ReplaceValue(lbContratosExpandido,null,"B",Replacer.ReplaceValue,{"CODRATINGSUG"})
in
    codRatingSugCorrigido;
shared #"status de clientes da esteira" = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("rVdNb9tGEP0rCwEFHMB1/CHZ8ZGRaIepRTqkZKBNcthQa3kDisvskkLQW9FDgQL9EUEPQQLkFPTSK/9YZ2aXIqXIHwF6oE1Sy52ZN2/ezL582Tvo7fZi/ypI6t8jNozGfjwMvAt4iT90n72w/u0iSHy4PXI/Hwxwjcqvha4/56nkbCbYTKXVQuSlMqzgmjP+RuiyghvBtMjVktcf678Vrkx1/WkmS7ULP6Ubu2iR8bL+qiWtXEojS77HkpKXlQETzEuSIJn44cTvOP1692XvEF2K/VEwieBu/WkjBPzxAG98U9YfWM7ZtczIOq/QL31LQHn9IZNGdEMg033YahTE/iSKAw/uT+Ea+4lHQIbDi2kwQi/6zXWMpheFFoZTTOe6KhS74pkYajFjqkILqhAaIftHGJaLuQJ8ZrC8AHyYeFfJAvyQWpQcd0j0Hn5ffwLcyKXBhu37XOrbhKZZJSt2sL//A1Os0CoVxlAirpVe8Ez+6pKILyTjsGLJwXzJASXEMfGH7F1lnxQ7C0ZD8ubYUiqYYAYG96Xm8GSVmlQtWthT8q/+MuO7jM8rrmc8B+OKLRSlCYEYqoUs6882YUZoxpfgNiBHfuDGXvxiGlxFbBzFkwaZ0A+fTcd0c+4RMCfO0WN0nb49QOIj+0JvMo0RyCdwnUXx2LsIfvHqP6COMLKLAMnpwqELt4jQ+1Jz8FJQZJ0AuDEy51gq1hDm4qmX+Gyw/4DMDVoysUIZU0mmpUkpHW84wDbYZztXsJ33yG6PoY188DP0RrcDMGgAwD8h5nwt40DDFuwdTDkGwoRzJK/ylBOFVeXMnpLeJH585W0Rmcs4Si79YYPiYSM0+OdF5fI8FyAkJWoEZHYJ+7NhePkcotYkG6lUObdMgBICT00hUuKrFQjcK5l44Yg9/Rkju9uFFRuftC7cYOKyebUAkkMa1+y5ypC5pamRS6ApuibeAyqA1qz+dymxhOHbDlRzxGiXyQWfixzkAMSIpzfAWOZQzXHbTKVUfvAaCutcqXkm0Kol00wUIp8RNivRtCVQqgV+g2omUmlWWGBkTy+iF1Mf0v3d8n/SFMUhvroIrmKfjXzWpne/yynksvty4P43hEUoKD4bh1FvVskFl3OR3wDWTdobmnVezYSRxkYNb1NUyFJDX3ErVaq0Fgtclsk5d2IOi933BiF0a2fiWubSUpuzEUorNQIgudAgvpnl8SEWHWoAaAgGsxbp8Nk0DjvqMXiyAgofE6/+a+QRUkEyXH3+fBrD+4BexP5weunHKzk5bnbCP5dCkw7nKWIz56VEnDjErUshNfDNiLfc1QMAIW372GWZhIYx480jlCU0PMTBcMAM8Cqw1RDmBXc47VJrlmDO9e0K2KsMGBFluseiFXAclU2kmAGOnd72KvvRqx5fyHn9YSmyVz3Wpb1NuHMMt5lryB5GtKDMGlFZHduz+J1uAPVw5PDG27DLgQYpWp4p6sBvKw19RaY4khhYBQVUuRAsh9aK8ab13K7JgWMAg8bdmoClsd2rtQxL+BvsSrSRwN5af8BBR2Bn0LR8KUEg3sIUAhDCDjvx3nP2mPnvRVpZU/BQpo/Yj+xS4ZBAzunVsAALqOw1rG9fbc4rR04J44kr6rvbcb8VwDYa12qaGclZWtRfclAyLCuIpshwPAFOoavUjHMQMcOxgV9LaBC5wOI1drjrdsR2IKMvSm66QcDgYwqV118IZTubUZvXrYM20AERI4yuvG5nuUPeqPFgwGM3U6BCKQzGikDDH0wQqD8JRzsmCpB/qlBbadYH1IBkCq0lTL6PuCfrjUdCWWiYFF1OAZLuiIhF30g8agJOk6mbJr9pz7yovwLgbEOQHKk7yVvVIpEZWupCwkBo2AKsUymUYgGP5BrQWYAFRsKZw4J1D20dH52uY96/l33UppvEOwLZcaCRezuD75P++sOfYIOJ0+BvprMNA3QdbWSbs7P1YXcptCS6klZ0km1UBjQAitNcns/sKcYxEvwT1JIft80KVbv+Csyx1OhjHY6jEIKPvTGMjU36z/1kgseyTqfYBs3ptsJEqtYfcSABGkBBZHAKc0NHsa1/7LSttz3gcHaOr612xCjDtvv1aW4AQM+D3i0D8Nk0HEVdfE9s56q/qNm2o8TbCoHnndNCnyTJG3nJJI6Yz/xxkCRt9d6XVFxztN9JqqBxjbmArPzSsTOlGkbRxnbg5nNMrKqAVmvJ/sZv66k7+X33APWkMwMh8t3K3TJ6WGM0AvjhqP4zhK3ZKBpOiTTJbWzfchbpNwMJHUJXE2DPHgPu2GE10tDpJFOAKJ0DaMBdHRNBDgoCfBtYRIQLb+KRmQcitTp/9REzmNEvI+hcD/z48P8U8v7pFrF+uJqfUiCv/wM=", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [CODSTATUSCLIENTE = _t, #"Status do cliente" = _t, ETAPA = _t, MACROPROCESSO = _t, FASE = _t, #"ORDEM FASE" = _t, #"ORDEM MACROPROCESSO" = _t, #"ORDEM STATUS" = _t, Descrição = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"CODSTATUSCLIENTE", Int64.Type}, {"Status do cliente", type text}, {"ETAPA", Int64.Type}, {"MACROPROCESSO", type text}, {"FASE", type text}, {"ORDEM FASE", Int64.Type}, {"ORDEM MACROPROCESSO", Int64.Type}, {"ORDEM STATUS", Int64.Type}}),
    #"Linhas Classificadas" = Table.Sort(#"Tipo Alterado",{{"FASE", Order.Ascending}})
in
    #"Linhas Classificadas";
shared replica_cad_contratos_clientes = let
    // Conecta-se ao banco de dados MySQL.
    conexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT#(lf)#(tab)CODCONTRATO ,#(lf)#(tab)CODCLIENTE ,#(lf)#(tab)DTINICONTRATO ,#(lf)#(tab)VALIDADELIMITE ,#(lf)#(tab)FATOR,#(lf)#(tab)LIMITEFOMENTO ,#(lf)#(tab)LIMITECOMISSARIA ,#(lf)#(tab)STATUS,#(lf)#(tab)PERCCONFIRMACAO/100 as PERCCONFIRMACAO ,#(lf)#(tab)TRANCHE,#(lf)--#(tab)OBSERVACOES, #(lf)#(tab)CASE WHEN OBSERVACOES LIKE '%#STATUSDIRETORIA%' THEN 1 ELSE 0 END AS STATUSDIRETORIA#(lf)FROM#(lf)#(tab)cad_contratos_clientes ccc#(lf)ORDER BY#(lf)#(tab)CODCONTRATO DESC", ReturnSingleDatabase=true])
in
    conexaoMySQL;
shared clientes = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    conexaoMySQL = replica_cad_clientes,

    // Remove duplicatas com base no CPFCNPJ.
    cpfCnpjDuplicatasRemovidas = Table.Distinct(conexaoMySQL, {"CPFCNPJ"}),

    // Extrai a raiz do CNPJ (primeiros 8 caracteres).
    raizCnpjExtraida = Table.AddColumn(cpfCnpjDuplicatasRemovidas, "raizCnpj", each Text.Start([CPFCNPJ], 8), type text),

    // Mescla com Silver_RelacionamentoGerenteCliente_Padronizacao usando codCliente.
    relacionamentoGerenteMesclado = Table.NestedJoin(raizCnpjExtraida, {"CODCLIENTE"},replica_rlc_brokers_clientes, {"CODCLIENTE"}, "Silver_RelacionamentoGerenteCliente_Padronizacao", JoinKind.LeftOuter),
    // Mescla com Silver_Emails_Padronizado usando cpfCnpj.
    emailsMesclados = Table.NestedJoin(relacionamentoGerenteMesclado, {"CPFCNPJ"}, emails, {"CPFCNPJ"}, "Silver_Emails_Padronizado", JoinKind.LeftOuter),
    // Mescla com Sivler_Telefones_Padronizacao usando cpfCnpj.
    telefonesMesclados = Table.NestedJoin(emailsMesclados, {"CPFCNPJ"}, telefones, {"CPFCNPJ"}, "Sivler_Telefones_Padronizacao", JoinKind.LeftOuter),
    // Mescla com Silver_Enderecos_Enriquecimento usando cpfCnpj.
    enderecosMesclados = Table.NestedJoin(telefonesMesclados, {"CPFCNPJ"}, enderecos, {"CPFCNPJ"}, "Silver_Enderecos_Enriquecimento", JoinKind.LeftOuter),
    // Mescla com Bronze_ClientesDesconsideradosPDD_JSON usando codCliente.
    clientesDesconsideradosPddMesclados = Table.NestedJoin(enderecosMesclados, {"CODCLIENTE"}, #"Clientes desconsiderados do PDD", {"CODCLIENTE"}, "Bronze_ClientesDesconsideradosPDD_JSON", JoinKind.LeftOuter),
    // Expande a coluna "codGerente" do relacionamento gerente-cliente.
    relacionamentoGerenteExpandido = Table.ExpandTableColumn(clientesDesconsideradosPddMesclados, "Silver_RelacionamentoGerenteCliente_Padronizacao", {"CODBROKER"}, {"codGerente"}),
    // Mescla com Silver_Gerentes_Enriquecimento usando codGerente e codBroker.
    gerentesMesclados = Table.NestedJoin(relacionamentoGerenteExpandido, {"codGerente"}, #"gerentes", {"CODBROKER"}, "Silver_Gerentes_Enriquecimento", JoinKind.LeftOuter),
    cadastroMesclado = Table.NestedJoin(gerentesMesclados, {"CPFCNPJ"}, #"replica_cad_geral_pf_pj", {"CPFCNPJ"}, "replica_cad_geral_pf_pj", JoinKind.LeftOuter),
    // Expande as colunas "nome" e "dataFundacao" de cadastros.
    cadastrosExpandidos = Table.ExpandTableColumn(cadastroMesclado, "replica_cad_geral_pf_pj", {"NOME", "DATAFUNDACAO", "RJ"}, {"nome", "dataFundacao","RECUPERACAOJUDICIAL"}),
    analistasMesclado = Table.NestedJoin(cadastrosExpandidos, {"CPFCNPJ"}, #"analista por cliente", {"CNPJ"}, "analista por cliente", JoinKind.LeftOuter),
    // Mescla com Bronze_CorRatingClientesInadimplencia_JSON usando nome e nomeDoCliente.
    corRatingMesclado = Table.NestedJoin(analistasMesclado, {"nome"}, Cor_Clientes_Inadimplencia, {"NOME DO CLIENTE"}, "Bronze_CorRatingClientesInadimplencia_JSON", JoinKind.LeftOuter),
    analistaExpandido = Table.ExpandTableColumn(corRatingMesclado, "analista por cliente", {"ANALISTA"}, {"analista por cliente.ANALISTA"}),
    // Expande as informações de gerente (nomeGerente, plataforma, gestor).
    gerentesExpandidos = Table.ExpandTableColumn(analistaExpandido, "Silver_Gerentes_Enriquecimento", {"nomeGerente", "plataforma", "gestor"}, {"nomeGerente", "plataforma", "gestor"}),
    // Expande a coluna "emails" de e-mails.
    emailsExpandidos = Table.ExpandTableColumn(gerentesExpandidos, "Silver_Emails_Padronizado", {"emails"}, {"emails"}),
    // Expande as colunas "telefones" e "contatos" de telefones.
    telefonesExpandidos = Table.ExpandTableColumn(emailsExpandidos, "Sivler_Telefones_Padronizacao", {"telefone"}, {"telefones"}),
    // Expande as informações de endereço (cidade, uf, cep, regiao).
    enderecosExpandidos = Table.ExpandTableColumn(telefonesExpandidos, "Silver_Enderecos_Enriquecimento", {"ENDERECO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF", "CEP", "Região"}, {"ENDERECO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF", "CEP", "Região"}),
    // Expande a coluna "desconsiderarPdd" de clientes desconsiderados PDD.
    clientesDesconsideradosPddExpandidos = Table.ExpandTableColumn(enderecosExpandidos, "Bronze_ClientesDesconsideradosPDD_JSON", {"desconsiderarPdd"}, {"desconsiderarPdd"}),
    // Substitui valores nulos por "N" na coluna "desconsiderarPdd".
    desconsiderarPddNulosSubstituidos = Table.ReplaceValue(clientesDesconsideradosPddExpandidos, null, "N", Replacer.ReplaceValue, {"desconsiderarPdd"}),
    // Expande as colunas "corPdd" e "ratingPdd" do cor rating.
    corRatingExpandido = Table.ExpandTableColumn(desconsiderarPddNulosSubstituidos, "Bronze_CorRatingClientesInadimplencia_JSON", {"corPdd","ratingPdd"}, {"corPdd","ratingPdd"}),
    // Remove linhas duplicadas com base na coluna "codCliente".
    clienteDuplicatasRemovidasFinal = Table.Distinct(corRatingExpandido, {"CODCLIENTE"}),
    // Renomeia as colunas para o padrão desejado (Title Case/UPPERCASE).
    colunasRenomeadasFinal = Table.RenameColumns(clienteDuplicatasRemovidasFinal,{ {"dataFundacao", "DATAFUNDACAO"}, {"telefones", "telefone"}, {"corPdd", "COR_INADIMPLENCIA"}, {"analista por cliente.ANALISTA", "ANALISTA DE CHECAGEM"}, {"desconsiderarPdd", "DESCONSIDERAR PDD"}, {"nome", "Nome do cliente"},  {"nomeGerente", "Nome do Gerente"}, {"plataforma", "Plataforma"}, {"gestor", "Gestor"}, {"raizCnpj", "Raiz CNPJ"}}),
    // Mescla com a tabela Clientes_diretoria_info_mercado usando CPFCNPJ e CNPJ.
    clientesDiretoriaInfoMercadoMesclado = Table.NestedJoin(colunasRenomeadasFinal, {"CPFCNPJ"}, Clientes_diretoria_info_mercado, {"CNPJ"}, "clientesDiretoriaInfoMercado", JoinKind.LeftOuter),
    // Expande a coluna 'DIRETORIA' da tabela mesclada para 'diretoria_info_mercado'.
    diretoriaInfoMercadoExpandido = Table.ExpandTableColumn(clientesDiretoriaInfoMercadoMesclado, "clientesDiretoriaInfoMercado", {"DIRETORIA"}, {"diretoria_info_mercado"}),
    // Substitui valores nulos por "não" na coluna 'diretoria_info_mercado'.
    diretoriaNulosSubstituidos = Table.ReplaceValue(diretoriaInfoMercadoExpandido,null,"não",Replacer.ReplaceValue,{"diretoria_info_mercado"}),
    // Remove linhas duplicadas com base na coluna 'CODCLIENTE'.
    clienteDuplicatasRemovidasPosDiretoria = Table.Distinct(diretoriaNulosSubstituidos, {"CODCLIENTE"})
in
    clienteDuplicatasRemovidasPosDiretoria;
shared replica_rlc_brokers_clientes = let
    // Conecta-se a um banco de dados MySQL para buscar dados de relacionamento broker-cliente.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODBROKER, CODCLIENTE, DATAINICIO, DATAINCLUSAO FROM rlc_brokers_clientes", ReturnSingleDatabase=true]),
    
    // Remove linhas duplicadas com base na coluna 'CODCLIENTE'.
    // Nota: Assumimos que o input para esta etapa deve ser 'navegacao', pois 'Colunas Removidas' não está definido.
    duplicatasRemovidas = Table.Distinct(origem, {"CODCLIENTE"}),

    // Adiciona a coluna 'BASE' com o valor constante 40.
    personalizacaoAdicionada = Table.AddColumn(duplicatasRemovidas, "BASE", each 40)
in
    personalizacaoAdicionada;
shared replica_cad_email = let
    // Conecta-se a um banco de dados MySQL para buscar dados de e-mail.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CPFCNPJ, EMAIL FROM cad_email", ReturnSingleDatabase=true]),
    // Remove linhas duplicadas da tabela.
    duplicatasRemovidas = Table.Distinct(origem)
in
    duplicatasRemovidas;
shared #"lista de emails" = let
    Fonte = replica_cad_email,
    #"Linhas Classificadas" = Table.Sort(Fonte,{{"CPFCNPJ", Order.Ascending}}),
    #"Duplicatas Removidas" = Table.Distinct(#"Linhas Classificadas")
in
    #"Duplicatas Removidas";
shared emails = let
    // Agrupando os telefones por CNPJ
    GroupedTable = Table.Group(#"lista de emails", {"CPFCNPJ"},{{"emails",each [EMAIL]}}),
    #"Valores Extraídos" = Table.TransformColumns(GroupedTable, {"emails", each Text.Combine(List.Transform(_, Text.From), ";"), type text})
in
    #"Valores Extraídos";
shared replica_cad_telefones = let
    // Conecta-se a um banco de dados MySQL para buscar dados de telefones.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CPFCNPJ, DDD, FONE, CONTATO, TIPO FROM cad_telefones", ReturnSingleDatabase=true]),
    
    // Remove hífens da coluna 'FONE'.
    foneHifenRemovido = Table.ReplaceValue(origem,"-","",Replacer.ReplaceText,{"FONE"}),
    
    // Filtra linhas onde 'FONE' não é nulo ou vazio.
    linhasFoneNaoNulo = Table.SelectRows(foneHifenRemovido, each [FONE] <> null and [FONE] <> ""),
    // Filtra linhas onde 'DDD' não é nulo ou vazio.
    linhasDddNaoNulo = Table.SelectRows(linhasFoneNaoNulo, each [DDD] <> null and [DDD] <> ""),
    
    #"Texto Limpo" = Table.TransformColumns(linhasDddNaoNulo,{{"FONE", Text.Trim, type text}}),
    #"Dividir Coluna por Delimitador" = Table.SplitColumn(#"Texto Limpo", "FONE", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv, false), {"FONE.1", "FONE.2"}),
    // Concatena 'DDD' e 'FONE' em uma nova coluna 'Mesclado'.
    colunaMescladaInserida = Table.AddColumn(#"Dividir Coluna por Delimitador", "Mesclado", each Text.Combine({[DDD], [FONE.1]}, "-"), type text),
    // Remove espaços em branco da coluna 'Mesclado'.
    mescladoEspacoRemovido = Table.ReplaceValue(colunaMescladaInserida," ","",Replacer.ReplaceText,{"Mesclado"}),
    
    // Insere a coluna 'Últimos caracteres' com o último caractere de 'Mesclado'.
    ultimosCaracteresInseridos = Table.AddColumn(mescladoEspacoRemovido, "Últimos caracteres", each Text.End([Mesclado], 1), type text),
    
    // Altera o tipo da coluna 'Últimos caracteres' para número.
    ultimosCaracteresTipoAlterado = Table.TransformColumnTypes(ultimosCaracteresInseridos,{{"Últimos caracteres", type number}}),
    
    // Remove linhas com erros de conversão na coluna 'Últimos caracteres'.
    errosRemovidos = Table.RemoveRowsWithErrors(ultimosCaracteresTipoAlterado, {"Últimos caracteres"}),
    
    // Adiciona a coluna 'Comprimento' com o número de caracteres de 'Mesclado'.
    comprimentoTextoInserido = Table.AddColumn(errosRemovidos, "Comprimento", each Text.Length([Mesclado]), Int64.Type),
    
    // Filtra linhas onde o 'Comprimento' de 'Mesclado' é menor que 13.
    linhasComprimentoValido = Table.SelectRows(comprimentoTextoInserido, each [Comprimento] < 13),
    #"Colunas Renomeadas" = Table.RenameColumns(linhasComprimentoValido,{{"FONE.1", "FONE"}, {"FONE.2", "FONTE"}}),
    
    // Remove as colunas auxiliares usadas para validação e criação da chave.
    colunasRemovidasFinal = Table.RemoveColumns(#"Colunas Renomeadas",{"DDD", "FONE", "Últimos caracteres", "Comprimento", "TIPO"}),
    
    // Renomeia a coluna 'Mesclado' para 'FONE'.
    colunasRenomeadas = Table.RenameColumns(colunasRemovidasFinal,{{"Mesclado", "FONE"}}),
    
    // Reordena as colunas para um layout padrão.
    colunasReordenadas = Table.ReorderColumns(colunasRenomeadas,{"CPFCNPJ", "FONE", "FONTE", "CONTATO"})
in
    colunasReordenadas;
shared #"lista de telefones" = let
    Fonte = replica_cad_telefones,
    #"Linhas Classificadas" = Table.Sort(Fonte,{{"CPFCNPJ", Order.Ascending}}),
    #"Duplicatas Removidas" = Table.Distinct(#"Linhas Classificadas")
in
    #"Duplicatas Removidas";
shared telefones = let
    // Agrupando os telefones por CNPJ
    GroupedTable = Table.Group(#"lista de telefones", {"CPFCNPJ"},{{"telefone",each [FONE]}}),
    #"Valores Extraídos" = Table.TransformColumns(GroupedTable, {"telefone", each Text.Combine(List.Transform(_, Text.From), ";"), type text})
in
    #"Valores Extraídos";
shared replica_cad_enderecos = let
    // Conecta-se ao banco de dados MySQL.
    conexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM cad_enderecos", ReturnSingleDatabase=true]),
    
    // Remove colunas desnecessárias.
    // Nota: Assumimos que o input para esta etapa deve ser 'conexaoMySQL', pois 'dbo_factvale40bi_cad_enderecos' não está definido.
    colunasRemovidas = Table.RemoveColumns(conexaoMySQL,{"PAIS", "FONE", "FAX", "TIPO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODMUNICIPIO", "CODENDERECO"}),

    // Filtra as linhas removendo endereços de teste/inválidos e onde a cidade é nula/vazia/"0".
    linhasFiltradas = Table.SelectRows(colunasRemovidas, each ([ENDERECO] <> "END PENDENTE" and [ENDERECO] <> "Rua Teste" and [ENDERECO] <> "S/ EDEREÇO" and [ENDERECO] <> "S/ ENDERECO" and [ENDERECO] <> "S/ ENDEREÇO" and [ENDERECO] <> "test2" and [ENDERECO] <> "teste") and ([CIDADE] <> null and [CIDADE] <> "" and [CIDADE] <> "0")),

    // Converte o texto das colunas de endereço para maiúsculas.
    textoEmMaiuscula = Table.TransformColumns(linhasFiltradas,{{"ENDERECO", Text.Upper, type text}, {"COMPLEMENTO", Text.Upper, type text},{"BAIRRO", Text.Upper, type text}, {"CIDADE", Text.Upper, type text}, {"UF", Text.Upper, type text}}),

    // Adiciona a coluna "BASE" com o valor constante 40.
    personalizacaoAdicionada = Table.AddColumn(textoEmMaiuscula, "BASE", each 40)
in
    personalizacaoAdicionada;
shared enderecos = let
    Fonte = replica_cad_enderecos,
    #"Linhas Classificadas" = Table.Sort(Fonte,{{"CPFCNPJ", Order.Ascending}}),
    #"Linhas Filtradas" = Table.SelectRows(#"Linhas Classificadas", each ([CEP] <> null and [CEP] <> "00      " and [CEP] <> "0000000" and [CEP] <> "00000000") and ([CPFCNPJ] <> "#(001E)â;#(0007)Ó#(001F)4#(0014)|ÒúÌÀW" and [CPFCNPJ] <> "!%×*ÿ‚íÆ¿‰•?ôè" and [CPFCNPJ] <> "(¢@Ñl=‰#(0081)HÄ#(0017)Ù#(0006)æ" and [CPFCNPJ] <> ",ãÍëŒÃ×#(001D)<5­„ î")),
    #"Duplicatas Removidas" = Table.Distinct(#"Linhas Filtradas"),
    #"Valor Substituído" = Table.ReplaceValue(#"Duplicatas Removidas","RP","PR",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído1" = Table.ReplaceValue(#"Valor Substituído","SÃ","SP",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído2" = Table.ReplaceValue(#"Valor Substituído1"," C","CE",Replacer.ReplaceValue,{"UF"}),
    #"Linhas Filtradas2" = Table.SelectRows(#"Valor Substituído2", each ([CEP] <> "0")),
    #"Valor Substituído3" = Table.ReplaceValue(#"Linhas Filtradas2","G","GO",Replacer.ReplaceValue,{"UF"}),
    #"Linhas Filtradas3" = Table.SelectRows(#"Valor Substituído3", each ([CEP] <> "#(000C)¯„ö<‘òR" and [CEP] <> "#(001A)#(009D)Â#(tab)`væ•" and [CEP] <> "#(001D)¨ãŸÊà’ " and [CEP] <> "#(001F)’+ftÝ„" and [CEP] <> "+z‰¤Ç«ÿ@" and [CEP] <> ";§dóƒê#(cr)Š" and [CEP] <> "D<#(000C)˜¬zA#(0007)" and [CEP] <> "JÀÑˆžrQB" and [CEP] <> "¡/‡{Å®Z@" and [CEP] <> "¦Ú±½#(0019)X®Å" and [CEP] <> "À#(0004)S#(0007)ü#(000B)¡#(tab)" and [CEP] <> "Ãrè(LÙL?" and [CEP] <> "çÄä¼<k#(cr)»" and [CEP] <> "ès|Ÿ¡9š¤" and [CEP] <> "Î¼Ñ#(001C)ÖŠ%Ý" and [CEP] <> "Î„Þ#(001E)Ð›¡Ü" and [CEP] <> "Ô|ë ªÎ}•" and [CEP] <> "ÝA#(0011)&,#(001E)ìx" and [CEP] <> "šB/•S²ÎO")),
    #"Valor Substituído4" = Table.ReplaceValue(#"Linhas Filtradas3","11","SP",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído5" = Table.ReplaceValue(#"Valor Substituído4","DP","SP",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído6" = Table.ReplaceValue(#"Valor Substituído5","SA","SP",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído7" = Table.ReplaceValue(#"Valor Substituído6","S","SP",Replacer.ReplaceValue,{"UF"}),
    #"Valor Substituído8" = Table.ReplaceValue(#"Valor Substituído7","MF","MG",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído9" = Table.ReplaceValue(#"Valor Substituído8","ED","ES",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído10" = Table.ReplaceValue(#"Valor Substituído9","31","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído11" = Table.ReplaceValue(#"Valor Substituído10","0","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído12" = Table.ReplaceValue(#"Valor Substituído11","A","",Replacer.ReplaceValue,{"UF"}),
    #"Valor Substituído13" = Table.ReplaceValue(#"Valor Substituído12","1","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído14" = Table.ReplaceValue(#"Valor Substituído13","3","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído15" = Table.ReplaceValue(#"Valor Substituído14","..","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído16" = Table.ReplaceValue(#"Valor Substituído15","7","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído17" = Table.ReplaceValue(#"Valor Substituído16","-","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído18" = Table.ReplaceValue(#"Valor Substituído17","+","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído19" = Table.ReplaceValue(#"Valor Substituído18",".","",Replacer.ReplaceText,{"UF"}),
    #"Duplicatas Removidas2" = Table.Distinct(#"Valor Substituído19", {"CPFCNPJ"}),
    #"Valor Substituído20" = Table.ReplaceValue(#"Duplicatas Removidas2","SS","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído21" = Table.ReplaceValue(#"Valor Substituído20","9","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído22" = Table.ReplaceValue(#"Valor Substituído21","2","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído23" = Table.ReplaceValue(#"Valor Substituído22","GU","",Replacer.ReplaceText,{"UF"}),
    #"Valor Substituído24" = Table.ReplaceValue(#"Valor Substituído23","",null,Replacer.ReplaceValue,{"UF"}),
    #"Valor Substituído25" = Table.ReplaceValue(#"Valor Substituído24","68",null,Replacer.ReplaceValue,{"UF"}),
    #"Duplicatas Removidas1" = Table.Distinct(#"Valor Substituído25", {"CPFCNPJ"}),
    #"Linhas Filtradas1" = Table.SelectRows(#"Duplicatas Removidas1", each ([CPFCNPJ] <> "00000000000000")),
    #"Coluna Mesclada Inserida" = Table.AddColumn(#"Linhas Filtradas1", "chave_base_cadastro", each Text.Combine({Text.From([BASE], "pt-BR"), "-", [CPFCNPJ]}), type text),
    #"Duplicatas Removidas3" = Table.Distinct(#"Coluna Mesclada Inserida", {"chave_base_cadastro"}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Duplicatas Removidas3",{{"CIDADE", type text}}),
    #"Valor Substituído26" = Table.TransformColumns(#"Tipo Alterado", {{"CIDADE", each 
Text.Replace(
Text.Replace(
Text.Replace(
Text.Replace(
Text.Replace(
Text.Replace(
Text.Replace(
Text.Replace(
_, "Ã", "A")
,"Â","A")
,"-"," ")
,"Á","A"),"É","E"),"Í","I"),"Ó","O"),"Ú","U")
, type text}}
),
    #"Consultas Mescladas" = Table.NestedJoin(#"Valor Substituído26", {"UF"}, regioes, {"Sigla"}, "regioes (2)", JoinKind.LeftOuter),
    #"regioes Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "regioes (2)", {"Estado", "Capital", "Região"}, {"Estado", "Capital", "Região"}),
    #"Personalização Adicionada" = Table.AddColumn(#"regioes Expandido", "Personalizar", each Number.FromText([CPFCNPJ])),
    #"Erros Substituídos" = Table.ReplaceErrorValues(#"Personalização Adicionada", {{"Personalizar", 1}}),
    #"Linhas Filtradas4" = Table.SelectRows(#"Erros Substituídos", each ([Personalizar] <> 1)),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas4",{"Personalizar"})
in
    #"Colunas Removidas";
shared regioes = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("bVPBcpswEP0Vxuf0I4DYtJ4QM+DxJZPDGra2MkJiJOghf5P21INP7h/ox7or2Q7gXHZAevt239vVy8sirg0uHhZxSqEUOkoMqFrTz7M2PS5eHwgi4aDBMuqJQg41CncOkAbtFdVC5z4YVATQ5XfE08K7VoEo9xgFg51AEjgKoJMkplCB/AWNNvNKKYLx1OmSwoqSQeI7zGGPwvZG9DpaYYMGJN0/rpjbgHUn6eukqHqjv21uSUvbuZPPqkD17MOyorATvTsbn1INnyUyLdwHK8g2HOjvt/qaNwfy9ej+MGPuxdF39DS4k533nQNVz4y21oO3TDcI2AfJ98Q3dNRQ1wPrzLnnFNqOr0A1+GWmoFlEGVkjuIc8Y29Q6ui7NoIG1eNMbXGxvYgD0v1tJ8Oje3CnPesvEgprzRoLpNbuhsNYFdhKL5A8F/vgrwwINAra/eCXseBJl1iLn3jHJIBcZMwPCls0aEnYHMWr3WC0BoXCMGO5viz85HQsly+De2xskEkpz0wNvd+nuxKf+DCIkgdRUKqOYokHgyOBpVaN+xf2pdzccDuUx+n7KzWNqPUw9irREO1ot2EC4nWFKKXGTJBf8YteSZolGX3utPRTvhb361fAIL3q4rqQt5ORDxWag+h85zyF2NDTfhvm6re6pg6E4iJbrwZkC6PX/fof", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Estado = _t, Sigla = _t, Capital = _t, Região = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"Estado", type text}, {"Sigla", type text}, {"Capital", type text}, {"Região", type text}})
in
    #"Tipo Alterado";
shared #"Clientes desconsiderados do PDD" = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("jVQ7agNBDL3KsrUJo7/mBGkDATfGxYKXYAjeYPtEyVF8sYyTNXFhrVJMpafRk/SeNpv+ZXgbDrup+xhOp2E39av+tT0QZMNK/Xa1AEFn9B/IengfO3kqfzE1Awxihg5l+WtCkKS6qJMkBCuUpFCjor+Q5/F0vnxN3XA4j8f9dLzrBQzqjDmOLdytL5+H/XCHIOCskLlLNJBCGMWcOWuTneY9xMOqBXgZYsDzVh5tk7KNXYkmJKgWpWTW4rcxxRg0lXzaQFEz1OSVMAW2mrCgYgVSpgaaLK82IUdMHQUiXSDMG32UJ2H317zMNmYUpbPd5PiorGKkIMcimc9EXDMJtaOUQLBqXTAaWGi0YsnXzYqWuYhAOVOOgUUs1Jg1iImohQ514Rp1DUUhuQ/tgiQQEqW4OqQXyIr/40glR79ZrmSttF8ygTe6rdD2Gw==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [MOTIVO = _t, #"DESCONSIDERAR PDD" = _t, CODCLIENTE = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"CODCLIENTE", Int64.Type}}),
    #"Colunas Reordenadas" = Table.ReorderColumns(#"Tipo Alterado",{"CODCLIENTE", "MOTIVO", "DESCONSIDERAR PDD"})
in
    #"Colunas Reordenadas";
shared #"analista por cliente" = let
    Fonte = Excel.Workbook(Web.Contents("https://valecred.sharepoint.com/sites/ValeCred_/Documentos%20Compartilhados/E.%20Tecnologia%20e%20Infraestrutura/06.%20Ci%C3%AAncia%20de%20Dados/99.%20Fontes%20de%20Dados%20Interdepartamentais/Gest%C3%A3o%20de%20Riscos%20de%20Cr%C3%A9dito%20e%20Antifraude/DIVIS%C3%83O%20CARTEIRA%20NO.xlsx"), null, true),
    #"DIVISÃO CARTEIRAS (NO_CM)_Sheet" = Fonte{[Item="GERAL",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"DIVISÃO CARTEIRAS (NO_CM)_Sheet", [PromoteAllScalars=true]),
    #"Tipo Alterado1" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos",{{"CLIENTE", type text}, {"ANALISTA", type text}, {"CNPJ", type text}, {"Tipo", type text}}),
    #"Texto Limpo" = Table.TransformColumns(#"Tipo Alterado1",{{"CLIENTE", Text.Trim, type text}})
in
    #"Texto Limpo";
shared Cor_Clientes_Inadimplencia = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("lVZLdtswDLwKn9dO29dF9xAFy0hIQuHHdZLmGD1BFzlAj5SLdSgrtmQrjbPws2ziM8AMQD09rXYcPbutrtaroDEVZ8iJ55A1GTaJ404sHl1uafW8flpF3Q+22TjtJGWxdHFI1rJzJqkrVjmZniIZDhw7wbehrhx8vkyd7gu5hz4KIktoS8pxMLbqOVrRU5IJYiSaGLdsPN0XCVSht5J6TZJlN4U/ce4oARjnrKckCNFwI22NIJGdTBF2FFEH4iJbU6TVOLOfNG4R667Ae7E0RLHkLLXvuG4c75HsPd8+altqWs8tyDJbTb1kchQXo4HQjGiNKzVM2LAdSDonUQJART5Gq5kiUyMOvywpQDgNHZkeoSlwsHKpBK8hw8mZjWqbwHzUYxVuEd0jdVQpqaaBu9e/KPIS3E6aSEncnH2KmTdUO1F5UbBltzQSaW4MexRgC/AO+G8LugUY5wjuRNnBXHyvcSh1bXj/9vyRJMGFNro3OVJI/YHaCKnNjFuurJYAAIiY8NnA3EqyemlH0ePvVs1Ycvo6P8dQhSBBLg5qO6zpJQ2jHHnHGNeJQj9uyRAmFPT3Jz2MHJ46Pupd3oi/Mh40lzneNNS9q2jglvZAY69OIPBeHAd949LzLGLD2cu+itlinhdaiBklc6GYMyavhN/VRQbxIlxAO068TBcDzeby1Chd0EHQHbDMdsMR10FFkB7PJXjS5icS9arO051JAMq+rixkKxlrSZasf/3+9u37j34+Yr2juvMHRZ3AjXq6pn0QIc4vfOd0PZimpP/bbNAg9R9fFaPk0C9X9teS//ry+ucd/I5qe0NtMENwlOn62j0mUgKuRpnvfrCGqI/Y/5EnA7bEiqVevKkO9dZKnvP8eibkYDesXjRPxj3meBz7up12QlGWfbhua6z0W6q3+zIBJ+vhGEbF3k1eBi51cebmpNtm4MqQ4wf8zXLZOwndZxINPtBaLeUTbsNkTZCtj8DWh/eh15dB/uMMHrRyiPP8Dw==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [COR = _t, #"NOME DO CLIENTE" = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"COR", type text}, {"NOME DO CLIENTE", type text}}),
    #"Linhas Classificadas" = Table.Sort(#"Tipo Alterado",{{"NOME DO CLIENTE", Order.Ascending}})
in
    #"Linhas Classificadas";
shared Clientes_diretoria_info_mercado = let
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("fZI9buMwEIWvMlCdAKQkUmQ5piYyFxQpkJJTGDnAFlvlBEGKrfZUvtgO7WL9g3VJDL43j2/e8dgIpQYrbCuEkK1oXpqMPu4R1oyxLCmvVIDAYZ6wwCvQDJnctlBGhwl+bKN3HgOEdUSGP3/+aj5ejk03GGOkVVVVGx6EcqvoMwX/f70rKdm3RnfWng121WAa04xxuxG82y+6rlVSnH81KB74yF/gf4TTn+gdwkgX3Net9+Y7o9UgK9xfw/tUln/g6ffp+xkq2/YW9Svn9GyvFFr1RuoK2wsMD3Qm3PnArypAEFKcEDg+ToQiR/cgaqXUVndV1NT43t0EE5W14jMU3Fj0jmmNNbrXpjJS8+BtnsClmbLzqZp4o5zZvq+duJzyitbCmHaQ9WC8lgfoHIUAJYXNJb7VghmBIuWJ7RKcvqbtwbbolVRC9tWCGHgw19ACN8bPS6CZ4poK1CocPGafHhugpOUWnhtoqotd2Ah2dOCmTbWB8YAFx5RvV3/8BQ==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [CNPJ = _t, RAZAOSOCIAL = _t, DIRETORIA = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"CNPJ", type text}, {"RAZAOSOCIAL", type text}})
in
    #"Tipo Alterado";
shared cobrancas = let
    Fonte = #"replica_rlc_titulos_ocorrencias_cobranca"
in
    Fonte;
shared empresas = let
    // Conecta-se a um banco de dados MySQL para selecionar dados de empresas.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODEMPRESA, CNPJ, ATIVA  FROM cad_empresas ", ReturnSingleDatabase=true]),

    // Adiciona a coluna "BASE" com o valor constante 40.
    colunaBaseAdicionada = Table.AddColumn(origem, "BASE", each 40),
    // Filtra as linhas onde CODEMPRESA é 6, 14 ou 24.
    linhasFiltradas = Table.SelectRows(colunaBaseAdicionada, each ([CODEMPRESA] = 6 or [CODEMPRESA] = 14 or [CODEMPRESA] = 24 or [CODEMPRESA] = 25)),

    // Cria a chave única "chave_base_empresa" combinando BASE e CODEMPRESA.
    chaveBaseEmpresaCriada = Table.AddColumn(linhasFiltradas, "chave_base_empresa", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODEMPRESA], "pt-BR")}), type text),

    // Cria a chave única "chave_base_cadastro" combinando BASE e CNPJ.
    chaveBaseCadastroCriada = Table.AddColumn(chaveBaseEmpresaCriada, "chave_base_cadastro", each Text.Combine({Text.From([BASE], "pt-BR"), "-", [CNPJ]}), type text),

    // Mescla com a tabela 'cadastros' usando 'chave_base_cadastro'.
    cadastrosMesclados = Table.NestedJoin(chaveBaseCadastroCriada, {"chave_base_cadastro"}, #"cadastros", {"chave_base_cadastro"}, "cadastros", JoinKind.LeftOuter),
    
    // Expande a coluna 'NOME' da tabela 'cadastros' para 'Nome da Empresa'.
    cadastrosExpandido = Table.ExpandTableColumn(cadastrosMesclados, "cadastros", {"NOME"}, {"Nome da Empresa"}),
    
    // Classifica as linhas por CODEMPRESA em ordem ascendente.
    linhasClassificadasPrimeira = Table.Sort(cadastrosExpandido,{"CODEMPRESA", Order.Ascending}),
    
    // Remove a coluna 'CODEMPRESA'.
    codEmpresaRemovida = Table.RemoveColumns(linhasClassificadasPrimeira,{"CODEMPRESA"}),
    
    // Classifica as linhas por 'chave_base_empresa' e 'chave_base_cadastro'.
    linhasClassificadasSegunda = Table.Sort(codEmpresaRemovida,{{"chave_base_empresa", Order.Ascending}, {"chave_base_cadastro", Order.Ascending}}),
    
    // Cria a coluna 'Nome' com lógica complexa de extração/combinação de texto.
    nomeEmpresaPersonalizado = Table.AddColumn(linhasClassificadasSegunda, "Nome", each 
        let 
            splitNomedaEmpresa = List.Reverse(Splitter.SplitTextByDelimiter(" SECURITIZADORA ", QuoteStyle.None)([Nome da Empresa])), 
            splitNomedaEmpresa2 = List.Reverse(Splitter.SplitTextByDelimiter(" ", QuoteStyle.None)([Nome da Empresa])), 
            splitNomedaEmpresa3 = Splitter.SplitTextByDelimiter("VALECRED ", QuoteStyle.None)([Nome da Empresa]), 
            splitsplitNomedaEmpresa31 = List.Reverse(Splitter.SplitTextByDelimiter(".", QuoteStyle.None)(splitNomedaEmpresa3{1}?)), 
            splitNomedaEmpresa4 = Splitter.SplitTextByDelimiter(" ", QuoteStyle.None)([Nome da Empresa]), 
            splitNomedaEmpresa5 = List.Reverse(Splitter.SplitTextByDelimiter(".", QuoteStyle.None)([Nome da Empresa])) 
        in 
            Text.Combine({
                Text.Start(splitNomedaEmpresa{1}?, 1), 
                Text.Middle(splitNomedaEmpresa2{3}?, 9, 1), 
                Text.Combine(List.Transform(splitsplitNomedaEmpresa31, each Text.Start(_, 1))), 
                Text.Middle(splitNomedaEmpresa4{4}?, 4, 1), 
                Text.Reverse(Text.Middle(Text.Reverse([Nome da Empresa]), 32, 1)), 
                Text.Reverse(Text.Middle(Text.Reverse(splitNomedaEmpresa4{5}?), 7)), 
                Text.Middle(splitNomedaEmpresa5{0}?, 4, 1)
            })
    , type text),
    #"Valor Substituído" = Table.ReplaceValue(nomeEmpresaPersonalizado,"ITCREDO","TATUHY",Replacer.ReplaceText,{"Nome"}),
    
    // Renomeia a coluna 'Nome' para 'Empresa'.
    colunasRenomeadasFinal = Table.RenameColumns(#"Valor Substituído",{{"Nome", "Empresa"}})
in
    colunasRenomeadasFinal;
shared replica_cad_geral_pareceres = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODPARECER, CPFCNPJ, CODTIPOPARECER, CODOPERACAO, DATAINCLUSAO, USUAINCLUSAO, TIPO, CAST(LEFT(OBS,255) AS CHAR(255)) AS OBS, DATAALTERACAO, USUAALTERACAO, ENCAMINHAR, CODTITULO, ALERTA, CODPASTA, CODTAREFA FROM cad_geral_pareceres WHERE YEAR(DATAINCLUSAO) >= 2024", ReturnSingleDatabase=true]),
  
    // Remove colunas desnecessárias como ENCAMINHAR, CODTITULO, ALERTA, CODPASTA, CODTAREFA.
    colunasIniciaisRemovidas = Table.RemoveColumns(origem, {"ENCAMINHAR", "CODTITULO", "ALERTA", "CODPASTA", "CODTAREFA"}),
    
    // Remove colunas de metadados de alteração.
    colunasMetadadosRemovidas = Table.RemoveColumns(colunasIniciaisRemovidas, {"USUAALTERACAO", "DATAALTERACAO"}),

    // Define os tipos de dados corretos para as colunas.
    tiposIniciaisAlterados = Table.TransformColumnTypes(
     colunasMetadadosRemovidas,
    {
      {"CODPARECER", Int64.Type},
      {"CPFCNPJ", type text},
      {"CODTIPOPARECER", Int64.Type},
      {"CODOPERACAO", Int64.Type},
      {"DATAINCLUSAO", type datetime},
      {"USUAINCLUSAO", Int64.Type},
      {"TIPO", type text},
      {"OBS", type text}
    }),

    // Renomeia as colunas para camelCase.
    colunasRenomeadasCamelCase = Table.RenameColumns(
    tiposIniciaisAlterados,
    {
      {"CODPARECER", "codParecer"},
      {"CPFCNPJ", "cpfCnpj"},
      {"CODTIPOPARECER", "codTipoParecer"},
      {"CODOPERACAO","codOperacao"},
      {"DATAINCLUSAO", "dataInclusao"},
      {"USUAINCLUSAO", "usuaInclusao"},
      {"TIPO", "tipo"},
      {"OBS", "obs"}
    }),
    
    // Filtra linhas onde codTipoParecer é igual a 1.
    linhasTipoParecerFiltradas = Table.SelectRows(colunasRenomeadasCamelCase, each [codTipoParecer] = 1),
    
    // Filtra linhas para garantir que CPFCNPJ, OBS, USUAINCLUSAO e DATAINCLUSAO não sejam nulos/vazios.
    linhasComDadosValidos = Table.SelectRows(linhasTipoParecerFiltradas, each ([cpfCnpj] <> null and [cpfCnpj] <> "") and ([obs] <> null and [obs] <> "") and ([usuaInclusao] <> null) and ([dataInclusao] <> null)),
    
    // Classifica as linhas pela data de inclusão em ordem decrescente.
    linhasClassificadasDataInclusao = Table.Sort(linhasComDadosValidos,{{"dataInclusao", Order.Descending}}),
    
    // Filtra as linhas onde a OBS começa com "STATUS ALTERADO PARA ".
    linhasFiltradasConteudoObs = Table.SelectRows(linhasClassificadasDataInclusao, each Text.StartsWith([obs], "STATUS ALTERADO PARA ")),
    
    // Extrai o status do cliente da coluna OBS.
    statusClienteExtraido = Table.AddColumn(linhasFiltradasConteudoObs, "statusCliente", each Text.AfterDelimiter([obs], "STATUS ALTERADO PARA "), type text),
    
    // Remove a coluna OBS original após a extração do status.
    colunasRemovidasPosTransformacao = Table.RemoveColumns(statusClienteExtraido,{"obs"}),
    
    // Remove as colunas codTipoParecer e tipo.
    tiposParecerRemovidos = Table.RemoveColumns(colunasRemovidasPosTransformacao,{"codTipoParecer", "tipo"}),

    // Renomeia as colunas para o formato final desejado.
    colunasRenomeadasFinal = Table.RenameColumns(tiposParecerRemovidos,{{"dataInclusao", "DATAINCLUSAO"}, {"usuaInclusao", "USUAINCLUSAO"}, {"cpfCnpj", "CPFCNPJ"}, {"statusCliente", "STATUS DO CLIENTE"}, {"codParecer", "CODPARECER"}, {"codOperacao", "CODOPERACAO"}}),
    
    // Adiciona a coluna 'BASE' com o valor constante 40.
    baseAdicionada = Table.AddColumn(colunasRenomeadasFinal, "BASE", each 40),
    
    // Altera o tipo das colunas CPFCNPJ, DATAINCLUSAO, USUAINCLUSAO, STATUS DO CLIENTE e BASE.
    tiposFinaisAlterados = Table.TransformColumnTypes(baseAdicionada,{{"CPFCNPJ", type text}, {"DATAINCLUSAO", type datetime}, {"USUAINCLUSAO", Int64.Type}, {"STATUS DO CLIENTE", type text}, {"BASE", Int64.Type}})
in
    tiposFinaisAlterados;
shared #"pareceres de alteracao de status" = let
// Origem dos dados: Silver_Pareceres_Padronizacao (tabela de pareceres padronizados)
fonte = replica_cad_geral_pareceres,
// Mescla com Bronze_Cliente_Producao para obter informações do cliente, usando CPFCNPJ como chave (Left Outer Join).
mescladoComCliente = Table.NestedJoin(fonte, {"CPFCNPJ"}, replica_cad_clientes, {"CPFCNPJ"}, "lb_clientes", JoinKind.LeftOuter),
expandidoCliente = Table.ExpandTableColumn(mescladoComCliente, "lb_clientes", {"CODCLIENTE"}, {"CODCLIENTE"}),
// Adiciona a chave composta para o cliente (BASE-CODCLIENTE).
colunaChaveClienteAdicionada = Table.AddColumn(expandidoCliente, "chave_base_cliente", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODCLIENTE], "pt-BR")}), type text),
// Remove linhas duplicadas.
duplicatasRemovidas = Table.Distinct(colunaChaveClienteAdicionada),
// Mescla com Silver_Usuarios_Enriquecida para obter informações do usuário (NOME), usando USUAINCLUSAO como chave (Left Outer Join).
mescladoComUsuario = Table.NestedJoin(duplicatasRemovidas, {"USUAINCLUSAO"}, #"replica_cad_usuarios", {"CODUSUARIO"}, "lb_usuarios", JoinKind.LeftOuter),
expandidoUsuario = Table.ExpandTableColumn(mescladoComUsuario, "lb_usuarios", {"NOME"}, {"NOME"}),
// Renomeia a coluna NOME para "Usuário".
colunaUsuarioRenomeada = Table.RenameColumns(expandidoUsuario, {{"NOME", "Usuário"}}),
// Remove a coluna USUAINCLUSAO, pois não é mais necessária.
colunaUSUAINCLUSAORemovida = Table.RemoveColumns(colunaUsuarioRenomeada, {"USUAINCLUSAO"}),
// Remove a coluna CPFCNPJ
colunaCPFCNPJRemovida = Table.RemoveColumns(colunaUSUAINCLUSAORemovida,{"CPFCNPJ"}),
// Mescla com "status de clientes da esteira" para obter informações sobre o status do cliente.
mescladoComStatusCliente = Table.NestedJoin(colunaCPFCNPJRemovida, {"STATUS DO CLIENTE"}, #"status de clientes da esteira", {"Status do cliente"}, "status de clientes da esteira", JoinKind.LeftOuter),
expandidoStatusCliente = Table.ExpandTableColumn(mescladoComStatusCliente, "status de clientes da esteira", {"CODSTATUSCLIENTE", "MACROPROCESSO", "FASE"}, {"CODSTATUSCLIENTE", "MACROPROCESSO", "FASE"}),
// Filtra as linhas onde CODCLIENTE não é nulo e não está vazio.
linhasFiltradas = Table.SelectRows(expandidoStatusCliente, each [CODCLIENTE] <> null and [CODCLIENTE] <> ""),
// Classifica as linhas por CODCLIENTE e DATAINCLUSAO.
linhasClassificadas = Table.Sort(linhasFiltradas, {{"CODCLIENTE", Order.Ascending}, {"DATAINCLUSAO", Order.Ascending}}),
// Adiciona um índice para cada cliente.
indiceAdicionado = Table.AddIndexColumn(linhasClassificadas, "Índice", 0, 1, Int64.Type),
// Adiciona uma coluna personalizada para criar uma chave original.
colunaPersonalizadaAdicionada = Table.AddColumn(indiceAdicionado, "Personalizar", each [Índice] * 1000000000 + [CODCLIENTE]),
// Renomeia as colunas "Personalizar" para "chave_original" e "DATAINCLUSAO" para "DATALOG".
colunasRenomeadas = Table.RenameColumns(colunaPersonalizadaAdicionada, {{"Personalizar", "chave_original"}, {"DATAINCLUSAO", "DATALOG"}}),
// Reordena as colunas para melhor visualização.
colunasReordenadas = Table.ReorderColumns(colunasRenomeadas, {"CODCLIENTE", "STATUS DO CLIENTE", "DATALOG", "BASE", "Usuário", "chave_base_cliente", "Índice", "chave_original"}),
// Converte a coluna "chave_original" para o tipo Int64.
tipoAlterado = Table.TransformColumnTypes(colunasReordenadas, {{"chave_original", Int64.Type}})
in
tipoAlterado // Retorna a tabela final com todas as transformações aplicadas.
;
shared #"esteira de propostas" = let
    Fonte = #"pareceres de alteracao de status",
    #"Adição Inserida" = Table.AddColumn(Fonte, "Adição", each [Índice] + 1, type number),
    #"Personalização Adicionada3" = Table.AddColumn(#"Adição Inserida", "chave_secundaria", each [Adição]*1000000000+[CODCLIENTE]),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Personalização Adicionada3",{{"chave_secundaria", Int64.Type}}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Tipo Alterado", {"chave_secundaria"}, #"pareceres de alteracao de status", {"chave_original"}, "esteira de propostas", JoinKind.LeftOuter),
    #"esteira de propostas Expandido1" = Table.ExpandTableColumn(#"Consultas Mescladas", "esteira de propostas", {"STATUS DO CLIENTE", "DATALOG",  "MACROPROCESSO", "FASE",  "chave_original"}, {"STATUS DO CLIENTE ANTERIOR", "DATALOG ANTERIOR", "MACROPROCESSO ANTERIOR", "FASE ANTERIOR", "chave_anterior"}),
    #"Personalização Adicionada" = Table.AddColumn(#"esteira de propostas Expandido1", "VALIDADOR", each [STATUS DO CLIENTE]=[STATUS DO CLIENTE ANTERIOR]),
    #"Linhas Filtradas1" = Table.SelectRows(#"Personalização Adicionada", each ([VALIDADOR] = false)),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas1",{"chave_original", "Adição", "chave_secundaria", "VALIDADOR"}),
    #"Personalização Adicionada1" = Table.AddColumn(#"Colunas Removidas", "Devolução", each [MACROPROCESSO]="CREDITO" and  [MACROPROCESSO ANTERIOR]="COMERCIAL"),
    #"Personalização Adicionada2" = Table.AddColumn(#"Personalização Adicionada1", "Recebida", each [MACROPROCESSO]="COMERCIAL" and [MACROPROCESSO ANTERIOR]="CREDITO"),
    #"Colunas Reordenadas" = Table.ReorderColumns(#"Personalização Adicionada2",{"Índice", "CODCLIENTE", "BASE", "DATALOG ANTERIOR", "DATALOG", "chave_base_cliente", "STATUS DO CLIENTE ANTERIOR", "STATUS DO CLIENTE", "MACROPROCESSO ANTERIOR", "MACROPROCESSO", "FASE ANTERIOR", "FASE", "Usuário", "CODSTATUSCLIENTE", "Devolução", "Recebida"}),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Colunas Reordenadas",{"CODSTATUSCLIENTE"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas1",{{"STATUS DO CLIENTE ANTERIOR", "Status do cliente posterior"}, {"STATUS DO CLIENTE", "Status do cliente"}, {"MACROPROCESSO ANTERIOR", "Macroprocesso posterior"}, {"MACROPROCESSO", "Macroprocesso"}, {"FASE ANTERIOR", "Fase posterior"}, {"FASE", "Fase"}, {"DATALOG ANTERIOR", "Data posterior"}, {"DATALOG", "Data"}})
in
    #"Colunas Renomeadas";
shared operacoes_recompras = let
    Fonte = #"stg_operacoes",
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([TTO] = "RC" or [TTO] = "RE") ),
    #"Linhas Filtradas1" = Table.SelectRows(#"Linhas Filtradas", each ([STATUSANALISE] = "D")),
    #"Linhas Filtradas2" = Table.SelectRows(#"Linhas Filtradas1", each ([STATUSACEITE] = "A")),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas2",{"TOTADVAL", "TOTTAXAADM", "DATAACEITE"}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Colunas Removidas", {"CODOPERACAO"}, boletos_titulos, {"CODOPERACAO"}, "boletos_titulos", JoinKind.LeftOuter),
    #"boletos_titulos Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "boletos_titulos", {"CODTITULO", "NDOC", "CPFCNPJSACADO", "CPFCNPJCEDENTE", "VALOR",  "AMORTIZACOES", "LIQUIDACAO"}, {"CODTITULO", "NDOC", "CPFCNPJSACADO", "CPFCNPJCEDENTE", "VALOR","AMORTIZACOES", "LIQUIDACAO"}),
    #"Colunas Removidas1" = Table.RemoveColumns(#"boletos_titulos Expandido",{"STATUSANALISE", "STATUSACEITE"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas1",{{"chave_base_operacao", "chave_base_operacao_recompra"}})
in
    #"Colunas Renomeadas";
shared #"pareceres de indeferimento de operacoes" = let
    Fonte = lb_pareceres_de_operacoes
in
    Fonte;
shared replica_tab_operacoes_prorrogacao = let
    // Conecta-se ao banco de dados MySQL 'factvale40bi'.
    conexaoMySQL = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT * FROM tab_operacoes_prorrogacao", ReturnSingleDatabase=true])
in
    conexaoMySQL;
shared #"prorrogações" = let
    // Etapa 1: Definir a fonte de dados inicial
    Fonte = replica_tab_operacoes_prorrogacao,

    // Etapa 2: Mesclar dados de títulos (Bronze_Titulos_Producao) com base no código do título (CODTITULO)
    // Utiliza LeftOuter Join para manter todas as linhas da tabela "Fonte" e adicionar informações correspondentes de "Bronze_Titulos_Producao".
    MesclarTitulos = Table.NestedJoin(Fonte, {"CODTITULO"}, #"replica_tab_titulos", {"CODTITULO"}, "lb_titulos", JoinKind.LeftOuter),

    // Etapa 3: Expandir a coluna "lb_titulos" para trazer a coluna "VALOR" para a tabela principal
    ExpandirTitulos = Table.ExpandTableColumn(MesclarTitulos, "lb_titulos", {"VALOR"}, {"VALOR"}),

    // Etapa 4: Mesclar dados de operações (Bronze_Operacoes_Producao) com base no código da operação (CODOPERACAO)
    // Utiliza LeftOuter Join para manter todas as linhas da tabela anterior e adicionar informações correspondentes de "Bronze_Operacoes_Producao".
    MesclarOperacoes = Table.NestedJoin(ExpandirTitulos, {"CODOPERACAO"}, #"replica_tab_operacoes", {"CODOPERACAO"}, "lb_operacoes", JoinKind.LeftOuter),

    // Etapa 5: Expandir a coluna "lb_operacoes" para trazer as colunas "STATUSANALISE" e "STATUSACEITE" para a tabela principal
    ExpandirOperacoes = Table.ExpandTableColumn(MesclarOperacoes, "lb_operacoes", {"STATUSANALISE", "STATUSACEITE"}, {"STATUSANALISE", "STATUSACEITE"}),

    // Etapa 6: Remover colunas desnecessárias para limpar e otimizar a tabela
    RemoverColunas = Table.RemoveColumns(ExpandirOperacoes,{"TARIFA", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "VALORDEVIDO", "VALORPROR", "VALORBOLETO"}),

    // Etapa 7: Adicionar uma coluna "BASE" com valor fixo (40)
    AdicionarColunaBase = Table.AddColumn(RemoverColunas, "BASE", each 40),

    // Etapa 8: Criar uma coluna "chave_base_titulo" concatenando "BASE" e "CODTITULO" para gerar uma chave única
    CriarChaveBaseTitulo = Table.AddColumn(AdicionarColunaBase, "chave_base_titulo", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODTITULO], "pt-BR")}), type text),

    // Etapa 9: Extrair a data da coluna "DATAINCLUSAO" e criar uma nova coluna "Data"
    ExtrairDataInclusao = Table.AddColumn(CriarChaveBaseTitulo, "Data", each DateTime.Date([DATAINCLUSAO]), type date)

in
    // Retornar a tabela resultante da última etapa
    ExtrairDataInclusao;
shared sacados = let
  fonteSQLDatabases = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT DISTINCT CPFCNPJSACADO FROM tab_titulos WHERE YEAR(DATAINCLUSAO) >= 2021", ReturnSingleDatabase=true]),
    #"Consultas Mescladas" = Table.NestedJoin(fonteSQLDatabases, {"CPFCNPJSACADO"}, #"replica_cad_geral_pf_pj", {"CPFCNPJ"}, "replica_cad_geral_pf_pj", JoinKind.LeftOuter),
    #"replica_cad_geral_pf_pj Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "replica_cad_geral_pf_pj", {"NOME"}, {"NOME"}),
    #"Consultas Mescladas1" = Table.NestedJoin(#"replica_cad_geral_pf_pj Expandido", {"CPFCNPJSACADO"}, enderecos, {"CPFCNPJ"}, "enderecos", JoinKind.LeftOuter),
    #"enderecos Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas1", "enderecos", {"ENDERECO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF", "CEP", "Região"}, {"ENDERECO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF", "CEP", "Região"}),
    #"Consultas Mescladas2" = Table.NestedJoin(#"enderecos Expandido", {"CPFCNPJSACADO"}, replica_cad_email, {"CPFCNPJ"}, "replica_cad_email", JoinKind.LeftOuter),
    #"replica_cad_email Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas2", "replica_cad_email", {"EMAIL"}, {"EMAIL"}),
    #"Consultas Mescladas3" = Table.NestedJoin(#"replica_cad_email Expandido", {"CPFCNPJSACADO"}, telefones, {"CPFCNPJ"}, "telefones", JoinKind.LeftOuter),
    #"replica_cad_telefones Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas3", "telefones", {"telefone"}, {"FONE"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"replica_cad_telefones Expandido",{{"NOME", "Nome do sacado"}, {"EMAIL", "emails"}, {"FONE", "telefone"}}),
    #"Duplicatas Removidas" = Table.Distinct(#"Colunas Renomeadas", {"CPFCNPJSACADO"})
in
    #"Duplicatas Removidas" // Retorna a tabela final com as colunas renomeadas.
;
shared replica_tab_statusoperacao = let
    // Conecta-se a um banco de dados MySQL para obter dados de status de operações.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODSTATUSOPERACAO, DESCRICAO FROM tab_statusoperacao", ReturnSingleDatabase=true])
in
    origem;
shared status_operacoes = let
    Fonte = replica_tab_statusoperacao
in
    Fonte;
shared tarifas_esporadicas = let
    // Conecta-se a um banco de dados MySQL para obter dados de tarifas extras de operações.
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT CODOPERACAO, DESCRICAO, TOTAL, DATAINCLUSAO, USUAINCLUSAO AS CODUSUARIO#(lf)FROM tab_operacoes_tarifas_extras#(lf)WHERE YEAR(DATAINCLUSAO) >= 2024", ReturnSingleDatabase=true]),
    
    // Mescla com a tabela 'usuarios' usando 'CODUSUARIO'.
    consultasMescladas = Table.NestedJoin(origem, {"CODUSUARIO"}, usuarios, {"CODUSUARIO"}, "usuarios", JoinKind.LeftOuter),
    
    // Expande a coluna 'usuarios' para incluir 'NOME'.
    usuariosExpandido = Table.ExpandTableColumn(consultasMescladas, "usuarios", {"NOME"}, {"ANALISTA"}),
    
    // Remove a coluna original 'CODUSUARIO'.
    codUsuarioRemovido = Table.RemoveColumns(usuariosExpandido,{"CODUSUARIO"}),
    
    // Filtra linhas onde 'ANALISTA' não é "RONALDO DANILO UREI GOBBI".
    linhasAnalistaFiltradas = Table.SelectRows(codUsuarioRemovido, each ([ANALISTA] <> "RONALDO DANILO UREI GOBBI")),
    
    // Cria a coluna 'chave_base_operacao' concatenando "40-" e 'CODOPERACAO'.
    chaveBaseOperacaoInserida = Table.AddColumn(linhasAnalistaFiltradas, "chave_base_operacao", each Text.Combine({"40-", Text.From([CODOPERACAO], "pt-BR")}), type text),
    
    // Remove linhas duplicadas com base nas colunas especificadas.
    duplicatasRemovidas = Table.Distinct(chaveBaseOperacaoInserida, {"CODOPERACAO", "DESCRICAO", "TOTAL", "DATAINCLUSAO", "ANALISTA"})
in
    duplicatasRemovidas;
shared usuarios = let
    Fonte = #"replica_cad_usuarios",
    #"Personalização Adicionada" = Table.AddColumn(Fonte, "BASE", each 40),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Personalização Adicionada",{{"BASE", type text}}),
    #"Coluna Mesclada Inserida" = Table.AddColumn(#"Tipo Alterado", "chave_base_cadastro", each Text.Combine({Text.From([BASE], "pt-BR"), "-", [CPFCNPJ]}), type text),
    #"Coluna Mesclada Inserida1" = Table.AddColumn(#"Coluna Mesclada Inserida", "chave_base_usuario", each Text.Combine({Text.From([BASE], "pt-BR"), "-", Text.From([CODUSUARIO], "pt-BR")}), type text)
in
    #"Coluna Mesclada Inserida1";

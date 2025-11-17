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
    origem = MySQL.Database("56.125.147.185", "lbfactor", [Query="SELECT NBORDERO, CODBROKER, CODOPERACAO, CODEMPRESA, DATAINCLUSAO, CODCLIENTE, CPFCNPJCLIENTE, STATUSANALISE, DATAANALISE, STATUSACEITE, DATAACEITE, DATAENVIOEMAIL, STTO, TTO, FATOR, TARIFA, TARIFARECOMPRA, TAC, NDOCS,#(lf)         NDOCSRECOMPRA, TOTADVAL, TOTTAXAADM, TOTTAR, APROVACAO1, DATAALTERACAO, CODINDEFERIMENTO, CONTRATOFISICO, USUAINCLUSAO, USUATRAVA, USUASTANALISE, NOTASERVICO FROM tab_operacoes ORDER BY DATAANALISE DESC", ReturnSingleDatabase=true]),
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
    mesclarTitulosComOperacoes = Table.NestedJoin(criarChaveBaseOperacao, {"chaveBaseOperacao"}, fact_operacoes, {"chave_base_operacao"}, "operacoes", JoinKind.LeftOuter),
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
    Fonte = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("fVVLbtswEL0K4XUCWJITt8sJRbkDSByFpIw0Qe7SoosCBbIqeiLdpCfpDKlUomUF8MI2H+fz3szjy8uu2N3sNFhtWuiMDaRqo6g3Dsaf4w+6ONy93rzsSv7TPGnjPSlLqsUOg1GaOvR+/O4Q5vOIr67ia/l4TZLSkuugvbh2uHoNbTCOc/Vgv15cuNvK01AqPoff828PGmpGc6eqhxMoUBLeoCP//ziijxdoKYFcgBz1aUb15P2A6mysxppy2OdIaw0+OFIexrcapE6HzMfiKGKLPf8z9eIBP0IWVwkwvjd6fGtQs7aSLpaRc1GIpqyFZp5m5RcVFyKibdQ0DDXEn+koE4oJdIz5Yi4z3KUMDbLUMYHIb+zjALVL8fi0xnQUB1AjWZ6KePt+Sk9dYjQK6lWNZ+NO/N34RUHHjN6B+7d+zZZoFaUE1juA0i1KIPX3228V4AmuH6e7IuBK4IYciwOsfDu+BdSQtkX066iGlrVj6VLM3tF56npZVFnMYs+Bc0i57A4tBDyvQSLXtF4yjKZTNAi+hzTdHzRXHuYhfmjpcTBwOb/lXZKjI4uBRD7Vm5YUtzTMOpQiGgvZYCu0ZKaywdUxxV3IqjzXroE3TeZek3PmPcZ7GhEyYBi4AB4LcopLVsV+f7Pfb2eKK9hQn7U4Raz2G/Rsk1YVK3+Io05Dzlx13Ty5Ndk8ww7UDGFwKwsVPWcC2aR8LGflzdUhB3I5DkmdeCfFcGUd2k1SqusOmpwd2Ng51glX5lHdLxxqY9uq48a23caRxOj+m+RureqtckaM2H20rNXnWRrxjwfAJ+DeHgeszXMuz6GII2vr8RcvH6THQ94mfF68h4u+DmUe3LEwDq8Y6KHKgdCGJNIfnnHu4t3DlldES/nEH1uv2/SEom1Wj+jrPw==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [CODINDEFERIMENTO = _t, #"MOTIVO INDEFERIMENTO" = _t, #"GRUPO MOTIVO INDEFERIMENTO" = _t]),
    #"Tipo Alterado" = Table.TransformColumnTypes(Fonte,{{"CODINDEFERIMENTO", Int64.Type}, {"MOTIVO INDEFERIMENTO", type text}, {"GRUPO MOTIVO INDEFERIMENTO", type text}})
in
    #"Tipo Alterado";
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
shared fact_operacoes = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "fact_operacoes", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";

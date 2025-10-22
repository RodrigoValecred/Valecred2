section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_tac_m_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "cod_tarifa_extra", DestinationColumnName = "cod_tarifa_extra"], [SourceColumnName = "cod_operacao", DestinationColumnName = "cod_operacao"], [SourceColumnName = "descricao", DestinationColumnName = "descricao"], [SourceColumnName = "total", DestinationColumnName = "total"], [SourceColumnName = "data_inclusao", DestinationColumnName = "data_inclusao"], [SourceColumnName = "usua_inclusao", DestinationColumnName = "cod_usuario"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared staging_tac_m = let
    origem = source_tab_operacoes_tarifas_extras,
  #"Colunas removidas 1" = Table.RemoveColumns(origem, {"CODENTRADA", "DATAALTERACAO", "USUAALTERACAO", "TIPO"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Colunas removidas 1", {{"CODTARIFAEXTRA", "cod_tarifa_extra"}, {"CODOPERACAO", "cod_operacao"}, {"DESCRICAO", "descricao"}, {"TOTAL", "total"}, {"DATAINCLUSAO", "data_inclusao"}, {"USUAINCLUSAO", "usua_inclusao"}}),
    linhas_filtradas_por_ano = Table.SelectRows(#"Colunas renomeadas", each Date.Year([data_inclusao]) >= 2024),
  #"Texto em maiúscula" = Table.TransformColumns(linhas_filtradas_por_ano, {{"descricao", each Text.Upper(_), type nullable text}}),
  #"Texto limpo" = Table.TransformColumns(#"Texto em maiúscula", {{"descricao", each Text.Trim(_), type nullable text}}),
  #"Valor substituído" = Table.ReplaceValue(#"Texto limpo", "TAC  M", "TAC M", Replacer.ReplaceText, {"descricao"}),
  #"Valor substituído 1" = Table.ReplaceValue(#"Valor substituído", "TAC MOP", "TAC M", Replacer.ReplaceText, {"descricao"}),
  #"Valor substituído 2" = Table.ReplaceValue(#"Valor substituído 1", "TAC M.", "TAC M", Replacer.ReplaceText, {"descricao"}),
  #"Valor substituído 3" = Table.ReplaceValue(#"Valor substituído 2", "TACM", "TAC M", Replacer.ReplaceText, {"descricao"}),
  #"Valor substituído 4" = Table.ReplaceValue(#"Valor substituído 3", "TACA M", "TAC M", Replacer.ReplaceText, {"descricao"}),
  #"Valor substituído 5" = Table.ReplaceValue(#"Valor substituído 4", "TAC M 300,00", "TAC M", Replacer.ReplaceText, {"descricao"}),
  #"Valor substituído 6" = Table.ReplaceValue(#"Valor substituído 5", "TAC", "TAC M", Replacer.ReplaceValue, {"descricao"}),
  #"Linhas filtradas" = Table.SelectRows(#"Valor substituído 6", each ([descricao] = "TAC M")),
  #"Linhas classificadas" = Table.Sort(#"Linhas filtradas", {{"data_inclusao", Order.Descending}})
in
    #"Linhas classificadas";
shared staging_tab_titulos_confirmacao = let
    Origem = source_tab_titulos_confirmacao,
    #"Colunas Selecionadas" = Table.SelectColumns(Origem, {"CODOPERACAO", "CODTITULO", "CONFAO", "CODOCORCONFIRMACAO", "DATAINCLUSAO", "USUAINCLUSAO", "CODTITULOSCONFIRMACAO", "DOCCONFIRMADO", "OBSERVACAO"}),
    #"OBS Processada" = Table.TransformColumns(#"Colunas Selecionadas", {{"OBSERVACAO", each Text.FromBinary(_, 1252)}}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"OBS Processada", {{"OBSERVACAO", "observacao"}}),
    #"Data Inserida" = Table.AddColumn(#"Colunas Renomeadas", "Data", each DateTime.Date([DATAINCLUSAO]), type nullable date)
in
    #"Data Inserida";
shared staging_rlc_titulos_ocorrencias_cobranca = let
  Origem = source_rlc_titulos_ocorrencias_cobranca,
  #"Colunas mescladas" = Table.CombineColumns(Table.TransformColumnTypes(Origem, {{"CODBANCO", type text}, {"TOCORRENCIA", type text}, {"CODOCORINTERNA", type text}}), {"TOCORRENCIA", "CODOCORINTERNA", "CODBANCO"}, Combiner.CombineTextByDelimiter("-", QuoteStyle.None), "chave_ocorrencia"),
  #"Colunas removidas" = Table.RemoveColumns(#"Colunas mescladas", {"VALORABATIM", "VALORDESCONTO", "VALORMORAJUROS", "VALORDESPCOBR", "DATAGRAVACAO", "DATACREDITO", "MOTIVOCONFINSTRUCAO", "MOTIVOCODOCORCOBRBANCO", "CODCONTACOBRANCA", "MD5", "BANCORECEBEDOR", "AGENCIARECEBEDORA", "VALORCREDITO", "VALOROUTROCRED", "VALOROUTRADESP"})
in
  #"Colunas removidas";
shared staging_notificacoes = let
  Origem = source_tab_titulos_cobranca,
  #"As outras colunas foram removidas" = Table.SelectColumns(Origem, {"CODOPERACAO", "CODTITULO", "COBRADOAO", "CODOCORCOBRANCA", "OBSERVACAO", "DATAINCLUSAO", "USUAINCLUSAO"}),
  #"OBS Processada" = Table.TransformColumns(#"As outras colunas foram removidas", {{"OBSERVACAO", each Text.FromBinary(_, 1252)}}),
  #"Linhas filtradas" = Table.SelectRows(#"OBS Processada", each [CODOCORCOBRANCA] = 12),
  #"Valor substituído" = Table.ReplaceValue(#"Linhas filtradas", "&ccedil;", "ç", Replacer.ReplaceText, {"OBSERVACAO"}),
  #"Valor substituído 1" = Table.ReplaceValue(#"Valor substituído", "&atilde;", "ã", Replacer.ReplaceText, {"OBSERVACAO"})
in
  #"Valor substituído 1";
shared staging_titulos_baixas = let
    Origem = source_titulos_baixas,
    #"Colunas Selecionadas" = Table.SelectColumns(Origem, {"CODTITULOBAIXAS", "CODTITULO", "DATABAIXA", "CODOPERACAO", "DATABAIXASIST", "VLPAGO", "DESCONTO", "JUROS", "TARIFARECOMPRA", "PAGOPELO", "MOTIVO", "MANTEMCOBRANCA", "FORMA", "DATAINCLUSAO", "USUAINCLUSAO", "TIPOBAIXA", "VLRECALCULADO", "ANTECIPACAO", "VALORBAIXA", "VALORDEVIDO"}),
    correcaoValoresNumericos = Table.TransformColumns(#"Colunas Selecionadas", {
        {"JUROS", each if _ = -858005.8 then 3912.5 else if _ = -4948525.71 then -56747.24 else if _ = -4140.75 then 0 else if _ = -1447.5 then 52.5 else if _ = -1825.72 then 66.28 else if _ = -965 then 35 else if _ = -26000 then 0 else if _ = -307208.46 then 0 else _ ?? 0, type number},
        {"DESCONTO", each _ ?? 0, type number},
        {"TARIFARECOMPRA", each _ ?? 0, type number}
    }),
    correcaoValoresTexto = Table.TransformColumns(correcaoValoresNumericos, {
        {"PAGOPELO", each if _ = "C" then "CEDENTE" else if _ = "S" then "SACADO" else _ ?? "NÃO INFORMADO", type text},
        {"FORMA", each if _ = "O" then "OPERAÇÃO" else if _ = "B" then "BOLETO" else if _ = "CR" then "CRÉDITO" else _ ?? "NÃO INFORMADO", type text},
        {"TIPOBAIXA", each if _ = "B" then "BANCO" else if _ = "R" then "MANUAL" else _ ?? "NÃO INFORMADO", type text},
        {"MOTIVO", each if _ = "PG" then "PAGAMENTO" else if _ = "RC" then "RECOMPRA" else if _ = "AM" then "AMORTIZAÇÃO" else _, type text}
    }),
    valorPagoLiquidoAdicionado = Table.AddColumn(correcaoValoresTexto, "VALOR PAGO LIQUIDO", each [VLPAGO]-[JUROS]+[DESCONTO]-[TARIFARECOMPRA], type number)
in
    valorPagoLiquidoAdicionado;
shared staging_clientes = let
  Origem = source_cad_clientes,
  #"Linhas filtradas" = Table.SelectRows(Origem, each [CODCLIENTE] > 4),
  #"Colunas removidas" = Table.RemoveColumns(#"Linhas filtradas", {"MEEPP", "CAPITALSOCIAL", "CONSTITUICAO", "CODTIPOCLI", "RAMOATIVIDADE", "CODNATJURIDICA", "CODREGIMETRIB", "CODATIVANFAC", "PROTESTACLI", "REAPRESENTACH", "CARTACOBRANCA", "COBRABANCARIA", "BLOQOPERACAO", "DTBLOQOPERACAO", "OBSERVACAOBLOQ", "GERACOMISSAO", "CONTACTB", "CONTACTBTRUSTEE", "CONTACTBPENDENCIAS", "CONTACTBCAUCAO", "STATUS", "OBSERVSTATUS", "OBSRATING", "DTHRRATING", "USUARATING", "PEFIN", "USUAINCPEFIN", "DTINCPEFIN", "BAIXADOPEFIN", "USUABXPEFIN", "OBSERVACOES", "FINGERPRINT", "DTULTPARECER", "CONTRATOFIDCANTIGO", "JUROSDIA", "SALDOOPERACIONALTITULOS", "PENDENCIASINICIAIS", "CREDITOSINICIAIS", "SALDOANTERIOREXTRATO", "MENSAGEM1", "MENSAGEM2", "MENSAGEM3", "MENSAGEM4", "MULTA", "FATORMULTA", "COBRACONSULTA", "UTILIZAGERALCONS", "MANTERCEDENTE", "VALORJUROSDIA", "CODCONTARETORNO", "FATURAMENTOMEDIO", "USUAATUALIZADOC", "ENDIVIDAMENTOMEDIO", "DTRESERVA", "DTRENOVACAO", "DTSEGUNDAREN", "PREIMPRESSO", "TAMANHORANGE", "RANGEDE", "RANGEATE", "STATUSPREIMPRESSO", "CONTRATOESPECIFICO", "BOLETOESPECIAL", "PRORROGACAO", "MPRORROGACAO", "VENCIMENTOMAXIMO", "VENCIMENTOMAXIMOFIDC", "NOTIFICACESSAOCREDITO", "USUACHK", "CODTIPOCLASSIFICACAO", "RECEBEBOLETO", "RECEBERETORNOBANCARIO", "FATORRECOMPRA", "FATORCESSAO", "OPERATRUSTEE", "CEDENTECCB", "TARIFARECOMPRA", "MOSTRARPESQUISA", "RIGIDO", "USUAINCLUSAO", "USUAALTERACAO"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Colunas removidas", {{"CODCLIENTE", "cod_cliente"}, {"CPFCNPJ", "cpf_cnpj"}, {"CODATIVIDADE", "cnae"}, {"DATASTATUS", "data_status"}, {"CODSTATUSCLIENTE", "cod_status_cliente"}, {"CODRATING", "cod_rating"}, {"CODRATINGSUG", "cod_rating_sug"}, {"DATAINCLUSAO", "data_inclusao"}, {"DATAALTERACAO", "data_alteracao"}, {"RECUPERACAOJUDICIAL", "recuperacao_judicial"}})
in
  #"Colunas renomeadas";
shared staging_abatimentos = let
  Origem = source_tab_titulos_abatimento,
  #"As outras colunas foram removidas" = Table.SelectColumns(Origem, {"CODTITULOABAT", "CODOPERACAO", "CODTITULO", "CODOPERACAOAB", "VALORDEVIDO", "ABATIMENTO", "DATAINCLUSAO", "CODBANCOCOBR", "USUAINCLUSAO"}),
  #"Data inserida" = Table.AddColumn(#"As outras colunas foram removidas", "Data", each DateTime.Date([DATAINCLUSAO]), type nullable date)
in
  #"Data inserida";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_contratos_clientes_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared staging_contratos_clientes = let
    origem = source_cad_contratos_clientes,
    colunas_renomeadas = Table.RenameColumns(origem, List.Transform(Table.ColumnNames(origem), each {_, Text.Lower(Text.Replace(_, "_", " "))})),
  #"Colunas renomeadas" = Table.RenameColumns(colunas_renomeadas, {{"codcontrato", "cod_contrato"}, {"codcliente", "cod_cliente"}, {"dtinicontrato", "dt_ini_contrato"}, {"validadelimite", "validade_limite"}, {"limitefomento", "limite_fomento"}, {"limitecomissaria", "limite_comissaria"}, {"percconfirmacao", "perc_confirmacao"}, {"statusdiretoria", "status_diretoria"}})
in
    #"Colunas renomeadas";
shared staging_eventos_protesto_limpa = let
    origem = source_rlc_titulos_ocorrencias_cobranca,
    colunas_renomeadas = Table.RenameColumns(origem, {{"DATAINCLUSAO", "data_inclusao"}, {"CODTITULO", "cod_titulo"}, {"CODOCORINTERNA", "cod_ocor_interna"}, {"CODOCORCOBRBANCO", "cod_ocor_cobr_banco"}, {"TOCORRENCIA", "t_ocorrencia"}, {"CODCONTACOBRANCA", "cod_conta_cobranca"}, {"STATUS", "status"}})
in
    colunas_renomeadas;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_enderecos_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared staging_enderecos = let
  Origem = source_cad_enderecos,
  #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJ", "cpf_cnpj"}, {"ENDERECO", "endereco"}, {"NUMERO", "numero"}, {"COMPLEMENTO", "complemento"}, {"CODENDERECO", "cod_endereco"}, {"BAIRRO", "bairro"}, {"CIDADE", "cidade"}, {"CODMUNICIPIO", "cod_municipio"}, {"UF", "uf"}, {"CEP", "cep"}, {"DATAINCLUSAO", "data_inclusao"}, {"USUAINCLUSAO", "usua_inclusao"}}),
  #"As outras colunas foram removidas" = Table.SelectColumns(#"Colunas renomeadas", {"cod_endereco", "cpf_cnpj", "endereco", "numero", "complemento", "bairro", "cidade", "cod_municipio", "uf", "cep", "data_inclusao", "usua_inclusao"})
in
  #"As outras colunas foram removidas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_cad_geral_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared staging_cad_geral = let
    origem = source_cad_geral,
    #"As outras colunas foram removidas" = Table.SelectColumns(origem, {"CPFCNPJ", "NOME", "DATAINCLUSAO", "USUAINCLUSAO"}),
    colunas_renomeadas = Table.RenameColumns(#"As outras colunas foram removidas", {{"CPFCNPJ", "cpf_cnpj"}, {"NOME", "nome"}, {"DATAINCLUSAO", "data_inclusao"}, {"USUAINCLUSAO", "usua_inclusao"}})
in
    colunas_renomeadas;
shared staging_status_clientes_esteira = let
    origem = sup_status_de_clientes_da_esteira,
    colunas_renomeadas = Table.RenameColumns(origem, {{"codstatuscliente", "cod_status_cliente"}, {"status_do_cliente", "status_do_cliente"}, {"macroprocesso", "macroprocesso"}, {"fase", "fase"}})
in
    colunas_renomeadas;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_operacoes_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared staging_operacoes = let
    origem = source_tab_operacoes,
  #"Colunas removidas" = Table.RemoveColumns(origem, {"CODENTRADA", "CODFUNDO", "CODREMESSA", "CODEMISSAO", "CODSERIEEMISSAO", "CODTIPOCABAD", "CODRATING", "STATUSDIGITACAO", "USUADIGITACAO", "DATADIGITACAO", "USUASTACEITE", "DATAACEITEANT", "DATACALCULO", "TDOC", "FLOATING", "PERCCUSTOFINANC", "FATORMORA", "ADVAL", "TACAUTOMATICO", "CPMF", "IOF", "IOFAD", "COFINS", "CSLL", "PIS", "NPENRECOMPRA", "NVENCIMENTOS", "VLMEDIOTITULOS", "PME", "PMPIOF", "TOTFAC", "TOTDCP", "PERCADVAL", "TOTCOFINS", "TOTCSLL", "TOTPIS", "TOTIOF", "TOTIRRF", "TOTCPMF", "TOTIOFAD", "TOTISS", "TOTTARESP", "TOTRECOMPRA", "TOTPENDENCIAS", "TOTTARRECOMPRA", "TOTDES", "TOTLIQANTESIOF", "LOTENF", "LOTENFOK", "TAXAFINAL", "ESTUDO1", "USUAPROVA1", "APROVACAO2", "ESTUDO2", "USUAPROVA2", "APROVACAO3", "ESTUDO3", "USUAPROVA3", "EXIGECANHOTO", "EXIGECONFIRMACAO", "ZERAIOF", "DATATRAVA", "COBRABANCARIA", "USUALTERACAO", "TOTRETENCAO", "PROTESTAR", "FCUSTOFIN", "CUSTOFINANCEIRO", "TOTSPREAD", "TARIFABAIXAPEN", "TAXAADM", "USUAENVIOEMAIL", "DISPENSAENVIOEMAIL", "MOTIVOCLIENTE", "TIPOBL", "VENCIMENTOBL", "PREIMPRESSO", "BOLETOESPECIAL", "CPFCNPJCEDENTECCB", "TED", "RECEBEBOLETO", "CLASSIFICACAOCLIENTE", "GARANTIA", "CODGERENTEPLATAFORMA", "OPORTUNIDADE", "CALCULOINICIAL", "SEPARACAOSACADO", "CALCULOOPERACAOTIPO", "CODIGOCONTRATOCCB", "CODPPC", "CODGRUPOALCADAOPERACAO", "CODTIPOSTATUSOPERACAO", "CODPRODUTO", "CODAGENCIA"}),
    colunas_renomeadas = Table.RenameColumns(#"Colunas removidas", {{"NBORDERO", "n_bordero"}, {"CODOPERACAO", "cod_operacao"}, {"CODEMPRESA", "cod_empresa"}, {"DATAINCLUSAO", "data_inclusao"}, {"CODCLIENTE", "cod_cliente"}, {"CPFCNPJCLIENTE", "cpf_cnpj_cliente"}, {"STATUSANALISE", "status_analise"}, {"DATAANALISE", "data_analise"}, {"STATUSACEITE", "status_aceite"}, {"DATAACEITE", "data_aceite"}, {"DATAENVIOEMAIL", "data_envio_email"}, {"STTO", "stto"}, {"TTO", "tto"}, {"FATOR", "fator"}, {"TARIFA", "tarifa"}, {"TARIFARECOMPRA", "tarifa_recompra"}, {"TAC", "tac"}, {"NDOCS", "n_docs"}, {"NDOCSRECOMPRA", "n_docs_recompra"}, {"TOTADVAL", "tot_adval"}, {"TOTTAXAADM", "tot_taxa_adm"}, {"TOTTAR", "tot_tar"}, {"APROVACAO1", "aprovacao_1"}, {"DATAALTERACAO", "data_alteracao"}, {"CODINDEFERIMENTO", "cod_indeferimento"}, {"CONTRATOFISICO", "contrato_fisico"}, {"USUAINCLUSAO", "usua_inclusao"}, {"USUATRAVA", "usua_trava"}, {"USUASTANALISE", "usua_st_analise"}, {"NOTASERVICO", "nota_servico"}, {"PMP", "pmp"}, {"NOTAFISCAL", "nota_fiscal"}, {"CODBROKER", "cod_gerente"}})
in
    colunas_renomeadas;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_titulos_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared staging_titulos = let
    origem = source_tab_titulos,
    colunas_renomeadas = Table.RenameColumns(origem, {{"CODTITULO", "cod_titulo"}, {"CODOPERACAO", "cod_operacao"}, {"NDOC", "n_doc"}, {"TDOC", "t_doc"}, {"VENCIMENTO", "vencimento"}, {"VENCPRORROGADO", "venc_prorrogado"}, {"PRAZO", "prazo"}, {"CPFCNPJSACADO", "cpf_cnpj_sacado"}, {"CPFCNPJCEDENTE", "cpf_cnpj_cedente"}, {"VALOR", "valor"}, {"DESAGIO", "desagio"}, {"LIQUIDO", "liquido"}, {"AMORTIZACOES", "amortizacoes"}, {"VALORDEVIDO", "valor_devido"}, {"LIQUIDACAO", "liquidacao"}, {"ACEITO", "aceito"}, {"CODBANCOCOBR", "cod_banco_cobr"}, {"DATACONF", "data_conf"}, {"USUACONF", "usua_conf"}, {"DATAINCLUSAO", "data_inclusao"}, {"DOCCONFIRMADO", "doc_confirmado"}, {"MOTIVO", "motivo"}, {"PRACA", "praca"}, {"CHAVEDANFE", "chave_danfe"}, {"NOSSONUMERO", "nosso_numero"}, {"CODFUNDO", "cod_fundo"}, {"TTO", "tipo_cobranca"}, {"FILIAL", "raiz_cnpj"}, {"CODEMISSAO", "cod_emissao"}, {"STATUSCONFIRMACAO", "status_confirmacao"}, {"SEUNUMERO", "seu_numero_bancario"}, {"CODREMESSA", "cod_remessa"}}),
  #"Colunas removidas" = Table.RemoveColumns(colunas_renomeadas, {"CODTITULOANT", "CODEMPRESA", "CODSERIE", "CONTROLE", "CODBANCOCH", "CODBANCO", "CMC7", "CODAGENCIA", "CODCC", "DESCONTO", "DTLIMITEDESCONTO", "FLOATING", "VALORSISCOB", "DESCSISCOB", "VLPAGSISCOB", "LIQUIDASISCOB", "DATAPROSISCOB", "STATUSSISCOB", "MORADESCSISCOB", "CODBANCOCUST", "CODCUSTODIA", "DATABAIXACLIE", "EXIGECONFIRMACAO", "EXIGECANHOTO", "DATACL", "INCLUIDOPEFIN", "USUAINCLPEFIN", "BAIXADOPEFIN", "USUABAIXPEFIN", "OBSPEFIN", "OBSERVACAO", "PRIORRECOMPRA", "BXCARTORIO", "PRIORCONF", "MARCADOSERASA", "DATAENVIOSERASA", "AGENDAMENTOCOB", "AGENDAMENTOCONF", "USUAAGENDACOB", "USUAAGENDACONF", "BAIXAAGENDACOB", "BAIXAAGENDACONF", "AR", "DATAAR", "CODTIPOCONF", "DATAEMISSAO", "FORMA", "PAGOPELO", "TIPOBAIXA", "ABATIMENTO", "LIQUIDACAOCS", "CPFCNPJCEDENTECESSAO", "VALORCESSAO", "VALORORIGINALTITULO", "CODESTADOLASTRO", "TIPONEGATIVACAO", "CODNOTIFICACAO", "DIASNEGATIVACAO", "JUROSCONFISSAO", "CODCONFISSAO", "NOSSONUMEROCORRESPONDENTE", "CODOPERACAOORIGINAL", "CODTITULOORIGINAL", "VENCIMENTOXML", "VALORXML", "INCLUIRCOBR", "INCLUIRCUST", "CL", "PEFIN", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CANHOTO", "VALORDEVIDOANT", "USUARIOCONFIRMACAO", "RETORNOBANCARIOREJEICAO", "PRAZOACEITO", "CSTATUS", "CODARQUIVOXML", "NPROTOCOLO", "SIGNATURE", "NF"}),
  #"Linhas classificadas" = Table.Sort(#"Colunas removidas", {{"data_inclusao", Order.Descending}})
in
    #"Linhas classificadas";
shared staging_pareceres_alteracao_status = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    tabela_origem = navegacao_2{[Id = "pareceres_de_alteracao_de_status", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared staging_esteira_propostas = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    tabela_origem = navegacao_2{[Id = "esteira_de_propostas", ItemKind = "Table"]}[Data]
in
    tabela_origem;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging_boletos_titulos_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared staging_boletos_titulos = let
    origem = staging_titulos,
    linhas_filtradas_tdoc = Table.SelectRows(origem, each [t_doc] = "BL"),
    linhas_filtradas_data = Table.SelectRows(linhas_filtradas_tdoc, each [data_inclusao] >= #datetime(2021, 1, 1, 0, 0, 0)),
    colunas_removidas = Table.RemoveColumns(linhas_filtradas_data, {"venc_prorrogado", "prazo", "desagio", "data_conf", "usua_conf", "doc_confirmado", "chave_danfe"})
in
    colunas_removidas;
shared staging_chave_danfe_detalhada = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    tabela_origem = navegacao_2{[Id = "staging_chave_danfe_detalhada", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_cad_agencias = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_agencias", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_cad_brokers = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_brokers", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_cad_clientes = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "cad_clientes", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared source_cad_conta_cobranca_ocorrencia = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_conta_cobranca_ocorrencia", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_cad_contratos_clientes = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  Navegação = navegacao_2{[Id = "staging_contratos_clientes_limpa", ItemKind = "Table"]}[Data]
in
    Navegação;
shared source_cad_geral = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "cad_geral_pf_pj", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared source_cad_enderecos = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_enderecos", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_cad_geral_pareceres = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_geral_pareceres", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_cad_usuarios = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_usuarios", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_rlc_clientes_sacados_limites = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "rlc_clientes_sacados_limites", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_rlc_titulos_ocorrencias_cobranca = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "rlc_titulos_ocorrencias_cobranca", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_atividades = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_atividades", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_titulos_baixas = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "tab_titulos_baixas", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared source_tab_estudo_op = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_estudo_op", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_feriados = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_feriados", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_operacoes_tarifas_extras = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_operacoes_tarifas_extras", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_geral_pendencias = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_geral_pendencias", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_operacoes_devolucoes = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_operacoes_devolucoes", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_operacoes_prorrogacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_operacoes_prorrogacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_statusoperacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_statusoperacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_titulos = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "tab_titulos", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared source_tab_subtipooperacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_subtipooperacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_tipooperacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_tipooperacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_tipoparecer = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_tipoparecer", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_titulos_abatimento = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_titulos_abatimento", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_titulos_cobranca = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_titulos_cobranca", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_titulos_confirmacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_titulos_confirmacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared source_tab_operacoes = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_operacoes_limpa", ItemKind = "Table"]}[Data],
  #"Linhas classificadas" = Table.Sort(#"Navegação 2", {{"DATAANALISE", Order.Descending}})
in
  #"Linhas classificadas";
shared sup_status_de_clientes_da_esteira = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "sup_status_de_clientes_da_esteira", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_gerentes = let
  Origem = source_cad_brokers,
    colunas_renomeadas = Table.RenameColumns(Origem, {{"CODBROKER", "cod_broker"}, {"CPFCNPJ", "cpf_cnpj"}, {"CODAGENCIA", "cod_agencia"}, {"DATAGARANTIDA", "data_garantida"}, {"DATAGP", "data_gp"}, {"CODBROKERNIVEL", "cod_broker_nivel"}, {"CONSULTAPESSOAL", "consulta_pessoal"}, {"CONSULTAEMPRESARIAL", "consulta_empresarial"}, {"CONSULTACLICK", "consulta_click"}, {"CONSULTACREDITBUREAU", "consulta_credit_bureau"}, {"CONSULTARECHEQUE", "consulta_recheque"}, {"USUAALTERACAO", "usua_alteracao"}, {"DATAALTERACAO", "data_alteracao"}, {"USUAINCLUSAO", "usua_inclusao"}, {"DATAINCLUSAO", "data_inclusao"}, {"OBSERVACAO", "observacao"}, {"CONSULTARELATO", "consulta_relato"}, {"COMISSAO", "comissao"}, {"TIPO", "tipo"}, {"GESTOR", "gestor"}, {"STATUS", "status"}, {"CODUSUARIO", "cod_usuario"}}),
  #"Colunas removidas" = Table.RemoveColumns(colunas_renomeadas, {"tipo", "comissao", "consulta_relato", "observacao", "data_alteracao", "usua_alteracao", "consulta_recheque", "consulta_credit_bureau", "consulta_click", "consulta_empresarial", "consulta_pessoal", "cod_broker_nivel", "data_gp", "data_garantida", "data_inclusao", "usua_inclusao", "gestor", "status"})
in
    #"Colunas removidas";
shared staging_plataformas = let
  Origem = source_cad_agencias,
    colunas_selecionadas = Table.SelectColumns(Origem, {"CODAGENCIA", "NOME"}),
    colunas_renomeadas = Table.RenameColumns(colunas_selecionadas, {{"CODAGENCIA", "cod_agencia"}, {"NOME", "nome_plataforma"}}),
  #"Consultas mescladas" = Table.NestedJoin(colunas_renomeadas, {"cod_agencia"}, sup_gestor_de_plataforma, {"cod_agencia"}, "sup_gestor_de_plataforma", JoinKind.LeftOuter),
  #"Expandido sup_gestor_de_plataforma" = Table.ExpandTableColumn(#"Consultas mescladas", "sup_gestor_de_plataforma", {"gestor_da_plataforma"}, {"gestor_da_plataforma"}),
  #"Inserted Text After Delimiter" = Table.AddColumn(#"Expandido sup_gestor_de_plataforma", "plataforma", each Text.AfterDelimiter([nome_plataforma], " "), type text)
in
    #"Inserted Text After Delimiter";
shared sup_gestor_de_plataforma = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "sup_gestor_de_plataforma", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_cad_geral_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_cad_geral", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared staging_operacoes_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_operacoes", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared staging_boletos_titulos_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_boletos_titulos", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared staging_titulos_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_titulos", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared staging_tac_m_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_tac_m", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared staging_enderecos_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_enderecos", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared staging_contratos_clientes_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "staging_contratos_clientes", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

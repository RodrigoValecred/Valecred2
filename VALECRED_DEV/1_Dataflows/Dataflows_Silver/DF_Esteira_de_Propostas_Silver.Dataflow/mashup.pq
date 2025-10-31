[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared source_tab_titulos_confirmacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_titulos_confirmacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared staging_tab_titulos_confirmacao = let
    Origem = source_tab_titulos_confirmacao,
    #"Colunas Selecionadas" = Table.SelectColumns(Origem, {"CODOPERACAO", "CODTITULO", "CONFAO", "CODOCORCONFIRMACAO", "DATAINCLUSAO", "USUAINCLUSAO", "CODTITULOSCONFIRMACAO", "DOCCONFIRMADO", "OBSERVACAO"}),
    #"OBS Processada" = Table.TransformColumns(#"Colunas Selecionadas", {{"OBSERVACAO", each Text.FromBinary(_, 1252), type text}})
in
    #"OBS Processada";

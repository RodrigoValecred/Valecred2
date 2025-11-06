[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared source_tab_titulos_confirmacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "tab_titulos_confirmacao", ItemKind = "Table"]}[Data]
in
    tabela_origem;

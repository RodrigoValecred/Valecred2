section Section1;

// ---------------- SOURCES (from Bronze/Silver Lakehouse) ----------------
shared source_tab_titulos = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="tab_titulos", ItemKind="Table"]}[Data]
in
    Tabela;

// ---------------- STAGING ----------------

shared staging_titulos_limpa = let
    Origem = source_tab_titulos,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {
        {"CODTITULO", "cod_titulo"}, {"NDOC", "n_doc"}, {"TDOC", "t_doc"}, {"CPFCNPJSACADO", "cpf_cnpj_sacado"},
        {"DATAINCLUSAO", "data_inclusao"}, {"VENCIMENTO", "vencimento"}, {"ACEITO", "aceito"},
        {"MOTIVO", "motivo"}, {"LIQUIDACAO", "liquidacao"}, {"CHAVEDANFE", "chave_danfe"}, {"PRACA", "praca"}
    })
in
    #"Colunas renomeadas";

// ---------------- DIMENSION ----------------

shared dim_titulo = let
    origem = staging_titulos_limpa,
    linhas_filtradas = Table.SelectRows(origem, each [data_inclusao] > #datetime(2021, 1, 1, 0, 0, 0)),
    adicionar_sk = Table.AddColumn(linhas_filtradas, "sk_titulo", each Text.From([cod_titulo]), type text),
    colunas_selecionadas = Table.SelectColumns(adicionar_sk, {
        "sk_titulo", "cod_titulo", "n_doc", "t_doc", "cpf_cnpj_sacado", "data_inclusao",
        "vencimento", "aceito", "motivo", "liquidacao", "chave_danfe", "praca"
    })
in
    colunas_selecionadas;


// ---------------- DATA DESTINATION ----------------
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_titulo_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_titulo_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_titulo", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
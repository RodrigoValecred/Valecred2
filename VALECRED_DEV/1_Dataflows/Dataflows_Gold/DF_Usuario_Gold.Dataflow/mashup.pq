section Section1;

// ---------------- FONTES (do Lakehouse Bronze/Silver) ----------------
shared source_cad_usuarios = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_usuarios", ItemKind="Table"]}[Data]
in
    Tabela;

// ---------------- DIMENSION ----------------

shared dim_usuario = let
    origem = source_cad_usuarios,
    linhas_classificadas = Table.Sort(origem, {{"CODUSUARIO", Order.Ascending}, {"DATAALTERACAO", Order.Descending}}),
    adicionar_sk = Table.AddColumn(linhas_classificadas, "sk_usuario", each Text.From([CODUSUARIO]), type text),
    colunas_selecionadas = Table.SelectColumns(adicionar_sk, {"sk_usuario", "CODUSUARIO", "NOME", "APELIDO", "EMAIL"}),
    texto_em_maiuscula_nome = Table.TransformColumns(colunas_selecionadas, {{"NOME", each Text.Upper(_), type nullable text}}),
    texto_em_maiuscula_apelido = Table.TransformColumns(texto_em_maiuscula_nome, {{"APELIDO", each Text.Upper(_), type nullable text}}),
    renomear_colunas = Table.RenameColumns(texto_em_maiuscula_apelido, {{"CODUSUARIO", "cod_usuario"}, {"NOME", "nome_usuario"}, {"APELIDO", "apelido_usuario"}, {"EMAIL", "email_usuario"}})
in
    renomear_colunas;


// ---------------- DATA DESTINATION ----------------
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_usuario_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_usuario_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_usuario", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data]{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data];
[BindToDefaultDestination = true]
shared #"lbfactor tab_titulos" = let
  Origem = MySQL.Database("56.125.147.185", "lbfactor"),
  #"Navegação 1" = Origem{[Schema = "lbfactor", Item = "tab_titulos"]}[Data]
in
  #"Navegação 1";

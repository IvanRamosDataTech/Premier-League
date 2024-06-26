// Season1993-1994
let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/IvanRamosDataTech/Premier-League/master/rawdata/Season1993-1994.csv"), [Delimiter = ",", Columns = 28, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Div", type text}, {"Date", type text}, {"HomeTeam", type text}, {"AwayTeam", type text}, {"FTHG", Int64.Type}, {"FTAG", Int64.Type}, {"FTR", type text}, {"", type text}, {"_1", type text}, {"_2", type text}, {"_3", type text}, {"_4", type text}, {"_5", type text}, {"_6", type text}, {"_7", type text}, {"_8", type text}, {"_9", type text}, {"_10", type text}, {"_11", type text}, {"_12", type text}, {"_13", type text}, {"_14", type text}, {"_15", type text}, {"_16", type text}, {"_17", type text}, {"_18", type text}, {"_19", type text}, {"_20", type text}}),
#"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Div"}),
#"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"FTHG", "FullTimeHomeTeamGoals"}, {"FTAG", "FullTimeAwayTeamGoals"}, {"FTR", "FullTimeResult"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"", "_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9", "_10", "_11", "_12", "_13", "_14", "_15", "_16", "_17", "_18", "_19", "_20"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed columns 1", "Season", each "1993-1994"), {{"Season", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"Season", "Date", "HomeTeam", "AwayTeam", "FullTimeHomeTeamGoals", "FullTimeAwayTeamGoals", "FullTimeResult"}),
  #"Removed bottom rows" = Table.RemoveLastN(#"Reordered columns", 172),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed bottom rows", {{"Date", type date}}, "es-MX")
in
#"Changed column type 1"
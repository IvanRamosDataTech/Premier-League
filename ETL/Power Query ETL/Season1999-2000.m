// Season1999-2000
let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/IvanRamosDataTech/Premier-League/master/rawdata/Season1999-2000.csv"), [Delimiter = ",", Columns = 22, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
#"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Div", type text}, {"Date", type text}, {"HomeTeam", type text}, {"AwayTeam", type text}, {"FTHG", Int64.Type}, {"FTAG", Int64.Type}, {"FTR", type text}, {"HTHG", Int64.Type}, {"HTAG", Int64.Type}, {"HTR", type text}, {"", type text}, {"_1", type text}, {"_2", type text}, {"_3", type text}, {"_4", type text}, {"_5", type text}, {"_6", type text}, {"_7", type text}, {"_8", type text}, {"_9", type text}, {"_10", type text}, {"_11", type text}}),
#"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Div"}),
#"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"FTHG", "FullTimeHomeTeamGoals"}, {"FTAG", "FullTimeAwayTeamGoals"}, {"FTR", "FullTimeResult"}, {"HTHG", "HalfTimeHomeTeamGoals"}, {"HTAG", "HalfTimeAwayTeamGoals"}, {"HTR", "HalfTimeResult"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"", "_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9", "_10", "_11"}),
  #"Removed bottom rows" = Table.RemoveLastN(#"Removed columns 1", 172),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed bottom rows", "Season", each "1999-2000"), {{"Season", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"Season", "Date", "HomeTeam", "AwayTeam", "FullTimeHomeTeamGoals", "FullTimeAwayTeamGoals", "FullTimeResult", "HalfTimeHomeTeamGoals", "HalfTimeAwayTeamGoals", "HalfTimeResult"}),
  #"Changed type to date" = Table.TransformColumnTypes(#"Reordered columns", {{"Date", type date}}, "es-MX")
in
  #"Changed type to date"
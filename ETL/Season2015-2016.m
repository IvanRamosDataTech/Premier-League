// Season2015-2016
let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/IvanRamosDataTech/Premier-League/master/rawdata/Season2015-2016.csv"), [Delimiter = ",", Columns = 65, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Div", type text}, {"Date", type text}, {"HomeTeam", type text}, {"AwayTeam", type text}, {"FTHG", Int64.Type}, {"FTAG", Int64.Type}, {"FTR", type text}, {"HTHG", Int64.Type}, {"HTAG", Int64.Type}, {"HTR", type text}, {"Referee", type text}, {"HS", Int64.Type}, {"AS", Int64.Type}, {"HST", Int64.Type}, {"AST", Int64.Type}, {"HF", Int64.Type}, {"AF", Int64.Type}, {"HC", Int64.Type}, {"AC", Int64.Type}, {"HY", Int64.Type}, {"AY", Int64.Type}, {"HR", Int64.Type}, {"AR", Int64.Type}, {"B365H", type number}, {"B365D", type number}, {"B365A", type number}, {"BWH", type number}, {"BWD", type number}, {"BWA", type number}, {"IWH", type number}, {"IWD", type number}, {"IWA", type number}, {"LBH", type number}, {"LBD", type number}, {"LBA", type number}, {"PSH", type number}, {"PSD", type number}, {"PSA", type number}, {"WHH", type number}, {"WHD", type number}, {"WHA", type number}, {"VCH", type number}, {"VCD", type number}, {"VCA", type number}, {"Bb1X2", Int64.Type}, {"BbMxH", type number}, {"BbAvH", type number}, {"BbMxD", type number}, {"BbAvD", type number}, {"BbMxA", type number}, {"BbAvA", type number}, {"BbOU", Int64.Type}, {"BbMx>2.5", type number}, {"BbAv>2.5", type number}, {"BbMx<2.5", type number}, {"BbAv<2.5", type number}, {"BbAH", Int64.Type}, {"BbAHh", type number}, {"BbMxAHH", type number}, {"BbAvAHH", type number}, {"BbMxAHA", type number}, {"BbAvAHA", type number}, {"PSCH", type number}, {"PSCD", type number}, {"PSCA", type number}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Div"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"FTHG", "FullTimeHomeTeamGoals"}, {"FTAG", "FullTimeAwayTeamGoals"}, {"FTR", "FullTimeResult"}, {"HTHG", "HalfTimeHomeTeamGoals"}, {"HTAG", "HalfTimeAwayTeamGoals"}, {"HTR", "HalfTimeResult"}, {"HS", "HomeTeamShots"}, {"AS", "AwayTeamShots"}, {"HST", "HomeTeamShotsOnTarget"}, {"AST", "AwayTeamShotsOnTarget"}, {"HF", "HomeTeamFouls"}, {"AF", "AwayTeamFouls"}, {"HC", "HomeTeamCorners"}, {"AC", "AwayTeamCorners"}, {"HY", "HomeTeamYellowCards"}, {"AY", "AwayTeamYellowCards"}, {"HR", "HomeTeamRedCards"}, {"AR", "AwayTeamReadCards"}, {"B365H", "B365HomeTeam"}, {"B365D", "B365Draw"}, {"B365A", "B365AwayTeam"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA", "Bb1X2", "BbMxH", "BbAvH", "BbMxD", "BbAvD", "BbMxA", "BbAvA", "BbOU", "BbMx>2.5", "BbAv>2.5", "BbMx<2.5", "BbAv<2.5", "BbAH", "BbAHh", "BbMxAHH", "BbAvAHH", "BbMxAHA", "BbAvAHA", "PSCH", "PSCD", "PSCA"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed columns 1", "Season", each "2015-2016"), {{"Season", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"Season", "Date", "HomeTeam", "AwayTeam", "FullTimeHomeTeamGoals", "FullTimeAwayTeamGoals", "FullTimeResult", "HalfTimeHomeTeamGoals", "HalfTimeAwayTeamGoals", "HalfTimeResult", "Referee", "HomeTeamShots", "AwayTeamShots", "HomeTeamShotsOnTarget", "AwayTeamShotsOnTarget", "HomeTeamFouls", "AwayTeamFouls", "HomeTeamCorners", "AwayTeamCorners", "HomeTeamYellowCards", "AwayTeamYellowCards", "HomeTeamRedCards", "AwayTeamReadCards", "B365HomeTeam", "B365Draw", "B365AwayTeam"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Reordered columns", {"LBH", "LBD", "LBA"}),
  #"Changed type to date" = Table.TransformColumnTypes(#"Removed columns 2", {{"Date", type date}}, "es-MX")
in
  #"Changed type to date"
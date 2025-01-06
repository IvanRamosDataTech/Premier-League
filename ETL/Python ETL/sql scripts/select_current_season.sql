-- Select all matches from current season to verify everything looks good at database.

SELECT "MatchID", "Season", "MatchWeek", "Date", "Time", "HomeTeam", "AwayTeam", "FullTimeHomeTeamGoals", "FullTimeAwayTeamGoals", "FullTimeResult"
	FROM public.match_history WHERE "Season" = '2024-2025'  ORDER BY "Date" DESC;
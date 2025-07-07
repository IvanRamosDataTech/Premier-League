-- Check how matchweek is distributed in the table
SELECT "MatchWeek", COUNT("MatchID"), MIN("Date"), MAX("Date")
FROM public.match_history WHERE "Season" = '2024-2025'
GROUP BY "MatchWeek";

SELECT "MatchID", "MatchWeek", "Date"
FROM public.match_history
WHERE "Season" = '2024-2025';

SELECT "matchid", "round_number", "match_date"
FROM public.fixtures;

SELECT "MatchID", "MatchWeek", "round_number", history."Date", fixtures."match_date"
FROM public.match_history AS history
LEFT JOIN public.fixtures AS fixtures ON history."MatchID" = fixtures."matchid"
WHERE
	history."Season" = '2024-2025'
ORDER BY "MatchWeek";

-- Update MatchWeek with values from fixtures
UPDATE public.match_history AS history
SET "MatchWeek" = fixtures.round_number
FROM public.fixtures AS fixtures
WHERE history."MatchID" = fixtures.matchid
  AND history."Season" = '2024-2025';


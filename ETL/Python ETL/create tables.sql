-- Postgresql comes with dateformat as MDY by default (US style)
-- Specify euro style for date formats to avoid "values out of range". Format changes to ISO, DMY
SET datestyle = "ISO, DMY";
-- You could also specify by running:
-- set datestyle = euro;

-- Table: public.match_history

DROP TABLE IF EXISTS public.match_history;

CREATE TABLE public.match_history
(
    "Season" character(9),
    "MatchWeek" integer NOT NULL DEFAULT 1,
    "Date" date NOT NULL,
    "Time" character varying(20),
    "HomeTeam" character varying NOT NULL,
    "AwayTeam" character varying NOT NULL,
    "FullTimeHomeTeamGoals" smallint NOT NULL,
    "FullTimeAwayTeamGoals" smallint NOT NULL,
    "FullTimeResult" "char" NOT NULL,
    "HalfTimeHomeTeamGoals" smallint,
    "HalfTimeAwayTeamGoals" smallint,
    "HalfTimeResult" "char",
    "Referee" character varying(50),
    "HomeTeamShots" smallint,
    "AwayTeamShots" smallint,
    "HomeTeamShotsOnTarget" smallint,
    "AwayTeamShotsOnTarget" smallint,
    "HomeTeamCorners" smallint,
    "AwayTeamCorners" smallint,
    "HomeTeamFouls" smallint,
    "AwayTeamFouls" smallint,
    "HomeTeamYellowCards" smallint,
    "AwayTeamYellowCards" smallint,
    "HomeTeamRedCards" smallint,
    "AwayTeamRedCards" smallint,
    "B365HomeTeam" real,
    "B365Draw" real,
    "B365AwayTeam" real,
    "B365Over2.5Goals" real,
    "B365Under2.5Goals" real,
    "MarketMaxHomeTeam" real,
    "MarketMaxDraw" real,
    "MarketMaxAwayTeam" real,
    "MarketAvgHomeTeam" real,
    "MarketAvgDraw" real,
    "MarketAvgAwayTeam" real,
    "MarketMaxOver2.5Goals" real,
    "MarketMaxUnder2.5Goals" real,
    "MarketAvgOver2.5Goals" real,
    "MarketAvgUnder2.5Goals" real,
    "HomeTeamPoints" smallint NOT NULL,
    "AwayTeamPoints" smallint NOT NULL
);

ALTER TABLE IF EXISTS public.match_history
    OWNER to admin;

COMMENT ON TABLE public.match_history
    IS 'All matches in Premier League History';
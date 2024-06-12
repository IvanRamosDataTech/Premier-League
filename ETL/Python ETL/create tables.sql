CREATE TABLE public."MatchHistory"
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
    "Referee" character varying(15),
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

ALTER TABLE IF EXISTS public."MatchHistory"
    OWNER to admin;

COMMENT ON TABLE public."MatchHistory"
    IS 'All matches in Premier League History';
import constants
import requests
import pandas as pd
import numpy as np
import psycopg2
import csv
import datetime

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)

# Extract:  Get data from website 

website_frame = pd.read_csv(constants.CSV_SOURCE_URL)

# Transform: 

# Remove unwanted columns
unwanted_cols = ["Div", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA",
                 "P>2.5", "P<2.5","AHh", "B365AHH", "B365AHA", "PAHH", "PAHA", "MaxAHH", "MaxAHA", "AvgAHH", "AvgAHA", "B365CH",
                 "B365CD", "B365CA", "BWCH", "BWCD", "BWCA", "IWCH", "IWCD", "IWCA", "PSCH", "PSCD", "PSCA", "WHCH", "WHCD", "WHCA",
                 "VCCH", "VCCD", "VCCA", "MaxCH", "MaxCD", "MaxCA", "AvgCH", "AvgCD", "AvgCA", "B365C>2.5", "B365C<2.5", "PC>2.5",
                 "PC<2.5", "MaxC>2.5", "MaxC<2.5", "AvgC>2.5", "AvgC<2.5", "AHCh", "B365CAHH", "B365CAHA", "PCAHH", "PCAHA", "MaxCAHH",
                 "MaxCAHA", "AvgCAHH", "AvgCAHA"]

website_frame.drop(columns = unwanted_cols, inplace = True)

# Rename columns
website_frame.rename(columns = {"FTHG": "FullTimeHomeTeamGoals",
                               "FTAG": "FullTimeAwayTeamGoals",
                               "FTR": "FullTimeResult",
                               "HTHG": "HalfTimeHomeTeamGoals",
                               "HTAG": "HalfTimeAwayTeamGoals",
                               "HTR": "HalfTimeResult",
                               "HS": "HomeTeamShots",
                               "AS": "AwayTeamShots",
                               "HST": "HomeTeamShotsOnTarget",
                               "AST": "AwayTeamShotsOnTarget",
                               "HF": "HomeTeamFouls",
                               "AF": "AwayTeamFouls",
                               "HC": "HomeTeamCorners",
                               "AC": "AwayTeamCorners",
                               "HY": "HomeTeamYellowCards",
                               "AY": "AwayTeamYellowCards",
                               "HR": "HomeTeamRedCards",
                               "AR": "AwayTeamRedCards",
                               "B365H": "B365HomeTeam",
                               "B365D": "B365Draw",
                               "B365A": "B365AwayTeam",
                               "MaxH": "MarketMaxHomeTeam",
                               "MaxD": "MarketMaxDraw",
                               "MaxA": "MarketMaxAwayTeam",
                               "AvgH": "MarketAvgHomeTeam",
                               "AvgD": "MarketAvgDraw",
                               "AvgA": "MarketAvgAwayTeam",
                               "B365>2.5": "B365Over2.5Goals",
                               "B365<2.5": "B365Under2.5Goals",
                               "Max>2.5": "MarketMaxOver2.5Goals",
                               "Max<2.5": "MarketMaxUnder2.5Goals",
                               "Avg>2.5": "MarketAvgOver2.5Goals",
                               "Avg<2.5": "MarketAvgUnder2.5Goals"},
                   inplace = True)

# Add MatchID column
website_frame.insert(0, "MatchID", constants.CURRENT_SEASON_TAG + "_" + website_frame["HomeTeam"] + "_" + website_frame["AwayTeam"])

# Add season column
website_frame.insert(1, "Season", constants.CURRENT_SEASON_TAG)

# Add MatchWeek value
website_frame.insert(2, "MatchWeek", constants.DEFAULT_MATCHWEEK)

# Add Points columns
conditions = [
     website_frame["FullTimeResult"] == 'H',
     website_frame["FullTimeResult"] == 'D',
     website_frame["FullTimeResult"] == 'A'
]

home_points = [ 3, 1, 0]
away_points = [ 0, 1, 3]

website_frame["HomeTeamPoints"] = np.select(conditions, home_points)
website_frame["AwayTeamPoints"] = np.select(conditions, away_points)

# Stablish a connection to Database data source and fetch all matches stored from current season
try:
    connection = psycopg2.connect(
        host = constants.DB_SERVER,
        port = constants.DB_PORT,
        user = constants.DB_USER,
        password = constants.DB_PASSWORD,
        database = constants.DB_NAME
    )
except psycopg2.Error as e:
    print (f'Can not connect to the postgress database "{constants.DB_NAME}". Make sure database server is running')
    print (e)
else:
    print (f'Connection to database "{constants.DB_NAME}" stablished. Listening at port {constants.DB_PORT}')

season_query = f"SELECT * FROM public.match_history WHERE \"Season\" = '{constants.CURRENT_SEASON_TAG}'"

cursor = connection.cursor()
cursor.execute(season_query)
matches_in_db = cursor.fetchall()

# Copy cursor into a dataframe
postgres_frame = pd.DataFrame(data = matches_in_db, columns = website_frame.columns)

# !! Getting error --> ValueError: Can only compare identically-labeled (both index and columns) DataFrame objects
# This does not work as our dataframes have identical lables, but different indexes as website_frame is always likely to have
# more entries that what we have persisted in DataBase
new_entries = website_frame.compare(postgres_frame)

# Load

# Pending step... Ideally we would insert new rows found in website_frame into postgresql 

website_frame



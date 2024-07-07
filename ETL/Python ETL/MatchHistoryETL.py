#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# In case iPython does not find our personalized modules and we want to import them manually
# import sys
# sys.path.append('my/path/to/module/folder')
# import module_of_interest

# We can also make sure what's the main directory iPhython consider for running
# import os
# os.getcwd()

import constants
import sqlquery
import pandas as pd
import numpy as np
import psycopg2
import csv
import os
from dotenv import load_dotenv
from pathlib import Path

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)

config_path = Path('configs/.env')
load_dotenv(dotenv_path=config_path)


# In[ ]:


APP_ENVIRONMENT = os.getenv('APP_ENVIRONMENT')
APP_VERSION = os.getenv('APP_VERSION')
print(f"Running Premier League ETL process Environment: {APP_ENVIRONMENT} version: {APP_VERSION}")


# In[ ]:


# -- EXTRACTION

website_frame = pd.read_csv(constants.CSV_SOURCE_URL)


# In[ ]:


# -- TRANSFORMATION

# Remove unwanted columns

unwanted_cols = ["Div", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA",
                 "P>2.5", "P<2.5","AHh", "B365AHH", "B365AHA", "PAHH", "PAHA", "MaxAHH", "MaxAHA", "AvgAHH", "AvgAHA", "B365CH",
                 "B365CD", "B365CA", "BWCH", "BWCD", "BWCA", "IWCH", "IWCD", "IWCA", "PSCH", "PSCD", "PSCA", "WHCH", "WHCD", "WHCA",
                 "VCCH", "VCCD", "VCCA", "MaxCH", "MaxCD", "MaxCA", "AvgCH", "AvgCD", "AvgCA", "B365C>2.5", "B365C<2.5", "PC>2.5",
                 "PC<2.5", "MaxC>2.5", "MaxC<2.5", "AvgC>2.5", "AvgC<2.5", "AHCh", "B365CAHH", "B365CAHA", "PCAHH", "PCAHA", "MaxCAHH",
                 "MaxCAHA", "AvgCAHH", "AvgCAHA"]

website_frame.drop(columns = unwanted_cols, inplace = True)


# In[ ]:


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


# In[ ]:


# Add MatchID column

website_frame.insert(0, "MatchID", constants.CURRENT_SEASON_TAG + "_" + website_frame["HomeTeam"] + "_" + website_frame["AwayTeam"])


# In[ ]:


# Add season column

website_frame.insert(1, "Season", constants.CURRENT_SEASON_TAG)


# In[ ]:


# Stablish a connection to Database data source and fetch last game so we can know current matchweek
DB_SERVER = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

try:
    connection = psycopg2.connect(
        host = DB_SERVER,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_NAME
    )
except psycopg2.Error as e:
    print (f'Can not connect to the postgress database "{DB_NAME}". Make sure database server is running')
    print (e)
else:
    print (f'Connection to database "{DB_NAME}" stablished. Listening at port {DB_PORT}')


# In[ ]:


# Find out current season matchweek
next_matchweek = sqlquery.calculate_matchweek(connection)


# In[ ]:


# Add MatchWeek column

website_frame.insert(2, "MatchWeek", next_matchweek)


# In[ ]:


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


# In[ ]:


# -- LOAD

# Keep up to date postgresql database

insert_statement = sqlquery.upsert_query(to_table="public.match_history", reference_column="MatchID",dataframe=website_frame)


# In[ ]:


try:
    with connection.cursor() as cursor:
        cursor.execute(insert_statement)
        rowsAffected = cursor.rowcount
        connection.commit()
except psycopg2.Error as e:
    print (f'Can not connect to the postgress database "{constants.DB_NAME}". Make sure database server is running')
    print (e)
else:
    print (f"New Rows in Match History: {rowsAffected}")
finally:
    cursor.close()


# In[ ]:


# Keep up to date master dataset
MASTER_CSV_FILE = os.getenv('MASTER_CSV_URL')

try:
    rowcount, columns, rows = sqlquery.select_all_matches(connection)
    with(open(MASTER_CSV_FILE, mode='w',newline="") as file):
        writer = csv.writer(file)
        # write column names
        writer.writerow(columns)
        writer.writerows(rows)
except psycopg2.Error as e:
    print (f'Can not connect to the postgress database "{constants.DB_NAME}". Make sure database server is running')
    print (e)
else:
    print (f"Rows in Master CSV Dataset: {rowcount}")
finally:
    cursor.close()
            


# In[ ]:


# Keep up to date Kaggle dataset


# In[ ]:


connection.close()


# In[ ]:





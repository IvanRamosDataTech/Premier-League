#!/usr/bin/env python
# coding: utf-8

# In[1]:


# In case iPython does not find our personalized modules and we want to import them manually
# import sys
# sys.path.append('my/path/to/module/folder')
# import module_of_interest

# We can also make sure what's the main directory iPhython consider for running
# import os
# os.getcwd()

import constants
import requests
import pandas as pd
import numpy as np
import psycopg2
import csv
import datetime
from psycopg2 import sql

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)


# In[2]:


def calculate_matchweek(conn):
    """
        Finds out last registered matchweek for current season in Database. 
        Returns last registerered matchweek plus one. If there's any problem 
        with databse or season has not even started, then returns default matchweek "1"
    """
    try:
        cursor = conn.cursor()
        matchweek_query = f"SELECT MAX(\"MatchWeek\") FROM public.match_history WHERE \"Season\" = '{constants.CURRENT_SEASON_TAG}'"
        cursor.execute(matchweek_query)
        matchweek_result = cursor.fetchone()
        if matchweek_result[0] is None:
            return constants.DEFAULT_MATCHWEEK
        else:
            # Next matchweek
            return matchweek_result[0] + 1
    except psycopg2.Error as e:
        print ("An error ocurred in database")
        print (e)
        return constants.DEFAULT_MATCHWEEK
    finally:
        cursor.close()


# In[3]:


def upsert_query(to_table, reference_column, dataframe):
    """
        Generates a Composed SQL object that contains all necessary SQL code to 
        perform upserts into a postgresql table from dataframe as source.
        If row attempted to insert is already present in database, then engine
        will skip insertion. 

        Input:
            to_table - target table name in format 'schema.table_name' to generate inserts 
            reference_column - Column name of your table that is considered PK to avoid overwritings
            dataframe - pandas dataframe containing all rows to be inserted
    """
    date_config = sql.SQL('SET datestyle = "ISO, DMY"; ')
    
    insert_header = sql.SQL("INSERT INTO " + to_table + " ({fields})").format(
        fields = sql.SQL(',').join([sql.Identifier(column) for column in dataframe.columns])
    )
    
    values_array = []
    for row in dataframe.iterrows():
        values_array.append(sql.SQL(" ({values})").format(
            values = sql.SQL(',').join([sql.Literal(value) for value in row[1].values])
        ))
    
    insert_values = sql.SQL(' VALUES ') + sql.SQL(',').join(values_array)
    
    insert_condition = sql.SQL(" ON CONFLICT({conflict_col}) DO NOTHING").format(
        conflict_col = sql.Identifier(reference_column)
    )
    
    insert_statement = date_config + insert_header + insert_values + insert_condition
   
    return insert_statement


# In[4]:


# -- EXTRACTION

website_frame = pd.read_csv(constants.CSV_SOURCE_URL)


# In[6]:


# -- TRANSFORMATION

# Remove unwanted columns

unwanted_cols = ["Div", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA",
                 "P>2.5", "P<2.5","AHh", "B365AHH", "B365AHA", "PAHH", "PAHA", "MaxAHH", "MaxAHA", "AvgAHH", "AvgAHA", "B365CH",
                 "B365CD", "B365CA", "BWCH", "BWCD", "BWCA", "IWCH", "IWCD", "IWCA", "PSCH", "PSCD", "PSCA", "WHCH", "WHCD", "WHCA",
                 "VCCH", "VCCD", "VCCA", "MaxCH", "MaxCD", "MaxCA", "AvgCH", "AvgCD", "AvgCA", "B365C>2.5", "B365C<2.5", "PC>2.5",
                 "PC<2.5", "MaxC>2.5", "MaxC<2.5", "AvgC>2.5", "AvgC<2.5", "AHCh", "B365CAHH", "B365CAHA", "PCAHH", "PCAHA", "MaxCAHH",
                 "MaxCAHA", "AvgCAHH", "AvgCAHA"]

website_frame.drop(columns = unwanted_cols, inplace = True)


# In[7]:


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


# In[8]:


# Add MatchID column

website_frame.insert(0, "MatchID", constants.CURRENT_SEASON_TAG + "_" + website_frame["HomeTeam"] + "_" + website_frame["AwayTeam"])


# In[9]:


# Add season column

website_frame.insert(1, "Season", constants.CURRENT_SEASON_TAG)


# In[10]:


# Stablish a connection to Database data source and fetch last game so we can know current matchweek

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


# In[11]:


# Find out current season matchweek
next_matchweek = calculate_matchweek(connection)


# In[12]:


# Add MatchWeek column

website_frame.insert(2, "MatchWeek", next_matchweek)


# In[13]:


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

insert_statement = upsert_query("public.match_history", "MatchID", website_frame)


# In[18]:


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


# In[ ]:


# Keep up to date Kaggle dataset


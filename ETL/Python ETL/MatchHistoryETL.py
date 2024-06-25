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

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)


# In[2]:


website_frame = pd.read_csv(constants.CSV_SOURCE_URL)


# In[3]:


website_frame.shape


# In[4]:


# Remove unwanted columns

unwanted_cols = ["Div", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA",
                 "P>2.5", "P<2.5","AHh", "B365AHH", "B365AHA", "PAHH", "PAHA", "MaxAHH", "MaxAHA", "AvgAHH", "AvgAHA", "B365CH",
                 "B365CD", "B365CA", "BWCH", "BWCD", "BWCA", "IWCH", "IWCD", "IWCA", "PSCH", "PSCD", "PSCA", "WHCH", "WHCD", "WHCA",
                 "VCCH", "VCCD", "VCCA", "MaxCH", "MaxCD", "MaxCA", "AvgCH", "AvgCD", "AvgCA", "B365C>2.5", "B365C<2.5", "PC>2.5",
                 "PC<2.5", "MaxC>2.5", "MaxC<2.5", "AvgC>2.5", "AvgC<2.5", "AHCh", "B365CAHH", "B365CAHA", "PCAHH", "PCAHA", "MaxCAHH",
                 "MaxCAHA", "AvgCAHH", "AvgCAHA"]

website_frame.drop(columns = unwanted_cols, inplace = True)


# In[5]:


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


# In[6]:


# Add MatchID column

website_frame.insert(0, "MatchID", constants.CURRENT_SEASON_TAG + "_" + website_frame["HomeTeam"] + "_" + website_frame["AwayTeam"])


# In[7]:


# Add season column

website_frame.insert(1, "Season", constants.CURRENT_SEASON_TAG)


# In[8]:


def calculate_matchweek(cursor):
    """
        Finds out last registered matchweek for current season in Database. 
        Returns last registerered matchweek plus one. If there's any problem 
        with databse or season has not even started, then returns default matchweek "1"
    """
    try:
        matchweek_query = f"SELECT MAX(\"MatchWeek\") FROM public.match_history WHERE \"Season\" = '{constants.CURRENT_SEASON_TAG}'"
        cursor.execute(matchweek_query)
        matchweek_result = cursor.fetchone()
        if matchweek_result[0] is None:
            return constants.DEFAULT_MATCHWEEK
        else:
            return matchweek_result[0] + 1
    except psycopg2.Error as e:
        print ("An error ocurred in database")
        print (e)
        return constants.DEFAULT_MATCHWEEK


# In[17]:


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


# In[18]:


# Find out current season matchweek
cursor = connection.cursor()
next_matchweek = calculate_matchweek(cursor)


# In[ ]:


# Add MatchWeek column

website_frame.insert(2, "MatchWeek", next_matchweek)


# In[12]:


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


# In[13]:


website_frame.head()


# In[14]:


def generate_upsert_stmn(table_name, conflict_column, dataframe):
    """
        Generates a sql statement for Postgresql SQL dialect equivalent to UPSERT operation.
        This is achieved by writing standard INSERT clause along with ON CONFLICT
        Resulting statement adopts posture of skipping insertions on rows that conflict with
        conflict_column field

        dataframe - source dataframe which contains data to be inserted
        conflict_column - collumn considered to avoid any duplications
        table_name - Schema Table name to generate insert values
    """
    def entitle_value(value):
        if type(value) is str:
            return "'" + value + "'"
        # Return stringified version of anything else (Numbers, Dates, Floats)
        return str(value)

    def generate_insert_stmn(table_name, dataframe):
        entitled_headers = [ '"' + column_name + '"' for column_name in  dataframe.columns ]
        return "INSERT INTO " + table_name + " (" + ", ".join(entitled_headers) + ") "
    
    def generate_values_stmn(row):
        values = row[1].values
        values_stmt = " ("
        value_idx = 1
        for value in values:
            values_stmt += entitle_value(value)
            value_idx += 1
            # Skip last comma in statement
            if value_idx <= len(values):
                values_stmt +=  ", "
    
        values_stmt +=")"
        return values_stmt

        
    sql_statement = ""
    sql_statement += generate_insert_stmn(table_name, dataframe)
    sql_statement += "VALUES "
    value_idx = 1
    for row in dataframe.iterrows():
        sql_statement += generate_values_stmn(row)
        value_idx += 1
        # Skip comma on last row
        if value_idx <= dataframe.shape[0]:
            sql_statement += ", "
        
    sql_statement +=" ON CONFLICT (\"" + conflict_column + "\")  DO NOTHING"
    return sql_statement


# In[ ]:


insert_statement = generate_upsert_stmn("public.match_history", "MatchID", website_frame)
print(insert_statement)


# In[20]:


cursor.execute(insert_statement)


# In[ ]:





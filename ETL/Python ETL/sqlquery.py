"""
    This module defines pre configured queries for project
"""
import psycopg2
import constants
from psycopg2 import sql

date_config = sql.SQL('SET datestyle = {date_style}; ').format(
    date_style = sql.Identifier(constants.DB_DATE_STYLE)
)

def calculate_matchweek(connection):
    """
        Finds out last registered matchweek for current season in Database. 
        
        Input:
            connection - An active connection to postgresql database
        Returns:
            Last registerered matchweek plus one. If there's any problem 
            with databse or season has not even started, then returns default matchweek "1"
    """
    try:
        cursor = connection.cursor()
        
        select = sql.SQL("SELECT MAX({matchweek}) FROM public.match_history WHERE {season} = {current_season}").format(
            matchweek = sql.Identifier("MatchWeek"),
            season = sql.Identifier("Season"),
            current_season = sql.Literal(constants.CURRENT_SEASON_TAG)
        )
        
        select_statement = date_config + select

        cursor.execute(select_statement)
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


def select_all_matches(connection):
    """
        Selects all matches from table public.match_history.
        
        Input:
            connection - An active connection to postgresql database
        Returns:
            rows_affected - Number of rows fetched from operation
            columns - Columns used to fetch results
            rows - A list of tuples. Each tuple represents a row entry
    """
    try:
        with connection.cursor() as cursor:
            select_statement = sql.SQL("SELECT * FROM public.match_history")
            final_statement = date_config + select_statement
            cursor.execute(final_statement)
            rows_affected = cursor.rowcount
            columns = [column_description[0] for column_description in cursor.description]
            rows = cursor.fetchall()

        return rows_affected, columns, rows
    except psycopg2.Error as e:
        print (f'Can not connect to the postgress database "{constants.DB_NAME}". Make sure database server is running')
        print (e)
    finally:
        cursor.close()


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
        Returns:
            A composable sql.SQL object with generated insert statement
    """
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
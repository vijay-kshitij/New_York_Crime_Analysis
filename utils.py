# -*- coding: utf-8 -*-
"""
Created on Monday Dec 05 10:02:33 2022
"""

"""
Script containing helper functions to insert data from clean_data.csv to a user-defined SQLite3 DB.

"""



# Importing libraries
import os
import multiprocessing as mp
from functools import partial
import pandas as pd
import time
import sqlite3
from sqlite3 import Error
from loguru import logger


FILEPATH = r"C:\Users\sanat\Desktop\Zia Project\clean_data.csv"
NUM_CORES = 7                    # Multiprocessing theory dictates that all the cpu cores should not be used
DB_NAME = "hatecrime_non_normalized.db"


def create_sqlite3_connection(db_name,overwrite_db=False):
    if overwrite_db and os.path.exists(db_name):
        os.remove(db_name)
    try:
        conn = sqlite3.connect(db_name)
        conn.cursor().execute("PRAGMA foreign_keys=1;")

    except Error as e:
        logger.error(str(e))

    return conn

def update_database_table(conn,create_query,delete_table=None,alter_table=False,alter_query=None):
    """Helper function to create a table or alter an existing table in a database

    Args:
        conn (SQLite3 connection object): Connection object for the database in which the table is to be created or altered.
        create_query (str): A valid SQLite3 query to create the table in the database.
        delete_table (str, optional): Name of table to be deleted. Defaults to None.
        alter_table (bool, optional): Set to True if alter query is to be executed instead of create query. Defaults to False.
        alter_query (str, optional): A valid SQLite3 query to alter the table in the database. Defaults to None.
    """

    exec_query = create_query
    if delete_table:
        with conn:
            try:
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS {delete_table};")
            except Error as e:
                logger.error(str(e))

    if alter_table:
        if not alter_query:
            raise Exception("Alter query not supplied for Alter mode.")
        else:
            exec_query = alter_query                  # On Alter mode, alter_query will get executed even if a seperate create_query is supplied
    with conn:
        try:
            cursor_obj = conn.cursor()
            cursor_obj.execute(exec_query)

        except Exception as e:
            logger.error(str(e))


def create_file_chunks(filename,chunksize):
    """
    Helper function to create chunks of .csv files from a user-defined csv file. The original file is chunked on the number fo rows.
    Args:
        filename (str): Filename of the original .csv file which is to be divided into chunks.
        chunksize (int): Number of rows in each chunk.

    Returns:
        filenames (list): List of filepaths corresponding to each file chunk.
    """

    filenames = []

    for i,chunk in enumerate(pd.read_csv(filename,chunksize=chunksize)):
        filename = f'chunk{i}.csv'
        chunk.to_csv(filename, index=False)
        logger.debug(f"Finished writing chunk at {filename}")
        filenames.append(filename)
    return filenames


def create_table_in_db(db_name,create_query,delete_table=None):
    """Function to create a table from a create query in a user-defined database.

    Args:
        db_name (str): The .db file in which the table is to be created.
        create_query (str): A valid SQLite3 query to create the table in the database.
        delete_table (str, optional): Name of table to be deleted. Defaults to None.
    """

    conn = create_sqlite3_connection(db_name,overwrite_db=False)
    logger.debug("Creating Table...")
    try:
        update_database_table(conn,create_query,delete_table)
        logger.debug(f"Table created successfully in {db_name}")
    except Exception as e:
        logger.error(str(e))
    return

def bulk_insert_into_table(conn_obj,table_name,cols,values):
    """Function to insert values into a predefined table using executemany

    Args:
        conn_obj: The connection object corresponding to a db
        table_name: Name of the table in which values are to be inserted
        cols: The columns of the table corresponding to the supplied values
        values (list): List of tuples of values which are to be inserted
    """
    
    cursor = conn_obj.cursor()
    col_str = ""
    for col in cols:
        col_str+=col +", "
    vals_str = '?,'*len(cols)
    vals_str = str(vals_str)[:-1]
    col_str = col_str.strip()[:-1] # Strips the ", " from the end of the column string once all the columns have been appended
    sql = f"INSERT INTO {table_name}({col_str}) VALUES (" +vals_str +");"


    try:
        cursor.executemany(sql,values)
        conn_obj.commit()
    except Exception as e:
        logger.error(str(e))
    return

def from_csv_to_db(csv_filename,db_name,table,unique=False,cols_subset=[],col_table_alias=dict()):
    """Helper function to move data from a specified .csv file to a user-specified table in a user-specified db.

    Args:
        csv_filename (str): The path of the .csv file containing the data.
        db_name (str): The name of the db containing the table to which data is to be moved.
        table (str): The name of the table in the db to which data is to be moved.
        cols_subset (list): The name of the columns of the table corresponding to the columns in the .csv which are to be moved. Defaults to empty list (Denotes all columns are to be inserted into).
    """
    clean_df = pd.read_csv(csv_filename)
    headers = list(clean_df.columns)
    values_to_insert = []
    for _,line in clean_df.iterrows():
        if not cols_subset:
            vals = [elem for elem in line]

        else:
            cols_subset = [col for col in headers if col.lower() in cols_subset]
            vals = [line[col] for col in cols_subset]
        if unique:
            if tuple(vals) not in values_to_insert:
                values_to_insert.append(tuple(vals))
            else:
                continue
        else:
            values_to_insert.append(tuple(vals))

    try:
        conn_obj = create_sqlite3_connection(db_name)

        if col_table_alias:
            cols = [col_table_alias[col] for col in col_table_alias.keys()]                        
            bulk_insert_into_table(conn_obj,table,cols,values_to_insert)

        
        logger.debug(f"Values inserted into {table} in {db_name}.")

    except Exception as e:
        logger.error(str(e))

    return

def multiprocessing_batch_insert(csv_filename,db_name,table,num_cores,unique,cols_subset,col_table_alias):
    """Helper function to move data from a specified .csv file to a user-specified table in a user-specified db using multiprocessing.

    Args:
        csv_filename (str): Path of .csv file containing the data to move.
        db_name (str): The name of the db containing the table to which data is to be moved.
        table (str): The name of the table in the db to which data is to be moved.
        table (str): The name of the table in the db to which data is to be moved.
        cols_subset (list): The name of the columns of the table corresponding to the columns in the .csv which are to be moved. Defaults to empty list (Denotes all columns are to be inserted into).
    """
    csv_filenames = create_file_chunks(csv_filename,path="/chunks",chunksize=1000)
    start = time.perf_counter()
    logger.debug(f"Starting Multiprocessing Pool with {num_cores} cores.")
    with mp.Pool(num_cores) as pool:
        partial_func = partial(from_csv_to_db,db_name=db_name,table=table,unique=unique,cols_subset=cols_subset,col_table_alias=col_table_alias)
        pool.map(partial_func,csv_filenames)
    finish = time.perf_counter()
    logger.debug(f"Time elapsed in process {round(finish-start,2)} seconds.")

    return
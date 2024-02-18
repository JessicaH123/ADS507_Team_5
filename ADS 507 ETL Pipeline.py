import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import pymysql as mysql

# Constant values
# Also config values to access the mysql database
# TODO figure out a secure way to store user credentials (this is a nice to have, but not necessary for the course)
HOST_NAME = "<HOST NAME>"
PORT_NUMBER = "<PORT NUMBER>"
USERNAME = "<USERNAME>"
PASSWORD = "<PASSWORD>"
DATABASE = "<DATABASE>"

# Transform the Data
# TODO 
# This section will need to be performed in SQL, not python
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if the dataframe is empty
    if df.empty:
        print("No information in the dataframe")
        return False

    # Primary Key Check
    if pd.Series(df["<PRIMARY KEY>"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check has been violated")

    # Check for null values
    if df.isnull().values.any():
        raise Exception("A null value was found")

    return True


# Extract the Data
if __name__ == "__main__":

    # Convert the csv data to a structured dataframe

    # TODO
    # Similaryly to the above code, we need to pull data from csv files
    # This will be much easier than pulling data from an API
    # pd.read_csv() -> parse for desired data/timeframe -> clean data (this is the transform step)

    # TODO
    # Create a connection as we have done in the course so far
    # conn = mysql.connect(host="usd-ads-507.mysql.database.azure.com",port=int(3306),user="student",passwd="ads507password",db='tpch')

    # Create tables with DDL
    #TODO FOR LOGAN modify the below table for our database
    sql = """
    CREATE TABLE IF NOT EXISTS <TABLE NAME>(
        <COLUMN> VARCHAR(200),
        <COLUMN> VARCHAR(200),
        <COLUMN> VARCHAR(200),
        <COLUMN> VARCHAR(200),
        <COLUMN> primary_key_constraint PRIMARY KEY (<COLUMN>)
    )
    """

    # Validate the Data (transform the data). Since this function needs to do reworked using MySQL, we should call this function after creating the MySQL tables
    if check_if_valid_data(df):
        print("Data valid, proceed to creating the database")

    # Execute the sql query and create the table above
    cursor.execute(sql)
    print("Successfully created the database")


    try:
        # TABLE NAME should be replaced with the name of the table we are trying to create from the DDL above. ( CREATE TABLE IF NOT EXISTS 
        # <TABLE NAME>(
        # <COLUMN> VARCHAR(200),
        # <COLUMN> VARCHAR(200),
        # <COLUMN> VARCHAR(200),
        # <COLUMN> VARCHAR(200),
        # <COLUMN> primary_key_constraint PRIMARY KEY (<COLUMN>))
        
        df.to_sql("<TABLE NAME>", con=engine,
                       index=False, if_exists="append")
        print("Data loaded into database")
    except:
        print("Data already exists in the database")

    # Close the database connection
    conn.close()
    print("Connection closed")

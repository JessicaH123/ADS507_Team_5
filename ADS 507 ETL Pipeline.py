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
# TODO figure out a secure way to store user credentials
HOST_NAME = "<Host NAME>"
PORT_NUMBER = "<PORT NUMBER>"
USERNAME = "<USERNAME>"
PASSWORD = "<PASSWORD>"
DATABASE = "<DATABASE>"

# Transform the Data
#TODO This section will need to be performed in SQL, not python
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

    #TODO
    # Similaryly to the above code, we need to pull data from csv files
    # This will be much easier than pulling data from an API
    # pd.read_csv() -> parse for desired data/timeframe -> clean data (this is the transform step)

# Validate the Data
    if check_if_valid_data(df):
        print("Data valid, proceed to load stage")

    # Load the Data
    # maybe use sqlalchemy to store data
    engine = sqlalchemy.create_engine("sqlite:///<DATASET>.sqlite")
    conn = sqlite3.connect('<FILENAME>.sqlite')
    cursor = conn.cursor()

    # Create a connection as we have done in the course so far
    #conn = mysql.connect(host="usd-ads-507.mysql.database.azure.com",port=int(3306),user="student",passwd="ads507password",db='tpch')

    # Create tables with DDL
    #TODO modify the below table for our database
    sql = """
    CREATE TABLE IF NOT EXISTS <TABLE NAME>(
        <COLUMN> VARCHAR(200),
        <COLUMN> VARCHAR(200),
        <COLUMN> VARCHAR(200),
        <COLUMN> VARCHAR(200),
        <COLUMN> primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    # Execute the sql query and create the table above
    cursor.execute(sql)
    print("Successfully created the database")


    try:
        df.to_sql("<TABLE NAME>", con=engine,
                       index=False, if_exists="append")
        print("Data loaded into database")
    except:
        print("Data already exists in the database")

    # Close the database connection
    conn.close()
    print("Connection closed")

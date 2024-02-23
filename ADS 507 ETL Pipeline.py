import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, date
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
    collision_dat = pd.read_csv('C:/Users/jessh/Documents/MS Applied Data Science/ADS507/Project/nyc_collsions/database.csv', parse_dates =['DATE'])
    uber_dat = pd.read_csv('C:/Users/jessh/Documents/MS Applied Data Science/ADS507/Project/uber/Uber_Trips_NYC_2016.csv', parse_dates =['Pickup Start Date', 'Pickup End Date'])
    weather_dat = pd.read_csv('C:/Users/jessh/Documents/MS Applied Data Science/ADS507/Project/nyc weather/NYC_Central_Park_weather_1869-2022.csv', parse_dates =['DATE'])

    # changing the spaces in the names to underscores for use in sql statements
    collision_dat.columns = collision_dat.columns.str.replace(' ', '_')
    uber_dat.columns = uber_dat.columns.str.replace(' ', '_')
    weather_dat.columns = weather_dat.columns.str.replace(' ', '_')

    #filtering out only the 2016 year
    collision_df= collision_dat[(collision_dat['DATE'].dt.year == 2016)].reset_index(drop = True)
    uber_df = uber_dat[(uber_dat['Pickup_Start_Date'].dt.year == 2016) & (uber_dat['Pickup_Start_Date'].dt.year == 2016)].reset_index(drop = True)
    weather_df= weather_dat[(weather_dat['DATE'].dt.year == 2016)].reset_index(drop = True)

    # TODO
    # Create a connection as we have done in the course so far
    # conn = mysql.connect(host="usd-ads-507.mysql.database.azure.com",port=int(3306),user="student",passwd="ads507password",db='tpch')

    # Create tables with DDL
    #TODO FOR LOGAN modify the below table for our database
    sql = """
    CREATE TABLE IF NOT EXISTS <Collisions>(
        `uniq_key` INT NULL DEFAULT NULL,
        `date` DATE NULL DEFAULT NULL,
        `time` TEXT NULL DEFAULT NULL,
        `borough` VARCHAR(45) NULL DEFAULT NULL,
        `zip_code` VARCHAR(5) NULL DEFAULT NULL,
        `lat` TEXT NULL DEFAULT NULL,
        `long` TEXT NULL DEFAULT NULL,
        `location` VARCHAR(45) NULL DEFAULT NULL,
        `on_street_name` VARCHAR(60) NULL DEFAULT NULL,
        `cross_street_name` VARCHAR(60) NULL DEFAULT NULL,
        `off_street_name` VARCHAR(60) NULL DEFAULT NULL,
        `persons_injured` INT NULL DEFAULT NULL,
        `persons_killed` INT NULL DEFAULT NULL,
        `peds_injured` INT NULL DEFAULT NULL,
        `peds_killed` INT NULL DEFAULT NULL,
        `cyclists_injured` INT NULL DEFAULT NULL,
        `cyclists_killed` INT NULL DEFAULT NULL,
        `motorists_injured` INT NULL DEFAULT NULL,
        `motorists_killed` INT NULL DEFAULT NULL,
        `vehicle_1_type` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_2_type` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_3_type` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_4_type` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_5_type` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_1_factor` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_2_factor` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_3_factor` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_4_factor` VARCHAR(60) NULL DEFAULT NULL,
        `vehicle_5_factor` VARCHAR(60) NULL DEFAULT NULL)

    CREATE TABLE IF NOT EXISTS <Uber>(
        `base_license_num` VARCHAR(6) NULL DEFAULT NULL,
        `wave_num` INT NULL DEFAULT NULL,
        `base_name` VARCHAR(45) NULL DEFAULT NULL,
        `dba` VARCHAR(45) NULL DEFAULT NULL,
        `years` INT NULL DEFAULT NULL,
        `week_num` INT NULL DEFAULT NULL,
        `pickup_start_date` DATE NULL DEFAULT NULL,
        `pickup_end_date` DATE NULL DEFAULT NULL,
        `total_dispatched_trips` INT NULL DEFAULT NULL,
        `unq_dispatched_vehicle` INT NULL DEFAULT NULL)

    CREATE TABLE IF NOT EXISTS <Weather>
        `date` DATE NULL DEFAULT NULL,
        `precip` DOUBLE NULL DEFAULT NULL,
        `snow_fall` DOUBLE NULL DEFAULT NULL,
        `snow_depth` DOUBLE NULL DEFAULT NULL,
        `tmin` INT NULL DEFAULT NULL,
        `tmax` INT NULL DEFAULT NULL,
    UNIQUE INDEX `DATE_UNIQUE` (`date` ASC))
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

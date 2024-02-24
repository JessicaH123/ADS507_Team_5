import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, date
import datetime
import sqlite3
import pymysql as mysql

# Constant values/database information
PORT = 3306
HOST_NAME = "127.0.0.1"
USERNAME = "root"
PASSWORD = "password"
DATABASE = "ads507"

# Validate the Data
def check_if_empty(df: pd.DataFrame, primaryKey) -> bool:
    # Check if the dataframe is empty
    if df.empty:
        print("No information in the dataframe")
        return False
    return True


# Extract the Data
if __name__ == "__main__":

    # Convert the csv data to a structured dataframe

    # Extract the raw data
    collision_data = pd.read_csv('C:/Users/jessh/Documents/MS Applied Data Science/ADS507/Project/nyc_collsions/database.csv', parse_dates =['DATE'])
    uber_data = pd.read_csv('C:/Users/jessh/Documents/MS Applied Data Science/ADS507/Project/uber/Uber_Trips_NYC_2016.csv', parse_dates =['Pickup Start Date', 'Pickup End Date'])
    weather_data = pd.read_csv('C:/Users/jessh/Documents/MS Applied Data Science/ADS507/Project/nyc weather/NYC_Central_Park_weather_1869-2022.csv', parse_dates =['DATE'])

    # Changing the spaces in the names to underscores for use in sql statements
    collision_data.columns = collision_data.columns.str.replace(' ', '_')
    uber_data.columns = uber_dat.columns.str.replace(' ', '_')
    weather_data.columns = weather_data.columns.str.replace(' ', '_')

    # Filtering out only the 2016 year
    collision_df= collision_data[(collision_data['DATE'].dt.year == 2016)].reset_index(drop = True)
    uber_df = uber_data[(uber_data['Pickup_Start_Date'].dt.year == 2016) & (uber_dat['Pickup_Start_Date'].dt.year == 2016)].reset_index(drop = True)
    weather_df= weather_data[(weather_data['DATE'].dt.year == 2016)].reset_index(drop = True)

    # Create a database connection
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST_NAME}:{PORT}/{DATABASE}")
    conn = engine.connect()

    # Create tables with DDL
    ddl1 = """
    CREATE TABLE IF NOT EXISTS `Daily_Weather` (
        `day_id` INT PRIMARY KEY AUTO_INCREMENT,
        `date` DATE NOT NULL,
        `prcp` DOUBLE NOT NULL,
        `snow` DOUBLE NOT NULL,
        `snwd` DOUBLE NULL DEFAULT NULL,
        `tmin` INT NOT NULL,
        `tmax` INT NOT NULL
    );
    """


    ddl2 = """
    CREATE TABLE IF NOT EXISTS `Collisions` (
        `collision_id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        `day_id` INT NOT NULL,
	    `unique_key` INT NOT NULL,
	    `date` DATE NOT NULL,
	    `time` TEXT NOT NULL,
	    `borough` VARCHAR(45) NULL DEFAULT NULL,
	    `zip_code` VARCHAR(5) NULL DEFAULT NULL,
	    `latitude` TEXT NULL DEFAULT NULL,
	    `longitude` TEXT NULL DEFAULT NULL,
	    `location` VARCHAR(45) NULL DEFAULT NULL,
	    `on_street_name` VARCHAR(60) NULL DEFAULT NULL,
	    `cross_street_name` VARCHAR(60) NULL DEFAULT NULL,
	    `off_street_name` VARCHAR(60) NULL DEFAULT NULL,
	    `persons_injured` INT NOT NULL,
	    `persons_killed` INT NOT NULL,
	    `pedestrians_injured` INT NOT NULL,
	    `pedestrians_killed` INT NOT NULL,
	    `cyclists_injured` INT NOT NULL,
	    `cyclists_killed` INT NOT NULL,
	    `motorists_injured` INT NOT NULL,
	    `motorists_killed` INT NOT NULL,
	    `vehicle_1_type` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_2_type` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_3_type` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_4_type` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_5_type` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_1_factor` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_2_factor` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_3_factor` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_4_factor` VARCHAR(60) NULL DEFAULT NULL,
	    `vehicle_5_factor` VARCHAR(60) NULL DEFAULT NULL,
	    FOREIGN KEY (`day_id`) REFERENCES `Daily_Weather`(`day_id`)
    );
    """

    ddl3 = """
    CREATE TABLE IF NOT EXISTS `Uber` (
        `ride_id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        `start_day_id` INT NOT NULL,
        `end_day_id` INT NOT NULL,
        `base_license_num` VARCHAR(7) NOT NULL,
        `wave_num` INT NOT NULL,
        `base_name` VARCHAR(45) NOT NULL,
        `dba` VARCHAR(45) NOT NULL,
        `years` INT NOT NULL,
        `week_num` INT NOT NULL,
        `pickup_start_date` DATE NOT NULL,
        `pickup_end_date` DATE NOT NULL,
        `total_dispatched_trips` INT NOT NULL,
        `unq_dispatched_vehicle` INT NOT NULL,
        FOREIGN KEY (`start_day_id`) REFERENCES `Daily_Weather`(`day_id`)
        FOREIGN KEY (`end_day_id`) REFERENCES `Daily_Weather`(`day_id`)
    );
    """
    # Validate the data sets
    if check_if_empty(weather_df, "date"):
        print("Weather data valid, checking Collision data")
    if check_if_empty(collision_df, "uniq_key"):
        print("Collision data valid, checking Uber data")
   # if check_if_empty(uber_dat, "collision_id"):
        #print("Uber data valid, proceed to creating the database")

    # Execute the sql query and create the table above
    conn.execute(text(ddl1))
    conn.execute(text(ddl2))
    conn.execute(ddl3)
    print("Successfully created the database")

    # populating the weather table
    weather_df.to_sql(name = "daily_weather",con = engine, if_exists= "append", index=False)

   # had to add this part in because at the time of table creation, this table doesnt know the day_id of the weather table
   weather_df['day_id'] = list(map(lambda x: x, range(1,367)))
   collision_df= pd.merge(weather_df, collision_df, on= ['DATE'], how = 'right')
   dayid_col = collision_df.pop('day_id')
   collision_df.insert(0,'day_id',dayid_col)

    # populating the collisions table
    collision_df.to_sql(name = "collisions", con = engine, if_exists="append", index=False)

    # had to add this part in because at the time of table creation, this table doesnt know the day_id of the weather table
    # first getting the day_id for the pickup start date 
    temp_df = uber_df.rename(mapper = {"Pickup_Start_Date":"DATE"}, axis = 1)
    temp_df = pd.merge(weather_df, temp_df, on = ['DATE'], how = 'right')
    uber_df.insert(0, 'start_day_id', temp_df['day_id'])

    # now getting the day_id for the pickup end date 
    temp_df2 = uber_df.rename(mapper = {"Pickup_End_Date":"DATE"}, axis = 1)
    temp_df2 = pd.merge(weather_df, temp_df2, on = ['DATE'], how = 'right')
    uber_df.insert(1, 'end_day_id', temp_df2['day_id'])

    # Close the database connection
    conn.close()
    print("Connection closed")

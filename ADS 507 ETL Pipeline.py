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
    collision_data = pd.read_csv(r'C:/Users/benog/OneDrive/Documents/ads507/nyc_collisions/nyc_collisions/database.csv')#, parse_dates=['DATE'])
    uber_data = pd.read_csv(r'C:/Users/benog/OneDrive/Documents/ads507/Uber_Trips_NYC_2016.csv')#, parse_dates=['Pickup Start Date', 'Pickup End Date'])
    weather_data = pd.read_csv(r'C:/Users/benog/OneDrive/Documents/ads507/NYC_Central_Park_weather_1869-2022.csv')#, parse_dates=['DATE'])

    # Changing the spaces in the names to underscores for use in sql statements
    collision_data.columns = collision_data.columns.str.replace(' ', '_')
    uber_data.columns = uber_data.columns.str.replace(' ', '_')
    weather_data.columns = weather_data.columns.str.replace(' ', '_')

    # Filtering out only the 2016 year
    #collision_df= collision_data[(collision_dat['DATE'].dt.year == 2016)].reset_index(drop = True)
    #uber_df = uber_data[(uber_data['Pickup_Start_Date'].dt.year == 2016) & (uber_dat['Pickup_Start_Date'].dt.year == 2016)].reset_index(drop = True)
    #weather_df= weather_data[(weather_data['DATE'].dt.year == 2016)].reset_index(drop = True)

    # Create a database connection
    conn = mysql.connect(host=HOST_NAME,port=PORT,user=USERNAME,passwd=PASSWORD,db=DATABASE)
    cursor = conn.cursor()

    # Create tables with DDL
    ddl = """
    CREATE TABLE IF NOT EXISTS `Daily_Weather` (
        `day_id` INT PRIMARY KEY AUTO_INCREMENT,
        `date` DATE NOT NULL,
        `precip` DOUBLE NOT NULL,
        `snow_fall` DOUBLE NOT NULL,
        `snow_depth` DOUBLE NULL DEFAULT NULL,
        `tmin` INT NOT NULL,
        `tmax` INT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS `Collisions` (
        `collision_id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        `day_id` INT NOT NULL,
        `uniq_key` INT NOT NULL,
        `date` DATE NOT NULL,
        `time` TEXT NOT NULL,
        `borough` VARCHAR(45) NULL DEFAULT NULL,
        `zip_code` VARCHAR(5) NULL DEFAULT NULL,
        `lat` TEXT NULL DEFAULT NULL,
        `long` TEXT NULL DEFAULT NULL,
        `location` VARCHAR(45) NULL DEFAULT NULL,
        `on_street_name` VARCHAR(60) NULL DEFAULT NULL,
        `cross_street_name` VARCHAR(60) NULL DEFAULT NULL,
        `off_street_name` VARCHAR(60) NULL DEFAULT NULL,
        `persons_injured` INT NOT NULL,
        `persons_killed` INT NOT NULL,
        `peds_injured` INT NOT NULL,
        `peds_killed` INT NOT NULL,
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

    CREATE TABLE IF NOT EXISTS `Uber` (
        `ride_id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        `day_id` INT NOT NULL,
        `collision_id` INT NOT NULL,
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
        FOREIGN KEY (`day_id`) REFERENCES `Daily_Weather`(`day_id`)
    );
    """

    # Validate the data sets
    if check_if_empty(weather_data, "date"):
        print("Weather data valid, checking Collision data")
    if check_if_empty(collision_data, "uniq_key"):
        print("Collision data valid, checking Uber data")
    if check_if_empty(uber_data, "collision_id"):
        print("Uber data valid, proceed to creating the database")

    # Execute the sql query and create the table above
    cursor.execute(ddl)
    print("Successfully created the database")


    try:    
        # Load the dataframes into the database    
        weather_data.to_sql("Daily_Weather", conn, if_exists="append", index=False)
        collision_data.to_sql("Collisions", conn, if_exists="append", index=False)
        uber_data.to_sql("Uber", conn, if_exists="append", index=False)
        
        # Filtering out only the 2016 year
        transform_collision = """
        SELECT *
        FROM Collisions
        WHERE YEAR(`DATE`) = 2016;
        """

        transform_uber = """
        SELECT *
        FROM Uber
        WHERE YEAR(`Pickup_Start_Date`) = 2016;
        """

        transform_weather = """
        SELECT *
        FROM Daily_Weather
        WHERE YEAR(`DATE`) = 2016;
        """
        print("Data was tranformed to show columns from 2016")
        
    except:
        print("Data already exists in the database")

    # Close the database connection
    conn.close()
    print("Connection closed")

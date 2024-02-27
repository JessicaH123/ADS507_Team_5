<ins>**Background**</ins>
<br/>
This pipeline is created to identify how different weather conditions impact vehicle collisions and uber rides in NYC during the 2016 calendar year. Three datasets were used from the open source Kaggle.com website (linked at the end of this document). The pipeline is built in VSCode using Python and SQL languages. In this project, the pipeline will be used to populate a AZURE SQL database. It has also been used to populate a local instance of a MySQL Workbench database.

<br/><br/>
<ins>**Purpose**</ins>

How does bad weather affect UBER rides and vehicle accidents in NYC? How can Uber capitalize on this?
- <b>Is there an increase in vehicle collisions during inclement weather? Does this also affect UBER rides?</b>
- <b>What are the weather conditions for the most rides received in a single day?</b>
- <b>Most rides during rainy season vs non-rainy season.</b>
- <b>Which weather condition is best vs. worst in collisions?</b>

<br/><br/>
<ins>**How to Deploy the Pipeline**</ins>
<br/>
<P>This pipeline is easily deployable since it involves running a single python script. Since I am most familiar with Jenkins automation for CI/CD, I will create steps for Jenkins. However, These steps can be modified based on your prefered CI/CD automation tool of choice.
<br/>
<ol>
<li><p>Download the data pipeline python file (ADS 507 ETL Pipeline.py)
<li><p>Download the csv files for each dataset (NYC_Central_Park_weather_1869-2022.csv, Uber_Trips_NYC_2016.csv, nyc_collisions.zip)
<li><p>Place the python file, and each dataset file in a dedicated location on your server
<li><p>Edit the ADS 507 ETL Pipeline.py to:<br/>
  - Update constants with Database credentials<br/>
  - Update csv paths to reflect where the csv files are located on your machine<br/>
<li><p>Create a freestyle Jenkins job and title it NYC_ETL_pipeline
<li><p>Within the freestyle Jenkins job, create a shell script section
<li><p>write the following commands in the shell script:<br/>
 cd {LOCATION FROM STEP 2}<br/>
 python ADS 507 ETL Pipeline.py<br/>
 <li><p>Add polling to poll everynight at midnight
 <li><p>If polling detects changes to the csv files, run the shell script<br/><br/></p>




<br/><br/>
<ins>**Link to Kaggle**</ins>
<br/><br/>
https://www.kaggle.com/datasets/danbraswell/new-york-city-weather-18692022
<br/><br/>
https://www.kaggle.com/datasets/nypd/vehicle-collisions
<br/><br/>
https://www.kaggle.com/datasets/danvargg/uber-nyc-2016

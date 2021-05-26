# Project: Data Lake



------------------------------------------
### Purpose of database
The Main objective of this project is to create an ETL script that will able to Load data from s3 bucket, process this data using spark to creat a dimensional tables  (parquet format) and then store them into S3 backet .


--------------------------------------------
### Databaase schema)
The following diagram show the star schema used for the starSchema and the staging tables used in this project 

   <img src="./sparkifaydb.png" width="600">


#### Fact Table 
1. **fact_songplay** - records in log data associated with song plays i.e. records with page `NextSong` 
    + *songplay_id , start_time , user_id , level, song_id , artist_id , session_id, location, user_agent*

#### Dimension Tables 
2. **users** - users in the app 
    + *user_id , first_name, last_name, gender, level*

3. **songs** - songs in music database
    + *song_id , title, artist_id, year, duration*

4. **artists** - artists in music database
    + *artist_id , name, location, lattitude, longitude*

5. **time** - timestamps of records 
    + *start_time , hour, day, week, month, year, weekday*

--------------------------------------------
### Project Structure 
this project includes the following files:
+ `etl.py` - this python script is our main ETL, using payspark to Load data from s3 bracket then process it and store it back to s3     ;
+ `etl.ipynb` - a pytho  notebook for development purposes help to test and devlope function that is definded in the main ETL (etl.pyà ;
+ `dl.cfg` - config file contains AWS credentials;
+ `resources_data_lake.txt` - Contain most of the websites i have used that helped me in this project;


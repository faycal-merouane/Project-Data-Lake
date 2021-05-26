import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


""" reading configuration from the config file in the workspace (AWS keyID/SecretKey)"""
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ creating spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from 'input_data'/song_data  and extract Data to create song_table and artists_table
    then storing this tables into 'output_data' directory
    
    Keyword arguments:
    
    spark -- spark session point to our spark cluster 
    input_data -- path to our song_data location
    output_data -- path to where to store our parquete files and floders 
    
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'year', 'duration', 'artist_id').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data+"/songs_table/songs_table.parquet", "overwrite")

    # extract columns to create artists table
    artists_table =  df.select('artist_id', 'artist_name', 'artist_location','artist_latitude','artist_longitude')\
    .dropDuplicates().withColumnRenamed("artist_name", "name").withColumnRenamed("artist_location", "location") \
    .withColumnRenamed("artist_latitude", "lattitude").withColumnRenamed("artist_longitude", "longitude")
 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"/artists_table/artists_table.parquet", "overwrite")


def process_log_data(spark, input_data, output_data):
    
    """
    Load data from 'input_data'/song_data & 'input_data'/log_data to create users_table, time_table and songplays_table
    then storing them into 'output_data' directory
    
    Keyword arguments:
    
    spark -- spark session point to our spark cluster 
    input_data -- path to our song_data location
    output_data -- path to where to store our parquete files and floders 
    
    """
    # get filepath to log data file
    log_data =input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').withColumnRenamed("userId", "user_id")\
    .withColumnRenamed("firstName", "first_name").withColumnRenamed("lastName", "last_name").dropDuplicates()

    
    # write users table to parquet files
    users_table.write.parquet(output_data+"/users_table/users_table.parquet", "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000)
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.start_time))
    
    # extract columns to create time table
    time_table = df.select('datetime').withColumn("start_time", df.datetime).withColumn("hour", hour(df.datetime))\
    .withColumn("day", dayofmonth(df.datetime)).withColumn("week", weekofyear(df.datetime))\
    .withColumn("month", month(df.datetime)).withColumn("year", year(df.datetime))\
    .withColumn("weekday", dayofweek(df.datetime)).dropDuplicates()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data+"/time_table/time_table.parquet", "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,((song_df.artist_name == df.artist) & (song_df.title == df.song) & (song_df.duration ==df.length))) 
    songplays_table = songplays_table.select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')\
    .withColumn('songplay_id', monotonically_increasing_id()).withColumnRenamed('userId', 'user_id')\
    .withColumnRenamed('sessionId', 'session_id').withColumnRenamed('userAgent', 'user_agent').dropDuplicates()
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+"/songplays_table/songplays_table.parquet", "overwrite")



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://faycal.merouane/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

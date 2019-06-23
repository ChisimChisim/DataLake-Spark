import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]=config['AWS']["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config['AWS']["AWS_SECRET_ACCESS_KEY"]

'''
create spark settion
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    return spark

'''
get song_data from S3 and create songs table and artists_table, and then save in S3
'''
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.filter(df.song_id.isNotNull())\
                    .select("song_id", "title", "artist_id", "year", "duration")\
                    .dropDuplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, "songs_table")
    songs_table\
          .repartition("year", "artist_id")\
          .write\
          .parquet(songs_output, mode="overwrite")
    # extract columns to create artists table
    artists_table = df.filter(df.artist_id.isNotNull())\
                      .selectExpr("artist_id", "artist_name as name", "artist_location as location", \
                               "artist_latitude as latitude", "artist_longitude as longitude")\
                      .dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_output = os.path.join(output_data, "artists_table")
    artists_table.write.parquet(artists_output, mode="overwrite")

'''
get song_data from S3 and create users, time and songolays tables, and then save in S3
'''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", \
                                "lastName as last_name", "gender", "level")\
                    .where(df.userId != "")\
                    .dropDuplicates(subset=['user_id'])
    
    # write users table to parquet files
    users_output = os.path.join(output_data, "users_table")
    users_table.write.parquet(users_output, mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts).cast("timestamp"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime(df.ts).cast("date"))
    
    # extract columns to create time table
    time_table = df.filter(df.timestamp.isNotNull())\
                   .select(df.timestamp.alias("start_time"),\
                       hour(df.timestamp).alias("hour"),\
                       dayofmonth(df.timestamp).alias("day"),\
                       weekofyear(df.timestamp).alias("week"),\
                       month(df.timestamp).alias("month"),\
                       year(df.timestamp).alias("year"),\
                       date_format(df.timestamp, 'E').alias("weekday"))\
                   .dropDuplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, "time_table")
    time_table\
          .repartition("year", "month")\
          .write\
          .parquet(time_output, mode="overwrite")

    # read in song data to use for songplays table
    song_df = song_df = songs_table.select("song_id", "artist_id", "title")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song==song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias('songplay_id'), \
                                df.timestamp.alias('start_time'),\
                                df.userId.alias('user_id'),\
                                df.level,\
                                song_df.song_id,\
                                song_df.artist_id,\
                                df.sessionId.alias('session_id'),\
                                df.location,\
                                df.userAgent.alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_output = os.path.join(output_data, "songplays_table")
    songplays_table\
          .repartition(year(songplays_table.start_time), month(songplays_table.start_time))\
          .write\
          .parquet(songplays_output, mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mydatalakeproject/"
    
    #get song_data from S3 and create songs table and artists_table, and then save in S3
    process_song_data(spark, input_data, output_data)    
    #get log_data from S3 and create other tables, and then save in S3
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

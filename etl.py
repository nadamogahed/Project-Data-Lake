import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data from given input_path and store it at output_data as parquet files.

    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/song/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/song/'
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    #     the schema of song files
    song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", DoubleType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType()),
    ])
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('artist_id', 'year').parquet(os.path.join(output_data,'songs_table.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists_table.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process log data from given input_path and store it at output_data as parquet files.

    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None 
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")
    #     the schema of log files
    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", StringType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", IntegerType()),
        StructField("userAgent", StringType()),
        StructField("userId", IntegerType()),
    ])
    # read log data file
    df = spark.read.json(log_data, schema=song_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    # drop  duplicate records
    users_table = songs_table.drop_duplicates(subset=['song_id'])
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users_table.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp =  udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime =  udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    df = df.withColumn('start_time', get_datetime(df.ts))
    # create hour column
    df.withColumn("hour", date_format(col("start_time"), "h"))
    # create day column
    df.withColumn("day", date_format(col("start_time"), "d"))
    # create week column
    df.withColumn("week", date_format(col("start_time"), "w"))
    # create month column
    df.withColumn("month", date_format(col("start_time"), "m"))
    # create year column
    df.withColumn("year", date_format(col("start_time"), "Y"))
    # create weekday column
    df.withColumn("weekday", date_format(col("start_time"), "E"))
    # extract columns to create time table
    time_table = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'time_table.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = os.path.join(input_data,"song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, log_data)
    songplays_table = df['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']
    songplays_table = df.withColumn("songplay_id", monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays_table.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

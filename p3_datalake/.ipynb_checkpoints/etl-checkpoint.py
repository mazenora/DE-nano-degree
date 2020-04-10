import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 'data/song_data/A/*/*/*.json'
    
    # read song data file
    df = spark.read.format("json").json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","artist_id","title","duration","year"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "/song/")

    # extract columns to create artists table
    exprs=["artist_id","artist_name as name","artist_location as location","artist_latitude as latitude","artist_longitude as longitude"]
    artists_table = df.selectExpr(*exprs)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artist/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 'data/log_data/*.json'

    # read log data file
    df = spark.read.format("json").json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    artists_fields=["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write users table to parquet files
    artists_table.write.parquet(output_data + "/user/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f'))
    df = df.withColumn("ts_col", get_datetime('ts').cast(TimestampType()))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_datetime('ts').cast(TimestampType()))
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", date_format(col("start_time"),'E')) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "/time/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + '/song/*/*/*')
    artist_df = spark.read.parquet(output_data + '/artist/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    songs_logs = df.join(song_df, (df.song == song_df.title))
    artists_songs_logs = songs_logs.join(artist_df, (songs_logs.artist == artist_df.name))
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, 'left'
    ).drop(artists_songs_logs.year)
    
    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + '/songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

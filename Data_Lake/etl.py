import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ create spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    parse the song data from s3 bucket into the dimensional tables include: 
    - songs_table
    - artists_table

    parameters:
    -----------
    spark: spark session
    input_data: bucket name of the data's source
    ouput_data: bucket name of the dimensional tables
    """
    # get filepath to song data file
    song_data = f'{input_data}song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ['song_id', 'title', 'artist_id', 'year', 'duration']).drop_duplicates(subset=['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(
        f'{output_data}songs.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.drop_duplicates(subset=['artist_id']).selectExpr('artist_id',
                                                                        'artist_name as name',
                                                                        'artist_location as location',
                                                                        'artist_latitude as latitude',
                                                                        'artist_longitude as longtitude')

    # write artists table to parquet files
    artists_table.write.parquet(
        f'{output_data}artists.parquet', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    parse the log and song data from s3 bucket into the dimensional tables include: 
    - users_table
    - time_table
    - songplays_table

    parameters:
    -----------
    spark: spark session
    input_data: bucket name of the data's source
    ouput_data: bucket name of the dimensional tables
    """
    # get filepath to log data file
    log_data = f'{input_data}log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.drop_duplicates(subset=['userId']).selectExpr('userId as user_id', 'firstName as first_name',
                                                                   'lastName as last_name', 'gender', 'level')

    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users.parquet', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda z: to_timestamp(z))
    df = df.withColumn(
        'ts_second', (col('ts').cast('double')/1000).cast('long'))
    df = df.withColumn('timestamp', expr('to_timestamp(ts_second)'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda z: to_date(z))
    df = df.withColumn('datetime', get_datetime('ts_second'))

    # extract columns to create time table
    time_table = df.selectExpr('timestamp as start_time',
                               'hour(timestamp) as hour',
                               'dayofmonth(timestamp) as day',
                               'weekofyear(timestamp) as week',
                               'month(timestamp) as month',
                               'year(timestamp) as year',
                               'dayofweek(timestamp) as weekday',
                               ).drop_duplicates(subset=['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(
        f'{output_data}time.parquet', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(f'{input_data}song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (df.join(song_df, [df.artist == song_df.artist_name,
                                         df.song == song_df.title,
                                         df.length == song_df.duration], 'left').selectExpr([
                                             'timestamp as start_time',
                                             'userId as user_id',
                                             'level as level',
                                             'song_id',
                                             'artist_id',
                                             'sessionId as session_id',
                                             'artist_location as location',
                                             'userAgent as user_agent']
    )).withColumn('songplay_id', monotonically_increasing_id())

    songplays_table = songplays_table.withColumn('year', year(
        songplays_table.start_time)).withColumn('month', month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(
        f'{output_data}songplays.parquet', mode='overwrite')


def main():
    """ Run the Spark Pipeline """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-vuongnd9-bucket/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

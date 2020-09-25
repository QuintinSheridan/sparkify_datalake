import configparser
from datetime import datetime
import os
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from datetime import datetime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('IAM', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('IAM', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''Function to create a spark session
    Args:
        None
    Returns:
        None
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Function that processes song data into tables
    Args:
        spark (SparkSession): SparkSession 
        input_data(str): path to input storage bucket
        output_data(str): path to output storage bucket
    Returns:
        None
    """
    # get filepath to song data file
    #song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    #df.printSchema()
    
    # create view for querying
    df.createOrReplaceTempView("song_data")
     
    # extract columns to create songs table
    songs_table = spark.sql('''
    SELECT 
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        song_data  
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artist_table = spark.sql('''
    SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        song_data  
    ''')

    # write artists table to parquet files
    artist_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """Function that processes log data into tables
    Args:
        spark (SparkSession): SparkSession 
        input_data(str): path to input storage bucket
        output_data(str): path to output storage bucket
    Returns:
        None
    """
    
    # get filepath to log data file
    # log_data = inpuut_data + 'log_data/*/*/*.json'
    log_data = input_data + 'log_data/2018/11/2018-11*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page']=='NextSong']
    
    #df.printSchema()
    df.createOrReplaceTempView("temp_data")
    
    # extract columns for users table  
    users_table = spark.sql('''
    SELECT 
        DISTINCT userId,
        firstName,
        lastName,
        gender, 
        level
    FROM
        temp_data
    WHERE 
        userId not IN ('')
    ''')
    
    # write users table to parquet files
    users_table.limit(5).write.parquet(os.path.join(output_data, 'users'))
    
    df = df.filter(df.ts.isNotNull())
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)) if x!='' else '', t.TimestampType()) 
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    df=df.dropDuplicates()\
        .withColumn("hour", hour("start_time"))\
        .withColumn("day", dayofmonth("start_time"))\
        .withColumn("week", weekofyear("start_time"))\
        .withColumn("month", month("start_time"))\
        .withColumn("year", year("start_time"))\
        .withColumn("weekday", dayofweek("start_time"))
    
    time_table = df.select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
        
    # write time table to parquet files partitioned by year and month
    time_table.limit(5).write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'))
    
    # read in song data to use for songplays table
    songs_parquet = output_data + 'songs'
    songs_df = spark.read.parquet(songs_parquet)
    artists_parquet = output_data + 'artists/*.parquet'
    artists_df = spark.read.parquet(artists_parquet)
    
    songs_df = songs_df.join(artists_df, ['artist_id'])
    songs_df = songs_df.drop('year')
    
    df = df.join(songs_df, ((df.song==songs_df.song_id) & (df.artist==songs_df.artist_name)), how='left')
    
    df.createOrReplaceTempView("temp_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    SELECT 
        year, 
        month,
        start_time,
        userID, 
        level,
        song_id, 
        artist_id, 
        sessionId,
        location,
        userAgent
    FROM
        temp_data  
    ''')

    # sort by ts and use row number for songplay_id
    window = Window.orderBy(col('start_time'))
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(window))
    
    # write songplays table to parquet files partitioned by year and month  
    songplays_table.limit(10).write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'song_plays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparklake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

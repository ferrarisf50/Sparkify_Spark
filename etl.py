import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Create a Spark session.
    
    INPUT: None
    OUTPUT: Spark session
    
    """
       
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''
def process_song_data(spark, input_data, output_data):
    """
    Read song JSON files, extract and transform data, create songs table and artists table,
    save into parquet files
    
    INPUT: 
    spark - Spark session
    input_data - Root path of the song JSON files
    output_data - Root path of the output files
    
    OUTPUT:
    songs table and artists table, stored as parquet file format in output path
    """
    
    
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(input_data+song_data)
    
    df.createOrReplaceTempView("song_data_view")
    
    # extract columns to create songs table
    songs_table = = spark.sql("""select
    song_id, title, artist_id, year, duration
    from song_data_view
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs.parquet",mode='overwrite',partitionBy=("year", "artist_id"))

    # extract columns to create artists table
    artists_table = = spark.sql("""select
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
    from song_data_view
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists.parquet",mode='overwrite')

'''

def process_log_song_data(spark, input_song_data, input_log_data,output_data):
    """
    Read log and song JSON files, extract and transform data, create songs table, artists table, time table, users table, and    songplays table,
    save into parquet files
    
    INPUT: 
    spark - Spark session
    input_song_data -Path of song JSON files
    input_log_data - Path of log JSON files
    output_data - Root path of the output files
    
    OUTPUT:
    songs table, artists table, time table, users table, and songplays table, stored as parquet file format in output path
    """
    
    # get filepath to log and song data file
    
    
    
    # read log and song data file
    print("Reading song data...")
    df = spark.read.json(input_song_data)
    print("Song data has %s obs." % df.count())
    
    print("Reading log data...")
    df2 = spark.read.json(input_log_data)
    print("Log data has %s obs." % df2.count())
    
    # read song data file
    
    df.createOrReplaceTempView("song_data_view")
    
    # extract columns to create songs table
    songs_table =  spark.sql("""select
    song_id, title, artist_id, year, duration
    from song_data_view
    """)
    print("songs_table has %s obs." % songs_table.count())
    
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table to parquet files...")
    songs_table.write.parquet(output_data+"songs.parquet",mode='overwrite',partitionBy=("year", "artist_id"))

    # extract columns to create artists table
    artists_table = spark.sql("""select
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
    from song_data_view
    """)
    print("artists_table has %s obs." % artists_table.count())
    
    # write artists table to parquet files
    print("Writing artists table to parquet files...")
    artists_table.write.parquet(output_data+"artists.parquet",mode='overwrite')
    
    # filter by actions for song plays
    df2 = df2.filter(df2.page == "NextSong")

    df2.createOrReplaceTempView("log_data_view")
    
    # extract columns for users table    
    users_table = spark.sql("""
    select distinct 
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
    from log_data_view order by user_id
    """)
    print("users_table has %s obs." % users_table.count())
    
    # write users table to parquet files
    print("Writing users table to parquet files...")
    users_table.write.parquet(output_data+"users.parquet",mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    
    # create datetime column from original timestamp column
    df2 = df2.withColumn("start_time", get_timestamp(df2.ts))
    df2.createOrReplaceTempView("log_data_view")
    
    # extract columns to create time table  
    
    time_table = spark.sql("""
    select start_time, 
           hour(start_time) as hour, 
           day(start_time)  as day, 
           weekofyear(start_time) as week,
           month(start_time) as month,
           year(start_time) as year,
           dayofweek(start_time) as weekday
    from log_data_view
    """)
    print("time_table has %s obs." % time_table.count())
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table to parquet files...")
    time_table.write.parquet(output_data+"time.parquet",mode='overwrite',partitionBy=("year", "month"))
    
    
    # read in song data to use for songplays table
    # song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    select 
    
    monotonically_increasing_id() as songplay_id,
    from_unixtime(ts/1000) as start_time,
    month(from_unixtime(ts/1000) ) as month,
    year(from_unixtime(ts/1000) ) as year,
    userId as user_id,
    level,
    a.song_id,
    a.artist_id,
    sessionId as session_id,
    location,
    userAgent as user_agent
    
    from song_data_view a right join log_data_view b
    on a.artist_name=b.artist and a.title=b.song and a.duration=b.length
    
    """)
    print("songplays_table has %s obs." % songplays_table.count())
    
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays table to parquet files...")
    songplays_table.write.parquet(output_data+"songplays.parquet",mode='overwrite',partitionBy=("year", "month"))


def main():
    
    answer = input("Load data from Udacity S3 (Y) or load test data from local directory (N)? ")
    if answer.upper() == "Y":
        input_log_data = "s3a://udacity-dend/log_data/*/*/*.json"
        input_song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
        #input_log_data = "s3a://udacity-dend/log_data/2018/11/*.json"
        #input_song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
    else:
        input_log_data = "data/log_data/*.json"
        input_song_data = "data/song_data/*/*/*/*.json"
        
    answer = input("Write data into my S3 (Y) or write into local directory (N)? ")
    if answer.upper() == "Y":
        output_data = "s3a://ferrarisf50/"
    else:
        output_data = "output/"
        
        
    spark = create_spark_session()
    
    
    
    
    process_log_song_data(spark, input_song_data, input_log_data, output_data)    
    


if __name__ == "__main__":
    main()

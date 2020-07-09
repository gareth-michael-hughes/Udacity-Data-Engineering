import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["CREDENTIALS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["CREDENTIALS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates or get spark session 
        
    Creates a spark session instance or gets
    and returns most recently created instance.
    
    Args:
        None
        
    Returns:
        spark: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes json song data from S3 bucket
        
    Processes json song data from S3 bucket to local directory
    Creates dimension tables artist and song and
    writes them to parquet files for later storage in S3
    
    Args:
        spark: SparkSession object
        input_data: Root URL for the given S3 bucket
        output_data: URL path to local directory
        
    Returns:
        None
    """
    print("-------- Begin Processing Song_Data json file --------")
    
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    
    # check schema at initial read
    df.printSchema()

    # create and name a view in spark from the data frame
    df.createOrReplaceTempView("song_data_table")
    print("Step1: Materialised view of song data created for querying")
    
    # extract columns to create songs table
    songs_table = spark.sql('''
          SELECT DISTINCT song_id,
          title as song_title,
          artist_id,
          artist_name,
          year as song_year,
          duration as song_duration
          
          FROM song_data_table
          '''
          )
    # check 1 row to see select has performed correctly and bypass lazy evaluation
    songs_table.take(1)
    
    # convert to a dataframe for easier writing to parquet file
    songs_table = songs_table.toPandas()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, \
                                                                            '/songs/', 'songs_data.parquet'), 'overwrite')
    print("Step2: song table data written to parquet file")
    
    # extract columns to create artists table
    artists_table = spark.sql('''
          SELECT DISTINCT artist_id,
          artist_name,
          artist_location,
          artist_latitude,
          artist_longitude
          
          FROM song_data_table
          '''
          )
    
    # check 1 row to see select has performed correctly and bypass lazy evaluation
    artists_table.take(1)
    
    # convert to a dataframe for easier writing to parquet file
    artists_table = artists_table.toPandas()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, '/artists/', 'artists_data.parquet'), 'overwrite')
    print("Step3: artists table data written to parquet file")
    
    print("-------- End Processing Song_Data json file --------")
    
    
def process_log_data(spark, input_data, output_data):
    """Processes json log data from S3 bucket
        
    Processes json log data from S3 bucket to local directory
    Creates dimension tables users, time and song plays and
    writes them to parquet files for later storage in S3
    
    Args:
        spark: SparkSession object
        input_data: Root URL for the given S3 bucket
        output_data: URL path to local directory
        
    Returns:
        None
    """
    print("-------- Begin Processing Log_Data json file --------")
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # check schema at initial read
    df.printSchema() 
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # create and name a view in spark from the data frame
    df.createOrReplaceTempView("log_data_table")
    print("Step1: Materialised view of log data created for querying")

    # extract columns for users table    
    users_table = spark.sql('''
          SELECT DISTINCT user_id,
          first_name,
          last_name,
          gender,
          level as product_level
          
          FROM log_data_table
          '''
          )
    
    # check 1 row to see select has performed correctly and bypass lazy evaluation
    users_table.take(1)
    
    # convert to a dataframe for easier writing to parquet file
    users_table = users_table.toPandas()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, '/users/', 'users_data.parquet'), 'overwrite')
    print("Step2: users table data written to parquet file")
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # re-create the view to access the newly added column
    df.createOrReplaceTempView("log_data_table")
    print("Step3: Materialised view of log data re-created with start_time for querying")
    
    # extract columns to create time table
    time_table = spark.sql('''
          SELECT start_time,
          hour(cast(ts/1000 as Timestamp)) as hour,
          day(cast(ts/1000 as Timestamp)) as day,
          weekofyear(cast(ts/1000 as Timestamp)) as week,
          month(cast(ts/1000 as Timestamp)) as month,
          year(cast(ts/1000 as Timestamp)) as year,
          weekday(cast(ts/1000 as Timestamp)) as weekday
          
          FROM log_data_table
          '''
          )
    
    # check 1 row to see select has performed correctly and bypass lazy evaluation
    time_table.take(1)
    
    # convert to a dataframe for easier writing to parquet file
    time_table = time_table.toPandas()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, '/time/', 'time_data.parquet'), 'overwrite')
    print("Step4: time table data written to parquet file")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("output/song_data.parquet")
    print("Step5: Read in song data parquet file")
    
    # create and name a view in spark from the data frame
    song_df.createOrReplaceTempView("songs_parquet")
    print("Step6: Materialised view of song parquet data created for querying")
        
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
          SELECT cast(l.ts/1000 as Timestamp) as start_time,
          month(cast(l.ts/1000 as Timestamp)) as month,
          year(cast(l.ts/1000 as Timestamp)) as year,
          l.user_id,
          l.level as product_level,
          s.song_id,
          s.artist_id,
          l.session_id,
          l.location,
          l.user_agent
          
          FROM log_data_table l
          INNER JOIN songs_parquet s on s.title = l.song and s.artist_id = l.artist_id 
          '''
          )
    
    # check 1 row to see select has performed correctly and bypass lazy evaluation
    songplays_table.take(1)
    
    # convert to a dataframe for easier writing to parquet file
    songplays_table = songplays_table.toPandas()
    
    # Add a monotonically increasing id to the dataframe as a unique identifier
    # This is preffered to using a SQL row_number over () which requires sorting on a column
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, \
                                                                '/plays/', 'songplays_data.parquet'), 'overwrite')
    
    print("Step7: song plays table data written to parquet file")
    print("-------- ENd Processing Log_Data json file --------")

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

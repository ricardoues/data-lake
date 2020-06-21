import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pandas as pd
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime            
import pyspark.sql.functions as F
            

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# It is possible to use the following instructions instead the ones above
#os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
#os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
            
    # get filepath to song data file        
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    
    # This web page was very useful to read the data from S3
    # https://knowledge.udacity.com/questions/49116
                
    # read song data file 
    df_song = spark.read.json(song_data)
            
    
    # extract columns for artists table    
    artists_table = df_song.select('artist_id', \
                                   col('artist_name').alias('name'), \
                                   col('artist_location').alias('location'), \
                                   col('artist_latitude').alias('latitude'), \
                                   col('artist_longitude').alias('longitude')                                  
                                  )
    
    # write artists table to parquet files
    dest = "{}/artists.parquet".format(output_data)
    artists_table.write.format("parquet").save(dest, mode="append")
    
    # extract columns for songs table
    song_table = df_song.select('song_id', \
                                'title',\
                                'artist_id', \
                                'year', \
                                'duration'
                                )
    
    # write songs table to parquet file 
    dest = "{}/songs.parquet".format(output_data)
    song_table.write.format("parquet").save(dest, mode="append")
    
    


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "{}/log_data/*/*/*.json".format(input_data)
    
    # get filepath to song data file 
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    
    # read log data file
    df_log = spark.read.json(log_data)
    
    # read song data file 
    df_song = spark.read.json(song_data)
            
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == "NextSong")
        
    # extract columns for users table    
    users_table = df_log.select(col('userId').alias('user_id'), \
                                col('firstName').alias('first_name'), \
                                col('lastName').alias('last_name'), \
                                'gender', 
                                'level'
                               )
    
    
    # write users table to parquet files
    dest = "{}/users.parquet".format(output_data)
    users_table.write.format("parquet").save(dest, mode="append")

    
    # It is not necessary to include the songplay_id field according to the 
    # following web page:
    # https://knowledge.udacity.com/questions/215981
    
    cond = [df_song.artist_name == df_log.artist, df_song.title == df_log.song]
    
    df_song_plays = df_song.join(df_log,
                                 cond, 'inner').select(df_log.ts,
                                                   df_log.userId, df_log.level,
                                                   df_song.song_id,
                                                   df_song.artist_id, df_log.sessionId,
                                                   df_log.location, df_log.userAgent,
                                                   df_log.page)
    
    # I have to add a column for start_time field the following web pages were very useful
    # https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
    # https://www.bmc.com/blogs/how-to-write-spark-udf-python/

    
    def calculate_time(x):        
        return int(x/1000)

    
    udf_calculate_time = udf(lambda x: calculate_time(x), IntegerType())    
    spark.udf.register("udf_calculate_time", udf_calculate_time)
    
        
    df_song_plays = df_song_plays.withColumn("start_time", \
                                             from_unixtime(udf_calculate_time('ts')))
    
        
    
    # In order to create the year and month field, I took ideas from the 
    # following links
    # https://knowledge.udacity.com/questions/60112
    # https://knowledge.udacity.com/questions/175498
    # https://spark.apachehttp://cms.dt.uh.edu/faculty/delavinae/Sp11/Math2405/Ch1_2_Propositional_Equiv.pdf.org/docs/latest/api/python/pyspark.sql.html?highlight=dateformat
    
    
    #  I have to add the columns for years and months 
   
    # extract columns to create songplays table
    song_plays_table = df_song_plays.select("start_time",  \
                                        F.year("start_time").alias("year"), \
                                        F.month("start_time").alias("month"), \
                                        col("userId").alias("user_id"), \
                                       "level", "song_id", "artist_id", \
                                       col("sessionId").alias("session_id"), \
                                       "location", col("userAgent").alias("user_agent")
                                      ) 
    

    # write songs table to parquet files partitioned by year and artist
    # the following link was very useful
    # https://stackoverflow.com/questions/43731679/how-to-save-a-partitioned-parquet-file-in-spark-2-1
    
    # write songplays table to parquet files 
    dest = "{}/songplays.parquet".format(output_data)
    song_plays_table.write.partitionBy(['year', 'month']).format("parquet").save(dest, mode="append")

    time_table = df_song_plays.select("start_time",  \
                                      F.hour("start_time").alias("hour"), \
                                      F.dayofmonth("start_time").alias("day"), \
                                      F.weekofyear("start_time").alias("week"), \
                                      F.month("start_time").alias("month"), \
                                      F.year("start_time").alias("year"), \
                                      F.dayofweek("start_time").alias("weekday")
                                     ) 
    
    
    
    # write time table to parquet files 
    dest = "{}/time_table.parquet".format(output_data)
    time_table.write.format("parquet").save(dest, mode="append")
    
    
                                      
                                      

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://ricrio"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()

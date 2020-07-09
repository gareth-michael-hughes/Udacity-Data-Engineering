import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config['IAM_ROLE']['ARN'] # Extract the Amazon Resource Name from the IAM Role in the config file
LOG_DATA = config['S3']['LOG_DATA'] # Extract the S3 bucket path for the event data
SONG_DATA = config['S3']['SONG_DATA'] # Extract the S3 bucket path for the song data
LOG_JSONPATH = config['S3']['LOG_JSONPATH'] # Extract the S3 bucket path format for the json event log data

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongPlays"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
 artist TEXT,
 auth TEXT,
 firstName TEXT,
 gender CHAR(1),
 itemInSession INT,
 lastName TEXT,
 length NUMERIC,
 level TEXT,
 location TEXT,
 method TEXT,
 page TEXT,
 registration NUMERIC,
 sessionId INT,
 song TEXT,
 status INT,
 ts BIGINT,
 userAgent TEXT,
 userId INT
 )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
 num_songs INT,
 artist_id TEXT,
 artist_latitude NUMERIC,
 artist_longitude NUMERIC,
 artist_location TEXT,
 artist_name TEXT,
 song_id TEXT,
 title TEXT,
 duration NUMERIC,
 year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS factSongPlays
(
  songplay_key INT IDENTITY(1,1) PRIMARY KEY,
  start_time TIMESTAMP,
  user_id INT NOT NULL,
  level text NOT NULL,
  song_id TEXT NOT NULL,
  artist_id TEXT NOT NULL,
  session_id INT NOT NULL,
  location TEXT NOT NULL,
  user_agent TEXT NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimUser
(
  user_id INT PRIMARY KEY,
  first_name text NOT NULL,
  last_name text NOT NULL,
  gender CHAR(1),
  level text NOT NULL
)

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimSong
(
  song_id TEXT PRIMARY KEY,
  song_title TEXT NOT NULL,
  artist_id TEXT NOT NULL,
  song_year INTEGER,
  song_duration NUMERIC
)

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimArtist
(
  artist_id TEXT PRIMARY KEY,
  artist_name TEXT NOT NULL,
  artist_location TEXT,
  artist_latitude NUMERIC,
  artist_longitude NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimTime
(
  start_time TIMESTAMP PRIMARY KEY,
  hour INT,
  day INT,
  week INT,
  month INT,
  year INT,
  weekday INT
)
""")

# STAGING TABLES

# Note region param explicitly required if your Redshift cluster is not located in the same region
staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    compupdate off
    REGION 'us-west-2'
    FORMAT AS JSON {};    
    """).format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

# Note region param explicitly required if your Redshift cluster is not located in the same region
staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    compupdate off
    REGION 'us-west-2'
    FORMAT AS JSON 'auto' truncatecolumns;
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO factSongPlays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
 e.userId,
 e.level,
 s.song_id,
 s.artist_id,
 e.sessionId,
 e.location,
 e.userAgent
 
 FROM staging_events e
 WHERE e.page = 'NextSong'
 LEFT JOIN staging_songs s on s.title = e.song AND s.artist_name = e.artist
""")

user_table_insert = ("""
INSERT INTO dimUser(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
firstName,
lastName TEXT,
gender,
level

FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO dimSong(song_id, song_title, artist_id, song_year, song_duration)
SELECT song_id,
title,
artist_id,
year,
duration

FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO dimArtist(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude

FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO dimTime(start_time, hour, day, week, month, year, weekday)
SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
extract(hour from timestamp 'epoch' + ts/1000 * interval '1 second') AS hour,
extract(day from timestamp 'epoch' + ts/1000 * interval '1 second') AS day,
extract(week from timestamp 'epoch' + ts/1000 * interval '1 second') AS week,
extract(month from timestamp 'epoch' + ts/1000 * interval '1 second') AS month,
extract(year from timestamp 'epoch' + ts/1000 * interval '1 second') AS year,
extract(dayofweek from timestamp 'epoch' + ts/1000 * interval '1 second')  AS weekday
  
FROM staging_events
""")
sample_query_1_desc = 'Number of Users by Gender and Service Level'
sample_query_1 = ("""
SELECT COUNT(DISTINCT user_id) AS number_users,
gender,
level

FROM dimUser
GROUP BY gender, level
ORDER BY COUNT(DISTINCT user_id) DESC
""")

sample_query_2_desc = 'Number of Song Plays, Users and Unique Sessions by Weekday'
sample_query_2 = ("""
SELECT t.weekday,
COUNT(DISTINCT songplay_key) AS number_song_plays,
COUNT(DISTINCT user_id) AS number_users,
COUNT(DISTINCT session_id) AS number_sessions

FROM factSongPlays fsp
LEFT JOIN dimTime t on t.start_time = fsp.start_time
GROUP BY t.weekday
""")

sample_query_3_desc = 'Number of Songs by Artist and Year of release after 2010'
sample_query_3 = ("""
SELECT a.artist_id,
a.artist_name,
s.song_year,
COUNT(DISTINCT s.song_id) AS number_songs

FROM dimArtist a
LEFT JOIN dimSong s on s.artist_id = a.artist_id
WHERE s.song_year >= 2010
GROUP BY a.artist_id, a.artist_name, s.song_year
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
sample_query_list = [sample_query_1, sample_query_2, sample_query_3]
sample_query_desc = [sample_query_1_desc, sample_query_2_desc, sample_query_3_desc]
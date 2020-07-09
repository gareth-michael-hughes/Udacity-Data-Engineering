# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY, start_time time, user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);"""

user_table_create = """CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar NOT NULL, last_name varchar, gender char(1), level varchar);"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar NOT NULL, year int, duration numeric);"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, latitude numeric, longitude numeric);"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int);"""

# INSERT RECORDS

# On conflict catch here is implemented in the event there is an attempt to insert a duplicate songplay record
songplay_table_insert = (""" 
                        INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id,
                        location, user_agent)
                        VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                        ON CONFLICT (songplay_id) 
                        DO NOTHING
                        """)

# On conflict catch here is implemented in the event there is an attempt to insert a duplicate user record
user_table_insert = (""" 
                    INSERT INTO users (user_id, first_name, last_name, gender, level)
                    VALUES (
                    %s, %s, %s, %s, %s
                            )
                    ON CONFLICT (user_id) 
                    DO NOTHING
                    """)

# On conflict catch here is implemented in the event there is an attempt to insert a duplicate song record
song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
                    VALUES (
                    %s, %s, %s, %s, %s
                            )
                    ON CONFLICT (song_id) 
                    DO NOTHING                            
                    """)

# On conflict catch here is implemented in the event there is an attempt to insert a duplicate artist record
artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude)
                    VALUES (
                    %s, %s, %s, %s, %s
                            )
                    ON CONFLICT (artist_id) 
                    DO NOTHING                    
                        """)

# No on conflict catch here as time records can relate to sessions of multiple users that
# Begin at the same time (in theory)
time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""")

# FIND SONGS
# The WHERE clause in the query accepts the parameter values provided by etl.py
# Hence the placeholders %s will assume the values of row.song, row.artist, row.length from the log data 
#  in the line: cur.execute(song_select, (row.song, row.artist, row.length))
song_select = (""" SELECT s.song_id, a.artist_id FROM songs s JOIN artists a on a.artist_id = s.artist_id 
                WHERE s.title = %s AND a.name = %s AND s.duration = %s """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
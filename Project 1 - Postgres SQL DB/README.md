**Note:** Right click this file in the file tray and select open with Markdown Preview to view with markdown fonts applied
## Purpose of the database

Sparkify is a start up that provides a music streaming app to allow users to listen to music on demand.  
The music streaming app records information about users, music and log data for user sessions.

## Analytical Goals

The analytics team is particularly interested in understanding what songs users are listening to.  
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Further Context

Sparkify have requested that a Postgres database be created with tables designed to optimize queries on song play analysis.

## Design Methodology

The table scturture will follow a simple star schema of a central fact table that captures user song play data and dimension tables capturing songs, artist, timestamps and user information. A simple star schema was chosen in this case where optimisation for data reads is prefered for analytical purposes at the potential expense of data duplication and slower data writes.

## How to run the provided files

There are several files provided in this submission:

- **create_tables.py** *(functions to create database, tables and drop any pre-exisitng tables for consistency)*
- **etl.ipynb** *(the test version extract, transform and load script for the song and log data provided)*
- **etl.py** *(the production version extract, transform and load script for the song and log data provided)*
- **sql_queries.py** *(code to create tables and skeleton queries for inserting data into each table)*
- **test.ipynb** *(code to test the table creation and data loading by selecting table records)*

**The following section details the chronological order in which to run these files in order to correctly create the database, load the data into each table and test the output**

### 1. Creating the database
- Open a new Launcher tab by selecting file in the top left of the ribbon and then new launcher
- Once open select the Python 3 console icon in the bottom left of the Console heading section
- In the newly opened console window type the following command and hit shift and enter:  
`exec(open('create_tables.py').read())`


### 2. Processing and ingesting the data
- Staying within the console opened in step 1. type the following command and hit shift and enter:  
`exec(open('etl.py').read())`
- The console output will record how many of each input files have been processed.
- Once all required files have been processed you can proceed to step 3.


### 3. Sample analytical queries:
- To access the sample analytical queries please open the file test.ipynb
- Select run from the tob ribbon and then run all cells
- Cells 4, 5 and 6 will produce sample output for the following questions

**The most frequently active users by number of unique sessions and level:**  
*Are there any frequent users who could beneift from the paid model?*

`%sql SELECT songplays.user_id, first_name, last_name, users.level, COUNT(DISTINCT session_id) as unique_sessions FROM songplays JOIN users on users.user_id = songplays.user_id  GROUP BY songplays.user_id, first_name, last_name, users.level ORDER BY COUNT(DISTINCT session_id) DESC;`

**The most frequently listened to artist by number of unique user sessions:**  
*Which artists are popular and could be reccomended to users?*

`%sql SELECT songplays.artist_id, name, COUNT(DISTINCT session_id) as unique_session_listens FROM songplays JOIN artists on artists.artist_id = songplays.artist_id  GROUP BY songplays.artist_id, name ORDER BY COUNT(DISTINCT session_id) DESC;`

**The average session length (in minutes) of users compared by level:**  
*Could the business/revenue model be redesigned if free users are spending far longer on the app than paid or vice versa*

`%sql SELECT session_lengths.level as product_tier, AVG(session_lengths.diff) AS average_session_length  FROM (SELECT songplays.user_id, songplays.session_id, MIN(songplays.start_time) AS session_start, MAX(songplays.start_time) AS session_end, ((DATE_PART('day', MAX(songplays.start_time) - MIN(songplays.start_time))* 24 + DATE_PART('hour', MAX(songplays.start_time) - MIN(songplays.start_time))) * 60 + DATE_PART('minute', MAX(songplays.start_time) - MIN(songplays.start_time))) AS diff, songplays.level FROM songplays GROUP BY songplays.session_id, songplays.user_id, songplays.level ) session_lengths GROUP BY session_lengths.level;`

## Troubleshooting:
Each time you run test.ipynb, **remember to restart the notebook to close the connection to your database**. Otherwise, you won't be able to run your code in create_tables.py (step 1) or etl.py (step 2) files since you can't make multiple connections to the same database (in this case, sparkifydb).
**Note:** Right click this file in the file tray and select open with Markdown Preview to view with markdown fonts applied
## Purpose of the database

Sparkify is a start up that provides a music streaming app to allow users to listen to music on demand.  
The music streaming app records information about users, music and log data for user sessions.

## Analytical Goals

The analytics team is particularly interested in understanding what songs users are listening to.  
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The JSON format data is currently stored in an AWS S3 instance however, Sparkify would like to fit a dimensional model around this data for optimal analytical query processing.

## Further Context

Sparkify have requested that a Postgres database be created with tables designed to optimize queries on song play analysis.

## Design Methodology

Sparkify have outlined that with an ever increasing amount of data recorded by their app and users, they would like to make use of a scalable, cloud storage environment. Furthermore, coupled with the necessity for a dimensional model, it was chosen to create a Relational Database Schema in AWS Redshift cluster that makes use of distributed storage for optimal loading and querying of large amounts of data.

The schema table structure will follow a simple star schema of a central fact table that captures user song play data and dimension tables capturing songs, artist, timestamps and user information. A simple star schema was chosen in this case where optimisation for data reads is prefered for analytical purposes at the potential expense of data duplication and slower data writes.

Data is initially copied over from an AWS S3 instance into staging tables in the Redshiftcluster environment. This staging tables are then transformed and translated into the star schema structure as desired. 

## How to run the provided files

There are several files provided in this submission:

- **create_tables.py** *(functions to create tables and drop any pre-exisitng tables for consistency)*
- **etl.py** *(functions to transform and load data into staging tables and analytical tables)*
- **sql_queries.py** *(code to create tables and skeleton queries for inserting data into each table)*
- **sample_queries.py** *(code to test the table creation and data loading by selecting table records)*
- **dwh.cfg** *(Details for connecting to Redshift cluster, IAM role and RDB configuration)*

**The following section details the chronological order in which to run these files to correctly create the table skeletons, load the data into the staging tables and then process the data into each analytical table**

### 1. Creating the database
- Open a new Launcher tab by selecting file in the top left of the ribbon and then new launcher
- Once open select the Terminal icon in the bottom left of the Console heading section under 'Other'
- In the newly opened console window type the following command and hit shift and enter:  
`python3 create_tables.py`
- The console will output the following message once the staging tables are loaded: 'Table skeletons successfully created'


### 2. Copying, processing and ingesting the data from the S3 bucket 
- Staying within the console opened in step 1. type the following command and hit shift and enter:  
`python3 etl.py`
- The console will output the following message once the staging tables are loaded: 'Staging Tables successfully loaded'
- The console will output the following message once the analytical tables are loaded: 'Staging Tables successfully transformed into star schema'


### 3. Sample analytical queries:
- Ensure that steps 1 and 2 have completed succesfully before executing analytical queries
- Staying within the console opened in step 1. type the following command and hit shift and enter:   
`python3 sample_queries.py`
- The console will output the context of the query along with the query results
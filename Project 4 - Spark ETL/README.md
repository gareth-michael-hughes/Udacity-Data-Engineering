**Note:** Right click this file in the file tray and select open with Markdown Preview to view with markdown fonts applied
## Purpose of the database

Sparkify is a start up that provides a music streaming app to allow users to listen to music on demand.  
The music streaming app records information about users, music and log data for user sessions.

## Analytical Goals

The analytics team is particularly interested in understanding what songs users are listening to.  
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The JSON format data is currently stored in an AWS S3 instance however, Sparkify would like to fit a dimensional model around this data for optimal analytical query processing.

## Further Context

Sparkify have requested that this data processing is conducted in Spark using AWS EMR clusters to process data before writing data back to S3 for storage.

## Design Methodology

Sparkify have outlined that with an ever increasing amount of data recorded by their app and users, they would like to make use of a scalable, cloud storage and processing environment. Furthermore, coupled with the necessity for a dimensional model, it was chosen to transform and model the json song and log data using Spark and AWS EMR. Elaastic Map Reduce allows for optimal distributed data processing at scale, while S3 makes use of distributed storage for optimal loading, storing and querying large amounts of data via parquet files which are columnar partitioned.

The schema table structure will follow a simple star schema of a central fact table that captures user song play data and dimension tables capturing songs, artist, timestamps and user information. A simple star schema was chosen in this case where optimisation for data reads is prefered for analytical purposes at the potential expense of data duplication and slower data writes.

Data is initially read from an AWS S3 instance into staging tables via Spark using the EMR cluster. These staging tables are then transformed and translated into the star schema structure as desired via parquet files and written back to the S3 instance for storage and further querying. 

## How to run the provided files

There are several files provided in this submission:

- **etl.py** *(functions to transform and load data into staging tables from S3, process and load into analytical tables in S3)*
- **dl.cfg** *(Details for connecting to EMR cluster)*

### Loading and processing the data from the S3 bucket and writing back via parquet
- Open a new Launcher tab by selecting file in the top left of the ribbon and then new launcher
- Once open select the Terminal icon in the bottom left of the Console heading section under 'Other'
- In the newly opened console window type the following command and hit shift and enter:  
`python3 etl.py`
- The console will guide you through the script process with various update messages in the console
- The console will output the following message once a table has been loaded back to S3: 'table data written to parquet file'
- The console will output the following message once all files have been processed: 'End Processing json file'

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,    
}

dag = DAG('s3_to_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
start_operator.doc = "Begins the boundary and execution of the s3 to redshift dag"

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)
stage_events_to_redshift.doc="Copies json log files on app usage to staging tables in Redshift"

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json="auto"
)
stage_songs_to_redshift.doc="Copies json log files on song data to staging tables in Redshift"

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="factSongplays",
    sql_query=SqlQueries.songplay_table_insert
)
load_songplays_table.doc="Loads songplay data from staging tables to Fact table in redshift"

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="dimUsers",
    sql_query=SqlQueries.user_table_insert
)
load_user_dimension_table.doc="Loads user data from staging tables to dimension table in redshift"

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="dimSongs",
    sql_query=SqlQueries.song_table_insert    
)
load_song_dimension_table.doc="Loads song data from staging tables to dimension table in redshift"

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="dimArtists",
    sql_query=SqlQueries.artist_table_insert
)
load_artist_dimension_table.doc="Loads artist data from staging tables to dimension table in redshift"

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="dimTime",
    sql_query=SqlQueries.time_table_insert
)
load_time_dimension_table.doc="Loads time data from staging tables to dimension table in redshift"

check_list=[{'sql_template': SqlQueries.data_quality_null, 'expected_result': 0, 'check_column': "userid", 'check_table': "users", 'check_type': "Null"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 0, 'check_column': "userid", 'check_table': "users", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_null, 'expected_result': 0, 'check_column': "songid", 'check_table': "songs", 'check_type': "Null"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 0, 'check_column': "songid", 'check_table': "songs", 'check_type': "Records"}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_list=check_list
)
run_quality_checks.doc="Performs a number of user specified data quality checks on loaded tables"

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
end_operator.doc="End the boundary and execution of the s3 to redshift dag"

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table >> \
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
run_quality_checks >> end_operator
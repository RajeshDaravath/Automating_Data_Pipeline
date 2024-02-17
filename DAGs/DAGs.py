import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# Define default arguments for the DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Define the DAG using a decorator
@dag(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    start_date=datetime(2019, 1, 12),
    catchup=False
)
def dag():

    # Define start and end operators
    @task(task_id='Begin_execution')
    def start_operator():
        pass

    @task(task_id='Stop_execution')
    def end_operator():
        pass

    # Define stage tasks
    @task(task_id='Stage_events')
    def stage_events_to_redshift():
        return StageToRedshiftOperator(
            table="staging_events",
            conn_id="redshift",
            aws_credentials_id="aws_credentials",
            s3_bucket="udacity-dend",
            s3_key="log_data",
            json_path="s3://udacity-dend/log_json_path.json"
        )

    @task(task_id='Stage_songs')
    def stage_songs_to_redshift():
        return StageToRedshiftOperator(
            table="staging_songs",
            conn_id="redshift",
            aws_credentials_id="aws_credentials",
            s3_bucket="udacity-dend",
            s3_key="song_data"
        )

    # Define load tasks
    @task(task_id='Load_songplays_fact_table')
    def load_songplays_table():
        return LoadFactOperator(
            conn_id="redshift",
            table="songplays",
            query=SqlQueries.songplay_table_insert
        )

    @task(task_id='Load_user_dim_table')
    def load_user_dimension_table():
        return LoadDimensionOperator(
            conn_id="redshift",
            table="users",
            query=SqlQueries.user_table_insert,
            truncate=True
        )

    @task(task_id='Load_song_dim_table')
    def load_song_dimension_table():
        return LoadDimensionOperator(
            conn_id="redshift",
            table="songs",
            query=SqlQueries.song_table_insert,
            truncate=True
        )

    @task(task_id='Load_artist_dim_table')
    def load_artist_dimension_table():
        return LoadDimensionOperator(
            conn_id="redshift",
            table="artists",
            query=SqlQueries.artist_table_insert,
            truncate=True
        )

    @task(task_id='Load_time_dim_table')
    def load_time_dimension_table():
        return LoadDimensionOperator(
            conn_id="redshift",
            table="time",
            query=SqlQueries.time_table_insert,
            truncate=True
        )

    # Define data quality check task
    @task(task_id='Run_data_quality_checks')
    def run_quality_checks():
        return DataQualityOperator()

    # Define task dependencies
    start_task = start_operator()
    end_task = end_operator()

    stage_events_task = stage_events_to_redshift()
    stage_songs_task = stage_songs_to_redshift()

    load_songplays_task = load_songplays_table()
    load_user_dim_task = load_user_dimension_table()
    load_song_dim_task = load_song_dimension_table()
    load_artist_dim_task = load_artist_dimension_table()
    load_time_dim_task = load_time_dimension_table()

    quality_check_task = run_quality_checks()

    start_task >> [stage_events_task, stage_songs_task] >> load_songplays_task
    load_songplays_task >> [load_user_dim_task, load_song_dim_task, load_artist_dim_task, load_time_dim_task] >> quality_check_task >> end_task

# Instantiate the DAG
dag_instance = dag()
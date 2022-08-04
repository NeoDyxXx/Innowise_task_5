from airflow import DAG
from airflow.utils.dates import days_ago
from transformater_operator import CSVTransform
from insert_operator import MongoInserterOperator
import os


default_args = {
    'owner': 'airflow',
    'retries': 1
}

with DAG(
    dag_id='Innowise_task_5',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['Innowise task'],
    max_active_runs=1
) as dag:
    transform_op = CSVTransform(
        task_id="transform_task",
        input_file_name='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews.csv',
        output_file_name='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews_output.csv'
    )

    mongo_inserter = MongoInserterOperator(
        task_id="insert_task",
        inserted_file='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews_output.csv',
        conn_id='mongo_test_hook',
        collection_name='users',
        db_name='test',
        insert_with_update=True
    )

    transform_op >> mongo_inserter
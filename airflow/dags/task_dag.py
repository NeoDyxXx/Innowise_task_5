from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from mongo_hook import MongoHook
from transformater_operator import CSVTransform
import os


default_args = {
    'owner': 'airflow',
    'retries': 1
}

def test_conn():
    hook = MongoHook(conn_id="mongo_test_hook")
    print(hook.find("users", {}, mongo_db="test"))

with DAG(
    dag_id='secret_vars',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['POC'],
    max_active_runs=1
) as dag:
    test_connection = PythonOperator(
        task_id="test_conn",
        python_callable=test_conn
    )
    transform_op = CSVTransform(
        task_id="transform_task",
        input_file_name='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews.csv',
        output_file_name='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews_output.csv'
    )

    transform_op >> test_connection
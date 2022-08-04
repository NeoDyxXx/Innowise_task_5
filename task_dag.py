from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from transformater_operator import CSVTransform
import os


default_args = {
    'owner': 'airflow',
    'retries': 1
}


def secret_function():
    secret_key = Variable.get("secret_key")
    print("secret key containes: {}".format(secret_key))
    print(os.getcwd())
    with open("dags/file.txt", 'w') as f:
        f.write(secret_key)


with DAG(
    dag_id='secret_vars',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['POC'],
    max_active_runs=1
) as dag:
    context_testing = PythonOperator(
        task_id="secret_task",
        python_callable=secret_function,
    )

    transform_op = CSVTransform(
        task_id="transform_task",
        input_file_name='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews.csv',
        output_file_name='/home/ndx/Innowise tasks/Innowise_task_5/data/tiktok_google_play_reviews_output.csv'
    )

    context_testing >> transform_op
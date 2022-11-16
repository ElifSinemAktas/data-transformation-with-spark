from airflow import DAG
from datetime import datetime, timedelta
from minio_spark_operations.operation import *
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

start_date = datetime(2022, 11, 8)
today = datetime.now().strftime("%Y%m%d")
the_day = 20221104
bronze = "tmdb-bronze"
silver = "tmdb-silver"
bronze_silver_dict = {"credits": ["crew", "cast"],
                      "movies": ["movies", "genres", "keywords", "production_companies",
                                 "production_countries", "spoken_languages"]}
default_args = {
    "owner": "train",
    "start_date": start_date,
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}

with DAG("transform_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dg:
    check_bucket = BranchPythonOperator(task_id="check_bucket", python_callable=check_bucket,
                                        op_args=[bronze, bronze_silver_dict, the_day],
                                        do_xcom_push=False)

    http_request_error = DummyOperator(task_id='http_request_error')

    enough_file = DummyOperator(task_id='enough_file')

    not_enough_file = DummyOperator(task_id='not_enough_file')

    transform_and_save_to_silver = BranchPythonOperator(task_id="transform_and_save_to_silver",
                                                        python_callable=transform_and_save_to_silver,
                                                        op_args=[bronze, silver, bronze_silver_dict, the_day],
                                                        do_xcom_push=False)

    transformation_and_save_succeeded = DummyOperator(task_id='transformation_and_save_succeeded')

    some_problem_occurred = DummyOperator(task_id='some_problem_occurred')

    another_task_1 = DummyOperator(task_id='another_task_1', trigger_rule="none_failed_or_skipped")

    check_bucket >> [http_request_error, enough_file, not_enough_file]

    enough_file >> transform_and_save_to_silver >> [transformation_and_save_succeeded, some_problem_occurred] >> another_task_1
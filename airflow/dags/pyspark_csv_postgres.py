"""
# Pyspark Postgres with CSV DAGs Ingestion
- Ingest data from local airflow to postgres container with PysparkPostgresJDBCOperator

"""

import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from utils.pyspark_util import PysparkCSVOperator
from utils.csv_postgres_util import copy_csv_files_to_postgres, check_if_schema_and_table_exists

STRATEGY = "pyspark_csv"
DAG_NAME = f"data_ingestion.postgres.{STRATEGY}"

postgres_config = {
    "user": "admin",
    "password": "admin",
    # for local test, use localhost. localhost refers to the container itself.
    # "host": "localhost",
    "host": "postgres-target",
    "port": "5432",  # postgres-target expose port
    "database_name": "mydb",
}

spark_config = {
    "app_name": "de-pyspark-airflow",
    "master": "16",
    "executor_memory": "4g",
    "executor_core": "4",  # per executor
    "executor_instance": "4",
    # "jar_file": "postgresql-42.7.3.jar"
}


default_args = {
    "owner": "de-patcharanat",
    "description": f"load to postgresDB from {STRATEGY}",
    "doc_md": __doc__,
    "start_date": datetime.datetime(2024, 1, 1),
    "depends_on_past": False,
    "catchup": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "email_on_failure": False,
    "default_view": "grid",
    "max_active_runs": 1,
    "max_active_tasks": 4,
}

with DAG(
    dag_id=DAG_NAME,
    start_date=datetime.datetime(2024, 7, 12),
    schedule="0 1 * * *",
    tags=["data_ingestion", "postgres", "csv"],
    default_args=default_args,
) as dag:
    start_task = EmptyOperator(task_id="start")

    transfrom_parquet_to_csv_task = PysparkCSVOperator(
        task_id="transform_parquet_to_csv_task",
        spark_config=spark_config,
        input_path="./data_sample/2023-01-01 00-00-00.parquet",
        # input_path="./data_sample/*.parquet",
        output_path="./data_sample/csv",
        mode="overwrite",
        max_records_per_file=1_000_000
    )
    
    load_csv_to_postgres_task = PythonOperator(
        task_id="load_csv_to_postgres_task",
        python_callable=copy_csv_files_to_postgres,
        op_kwargs={
            "postgres_config": postgres_config,
            "input_path": "data_sample/csv",
            "table_name": "product.product_master"
        },
        provide_context=True
    )

    delete_csv_file_task = BashOperator(
        task_id="delete_csv_file_task",
        bash_command=f"rm -rf ./data_sample/csv"
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> \
    transfrom_parquet_to_csv_task >> \
    load_csv_to_postgres_task >> \
    delete_csv_file_task >> \
    end_task

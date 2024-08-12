"""
# Pyspark Postgres with JDBC DAGs Ingestion
- Ingest data from local airflow to postgres container with PysparkPostgresJDBCOperator

"""

import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

from utils.pyspark_util import PysparkPostgresJDBCOperator


STRATEGY = "parquet_with_pyspark"
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
    "jar_file": "postgresql-42.7.3.jar",
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
    tags=["data_ingestion", "postgres"],
    default_args=default_args,
) as dag:
    start_task = EmptyOperator(task_id="start")

    load_parquet_to_postgres_task = PysparkPostgresJDBCOperator(
        task_id="load_parquet_to_postgres_task",
        postgres_connection=postgres_config,
        spark_config=spark_config,
        file_path="./data_sample/2023-01-01 00-00-00.parquet",
        # file_path="./data_sample/*.parquet",
        table_name="product.product_master",
        mode="overwrite",
        # mode="append",
        # chunk_size=1_000_000
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> load_parquet_to_postgres_task >> end_task

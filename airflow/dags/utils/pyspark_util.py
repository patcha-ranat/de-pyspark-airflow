"""
# Pyspark-Postgres Utilities
- Custom Operator
    - Being used in dag
- Loader
    - For unit test

"""

import logging

from pyspark.sql import SparkSession
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PysparkPostgresJDBCOperator(BaseOperator):
    """
    Custom Operator to Load Local file in Airlfow Container to Postgres Conainer

    :param postgres_connection: target postgres's connection to database
    :type postgres_connection: dict
        Example:
            postgres_config = {
                "user": "admin",
                "password": "admin",
                "host": "localhost",
                "port": "3000",
                "database_name": "mydb"
            }

    :param spark_config: pyspark configuration
    :type postgres_connection: dict
        Example:
            spark_config = {
                "app_name": "de-pyspark-airflow",
                "cpu_core": "*"
            }

    :param file_path: source path in local airflow container
    :type file_path: str

    :param table_name: target schema.table_name in postgres database
    :type table_name: str

    :param mode: mode to wrtie postgres database
    :type mode: str
        Example:
            "append", "overwrite", "ignore", "error"
    """

    @apply_defaults
    def __init__(
        self,
        postgres_connection: dict,
        spark_config: dict,
        file_path: str,
        table_name: str,
        mode: str = "overwrite",
        chunk_size: int = None,
        **kwargs,
    ) -> None:
        # BaseOperator attributes
        super().__init__(**kwargs)

        # postgres config
        self.user = postgres_connection["user"]
        self.password = postgres_connection["password"]
        self.host = postgres_connection["host"]
        self.port = postgres_connection["port"]
        self.database_name = postgres_connection["database_name"]

        # pyspark config
        self.app_name = spark_config["app_name"]
        self.master = spark_config["master"]
        self.executor_memory = spark_config.get("executor_memory", 1)
        self.executor_core = spark_config.get("executor_core", 1)
        self.executor_instance = spark_config.get("executor_instance", 1)
        self.jar_file = spark_config["jar_file"]

        # input-output
        self.file_path = file_path
        self.table_name = table_name
        self.mode = mode
        self.chunk_size = chunk_size

    def create_spark_session(self) -> SparkSession:
        """
        Initialize PySpark Session using 'spark_config'.
        """
        logging.info(f"Starting Pyspark Session: {self.app_name}")
        spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars", self.jar_file)
            .master(f"local[{self.master}]")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", self.executor_core)
            .config("spark.executor.instances", self.executor_instance)
            .getOrCreate()
        )
        logging.info(f"Pyspark Session initialized.")
        return spark

    def get_postgres_connection(self) -> tuple:
        """
        Get Postgres connection detail from 'postgres_config'.
        """
        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database_name}"
        connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        return jdbc_url, connection_properties

    def write_chunk(self, spark_df, table, url, mode, properties):
        """
        Execute Writing DataFrame to PostgresDB
        """
        spark_df.write.jdbc(url=url, table=table, mode=mode, properties=properties)

    def terminate_spark_session(self, spark_session: SparkSession) -> None:
        """
        Terminate PySpark Session.
        """
        spark_session.stop()
        logging.info("Terminated Pyspark Session Successfully.")

    def execute(self, context) -> None:
        """
        Main Process for being an Airflow Operator
        """
        # initialize initial and components
        spark = self.create_spark_session()
        jdbc_url, connection_properties = self.get_postgres_connection()

        # main process
        logging.info(
            f"Reading Local File(s): '{self.file_path}' to Pyspark DataFrame..."
        )
        spark_df = spark.read.parquet(self.file_path)
        logging.info("Read File(s) Success.")

        # https://stackoverflow.com/questions/69101389/load-sparksql-dataframe-into-postgres-database-with-automatically-defined-schema
        logging.info("Loading to PostgresDB...")
        if self.chunk_size:
            logging.info(
                f"loading with chunk_size: {self.chunk_size}, mode: {self.mode}"
            )

            total_row = spark_df.count()
            num_chunks = total_row // self.chunk_size + 1
            logging.info(f"Total rows: {total_row}")
            logging.info(f"Total chunk per subset of partitions: {num_chunks}")

            for i in range(num_chunks):
                chunk_df = spark_df.offset(i * self.chunk_size).limit(self.chunk_size)
                self.write_chunk(
                    spark_df=chunk_df.repartition(int(self.executor_core)),
                    table=self.table_name,
                    url=jdbc_url,
                    mode=self.mode,
                    properties=connection_properties,
                )
                logging.info(f"Chunk: {i} loaded successfully.")
        else:
            logging.info(f"loading without chunk_size, mode: {self.mode}")
            self.write_chunk(
                spark_df=spark_df.repartition(
                    int(self.executor_core) * int(self.executor_instance) * 2
                ),
                table=self.table_name,
                url=jdbc_url,
                mode=self.mode,
                properties=connection_properties,
            )
        logging.info(
            f"Job Success! Data is loaded to '{self.table_name}' in '{self.database_name}' database."
        )

        self.terminate_spark_session(spark_session=spark)


class PysparkCSVOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        spark_config: dict,
        input_path: str,
        output_path: str,
        mode: str = "overwrite",
        max_records_per_file: int = 100_000,
        **kwargs,
    ) -> None:
        # BaseOperator attributes
        super().__init__(**kwargs)

        # pyspark config
        self.app_name = spark_config["app_name"]
        self.master = spark_config["master"]
        self.executor_memory = spark_config.get("executor_memory", 1)
        self.executor_core = spark_config.get("executor_core", 1)
        self.executor_instance = spark_config.get("executor_instance", 1)
        self.repartition_number = (
            int(self.executor_core) * int(self.executor_instance) * 1
        )

        # input-output
        self.input_path = input_path
        self.output_path = output_path
        self.mode = mode
        self.max_records_per_file = max_records_per_file

    def create_spark_session(self) -> SparkSession:
        """
        Initialize PySpark Session using 'spark_config'.
        """
        logging.info(f"Starting Pyspark Session: {self.app_name}")
        spark = (
            SparkSession.builder.appName(self.app_name)
            .master(f"local[{self.master}]")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", self.executor_core)
            .config("spark.executor.instances", self.executor_instance)
            .getOrCreate()
        )
        logging.info(f"Pyspark Session initialized.")
        return spark

    def write_csv(
        self, spark_df, output_path, repartition_number, max_records_per_file, mode
    ):
        """
        Execute Writing DataFrame to PostgresDB
        """
        spark_df.repartition(repartition_number).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_path)

    def terminate_spark_session(self, spark_session: SparkSession) -> None:
        """
        Terminate PySpark Session.
        """
        spark_session.stop()
        logging.info("Terminated Pyspark Session Successfully.")

    def execute(self, context) -> None:
        """
        Main Process for being an Airflow Operator
        """
        # initialize initial and components
        spark = self.create_spark_session()

        # main process
        logging.info(
            f"Reading Local File(s): '{self.input_path}' to Pyspark DataFrame..."
        )
        spark_df = spark.read.parquet(self.input_path)
        logging.info("Read File(s) Success.")

        # https://stackoverflow.com/questions/58676909/how-to-speed-up-spark-df-write-jdbc-to-postgres-database
        logging.info("Exporting to csv...")
        logging.info(f"Output path is: {self.output_path}")
        self.write_csv(
            spark_df=spark_df,
            output_path=self.output_path,
            repartition_number=self.repartition_number,
            max_records_per_file=self.max_records_per_file,
            mode=self.mode,
        )
        logging.info(f"Job Success! Data is loaded to '{self.output_path}'.")

        self.terminate_spark_session(spark_session=spark)

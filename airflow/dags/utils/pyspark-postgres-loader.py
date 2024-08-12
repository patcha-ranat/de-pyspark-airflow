import logging

from pyspark.sql import SparkSession


# for unit test
class PysparkPostgresLoader:
    def __init__(self, postgres_connection: dict, spark_config: dict) -> None:
        # postgres config
        self.user = postgres_connection["user"]
        self.password = postgres_connection["password"]
        self.host = postgres_connection["host"]
        self.port = postgres_connection["port"]
        self.database_name = postgres_connection["database_name"]

        # pyspark config
        self.app_name = spark_config["app_name"]
        self.cpu_core = spark_config["cpu_core"]

    def create_spark_session(self) -> SparkSession:
        logging.info(f"Starting Pyspark Session: {self.app_name}")
        spark = (
            SparkSession.builder.appName(self.app_name)
            .master(f"local[{self.cpu_core}]")
            .getOrCreate()
        )
        logging.info(f"Pyspark Session initialized.")
        return spark

    def get_postgres_connection(self) -> tuple:
        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database_name}"
        connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        return jdbc_url, connection_properties

    def terminate_spark_session(self, spark_session: SparkSession) -> None:
        spark_session.stop()
        logging.info("Terminated Pyspark Session Successfully.")

    def load_local_parquet_to_postgres(
        self, file_path: str, table_name: str, mode: str = "overwrite"
    ) -> None:
        # initialize initial and components
        spark = self.create_spark_session()
        jdbc_url, connection_properties = self.get_postgres_connection()

        # process
        logging.info("Reading Local File(s) to Pyspark DataFrame...")
        spark_df = spark.read.parquet(file_path)
        logging.info("Read File(s) Success.")
        row_affect = spark_df.write.jdbc(
            url=jdbc_url, table=table_name, mode=mode, properties=connection_properties
        )
        logging.info(f"Job Success! {row_affect} rows affect to {self.database_name}")

        self.terminate_spark_session(spark_session=spark)

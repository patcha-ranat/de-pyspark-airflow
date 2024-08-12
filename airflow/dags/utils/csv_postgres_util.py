import os
import psycopg2
import logging
from io import StringIO


def check_if_schema_and_table_exists(conn, schema_name, table_name):
    cursor = conn.cursor()
    
    # Create schema if it doesn't exist
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    cursor.execute(create_schema_query)
    
    # Check if the table exists
    check_table_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE
            table_schema = '{schema_name}'
            AND table_name   = '{table_name}'
    );
    """
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]
    
    # Create table if it doesn't exist
    if not table_exists:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            department_name varchar,
            sensor_serial varchar,
            create_at timestamp,
            product_name varchar,
            product_expire timestamp
        );
        """)
    
    conn.commit()
    cursor.close()

def copy_csv_files_to_postgres(postgres_config: dict, input_path: str, table_name: str):
    # Database connection
    conn = psycopg2.connect(
        dbname=postgres_config["database_name"],
        user=postgres_config["user"],
        password=postgres_config["password"],
        host=postgres_config["host"],
        port=postgres_config["port"],
    )

    check_if_schema_and_table_exists(
        conn=conn, 
        schema_name=table_name.split(".")[0], 
        table_name=table_name.split(".")[1]
    )

    cursor = conn.cursor()

    # Iterate over all CSV files in the directory
    if os.path.exists(input_path):
        for file_name in os.listdir(input_path):
            if file_name.endswith(".csv"):
                csv_file_path = os.path.join(input_path, file_name)
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_data = f.read()
                csv_file = StringIO(csv_data)
                copy_query = f"""
                    COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ',' ENCODING 'utf-8';
                """
                # cursor.copy_from(csv_file_path, table=table_name, sep=",")

                try:
                    # cursor.execute(copy_query)
                    cursor.copy_expert(copy_query, csv_file)
                    conn.commit()
                    logging.info(f"Successfully copied {file_name}")
                except Exception as e:
                    conn.rollback()
                    logging.info(f"Failed to copy {file_name}: {e}")
            else:
                continue
    else:
        pass

    cursor.close()
    conn.close()

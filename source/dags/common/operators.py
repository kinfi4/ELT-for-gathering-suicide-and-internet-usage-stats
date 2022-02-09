from typing import Any, Callable

import psycopg2
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class FileProcessingOperator(BaseOperator):
    def __init__(
            self,
            input_file_path: str,
            destination_folder_path: str,
            processing_callable: Callable,
            op_kwargs: dict = None,
            *args,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.input_file_path = input_file_path
        self.destination_folder_path = destination_folder_path
        self.processing_callable = processing_callable
        self.op_kwargs = op_kwargs if op_kwargs else dict()

    def execute(self, context: Any):
        self.log.info(f'Running processing function: {self.processing_callable.__name__} for file: {self.input_file_path}')

        try:
            self.processing_callable(self.input_file_path, self.destination_folder_path, **self.op_kwargs)

            self.log.info(f'Successfully processed the file: {self.input_file_path}')
        except Exception as ex:
            self.log.error(f'Could not process {self.input_file_path} - {ex.__class__.__name__}: {ex}')
            raise AirflowException(f'Could not process {self.input_file_path} - {ex.__class__.__name__}: {ex}')


class CsvToPostgresOperator(BaseOperator):
    def __init__(
            self,
            csv_file_path: str,
            destination_table_id: str,
            db_name: str,
            postgres_username: str,
            postgres_password: str,
            host: str = 'localhost',
            port: str = '5432',
            delimiter: str = ',',
            postgres_connector=None,
            *args,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.csv_file_path = csv_file_path
        self.destination_table_id = destination_table_id
        self.delimiter = delimiter
        self.db_name = db_name
        self.postgres_username = postgres_username
        self.postgres_password = postgres_password
        self.host = host
        self.port = port

        if postgres_connector is None:
            self.conn = psycopg2.connect(
                f"host='{host}' port='{port}' dbname='{db_name}' user='{postgres_username}' password='{postgres_password}'"
            )
        else:
            self.conn = postgres_connector

        self.cursor = self.conn.cursor()

    def execute(self, context: Any):
        self.log.info(f'Start moving {self.csv_file_path} into postgres table: {self.destination_table_id}')

        copy_sql = f"""
                       TRUNCATE TABLE {self.destination_table_id} CASCADE;
                       COPY {self.destination_table_id} FROM stdin
                       WITH CSV HEADER
                       DELIMITER as '{self.delimiter}'
                   """

        with open(self.csv_file_path, 'r') as csv_file:
            self.cursor.copy_expert(sql=copy_sql, file=csv_file)
            self.conn.commit()
            self.cursor.close()

        self.log.info(f'Successfully copied data from {self.csv_file_path} into {self.destination_table_id} table')

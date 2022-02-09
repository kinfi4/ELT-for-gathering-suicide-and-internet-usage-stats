import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import config as conf
import common.const as const
from common.callables import download_and_unzip
from common.operators import FileProcessingOperator, CsvToPostgresOperator
from common.processors import process_gdp_data, process_internet_data, process_suicide_data, process_data


INTERNET_USAGE_DATASET = 'pavan9065/internet-usage'
SUICIDE_STATS_DATASET = 'russellyates88/suicide-rates-overview-1985-to-2016'
GDP_STATS_DATASET = 'nitishabharathi/gdp-per-capita-all-countries'


with DAG(
    'Load_and_Process_data',
    start_date=days_ago(1),
    catchup=False
) as dag:

    download_and_unzip_suicide_stats_op = PythonOperator(
        task_id='download_and_unzip_suicide_stats',
        python_callable=download_and_unzip,
        op_kwargs={
            'dataset_name': SUICIDE_STATS_DATASET,
            'datalake_folder_path': conf.DATALAKE_FOLDER_PATH,
        }
    )

    download_and_unzip_gdp_stats_op = PythonOperator(
        task_id='download_and_unzip_gdp_stats',
        python_callable=download_and_unzip,
        op_kwargs={
            'dataset_name': GDP_STATS_DATASET,
            'datalake_folder_path': conf.DATALAKE_FOLDER_PATH,
        }
    )

    download_and_unzip_internet_usage_stats_op = PythonOperator(
        task_id='download_and_unzip_internet_usage_stats',
        python_callable=download_and_unzip,
        op_kwargs={
            'dataset_name': INTERNET_USAGE_DATASET,
            'datalake_folder_path': conf.DATALAKE_FOLDER_PATH,
        }
    )

    gdp_raw_data_path = os.path.join(conf.DATALAKE_FOLDER_PATH, const.GDP_STATS_FILE_NAME)
    preprocess_gdp_data_op = FileProcessingOperator(
        task_id='preprocess_gdp_data',
        destination_folder_path=conf.PROCESSING_FOLDER_PATH,
        input_file_path=gdp_raw_data_path,
        processing_callable=process_gdp_data,
        op_kwargs={'output_file_name': const.GDP_STATS_FILE_NAME}
    )

    internet_usage_raw_data_path = os.path.join(conf.DATALAKE_FOLDER_PATH, const.INTERNET_STATS_FILE_NAME)
    preprocess_internet_usage_data_op = FileProcessingOperator(
        task_id='preprocess_internet_usage_data',
        destination_folder_path=conf.PROCESSING_FOLDER_PATH,
        input_file_path=internet_usage_raw_data_path,
        processing_callable=process_internet_data,
        op_kwargs={'output_file_name': const.INTERNET_STATS_FILE_NAME}
    )

    suicide_raw_data_path = os.path.join(conf.DATALAKE_FOLDER_PATH, const.SUICIDE_STATS_FILE_NAME)
    preprocess_suicide_data_op = FileProcessingOperator(
        task_id='preprocess_suicide_data',
        destination_folder_path=conf.PROCESSING_FOLDER_PATH,
        input_file_path=suicide_raw_data_path,
        processing_callable=process_suicide_data,
        op_kwargs={'output_file_name': const.SUICIDE_STATS_FILE_NAME}
    )

    load_suicide_raw_data_to_stage_zone_op = CsvToPostgresOperator(
        task_id='load_suicide_raw_data_to_stage_zone',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSING_FOLDER_PATH, const.SUICIDE_STATS_FILE_NAME),
        destination_table_id='suicide_data_raw'
    )

    load_gdp_raw_data_to_stage_zone_op = CsvToPostgresOperator(
        task_id='load_gdp_raw_data_to_stage_zone',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSING_FOLDER_PATH, const.GDP_STATS_FILE_NAME),
        destination_table_id='country_data_raw'
    )

    load_internet_usage_raw_data_to_stage_zone_op = CsvToPostgresOperator(
        task_id='load_internet_usage_raw_data_to_stage_zone',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSING_FOLDER_PATH, const.INTERNET_STATS_FILE_NAME),
        destination_table_id='internet_usage_data_raw'
    )

    process_data_op = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        op_kwargs={
            'gdp_file_path': os.path.join(conf.PROCESSING_FOLDER_PATH, const.GDP_STATS_FILE_NAME),
            'suicide_file_path': os.path.join(conf.PROCESSING_FOLDER_PATH, const.SUICIDE_STATS_FILE_NAME),
            'internet_file_path': os.path.join(conf.PROCESSING_FOLDER_PATH, const.INTERNET_STATS_FILE_NAME),
            'output_folder_path': os.path.join(conf.PROCESSED_DATA_FOLDER_PATH)
        }
    )

    load_country_data_op = CsvToPostgresOperator(
        task_id='load_country_data',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'countries.csv'),
        destination_table_id='country'
    )

    load_age_category_data_op = CsvToPostgresOperator(
        task_id='load_age_category_data',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'age_categories.csv'),
        destination_table_id='ages'
    )

    load_sex_data_op = CsvToPostgresOperator(
        task_id='load_sex_data',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'sexes.csv'),
        destination_table_id='sex'
    )

    load_generations_data_op = CsvToPostgresOperator(
        task_id='load_generations_data',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'generations.csv'),
        destination_table_id='generation'
    )

    load_years_data_op = CsvToPostgresOperator(
        task_id='load_years_data',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'years.csv'),
        destination_table_id='year'
    )

    load_country_depending_facts_op = CsvToPostgresOperator(
        task_id='load_country_depending_facts',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'country_depending_facts.csv'),
        destination_table_id='country_depending_facts'
    )

    load_people_depending_facts_op = CsvToPostgresOperator(
        task_id='load_people_depending_facts',
        db_name=conf.POSTGRES_DB_NAME,
        host=conf.POSTGRES_HOST,
        port=conf.POSTGRES_PORT,
        postgres_username=conf.POSTGRES_USERNAME,
        postgres_password=conf.POSTGRES_PASSWORD,
        csv_file_path=os.path.join(conf.PROCESSED_DATA_FOLDER_PATH, 'people_depending_facts.csv'),
        destination_table_id='people_depending_facts'
    )

    download_and_unzip_internet_usage_stats_op >> preprocess_internet_usage_data_op >> load_internet_usage_raw_data_to_stage_zone_op >> process_data_op
    download_and_unzip_gdp_stats_op >> preprocess_gdp_data_op >> load_gdp_raw_data_to_stage_zone_op >> process_data_op
    download_and_unzip_suicide_stats_op >> preprocess_suicide_data_op >> load_suicide_raw_data_to_stage_zone_op >> process_data_op

    process_data_op >> [
        load_country_data_op, load_years_data_op, load_sex_data_op, load_generations_data_op, load_age_category_data_op
    ] >> load_people_depending_facts_op >> load_country_depending_facts_op

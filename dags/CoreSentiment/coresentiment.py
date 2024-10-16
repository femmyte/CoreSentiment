from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from CoreSentiment.includes.fetch_page import fetch_page
from CoreSentiment.includes.download_file import download_file
from CoreSentiment.includes.analysis import save_highest_pageviews_to_file
base_path = "/opt/airflow/dags/CoreSentiment/files"
local_filename = ''

with DAG(
    'coresentiment',
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date= days_ago(1),
    tags=['example'],
    template_searchpath="/opt/airflow/dags/sql/"
) as dag:
    
    download_zip_file = PythonOperator(
        task_id="download_zip_file",
        python_callable=download_file,
    )
    print('downloading zip files', local_filename)

    unzip_file = BashOperator(
       task_id="unzip_file",
       bash_command=f'gzip -dv /opt/airflow/dags/CoreSentiment/files/pageviews-20241012-130000.gz',
   )
    print('extracting files')

    fetch_page_views = PythonOperator(
        task_id = 'fetch_page_views',
        python_callable = fetch_page
    )
    print('fetching page views ans creating SQL file')

    create_table = PostgresOperator(
        task_id = 'create_table_new',
        postgres_conn_id = 'postgres_default',
        sql= "create_table_schema.sql",
    )

    load_data_into_db = PostgresOperator(
    task_id="load_data",
    postgres_conn_id="postgres_default",
    sql="coresentiment_schema.sql",
    )
    print('loading data into the database table')

    
    download_zip_file >> unzip_file >> fetch_page_views  >> create_table >> load_data_into_db


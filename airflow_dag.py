import datetime
import requests
import pandas as pd

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
LOCAL_FILE_PATH = "/tmp/yellow_tripdata_2022-01.parquet"
LOCAL_CSV_PATH = "/tmp/yellow_tripdata_2022-01.csv"

def download_parquet_file(url, local_path):
    try:        
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(local_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"File downloaded successfully: {local_path}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def parquet_to_csv(parquet_file, csv_file):
    try: 
        df = pd.read_parquet(parquet_file)
        df.to_csv(csv_file, index=False)
    except Exception as e:
        print(f"Error converting file: {e}")

with DAG(
     dag_id="nyc_yellow_taxi_trips",
     start_date=datetime.datetime(2022, 1, 1),
     schedule="@monthly",
) as dag:

    download_task = PythonOperator(
        task_id='download_parquet_data_file_from_url',
        python_callable=download_parquet_file,
        op_kwargs= {
            'url': PARQUET_URL,
            'local_path': LOCAL_FILE_PATH,
        },
    )

    convert_to_csv = PythonOperator(
        task_id='convert_parquet_to_csv',
        python_callable=parquet_to_csv,
        op_kwargs= {
            'parquet_file': LOCAL_FILE_PATH,
            'csv_file': LOCAL_CSV_PATH
        },
    )

    copy_csv_staging = SQLExecuteQueryOperator(
        task_id="copy_csv_staging",
        conn_id="postgres_connection",
        sql=f"""
                COPY nyc_taxi_staging
                FROM '{LOCAL_CSV_PATH}'
                WITH (format = 'csv', empty_string_as_null = true)
                RETURN SUMMARY;
            """,
    )


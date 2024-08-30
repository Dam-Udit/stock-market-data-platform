import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from datetime import timedelta
from utils.stock_data_utils import AlphaVantageAPI, SQLRepository
from utils.config import api_key, db_config


def run_stock_data_etl():
    stock_tickers = ["^BSESN", "^CRSMID", "^NSEI"]

    engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )

    for ticker in stock_tickers:
        api = AlphaVantageAPI(api_key)
        data = api.get_daily_data(ticker, '1m')
        try:
            repo = SQLRepository(engine)
            repo.insert_stock_data(ticker, data)
        except Exception as e:
            raise e


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'daily_stock_data',
    default_args=default_args,
    description='Fetch and store daily stock data',
    schedule_interval='@daily',
)

run_etl_task = PythonOperator(
    task_id='run_stock_data_etl',
    python_callable=run_stock_data_etl,
    dag=dag,
)

run_etl_task

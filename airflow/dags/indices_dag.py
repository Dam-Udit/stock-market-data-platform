from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from utils.index_data_utils import YahooFinanceAPI, SQLRepository
from utils.config import db_config
import joblib
from arch import arch_model


def extract(**kwargs):
    trading_symbols = ["^BSESN", "^CRSMID", "^NSEI"]
    api = YahooFinanceAPI()
    extracted_data = {}
    for index in trading_symbols:
        data = api.get_data(index, '1d')
        extracted_data[index] = data
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)


def load(**kwargs):
    engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    table_name_mapping = {
        "^BSESN": "sensex",
        "^CRSMID": "midcap100",
        "^NSEI": "nifty50"
    }
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data')
    for index, table_name in table_name_mapping.items():
        try:
            repo = SQLRepository(engine)
            extracted_data[index].reset_index(drop=True, inplace=True)
            repo.insert_data(
                table_name, extracted_data[index], if_exists='append')
        except Exception as e:
            raise e
    engine.dispose()


def train_updated_model(**kwargs):
    engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    try:
        repo = SQLRepository(engine)
        nifty50_df = repo.read_table('nifty50', limit=252)
        sensex_df = repo.read_table('sensex', limit=252)
        midcap100_df = repo.read_table('midcap100', limit=252)
    except Exception as e:
        raise e

    nifty50_model = arch_model(
        nifty50_df['returns'].dropna(), vol='Garch', p=1, q=1)
    fitted_nifty50_model = nifty50_model.fit(disp='off')

    sensex_model = arch_model(
        sensex_df['returns'].dropna(), vol='Garch', p=1, q=1)
    fitted_sensex_model = sensex_model.fit(disp='off')

    midcap100_model = arch_model(
        midcap100_df['returns'].dropna(), vol='Garch', p=1, q=1)
    fitted_midcap100_model = midcap100_model.fit(disp='off')

    joblib.dump(fitted_nifty50_model, "nifty50_garch_model.pkl")
    joblib.dump(fitted_sensex_model, "sensex_garch_model.pkl")
    joblib.dump(fitted_midcap100_model, "midcap100_garch_model.pkl")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'indices_data',
    default_args=default_args,
    description='Extract, Load, Analyse volatility of, and Visualize Indian indices data',
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

update_model_task = PythonOperator(
    task_id='update',
    python_callable=train_updated_model,
    dag=dag
)

extract_task >> load_task >> update_model_task

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime
import pandas as pd 
from datetime import datetime,timedelta
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_data_from_api():
    try:
        url = f'https://openexchangerates.org/api/latest.json?app_id=****'
        response = requests.get(url)
        data_forex = response.json()
        return data_forex
    except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
    
def transform_data():
    df = pd.read_csv('/opt/airflow/dags/files/forex_file.csv')
    data_forex = get_data_from_api()
    #dateig = pd.to_datetime('today').date()
    try:
        dataf = []
        for index,row in df.iterrows():
            for rates in row['pairs'].split(','):
                dt = datetime.fromtimestamp(data_forex['timestamp'])
                values = {'base_currency_id' : str(data_forex['base']),
                          'target_currency_id':str(rates),
                          'amount': data_forex['rates'][rates],
                          'data_time': dt,
                          'time_insert': pd.to_datetime('today')}
                dataf.append(values)
            dataf = pd.DataFrame(dataf)
            dataf.to_csv('/tmp/processed_forex.csv', index=None, header=False)
            #output = f"processed_forex_{dateig}.csv"
            dataf.to_csv(f'abfs://data@forexratesdl.dfs.core.windows.net/forex_rate.csv',
            storage_options={'account_name': 'forexratesdl',
                'account_key': '****'
            }, index=False)
    except Exception as e: 
        print(f"Error en el procesado de datos: {e}") 
        return None
    
def _store_user():
    hook = PostgresHook(postgres_conn_id='postgresown')
    hook.copy_expert(
        sql="COPY forex_rates FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_forex.csv'
    )
        
with DAG('forex_data',start_date=datetime(2024,4,16),schedule_interval="@daily",
         default_args=default_args, catchup = False) as dag:
    
    api_available = HttpSensor(
        task_id = 'api_available',
        http_conn_id = 'forex_api',
        endpoint = 'api/latest.json?app_id=0546829a7f204fd7a870cefa722001c1',
        response_check = lambda response: 'rates' in response.text,
        poke_interval =5,
        timeout = 20 
    )

    forex_file_available = FileSensor(
        task_id = 'forex_file_available',
        fs_conn_id = 'forex_path',
        filepath ='forex_file.csv',
        poke_interval =5,
        timeout = 20 
    )

    get_process =  PythonOperator(
        task_id='get_process',
        python_callable=transform_data
        )
    
    create_table_monedas = PostgresOperator(
        task_id='create_table_monedas',
        postgres_conn_id='postgresown',
        sql='''
            CREATE TABLE IF NOT EXISTS monedas(
            codigo_id VARCHAR(10) PRIMARY KEY
            );
            '''
        )
    
    create_table_forex = PostgresOperator(
        task_id='create_table_forex',
        postgres_conn_id='postgresown',
        sql='''
            CREATE TABLE IF NOT EXISTS forex_rates(
            base_currency_id VARCHAR(10),
            target_currency_id VARCHAR(10),
            amount NUMERIC,
            data_time TIMESTAMP,
            time_insert TIMESTAMP,
            FOREIGN KEY (base_currency_id) REFERENCES monedas(codigo_id),
            FOREIGN KEY (target_currency_id) REFERENCES monedas(codigo_id)
            );
        '''
    )

    store_forex = PythonOperator(
        task_id='store_forex',
        python_callable=_store_user
    )
 
api_available >> forex_file_available >> [create_table_monedas,create_table_forex] >> get_process >> store_forex
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from include.stock_market.tasks import _get_stock_prices , _store_prices
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException


SYMBOL = 'AAPL'

@dag(
    start_date=datetime(2024,1,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)

def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        
        #VERIFICAR SE O LINK, CONEXÃO CHAMADA ABAIXO, ESTÁ DISPOÍVEL


        session = requests.Session()
        # retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        # adapter = HTTPAdapter(max_retries=retry)
        # session.mount('https://', adapter)

        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)

        try:
            response = session.get(url, headers=api.extra_dejson['headers'])
            print('OOOKKKK!')
            response.raise_for_status()

            # Imprime o status code e o conteúdo da resposta
            print(f"Status Code: {response.status_code}")
            print(response.text)

        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  # Exceções de HTTP
        except ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")  # Erros de conexão
        except Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")  # Erro de timeout
        except RequestException as req_err:
            print(f"An error occurred: {req_err}")  # Qualquer outro erro de requisição
        
        condition = True

        # response = session.get(url, headers=api.extra_dejson['headers'])
        # response = requests.get(url, headers=api.extra_dejson['headers'])
        # condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{task_instance.xcom_pull(task_ids="is_api_available")}}', 'symbol': SYMBOL}
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}
    )
    
    is_api_available() >> get_stock_prices >> store_prices

stock_market()
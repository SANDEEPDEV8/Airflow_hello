from datetime import datetime
import json
import pandas as pd

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _process_user(ti):
    # ti stands for `task instance`
    user = ti.xcom_pull(task_ids='extract_user')
    user = user['results'][0]
    processed_user = pd.json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)


def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


with DAG(
    dag_id='user_processing', 
    start_date=datetime(2023, 4, 28), 
    schedule_interval='@daily',
    catchup=False, # by default its True, means that now and start date your dagg hasnt been triggered, as soon as you start scheduling your datapipeline in airflow UI, you are going to catch up and you run all non trigerred dag runs betweeen this time. ie, betweeen now and start date or the last time dag as been trigerred. so better keep false
) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        # for connecttion goto airflow>admin>connections
        # create Connection Id : postgres, Connection Type : Postgres, Host : postgres, Login : airflow, Password : airflow, port : 5432
        # you can create a connection like this for postgres, dbt, aws,...etc
        sql='sql/create_table.sql',
    )
    
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        # connection user_api | Name: user_api, Connection type: HTTP, Host: https://randomuser.me/
        endpoint='api/'
    )
    
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )

    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user 

# EXECUTION
# docker exec -it <airflow-scheduler-name>
# docker exec -it materials_airflow-scheduler_1 /bin/bash
# // now you will be inside container "/opt/airflow$>>

# airflow -h  #see all commands
# airflow tasks test <dag-id> <task-id> <date>
# airflow tasks test user_processing create_table 2022-01-01
# // if executed it shows success

# CHECK FILE 
# docker-compose ps
# docker exec -it materials_airflow-worker_1 /bin/bash
## inside container

# ls /tmp/
## this contains file processed_user.csv


# CHECK POSTGRES FOR USERS DATA
# docker exec -it materials_postgres_1 /bin/bash
## inside container
# psql -Uairflow
# SELECT * FROM users;
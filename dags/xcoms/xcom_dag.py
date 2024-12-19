from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
# def _t1():
#     return 42 # creates an XCom with key='return_value' and value=42

# OR
def _t1(ti): # ti is the task instance
    ti.xcom_push(key='my_key', value=42)
 
def _t2(ti):
    print(ti.xcom_pull(key='my_key', task_ids='t1'))

def _branch(ti):
    value = ti.xcom_pull(key="my_key", task_ids='t1')
    if(value == 42):
        return 't2'
    return 't3'
 
with DAG("xcom_dag", start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    branch = BranchPythonOperator( # this conditionally returns the task ids of next task to execute
        task_id='branch',
        # python_callable=lambda: 't3' if 42 > 1 else 't2'
        python_callable=_branch
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )

    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''"
    )
 
    # t1 >> t2 >> t3
    t1 >> branch >> [t2, t3] > t4 # branch will choose whether to run t2 or t3 based on the value of t1 conditon
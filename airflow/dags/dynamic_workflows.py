from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime, timedelta
#from functions import * # getReqAemet, getPredictionsAemetHourly
import os

# Update the default arguments and apply them to the DAG.
# You can override them on a per-task basis during operator initialization.
default_args = {
    'start_date': datetime(2021,12,7),
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [os.environ['AIRFLOW_TO_EMAIL']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'

    # CUSTOM
    'aemet_api_key': os.environ['AEMET_API_KEY'],
    'target_datalake': os.environ["AIRFLOW_TARGET_DATALAKE"],
    'aemet_codigos': [11010, 26002, 24014, 45157, 18003, 18153, 45054, 45199, 23003, 28022, 45099, 45031, 45180, 45038],
}


def values_function(**kwargs):
    target_folder = kwargs['target_datalake'] + "/transformed/aemet/predictions/hourly/" + kwargs["ts_nodash"]
    # Get number of files
    values = os.listdir(target_folder)
    return values


def group(number, **kwargs):
    #load the values if needed in the command you plan to execute
    dyn_value = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
    return BashOperator(
            task_id='JOB_NAME_{}'.format(number),
            bash_command='script.sh {} {}'.format(dyn_value, number),
            dag=dag)


'''with DAG(dag_id='workflows_dynamic_test_1', default_args=default_args) as dag:

    push_func = PythonOperator(
        task_id='push_func',
        provide_context=True,
        python_callable=values_function,
        dag=dag)

    complete = DummyOperator(
            task_id='All_jobs_completed',
            dag=dag)

    for i in values_function():
            push_func >> group(i) >> complete'''


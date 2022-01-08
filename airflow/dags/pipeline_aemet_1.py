from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime, timedelta
from functions import * # getReqAemet, getPredictionsAemetHourly
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

with DAG(dag_id='etl_aemet_1', default_args=default_args) as dag:

    sensor = FileSensor(task_id='sense_file', 
                        filepath=default_args['target_datalake'] + '/raw/aemet/startprocess.txt',
                        poke_interval=45,
                        dag=dag)

    '''bash_task = BashOperator(task_id='cleanup_tempfiles', 
                            bash_command='rm -f /home/repl/*.tmp',
                            dag=dag)'''

    # python_hello = PythonOperator(task_id='python_hello', python_callable=my_func)

    python_task_get = PythonOperator(task_id='get_aemet_hourly_predictions', 
                                python_callable=getPredictionsAemetHourly,
                                provide_context=True,
                                op_kwargs=default_args,
                                dag=dag)
    
    python_task_transform = PythonOperator(task_id='transform_aemet_hourly_predictions', 
                                python_callable=transformPredictionsAemetHourly,
                                provide_context=True,
                                op_kwargs=default_args,
                                dag=dag)

    '''print_context = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
        provide_context=True,
        dag=dag,
    )'''

    email_subject="""
    Aemet files ingested in datalake on {{ ds_nodash }} {{ ts_nodash }}
    """

    '''email_notif_task = EmailOperator(task_id='email_notif_task',
                                    to=os.environ['AIRFLOW_TO_EMAIL'],
                                    subject=email_subject,
                                    html_content='Contenido',
                                    dag=dag)'''


    '''email_report_task = EmailOperator(task_id='email_report_task',
                                    to='sales@mycompany.com',
                                    subject=email_subject,
                                    html_content='',
                                    params={'department': 'Data subscription services'},
                                    dag=dag)


    no_email_task = DummyOperator(task_id='no_email_task', dag=dag)


    def check_weekend(**kwargs):
        dt = datetime.strptime(kwargs['execution_date'],"%Y-%m-%d")
        # If dt.weekday() is 0-4, it's Monday - Friday. If 5 or 6, it's Sat / Sun.
        if (dt.weekday() < 5):
            return 'email_report_task'
        else:
            return 'no_email_task'

        
    branch_task = BranchPythonOperator(task_id='check_if_weekend',
                                    python_callable=check_weekend,
                                    provide_context=True,
                                    dag=dag)
    '''

    #### Order of tasks

    #sensor >> bash_task >> python_task
    #python_task >> branch_task >> [email_report_task, no_email_task]

    sensor >> python_task_get >> python_task_transform

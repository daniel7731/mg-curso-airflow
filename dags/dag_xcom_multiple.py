#/**
#Crear un DAG llamado `dag_xcom_multiple` que:
#
#* Tenga una tarea `generar_datos` que envíe dos valores distintos usando `xcom_push` con las claves `"mensaje"` y `"codigo"`.
#* Tenga una tarea `mostrar_datos` que recupere ambos valores con `xcom_pull` e imprima un mensaje combinado.

#/
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def generar_datos(**kwargs):
    kwargs['ti'].xcom_push(key='mensaje', value='dag_xcom_multiple :mensaje')
    kwargs['ti'].xcom_push(key='codigo', value='dag_xcom_multiple  :200')
def mostrar_datos(**kwargs):
    mensaje = kwargs['ti'].xcom_pull(task_ids='xcom_multiple_generar_datos', key='mensaje')
    mensaje = kwargs['ti'].xcom_pull(task_ids='xcom_multiple_mostar_datos', key='codigo')
    print(f"Mensaje recibido: {mensaje}")

with DAG('dag_xcom_multiple', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    generar_datos_task = PythonOperator(task_id='xcom_multiple_generar_datos', python_callable=generar_datos, provide_context=True)
    mostrar_datos_task = PythonOperator(task_id='xcom_multiple_mostar_datos', python_callable=mostrar_datos , provide_context=True)
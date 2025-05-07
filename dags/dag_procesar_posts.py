from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def descargar_posts(**kwargs):
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    if response.status_code == 200:
        posts = response.json()
        titles =[]
        for post in posts:
            print(post)
            titles.append(post['title'])
        kwargs['ti'].xcom_push(key='posts', value=titles)
    else:
        raise Exception(f"Error al obtener los posts: {response.status_code}")

def mostrar_titulos(**kwargs):
    titles = kwargs['ti'].xcom_pull(task_ids='descargar_posts', key='posts')
    print("Títulos de los primeros 5 posts:")
    for title in titles[:5]:
        print(f"- {title}")

with DAG('dag_procesar_posts',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='descargar_posts', python_callable=descargar_posts, provide_context=True)
    t2 = PythonOperator(task_id='mostrar_titulos', python_callable=mostrar_titulos, provide_context=True)

    t1 >> t2
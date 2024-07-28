'''
Program ini dibuat untuk mengakses postgre SQL menggunakan Airflow untuk mengambil data.
Kemudian dilanjutkan dengan cleaning data yang akan diupload ke Elastic Search. 
Data yang sudah diupload nantinya akan dilakukan analisa EDA dengan Kibana
=================================================
'''



from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
import pandas as pd


# Fetch from Postgresql
def connect_df():
    # Airflow Connection
    dfbase = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Connection to Postgres
    postgres_engine = f"postgresql+psycopg2://{username}:{password}@{host}/{dfbase}"

    # Connect to SQL Alchemy
    engine = create_engine(postgres_engine)
    conn = engine.connect()
    df = pd.read_sql_query("SELECT * FROM public.table_m3", conn) 
    df.to_csv("/opt/airflow/dags/P2M3_Darly_Purba_df_raw.csv", sep=',', index=False)

# Data Cleaning
def preprocessing(): 
    # Read df
    df = pd.read_csv("/opt/airflow/dags/P2M3_Darly_Purba_df_raw.csv")
    
    # Cleaning df 
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('/', '_')  
    df.columns = df.columns.str.replace('-', '_')
    df['id'] = df['class'].astype(str) + df['flight_distance'].astype(str) + df.index.astype(str)  
    df.to_csv("/opt/airflow/dags/P2M3_Darly_Purba_df_clean.csv", index=False)

# Post to Elasticsearch
def insertElasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_Darly_Purba_df_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response: {res}")

default_args = {
    'owner': 'Darly Purba',
    'start_date': datetime(2024, 6, 22, 13, 30, 0) - timedelta(hours=7),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    "P2M3_Darly_Purba_DAG", 
    description='Milestone_3', 
    schedule_interval='30 6 * * *', 
    default_args=default_args,
) as dag: 
    
    # Task 1
    connect_df_task = PythonOperator(
        task_id='connect',
        python_callable=connect_df
    ) 
    
    # Task 2
    preprocessing_df_task = PythonOperator(
        task_id='preprocessing',
        python_callable=preprocessing
    )

    # Task 3
    insert_df_task = PythonOperator(
        task_id='insert_df_elastic',
        python_callable=insertElasticsearch
    )

# Running Airflow
connect_df_task >> preprocessing_df_task >> insert_df_task

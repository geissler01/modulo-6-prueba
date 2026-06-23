from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importamos las tareas modulares
from test_connections import test_kaggle_connection, test_postgres_connection
from setup_db import setup_data_warehouse
from extract import download_kaggle_dataset, ingest_to_bronze

# =========================================================================
# CONFIGURACIÓN DEL DAG
# =========================================================================
default_args = {
    'owner': 'datamart_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# =========================================================================
# DEFINICIÓN DEL DAG
# =========================================================================
with DAG(
    'etl_datamart_pipeline',
    default_args=default_args,
    description='Pipeline ETL - Fases: Conexiones -> Setup -> Extract -> Bronze',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['datamart', 'etl', 'medallion'],
) as dag:

    # ---------------------------------------------------------
    # 1. FASE DE PRUEBAS Y APROVISIONAMIENTO
    # ---------------------------------------------------------
    task_test_kaggle = PythonOperator(
        task_id='test_kaggle_connection',
        python_callable=test_kaggle_connection,
    )
    
    task_test_postgres = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )
    
    task_setup_dwh = PythonOperator(
        task_id='setup_data_warehouse',
        python_callable=setup_data_warehouse,
    )

    # ---------------------------------------------------------
    # 2. FASE DE EXTRACCIÓN (Kaggle -> Servidor)
    # ---------------------------------------------------------
    task_download_ecommerce = PythonOperator(
        task_id='download_ecommerce_dataset',
        python_callable=download_kaggle_dataset,
        op_kwargs={'dataset_name': 'carrie1/ecommerce-data'}
    )
    
    task_download_retail = PythonOperator(
        task_id='download_retail_dataset',
        python_callable=download_kaggle_dataset,
        op_kwargs={'dataset_name': 'lakshmi25npathi/online-retail-dataset'}
    )

    # ---------------------------------------------------------
    # 3. FASE DE INGESTA BRONZE (Servidor -> PostgreSQL)
    # ---------------------------------------------------------
    task_ingest_ecommerce = PythonOperator(
        task_id='ingest_bronze_ecommerce',
        python_callable=ingest_to_bronze,
        op_kwargs={
            'dataset_name': 'carrie1/ecommerce-data',
            'table_name': 'ecommerce_data'
        }
    )
    
    task_ingest_retail = PythonOperator(
        task_id='ingest_bronze_retail',
        python_callable=ingest_to_bronze,
        op_kwargs={
            'dataset_name': 'lakshmi25npathi/online-retail-dataset',
            'table_name': 'online_retail'
        }
    )

    # =========================================================================
    # ORQUESTACIÓN (DEPENDENCIAS)
    # =========================================================================
    # Pruebas y Setup Secuencial
    task_test_kaggle >> task_test_postgres >> task_setup_dwh
    
    # Las descargas inician en paralelo después del Setup
    task_setup_dwh >> [task_download_ecommerce, task_download_retail]
    
    # Cada ingesta depende de su respectiva descarga
    task_download_ecommerce >> task_ingest_ecommerce
    task_download_retail >> task_ingest_retail

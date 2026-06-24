from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Importamos las tareas modulares
from test_connections import test_kaggle_connection, test_postgres_connection
from setup_db import setup_data_warehouse
from extract import download_kaggle_dataset, ingest_to_bronze
from eda import generate_ydata_profile, generate_manual_eda
from transform_silver import transform_ecommerce, transform_retail, unify_silver

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
    description='Pipeline ETL - Fases: Conexiones -> Setup -> Extract -> Bronze -> EDA',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['datamart', 'etl', 'medallion', 'eda'],
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

    # ---------------------------------------------------------
    # 4. FASE DE ANÁLISIS EXPLORATORIO (EDA)
    # ---------------------------------------------------------
    # Ecommerce Data
    task_eda_ydata_ecommerce = PythonOperator(
        task_id='eda_ydata_ecommerce',
        python_callable=generate_ydata_profile,
        op_kwargs={'table_name': 'ecommerce_data'}
    )
    
    task_eda_manual_ecommerce = PythonOperator(
        task_id='eda_manual_ecommerce',
        python_callable=generate_manual_eda,
        op_kwargs={'table_name': 'ecommerce_data'}
    )

    # Online Retail Data
    task_eda_ydata_retail = PythonOperator(
        task_id='eda_ydata_retail',
        python_callable=generate_ydata_profile,
        op_kwargs={'table_name': 'online_retail'}
    )
    
    task_eda_manual_retail = PythonOperator(
        task_id='eda_manual_retail',
        python_callable=generate_manual_eda,
        op_kwargs={'table_name': 'online_retail'}
    )

    # ---------------------------------------------------------
    # 5. FASE SILVER (Transformación y Unificación)
    # ---------------------------------------------------------
    task_silver_ecommerce = PythonOperator(
        task_id='transform_silver_ecommerce',
        python_callable=transform_ecommerce,
    )
    
    task_silver_retail = PythonOperator(
        task_id='transform_silver_retail',
        python_callable=transform_retail,
    )
    
    task_silver_unify = PythonOperator(
        task_id='unify_silver_transactions',
        python_callable=unify_silver,
    )

    # ---------------------------------------------------------
    # 6. FASE GOLD (Modelado Dimensional con dbt)
    # ---------------------------------------------------------
    task_dbt_run = BashOperator(
        task_id='run_dbt_gold_models',
        bash_command='dbt run --project-dir /opt/airflow/dags/dbt_datamart --profiles-dir /opt/airflow/dags/dbt_datamart',
        env={
            'DBT_HOST': '{{ conn.results_postgres_db.host }}',
            'DBT_USER': '{{ conn.results_postgres_db.login }}',
            'DBT_PASS': '{{ conn.results_postgres_db.password }}',
            'DBT_PORT': '{{ conn.results_postgres_db.port }}',
            'DBT_DBNAME': 'db_geisler_prueba'
        }
    )

    # =========================================================================
    # ORQUESTACIÓN (DEPENDENCIAS)
    # =========================================================================
    
    # --- FLUJO DE INGESTA (Comentado temporalmente para desarrollo ágil) ---
    # task_test_kaggle >> task_test_postgres >> task_setup_dwh
    # task_setup_dwh >> [task_download_ecommerce, task_download_retail]
    # task_download_ecommerce >> task_ingest_ecommerce
    # task_download_retail >> task_ingest_retail
    # task_ingest_ecommerce >> [task_eda_ydata_ecommerce, task_eda_manual_ecommerce]
    # task_ingest_retail >> [task_eda_ydata_retail, task_eda_manual_retail]

    # --- FLUJO DE DESARROLLO ACTUAL ---
    # Ejecutamos las tareas de EDA de forma ESTRICTAMENTE SECUENCIAL
    # para evitar saturar la memoria RAM y las conexiones a Postgres.
    task_eda_manual_ecommerce >> task_eda_ydata_ecommerce >> task_eda_manual_retail >> task_eda_ydata_retail
    
    # Posteriormente corremos la limpieza a Silver
    # Aquí SÍ aprovechamos el paralelismo ya que cada tarea escribe en su propia tabla temporal
    task_eda_ydata_retail >> [task_silver_ecommerce, task_silver_retail]
    [task_silver_ecommerce, task_silver_retail] >> task_silver_unify
    
    # Finalmente, corremos el modelo de dbt
    task_silver_unify >> task_dbt_run

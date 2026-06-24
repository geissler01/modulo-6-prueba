from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# Importamos las tareas modulares
from test_connections import test_kaggle_connection, test_postgres_connection
from setup_db import setup_data_warehouse
from extract import download_kaggle_dataset, ingest_to_bronze
from eda import generate_ydata_profile, generate_manual_eda
from transform_silver import transform_ecommerce, transform_retail, unify_silver
from business_queries import run_business_queries

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
        op_kwargs={'dataset_name': 'thedevastator/online-retail-transaction-data'}
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
            'dataset_name': 'thedevastator/online-retail-transaction-data',
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
        bash_command="""
            export DBT_HOST='{{ conn.results_postgres_db.host }}'
            export DBT_USER='{{ conn.results_postgres_db.login }}'
            export DBT_PASS='{{ conn.results_postgres_db.password }}'
            export DBT_PORT='{{ conn.results_postgres_db.port }}'
            export DBT_DBNAME='db_geisler_prueba'
            
            dbt run --project-dir /opt/airflow/dbt_datamart --profiles-dir /opt/airflow/dbt_datamart
        """
    )

    # ---------------------------------------------------------
    # 7. FASE DE CONSULTAS DE NEGOCIO (Business Queries)
    # ---------------------------------------------------------
    task_business_queries = PythonOperator(
        task_id='run_business_queries',
        python_callable=run_business_queries,
    )

    # =========================================================================
    # ORQUESTACIÓN (DEPENDENCIAS)
    # =========================================================================
    
    # 1. Pruebas -> Configuración de BD
    [task_test_kaggle, task_test_postgres] >> task_setup_dwh

    # 2. Configuración de BD -> Descargas Paralelas
    task_setup_dwh >> [task_download_ecommerce, task_download_retail]

    # 3. Descarga -> Ingesta (Se paralelizan individualmente)
    task_download_ecommerce >> task_ingest_ecommerce
    task_download_retail >> task_ingest_retail

    # 4. Ingesta -> EDA (Ejecución estrictamente secuencial para cuidar la memoria)
    task_ingest_ecommerce >> task_eda_manual_ecommerce >> task_eda_ydata_ecommerce
    task_ingest_retail >> task_eda_manual_retail >> task_eda_ydata_retail

    # Evitamos que los EDAs se crucen
    task_eda_ydata_ecommerce >> task_eda_manual_retail

    # 5. EDA -> Limpieza Silver (Se ejecuta cuando acaben las dos tareas EDA anteriores)
    task_eda_ydata_ecommerce >> task_silver_ecommerce
    task_eda_ydata_retail >> task_silver_retail

    # Evitamos que Silver se cruce con el EDA del otro dataset
    task_eda_ydata_retail >> task_silver_ecommerce 

    # 6. Silver -> Unificación (Espera a que acaben las dos transformaciones Silver)
    [task_silver_ecommerce, task_silver_retail] >> task_silver_unify
    
    # 7. Unificación -> Modelado Dimensional dbt
    task_silver_unify >> task_dbt_run

    # 8. DBT (Gold Layer) -> Consultas de Reglas de Negocio
    task_dbt_run >> task_business_queries

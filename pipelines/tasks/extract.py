import os
import io
import csv
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
SOURCES_DIR = Path("/opt/airflow/sources")

def setup_kaggle_credentials():
    """Configura las credenciales de Kaggle en el entorno desde Airflow Variables."""
    os.environ['KAGGLE_USERNAME'] = Variable.get("kaggle_username", default_var="")
    os.environ['KAGGLE_KEY'] = Variable.get("kaggle_api_token", default_var="")

def download_kaggle_dataset(dataset_name: str):
    """
    Descarga un dataset desde Kaggle, lo descomprime y retorna la ruta del directorio.
    """
    logger.info("======================================================")
    logger.info(f"Iniciando descarga del dataset: {dataset_name}")
    setup_kaggle_credentials()
    
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    
    # Extraer nombre base (ej. ecommerce-data)
    dataset_folder = dataset_name.split('/')[1]
    dest_path = SOURCES_DIR / dataset_folder
    dest_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Descargando archivos en {dest_path}...")
    api.dataset_download_files(dataset_name, path=str(dest_path), unzip=True)
    logger.info(f"Descarga de {dataset_name} completada exitosamente.")
    logger.info("======================================================")
    
    return str(dest_path)

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Ejecuta el comando COPY de PostgreSQL en lugar de múltiples INSERTs.
    Mucho más veloz y escalable para DataFrames masivos.
    """
    # Obtiene la conexión subyacente de la base de datos
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        
        columns = ', '.join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name
            
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)

def ingest_to_bronze(dataset_name: str, table_name: str):
    """
    Busca todos los archivos iterativamente, los lee detectando si son CSV o Excel,
    y los inserta masivamente en el esquema bronze.
    """
    logger.info("======================================================")
    logger.info(f"Iniciando ingesta a Bronze para la tabla: {table_name}")
    dataset_folder = dataset_name.split('/')[1]
    source_path = SOURCES_DIR / dataset_folder
    
    # Buscar todos los archivos de datos
    data_files = list(source_path.glob("*.csv")) + list(source_path.glob("*.xlsx"))
    if not data_files:
        raise FileNotFoundError(f"No se encontró ningún archivo CSV o Excel en {source_path}")
    
    # Conexión dinámica a PostgreSQL
    hook = PostgresHook(postgres_conn_id="results_postgres_db")
    conn_obj = hook.get_connection("results_postgres_db")
    
    db_name = "db_geisler_prueba"
    sqlalchemy_uri = f"postgresql://{conn_obj.login}:{conn_obj.password}@{conn_obj.host}:{conn_obj.port}/{db_name}"
    engine = create_engine(sqlalchemy_uri)
    
    is_first_file = True
    
    for file_path in data_files:
        logger.info(f"Procesando archivo: {file_path.name}")
        
        # Detectar el formato y cargar a Pandas
        if file_path.suffix == '.csv':
            try:
                df = pd.read_csv(file_path)
            except UnicodeDecodeError:
                logger.warning("Fallo al leer CSV en UTF-8, intentando con ISO-8859-1...")
                df = pd.read_csv(file_path, encoding='ISO-8859-1')
        elif file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path)
            
        logger.info(f"[{file_path.name}] Leídas {df.shape[0]} filas, {df.shape[1]} columnas.")
        
        # Idempotencia por ejecución: el 1er archivo reemplaza la tabla, el resto se anexa
        insert_mode = 'replace' if is_first_file else 'append'
        
        logger.info(f"Volcando a base de datos (modo={insert_mode}) usando COPY hyper-rápido...")
        
        df.to_sql(
            name=table_name,
            con=engine,
            schema='bronze',
            if_exists=insert_mode,
            index=False,
            method=psql_insert_copy
        )
        
        is_first_file = False
        logger.info(f"[{file_path.name}] Volcado completo.")
    
    logger.info(f"¡Ingesta exitosa y unificada en la tabla bronze.{table_name}!")
    logger.info("======================================================")

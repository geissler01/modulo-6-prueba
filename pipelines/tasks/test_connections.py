import logging
import os
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configurar logger para que salga formateado en la UI de Airflow
logger = logging.getLogger(__name__)

def test_kaggle_connection():
    """
    Verifica que las credenciales de Kaggle cargadas en Airflow Variables
    funcionen correctamente y puedan autenticarse contra la API.
    """
    logger.info("======================================================")
    logger.info("Iniciando prueba de conexión con Kaggle API...")
    try:
        # 1. Extraer variables
        username = Variable.get("kaggle_username", default_var=None)
        key = Variable.get("kaggle_api_token", default_var=None)
        
        if not username or not key:
            raise ValueError("Las credenciales (kaggle_username o kaggle_api_token) no existen en Airflow.")
            
        logger.info(f"Credenciales encontradas en Airflow. Probando login para usuario: {username}")
        
        # 2. Configurar entorno y autenticar
        os.environ['KAGGLE_USERNAME'] = username
        os.environ['KAGGLE_KEY'] = key
        
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        
        logger.info("¡ÉXITO! La autenticación en Kaggle fue válida y permitida.")
        logger.info("======================================================")
        
    except Exception as e:
        logger.error("¡ERROR! Falló la conexión con Kaggle.")
        logger.error(str(e))
        logger.info("======================================================")
        raise

def test_postgres_connection():
    """
    Verifica la conexión a la base de datos Data Warehouse (Postgres)
    utilizando el Connection String configurado en .env.global.
    """
    logger.info("======================================================")
    logger.info("Iniciando prueba de conexión con PostgreSQL (Data Warehouse)...")
    try:
        # El hook lee automáticamente la conexión que se llama 'results_postgres_db'
        # Airflow la mapea desde AIRFLOW_CONN_RESULTS_POSTGRES_DB
        hook = PostgresHook(postgres_conn_id="results_postgres_db")
        
        # Hacer una consulta mínima para confirmar que hay comunicación
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        logger.info("Lanzando query de prueba: 'SELECT 1;'")
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        
        logger.info(f"¡ÉXITO! Conexión a PostgreSQL establecida correctamente. Resultado DB: {result}")
        logger.info("======================================================")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        logger.error("¡ERROR! Falló la conexión con la base de datos Postgres.")
        logger.error(str(e))
        logger.info("======================================================")
        raise

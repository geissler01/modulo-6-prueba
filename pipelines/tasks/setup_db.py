import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def setup_data_warehouse():
    """
    Se conecta a la instancia de PostgreSQL, crea la base de datos analítica 
    y provisiona los esquemas de la arquitectura Medallón.
    """
    logger.info("======================================================")
    logger.info("Iniciando aprovisionamiento del Data Warehouse...")
    
    # 1. Conexión a la BD por defecto para crear la nueva BD
    try:
        hook = PostgresHook(postgres_conn_id="results_postgres_db")
        conn = hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()
        
        db_name = "db_geisler_prueba"
        
        # Verificar si existe para mantener la idempotencia
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if not cursor.fetchone():
            logger.info(f"Creando base de datos: {db_name}...")
            cursor.execute(f"CREATE DATABASE {db_name};")
            logger.info(f"¡ÉXITO! Base de datos {db_name} creada.")
        else:
            logger.info(f"La base de datos {db_name} ya existe. Saltando creación.")
            
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Fallo al crear la base de datos: {str(e)}")
        raise
        
    # 2. Conectarse a la NUEVA base de datos para crear los esquemas
    try:
        # Extraemos las credenciales originales para forzar una conexión directa a la nueva BD
        conn_obj = hook.get_connection("results_postgres_db")
        
        import psycopg2
        new_conn = psycopg2.connect(
            host=conn_obj.host,
            port=conn_obj.port,
            user=conn_obj.login,
            password=conn_obj.password,
            dbname=db_name
        )
        new_conn.autocommit = True
        new_cursor = new_conn.cursor()
        
        schemas = ['bronze', 'silver', 'gold']
        for schema in schemas:
            logger.info(f"Verificando/Creando esquema: {schema}...")
            new_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            
        logger.info("¡ÉXITO! Todos los esquemas de la arquitectura Medallón fueron provisionados.")
        new_cursor.close()
        new_conn.close()
        
    except Exception as e:
        logger.error(f"Fallo al crear los esquemas en {db_name}: {str(e)}")
        raise
        
    logger.info("Aprovisionamiento finalizado correctamente.")
    logger.info("======================================================")

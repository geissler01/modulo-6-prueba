import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def _get_engine():
    """Retorna el engine de SQLAlchemy para conectarse a db_geisler_prueba."""
    hook = PostgresHook(postgres_conn_id="results_postgres_db")
    conn_obj = hook.get_connection("results_postgres_db")
    db_name = "db_geisler_prueba"
    sqlalchemy_uri = f"postgresql://{conn_obj.login}:{conn_obj.password}@{conn_obj.host}:{conn_obj.port}/{db_name}"
    return create_engine(sqlalchemy_uri)

def _read_from_bronze(table_name: str) -> pd.DataFrame:
    """Lee una tabla cruda de la capa Bronze."""
    engine = _get_engine()
    logger.info(f"Leyendo bronze.{table_name}...")
    df = pd.read_sql_table(table_name, con=engine, schema='bronze')
    return df

def _save_to_silver(df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
    """Guarda un DataFrame en la capa Silver de Postgres."""
    if df.empty:
        logger.info(f"No hay datos para guardar en silver.{table_name}")
        return
        
    engine = _get_engine()
    logger.info(f"Guardando {len(df)} registros en silver.{table_name}...")
    df.to_sql(table_name, con=engine, schema='silver', if_exists=if_exists, index=False)

def _apply_business_rules(df: pd.DataFrame, source_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica las reglas de negocio de la capa Silver:
    Retorna (df_valido, df_rechazado)
    """
    logger.info(f"Aplicando reglas de negocio a {source_name}...")
    
    # 1. Homologación de Nombres de Columnas
    # Si la tabla es Retail, las columnas pueden llamarse 'Invoice', 'Price', 'Customer ID'
    # Las estandarizaremos todas al formato de ecommerce-data
    column_mapping = {
        'Invoice': 'invoice_no',
        'InvoiceNo': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'Quantity': 'quantity',
        'InvoiceDate': 'invoice_date',
        'Price': 'unit_price',
        'UnitPrice': 'unit_price',
        'Customer ID': 'customer_id',
        'CustomerID': 'customer_id',
        'Country': 'country'
    }
    df = df.rename(columns=column_mapping)
    
    # Forzar lowercase total en las cabeceras por buenas prácticas en BBDD
    df.columns = [c.lower() for c in df.columns]

    # Eliminar columna 'index' si viene arrastrada desde el dataset original (como pasa en el Retail)
    if 'index' in df.columns:
        df = df.drop(columns=['index'])

    # 2. Fechas a UTC
    df['invoice_date'] = pd.to_datetime(df['invoice_date'], utc=True)
    
    # 3. Normalización de Strings
    df['stock_code'] = df['stock_code'].astype(str).str.upper().str.strip()
    df['description'] = df['description'].astype(str).str.upper().str.strip()
    
    # 4. Manejo de Nulos en Customer ID
    df['is_guest_customer'] = df['customer_id'].isnull()
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    
    # 5. Regla: Identificar devoluciones
    # Las devoluciones tienen cantidad <= 0
    df['is_return'] = df['quantity'] <= 0
    
    # 6. Linaje
    df['source_dataset'] = source_name
    
    # 7. Regla: Rechazar precios <= 0
    valid_mask = df['unit_price'] > 0
    
    df_valid = df[valid_mask].copy()
    df_rejected = df[~valid_mask].copy()
    
    # Añadir campos de auditoría a los rechazados
    if not df_rejected.empty:
        df_rejected['rejection_reason'] = "Precio unitario menor o igual a cero"
        df_rejected['rejection_date'] = pd.Timestamp.now(tz='UTC')
        
    logger.info(f"{source_name} -> Válidos: {len(df_valid)}, Rechazados: {len(df_rejected)}")
    
    return df_valid, df_rejected

def transform_ecommerce():
    df_raw = _read_from_bronze("ecommerce_data")
    df_clean, df_rejected = _apply_business_rules(df_raw, "ecommerce_data")
    
    # Guardamos el limpio en STG
    _save_to_silver(df_clean, "stg_ecommerce", if_exists='replace')
    
    # Agregamos al log de rechazados (append para no pisar si hay otros)
    _save_to_silver(df_rejected, "rejected_log", if_exists='append')

def transform_retail():
    df_raw = _read_from_bronze("online_retail")
    df_clean, df_rejected = _apply_business_rules(df_raw, "online_retail")
    
    # Guardamos el limpio en STG
    _save_to_silver(df_clean, "stg_retail", if_exists='replace')
    
    # Agregamos al log de rechazados
    _save_to_silver(df_rejected, "rejected_log", if_exists='append')

def unify_silver():
    """Une las dos tablas STG, elimina duplicados y crea la tabla Silver final."""
    logger.info("======================================================")
    logger.info("Iniciando Fusión y Deduplicación en la capa Silver...")
    
    engine = _get_engine()
    
    logger.info("Leyendo tablas transitorias (STG)...")
    df_ecom = pd.read_sql_table("stg_ecommerce", con=engine, schema='silver')
    df_ret = pd.read_sql_table("stg_retail", con=engine, schema='silver')
    
    # Union
    df_unified = pd.concat([df_ecom, df_ret], ignore_index=True)
    logger.info(f"Total antes de deduplicar: {len(df_unified)} registros.")
    
    # Deduplicación basada en la transacción lógica
    # Consideramos un duplicado exacto si es la misma factura, mismo producto, misma fecha y cantidad.
    # El keep='last' retendrá el dataset más reciente en caso de cruce temporal idéntico.
    df_dedup = df_unified.drop_duplicates(
        subset=['invoice_no', 'stock_code', 'customer_id', 'invoice_date', 'quantity'], 
        keep='last'
    )
    logger.info(f"Total después de deduplicar: {len(df_dedup)} registros.")
    
    # Guardar la tabla oficial y unificada
    _save_to_silver(df_dedup, "transactions", if_exists='replace')
    
    # Eliminar las tablas transitorias de Silver para mantener limpia la BBDD
    try:
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS silver.stg_ecommerce;")
            conn.execute("DROP TABLE IF EXISTS silver.stg_retail;")
            logger.info("Tablas STG temporales eliminadas exitosamente.")
    except Exception as e:
        logger.warning(f"No se pudieron borrar las tablas STG: {e}")
        
    logger.info("¡Tabla silver.transactions creada exitosamente!")
    logger.info("======================================================")

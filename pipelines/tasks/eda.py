import logging
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
DOCS_DIR = Path("/opt/airflow/docs/EDA")
DOCS_DIR.mkdir(parents=True, exist_ok=True)

def _get_bronze_dataframe(table_name: str) -> pd.DataFrame:
    """Helper interno para descargar datos desde PostgreSQL (Bronze) a Pandas."""
    hook = PostgresHook(postgres_conn_id="results_postgres_db")
    conn_obj = hook.get_connection("results_postgres_db")
    
    db_name = "db_geisler_prueba"
    sqlalchemy_uri = f"postgresql://{conn_obj.login}:{conn_obj.password}@{conn_obj.host}:{conn_obj.port}/{db_name}"
    engine = create_engine(sqlalchemy_uri)
    
    logger.info(f"Descargando tabla bronze.{table_name}...")
    # Leemos la tabla completa desde Postgres
    df = pd.read_sql_table(table_name, con=engine, schema='bronze')
    logger.info(f"Descarga completa. Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
    return df

def generate_ydata_profile(table_name: str):
    """Genera el reporte HTML interactivo con ydata-profiling."""
    logger.info("======================================================")
    logger.info(f"Iniciando YData Profiling para bronze.{table_name}...")
    
    from ydata_profiling import ProfileReport
    
    df = _get_bronze_dataframe(table_name)
    
    # minimal=True acelera mucho el proceso para archivos masivos
    profile = ProfileReport(df, title=f"EDA Profiling - {table_name}", minimal=True)
    
    output_file = DOCS_DIR / f"{table_name}_ydata_report.html"
    profile.to_file(output_file)
    
    logger.info(f"Reporte YData Profiling guardado exitosamente en: {output_file}")
    logger.info("======================================================")

def _df_to_markdown_table(df: pd.DataFrame) -> str:
    """Convierte un DataFrame a formato tabla Markdown sin requerir 'tabulate'."""
    if df.empty:
        return "*Tabla vacía*"
    # Formatear números a 2 decimales si son floats
    df_fmt = df.copy()
    for col in df_fmt.columns:
        if df_fmt[col].dtype == 'float64':
            df_fmt[col] = df_fmt[col].apply(lambda x: f"{x:.2f}")
            
    header = "| " + " | ".join(str(c) for c in df_fmt.columns) + " |"
    separator = "|-" + "-|-".join(["-" * len(str(c)) for c in df_fmt.columns]) + "-|"
    rows = []
    for _, row in df_fmt.iterrows():
        rows.append("| " + " | ".join(str(x) for x in row.values) + " |")
    return "\n".join([header, separator] + rows)

def generate_manual_eda(table_name: str):
    """Genera gráficos manuales y un reporte consolidado Markdown leyendo desde Bronze."""
    logger.info("======================================================")
    logger.info(f"Iniciando EDA Manual (Reporte Markdown + PNGs) para bronze.{table_name}...")
    
    # 1. Limpieza de artefactos anteriores (Idempotencia)
    old_files = list(DOCS_DIR.glob(f"{table_name}_*"))
    for f in old_files:
        try:
            f.unlink()
            logger.info(f"Limpieza: Archivo anterior eliminado -> {f.name}")
        except Exception as e:
            logger.warning(f"No se pudo eliminar {f.name}: {e}")
            
    df = _get_bronze_dataframe(table_name)
    
    # Métricas base
    total_rows, total_cols = df.shape
    
    # Porcentaje exacto de nulos
    null_pct = (df.isnull().sum() / len(df)) * 100
    null_df = null_pct[null_pct > 0].reset_index()
    null_df.columns = ['Columna', '% Nulos']
    null_df = null_df.sort_values(by='% Nulos', ascending=False)
    
    # Estadísticas descriptivas
    desc_df = df.describe().reset_index()
    desc_df.rename(columns={'index': 'Statistic'}, inplace=True)
    
    # 1. Gráfico de Nulos
    plt.figure(figsize=(10, 6))
    if not null_df.empty:
        sns.barplot(x='Columna', y='% Nulos', data=null_df, palette='viridis')
        plt.title(f'Porcentaje de Valores Nulos - {table_name}')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
    else:
        plt.text(0.5, 0.5, '0% Nulos en todas las columnas', horizontalalignment='center', verticalalignment='center', fontsize=15)
        plt.axis('off')
    
    plt.savefig(DOCS_DIR / f"{table_name}_nulls.png")
    plt.close()
    
    # 2. Tipos de Datos (Distribución)
    plt.figure(figsize=(8, 5))
    df.dtypes.astype(str).value_counts().plot(kind='bar', color='skyblue')
    plt.title(f'Distribución de Tipos de Datos - {table_name}')
    plt.xlabel('Tipo de Dato Crudo (Bronze)')
    plt.ylabel('Cantidad de Columnas')
    plt.tight_layout()
    plt.savefig(DOCS_DIR / f"{table_name}_dtypes.png")
    plt.close()
    
    # 3. Matriz de Correlación Numérica
    numeric_df = df.select_dtypes(include=['float64', 'int64'])
    plt.figure(figsize=(10, 8))
    if not numeric_df.empty and numeric_df.shape[1] > 1:
        sample_df = numeric_df.sample(min(10000, numeric_df.shape[0]))
        sns.heatmap(sample_df.corr(), annot=True, cmap='coolwarm', fmt=".2f")
        plt.title(f'Matriz de Correlación (Muestra 10k) - {table_name}')
        plt.tight_layout()
    else:
        plt.text(0.5, 0.5, 'No hay suficientes columnas numéricas', horizontalalignment='center', verticalalignment='center', fontsize=12)
        plt.axis('off')
        
    plt.savefig(DOCS_DIR / f"{table_name}_correlation.png")
    plt.close()
    
    # 4. Construcción del Reporte Consolidado en Markdown
    nulls_table_md = _df_to_markdown_table(null_df) if not null_df.empty else "*¡Excelente! No se encontraron valores nulos en el dataset.*"
    desc_table_md = _df_to_markdown_table(desc_df)
    
    markdown_content = f"""# Data Quality Report - Capa Bronze: `{table_name}`

## Dimensiones del Dataset
- **Filas Totales:** {total_rows:,}
- **Columnas Totales:** {total_cols}

## Análisis de Valores Nulos
{nulls_table_md}

![Gráfico de Nulos](./{table_name}_nulls.png)

## Resumen Estadístico (Variables Numéricas)
{desc_table_md}

![Matriz de Correlación](./{table_name}_correlation.png)

## Diccionario Físico (Tipos de Datos)
![Distribución de Tipos](./{table_name}_dtypes.png)
"""
    report_path = DOCS_DIR / f"{table_name}_manual_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)
        
    logger.info(f"Reporte EDA consolidado guardado exitosamente en: {report_path}")
    logger.info("======================================================")


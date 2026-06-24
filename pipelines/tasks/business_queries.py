import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def run_business_queries():
    """
    Ejecuta las consultas de lógica de negocio (Preguntas 1 a 6) sobre el modelo dimensional en la capa Gold,
    e imprime los resultados en los logs para poder redactar la recomendación final.
    """
    logger.info("======================================================")
    logger.info("Ejecutando Consultas de Negocio en la Capa Gold")
    logger.info("======================================================")

    # Conexión al DWH usando el PostgresHook
    hook = PostgresHook(postgres_conn_id="results_postgres_db")
    
    queries = {
        "1. Evolución mensual de ventas netas": """
            SELECT EXTRACT(YEAR FROM date_id) AS anio, EXTRACT(MONTH FROM date_id) AS mes, SUM(gross_revenue) AS ventas_netas
            FROM gold.fact_sales
            GROUP BY 1, 2 ORDER BY 1, 2;
        """,
        "2. Revenue bruto y % devoluciones por categoría": """
            SELECT p.category, SUM(f.gross_revenue) AS total_revenue_bruto, 
                   (COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0) / COUNT(*) AS porcentaje_devoluciones
            FROM gold.fact_sales f JOIN gold.dim_products p ON f.stock_code = p.stock_code
            GROUP BY p.category ORDER BY total_revenue_bruto DESC;
        """,
        "3a. Top 10 Productos por Revenue Neto": """
            SELECT p.description, SUM(f.gross_revenue) AS revenue_neto
            FROM gold.fact_sales f JOIN gold.dim_products p ON f.stock_code = p.stock_code
            GROUP BY p.description ORDER BY revenue_neto DESC LIMIT 10;
        """,
        "3b. Top 10 Productos por Tasa de Devolución (>50 ventas)": """
            SELECT p.description, (COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0) / COUNT(*) AS tasa_devolucion
            FROM gold.fact_sales f JOIN gold.dim_products p ON f.stock_code = p.stock_code
            GROUP BY p.description HAVING COUNT(*) > 50 ORDER BY tasa_devolucion DESC LIMIT 10;
        """,
        "4. Países con más transacciones y ticket promedio": """
            SELECT country, COUNT(DISTINCT invoice_no) AS total_transacciones, 
                   SUM(gross_revenue) / COUNT(DISTINCT invoice_no) AS ticket_promedio
            FROM gold.fact_sales GROUP BY country ORDER BY total_transacciones DESC LIMIT 10;
        """,
        "5. Comportamiento por tipo de cliente": """
            SELECT c.customer_type, COUNT(DISTINCT f.invoice_no) AS total_transacciones, 
                   SUM(f.gross_revenue) AS total_revenue, 
                   SUM(f.gross_revenue) / COUNT(DISTINCT f.invoice_no) AS ticket_promedio,
                   (COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0) / COUNT(*) AS tasa_devolucion
            FROM gold.fact_sales f JOIN gold.dim_customers c ON f.customer_id = c.customer_id
            GROUP BY c.customer_type;
        """,
        "6. Productos sin descripción consistente": """
            SELECT stock_code, COUNT(*) AS veces_vendido FROM gold.fact_sales
            WHERE stock_code NOT IN (SELECT stock_code FROM gold.dim_products WHERE description IS NOT NULL AND description != '')
            GROUP BY 1 ORDER BY veces_vendido DESC LIMIT 10;
        """
    }

    try:
        # Ejecutar cada query usando get_records
        for title, query in queries.items():
            logger.info(f"\n---> {title}")
            records = hook.get_records(query)
            if not records:
                logger.info("   (Sin resultados)")
            else:
                for row in records:
                    logger.info(f"   {row}")
    except Exception as e:
        logger.error(f"Error al ejecutar las consultas de negocio: {e}")
        raise

    logger.info("======================================================")
    logger.info("Consultas Finalizadas. Revisa estos logs para responder la Pregunta 7.")
    logger.info("======================================================")

import io
import csv

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Ejecuta el comando COPY de PostgreSQL en lugar de múltiples INSERTs.
    Mucho más veloz y escalable para DataFrames masivos.
    
    Uso en Pandas: df.to_sql(..., method=psql_insert_copy)
    """
    # Obtiene la conexión subyacente de la base de datos
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        # Crea un buffer en memoria RAM para simular un archivo CSV volátil
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        
        # Ensambla los nombres de columnas
        columns = ', '.join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name
            
        # Ejecuta el comando COPY de PostgreSQL directamente desde la RAM
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)

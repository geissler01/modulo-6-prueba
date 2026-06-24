# Documento de Decisiones Técnicas y de Negocio
**Proyecto:** Pipeline ETL DataMart S.A.S.

## 1. Análisis Exploratorio de Datos (EDA)
Previo a cualquier transformación, se desarrolló un pipeline en Airflow que extrae los datos desde la capa Bronze hacia memoria (Pandas) y genera reportes consolidados por cada fuente en la ruta local `docs/EDA/`. 
- **Hallazgos principales:**
  - Los datasets `ecommerce_data` (diario) y `online_retail` (histórico) presentaban un solapamiento semántico en las columnas, pero con nombres distintos (ej. `InvoiceNo` vs `Invoice`).
  - Alrededor del 20-25% de las transacciones no contaban con un identificador de cliente (`Customer ID`).
  - Se identificaron cantidades negativas (`Quantity <= 0`), correspondientes a devoluciones, cancelaciones o ajustes de stock.
  - Se identificaron precios unitarios en cero o negativos (`Unit Price <= 0`), lo cual viola las reglas de negocio de ventas y facturación.
  - Se encontraron inconsistencias semánticas en la descripción de los productos (mayúsculas y minúsculas mezcladas, espacios extra) para un mismo código de producto.

## 2. Resolución de Casos Ambiguos y Reglas de Negocio (Capa Silver)
Durante la limpieza distribuida utilizando el clúster (Capa Silver), se tomaron las siguientes decisiones de negocio y arquitectura:

### 2.1 Transacciones sin Customer ID
**Problema:** Eliminar directamente las transacciones sin cliente causaría un agujero ciego financiero, provocando una caída irreal en el cálculo de *Revenue Global* y el flujo de caja de la compañía.
**Decisión:** Se optó por conservar estas transacciones imputando el valor `-1` al campo `Customer ID`. Adicionalmente, se añadió una bandera booleana (`is_guest_customer = True`) para identificar clara e independientemente que esa venta fue realizada por un usuario no registrado o "invitado", permitiendo al equipo de producto aislar o analizar su comportamiento de compra respecto a los registrados.

### 2.2 Cantidades y Precios Negativos
**Problema:** Devoluciones (cantidades negativas) y precios inválidos estaban mezclados en la facturación del ERP operacional.
**Decisión:**
- **Cantidades:** Se conserva el signo negativo original para no alterar el cálculo de flujo de inventario y poder restar matemáticamente las devoluciones del Revenue Bruto. Se añadió una bandera booleana explícita `is_return = True` a todos los registros donde `Quantity <= 0`.
- **Precios:** Siguiendo la regla de oro del negocio, todo registro con `Unit Price <= 0` es considerado un error operacional irrecuperable. Se programó un filtro que rechaza en caliente estos registros; no llegan a la tabla limpia, sino que son re-dirigidos a una bitácora de auditoría transaccional (`silver.rejected_log`) con el motivo del fallo adjunto.

### 2.3 Normalización y Fusión de Datasets
**Problema:** Dos datasets (uno que fungió como histórico y otro como el flujo del día actual) con esquemas parecidos y posibles datos superpuestos.
**Decisión:** 
- Todas las columnas se homologaron forzosamente al estándar principal en memoria.
- Las descripciones de productos se unificaron utilizando mayúsculas sostenidas y eliminación de espacios laterales (`str.upper().str.strip()`), resolviendo el problema de múltiples descripciones como `Candle Holder` vs `CANDLE HOLDER` para el mismo código de barras.
- Todas las fechas de facturación se estandarizaron al formato ISO y se forzó su zona horaria a `UTC`.
- **Deduplicación:** Se implementó una deduplicación semántica. Si dos fuentes reportan exactamente el mismo `invoice_no, stock_code, customer_id, invoice_date, quantity`, se conserva solo el más reciente. Además, se inyectó linaje de datos (`source_dataset`) para conocer el origen exacto de cada fila en el almacén de datos unificado (`silver.transactions`).

## 3. Garantía de Idempotencia
**Problema:** Si el Scheduler de Airflow se dispara varias veces en un mismo día por fallos mecánicos o para aplicar un re-procesamiento (backfill), los resultados deben ser idénticos y no multiplicarse exponencialmente afectando los BI Dashboards.
**Decisión:**
- **Capa Bronze (Ingesta):** Se utiliza el modo `if_exists='replace'` sobre la inserción del archivo inicial de cada lote. Si un DAG se reejecuta, la tabla cruda vuelve a su estado exacto, eliminando la duplicidad desde la raíz de ingesta.
- **EDA Local:** Los reportes de EDA buscan y ejecutan un `unlink()` profundo borrando todo rastro de los gráficos (`.png`) o reportes Markdown del ciclo anterior antes de calcular nuevas métricas.
- **Capa Silver (Transformación):** Durante el cruce unificado, la tabla maestra de Data Quality (`silver.transactions`) se regenera o reemplaza. Como el filtrado ya deduplicó en memoria, el volcado a la BD es atómico. Adicionalmente, el pipeline borra (`DROP TABLE`) activamente las tablas transitorias `silver.stg_*` al terminar, dejando la base de datos sin carga residual y garantizando un estado prístino tras cada ejecución.

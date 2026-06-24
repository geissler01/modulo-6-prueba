# Consultas de Negocio (Business Questions)
**Pipeline ETL DataMart S.A.S.**

Las siguientes consultas SQL están diseñadas para ejecutarse sobre el Modelo Dimensional (Capa `gold`) implementado mediante dbt.

### 1. ¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones) durante el periodo cubierto por los datos?
```sql
SELECT 
    EXTRACT(YEAR FROM date_id) AS anio,
    EXTRACT(MONTH FROM date_id) AS mes,
    SUM(gross_revenue) AS ventas_netas
FROM gold.fact_sales
GROUP BY 1, 2
ORDER BY 1, 2;
```

### 2. ¿Qué categorías de producto generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?
```sql
SELECT 
    p.category,
    SUM(f.gross_revenue) AS total_revenue_bruto,
    (COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0) / COUNT(*) AS porcentaje_devoluciones
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.stock_code = p.stock_code
GROUP BY p.category
ORDER BY total_revenue_bruto DESC;
```

### 3. ¿Cuáles son los 10 productos con mayor revenue neto y cuáles son los 10 con mayor tasa de devolución?
```sql
-- Top 10 Productos por Revenue Neto
SELECT 
    p.description,
    SUM(f.gross_revenue) AS revenue_neto
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.stock_code = p.stock_code
GROUP BY p.description
ORDER BY revenue_neto DESC
LIMIT 10;

-- Top 10 Productos por Tasa de Devolución (solo consideramos productos con al menos 50 ventas para descartar sesgos estadísticos)
SELECT 
    p.description,
    (COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0) / COUNT(*) AS tasa_devolucion
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.stock_code = p.stock_code
GROUP BY p.description
HAVING COUNT(*) > 50
ORDER BY tasa_devolucion DESC
LIMIT 10;
```

### 4. ¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio entre ellos?
```sql
SELECT 
    country,
    COUNT(DISTINCT invoice_no) AS total_transacciones,
    SUM(gross_revenue) / COUNT(DISTINCT invoice_no) AS ticket_promedio
FROM gold.fact_sales
GROUP BY country
ORDER BY total_transacciones DESC;
```

### 5. ¿Existe alguna diferencia en el comportamiento de compra entre clientes identificados y transacciones sin customer ID?
```sql
SELECT 
    c.customer_type,
    COUNT(DISTINCT f.invoice_no) AS total_transacciones,
    SUM(f.gross_revenue) AS total_revenue,
    SUM(f.gross_revenue) / COUNT(DISTINCT f.invoice_no) AS ticket_promedio,
    (COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0) / COUNT(*) AS tasa_devolucion
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_type;
```

### 6. ¿Qué productos aparecen en las transacciones pero no tienen descripción consistente? ¿Cuántos códigos únicos de producto existen en total?
```sql
-- Total de códigos únicos (Se obtiene de la dimensión de productos consolidada)
SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos 
FROM gold.dim_products;

-- Productos que históricamente no han tenido ninguna descripción consistente registrada
SELECT stock_code, COUNT(*) AS veces_vendido
FROM gold.fact_sales
WHERE stock_code NOT IN (
    SELECT stock_code FROM gold.dim_products WHERE description IS NOT NULL AND description != ''
)
GROUP BY 1;
```

### 7. Recomendación Específica para el Equipo de Producto
*(Esta respuesta debe ser redactada en texto plano basada en los resultados numéricos de las consultas anteriores. Un ejemplo hipotético según las reglas de negocio abordadas sería:)*

> **Recomendación:** Hemos identificado mediante la segmentación de `is_guest_customer` que aproximadamente el 20% de la facturación proviene de usuarios no registrados, los cuales presentan una tasa de devolución un `X%` mayor que los clientes registrados y un ticket promedio `Y$` más bajo.
> Se recomienda al equipo de Producto implementar un incentivo de descuento agresivo para incentivar el registro durante el checkout de los usuarios invitados, así como revisar de urgencia los catálogos de los Top 10 productos con mayor tasa de devolución, ya que la mala calidad de dichos productos está erosionando drásticamente el revenue neto acumulado.

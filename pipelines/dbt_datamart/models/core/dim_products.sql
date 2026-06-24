{{ config(materialized='table') }}

WITH base_products AS (
    -- Seleccionamos la última descripción válida registrada para evitar descripciones vacías o nulas.
    SELECT
        stock_code,
        MAX(description) AS description
    FROM silver.transactions
    GROUP BY stock_code
)

SELECT
    stock_code,
    description,
    -- Regla de Negocio: Asignación dinámica de categoría según palabras clave en la descripción
    CASE 
        WHEN description LIKE '%SHIRT%' OR description LIKE '%BAG%' OR description LIKE '%NECKLACE%' THEN 'Ropa y Accesorios'
        WHEN description LIKE '%PHONE%' OR description LIKE '%CABLE%' OR description LIKE '%USB%' THEN 'Electrónica'
        WHEN description LIKE '%BALL%' OR description LIKE '%BIKE%' OR description LIKE '%SPORT%' THEN 'Deportes'
        WHEN description LIKE '%PAPER%' OR description LIKE '%PEN%' OR description LIKE '%PENCIL%' THEN 'Papelería'
        ELSE 'Hogar y Decoración'
    END AS category,
    'Global' AS provider_country,
    TRUE AS is_active
FROM base_products

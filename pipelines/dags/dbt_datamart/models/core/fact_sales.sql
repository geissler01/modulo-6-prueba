{{ config(materialized='table') }}

SELECT
    invoice_no,
    stock_code,
    customer_id,
    CAST(invoice_date AS DATE) AS date_id,
    invoice_date AS exact_timestamp,
    country,
    quantity,
    unit_price,
    -- Regla de negocio: El Gross Revenue es la cantidad * el precio unitario.
    -- Las devoluciones restan revenue.
    (quantity * unit_price) AS gross_revenue,
    is_return,
    source_dataset
FROM silver.transactions

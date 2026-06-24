{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id,
    is_guest_customer,
    CASE WHEN is_guest_customer THEN 'Guest' ELSE 'Registered' END AS customer_type
FROM silver.transactions

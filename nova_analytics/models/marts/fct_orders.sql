SELECT
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    amount,
    status,
    order_date
FROM shop.fact_orders;

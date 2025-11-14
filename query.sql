-- E-commerce Data Report Query
-- This query joins users, orders, order_items, products, and payments tables
-- to generate a comprehensive order report

SELECT 
    o.id AS order_id,
    u.name AS user_name,
    p.name AS product_name,
    oi.quantity,
    oi.price,
    (oi.quantity * oi.price) AS total_amount,
    pay.payment_status
FROM 
    order_items oi
INNER JOIN 
    orders o ON oi.order_id = o.id
INNER JOIN 
    users u ON o.user_id = u.id
INNER JOIN 
    products p ON oi.product_id = p.id
LEFT JOIN 
    payments pay ON o.id = pay.order_id
ORDER BY 
    o.id, p.name;



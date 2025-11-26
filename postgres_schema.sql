CREATE TABLE public.factinternetsales_streaming (
    sales_order_number VARCHAR(20) NOT NULL,
    sales_order_line_number INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_date DATE NOT NULL,
    due_date DATE NOT NULL,
    ship_date DATE,
    order_quantity INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    total_price NUMERIC(12,2) GENERATED ALWAYS AS (order_quantity * unit_price) STORED,
    currency_code CHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sales_order_number, sales_order_line_number)
);


INSERT INTO public.factinternetsales_streaming
(sales_order_number, sales_order_line_number, customer_id, product_id, order_date, due_date, ship_date, order_quantity, unit_price)
VALUES
('SO001', 1, 101, 1001, '2025-11-01', '2025-11-05', '2025-11-03', 10, 25.50),
('SO001', 2, 101, 1002, '2025-11-01', '2025-11-05', '2025-11-04', 5, 15.00),
('SO002', 1, 102, 1001, '2025-11-02', '2025-11-06', '2025-11-05', 7, 25.50),

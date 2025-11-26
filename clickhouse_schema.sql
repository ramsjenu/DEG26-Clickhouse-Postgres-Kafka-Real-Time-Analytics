CREATE TABLE default.factinternetsales_streaming_kafka
(
    sales_order_number String,
    sales_order_line_number UInt32,
    customer_id UInt32,
    product_id UInt32,
    order_date Date,
    due_date Date,
    ship_date Date,
    order_quantity UInt32,
    unit_price Float32,
    total_price Float32,
    currency_code String,
    created_at DateTime
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'streaming.public.factinternetsales_streaming',
    kafka_group_name = 'clickhouse_consumer_group_2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE default.sales
(
    sales_order_number String,
    sales_order_line_number UInt32,
    customer_id UInt32,
    product_id UInt32,
    order_date Date,
    due_date Date,
    ship_date Date,
    order_quantity UInt32,
    unit_price Float32,
    currency_code String,
    created_at DateTime64(6)
)
ENGINE = MergeTree()
ORDER BY sales_order_number;


CREATE MATERIALIZED VIEW default.sales_mv
TO default.sales
AS
SELECT *
FROM default.factinternetsales_streaming_kafka;


CREATE VIEW default.vw_sales AS
SELECT
    s.*,
    toDateTime64(created_at, 3, 'America/New_York') AS created_date,
    toDateTime64(now64(), 3, 'America/New_York') AS dt
FROM default.sales s;


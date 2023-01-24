CREATE DATABASE IF NOT EXISTS analytics ON CLUSTER cluster_1;

CREATE TABLE IF NOT EXISTS analytics.driver_ratings ON CLUSTER cluster_1(
    rate UInt8,
    userID Int64,
    driverID String,
    orderID String,
    inserted_time DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
PARTITION BY driverID
ORDER BY (inserted_time);

CREATE TABLE analytics.driver_ratings_queue ON CLUSTER cluster_1(
    rate UInt8,
    userID Int64,
    driverID String,
    orderID String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'driver-ratings',
    kafka_group_name = 'rating_readers',
    kafka_format = 'Avro',
    kafka_max_block_size = 1048576;

CREATE MATERIALIZED VIEW analytics.driver_ratings_queue_mv ON CLUSTER cluster_1 TO analytics.driver_ratings AS
SELECT rate, userID, driverID, orderID
FROM analytics.driver_ratings_queue;

CREATE TABLE IF NOT EXISTS analytics.user_ratings ON CLUSTER cluster_1(
    rate UInt8,
    userID Int64,
    driverID String,
    orderID String,
    inserted_time DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
    PARTITION BY userID
    ORDER BY (inserted_time);

CREATE TABLE analytics.user_ratings_queue ON CLUSTER cluster_1(
    rate UInt8,
    userID Int64,
    driverID String,
    orderID String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'user-ratings',
    kafka_group_name = 'rating_readers',
    kafka_format = 'JSON',
    kafka_max_block_size = 1048576;

CREATE MATERIALIZED VIEW analytics.user_ratings_queue_mv ON CLUSTER cluster_1 TO analytics.user_ratings AS
SELECT rate, userID, driverID, orderID
FROM analytics.user_ratings_queue;

CREATE TABLE IF NOT EXISTS analytics.orders ON CLUSTER cluster_1(
    from_place String,
    to_place String,
    userID Int64,
    driverID String,
    orderID String,
    inserted_time DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
    PARTITION BY driverID
    ORDER BY (inserted_time);

CREATE TABLE analytics.orders_queue ON CLUSTER cluster_1(
    from_place String,
    to_place String,
    userID Int64,
    driverID String,
    orderID String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'order_readers',
    kafka_format = 'Avro',
    kafka_max_block_size = 1048576;

CREATE MATERIALIZED VIEW analytics.orders_queue_mv ON CLUSTER cluster_1 TO orders AS
SELECT from_place, to_place, userID, driverID, orderID
FROM analytics.orders_queue;

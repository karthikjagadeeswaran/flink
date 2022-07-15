SET sql-client.execution.result-mode=TABLEAU;

CREATE TABLE KafkaTable (
        `asset_id` STRING,
        `name` STRING,
        `tag` STRING,
        `timestamp` TIMESTAMP(3),
        `value` DOUBLE,
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '20' SECOND
        ) WITH (
        'connector' = 'kafka',
        'topic' = 'test_input_topic',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'earliest-offset',
        'value.format' = 'json'
        );

CREATE TABLE sinkKafkaMin (
            asset_id STRING,
            name STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `min_value` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_test_topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'json'
        );

CREATE VIEW IF NOT EXISTS KafkaTableView
  AS
SELECT * FROM(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY `timestamp`,asset_id,name,tag
        ORDER BY `timestamp` desc) AS rownum
    FROM KafkaTable)
WHERE rownum=1;


INSERT INTO sinkKafkaMin 
    SELECT
        asset_id,
        name,
        tag,
        TUMBLE_START(`timestamp`, INTERVAL '20' SECONDS) AS `timestamp`,
        TUMBLE_END(`timestamp`, INTERVAL '20' SECONDS) AS `agg_ts`,
        MIN(`value`) AS min_value
    FROM KafkaTableView
    GROUP BY
        TUMBLE(`timestamp`, INTERVAL '20' SECONDS),
        asset_id,
        name,
        tag;

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

CREATE TABLE sinkParquet (
            asset_id STRING,
            name STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `run_time` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'parquet',
            'path' = '/home/karthik/work/flink/runtime.parquet'
        );

CREATE VIEW IF NOT EXISTS KafkaTableView
  AS
SELECT * FROM KafkaTable;

create temporary function runtime as 'ai.plantsense.runminutehourly2.RunTimeOverWindow';

INSERT INTO sinkParquet 
    SELECT
        asset_id,
        name,
        tag,
        `timestamp`,
        runtime(`timestamp`, `value`) OVER (
            PARTITION BY asset_id,name,tag
            ORDER BY `timestamp`
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS `run_time`
    FROM
        KafkaTableView;
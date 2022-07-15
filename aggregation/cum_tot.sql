SET 'execution.savepoint.path'='file:/home/karthik/work/flink/savepoints/savepoint-83fc81-074ec6ef1e2b';
SET 'state.checkpoints.dir' = 'file:///home/karthik/work/flink/checkpoints';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '1min';
-- SET 'execution.checkpointing.min-pause' = '20min';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.prefer-checkpoint-for-recovery' = 'true';
CREATE TABLE KafkaTable (
        `asset_id` STRING,
        `name` STRING,
        `tag` STRING,
        `timestamp` TIMESTAMP(3),
        `value` DOUBLE,
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '20' SECOND
        ) WITH (
        'connector' = 'kafka',
        'topic' = 'input_topic_1',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup1',
        'scan.startup.mode' = 'group-offsets',
        'value.format' = 'json'
        );
        
CREATE TABLE sinkKafkaCum (
            asset_id STRING,
            name STRING,
            tag STRING,
            `timestamp` TIMESTAMP(3),
            `agg_ts` TIMESTAMP(3),
            `cummulative_total` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_topic_1',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'json'
        );
        
create temporary function cumulativetotalflow as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlow';

CREATE VIEW IF NOT EXISTS KafkaTableView
  AS
SELECT 
    asset_id,
    name,
    tag,
    `timestamp`,
    cumulativetotalflow(`value`) OVER (
    PARTITION BY asset_id,name,tag
    ORDER BY `timestamp`
    RANGE BETWEEN INTERVAL '5' SECONDS PRECEDING AND CURRENT ROW
  ) AS `value`
FROM KafkaTable;

INSERT INTO sinkKafkaCum SELECT
    asset_id,
    name,
    tag,
    TUMBLE_START(`timestamp`, INTERVAL '60' SECONDS) AS `timestamp`,
    TUMBLE_END(`timestamp`, INTERVAL '60' SECONDS) AS `agg_ts`,
    LAST_VALUE(`value`) AS cumulative_total
FROM KafkaTableView
GROUP BY
    TUMBLE(`timestamp`, INTERVAL '60' SECONDS),
    asset_id,
    name,
    tag;

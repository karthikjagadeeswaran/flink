SET 'execution.runtime-mode' = 'batch';

CREATE TABLE csv_table (
            `timestamp` STRING,
            device_id STRING,
            `name` STRING,
            tag STRING,
            `value` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/NWLAL002PUM003.RUN_FB-update.csv'
        );

CREATE TABLE sinkKafkaCum (
            asset_id STRING,
            name STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `run_time` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_test_topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'json'
        );

CREATE VIEW IF NOT EXISTS t_view
   AS
 SELECT device_id,
        name,
        tag,
        `timestamp`,
        MINUTE(`timestamp`) as rm,
        `value`
 FROM(SELECT 
         device_id,
         name,
         tag,
         CAST(`timestamp` as TIMESTAMP_LTZ(3)) as `timestamp`,
         `value`,
         ROW_NUMBER() OVER (PARTITION BY `timestamp`,device_id,name,tag
         ORDER BY `timestamp` asc) AS rownum
     FROM csv_table)
 WHERE rownum=1;


CREATE TABLE csv_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3) NOT NULL,
            `run_time` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/output/run_fb_ow.csv'
        );

create temporary function runtime as 'ai.plantsense.runminutehourly2.RunTimeOverWindow';

INSERT INTO sinkKafkaCum 
    SELECT
        asset_id,
        name,
        tag,
        `timestamp`,
        runtime(rm, `value`) OVER (
            PARTITION BY asset_id,name,tag
            ORDER BY `timestamp`
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS `run_time`
    FROM
        KafkaTableView;
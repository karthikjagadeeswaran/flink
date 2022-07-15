SET 'state.checkpoints.dir' = 'file:///home/karthik/work/flink/checkpoints';
CREATE TABLE csv_table (
             `timestamp` TIMESTAMP(3),
             device_id STRING,
             `name` STRING,
             tag STRING,
             `value` STRING,
             WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '20' SECOND
         ) WITH (
             'connector' = 'custom',
             'format'='csv',
             'path' = '/home/karthik/work/flink/csv/hourlycsv'
         );

CREATE VIEW IF NOT EXISTS csv_table_view
  AS
SELECT device_id,
       name,
       tag,
       `timestamp`,
       CEIL(`timestamp` TO HOUR) as agg_ts,
       MINUTE(`timestamp`) as rm,
       `value` 
FROM(SELECT 
        device_id,
        name,
        tag,
        CAST(`timestamp` as TIMESTAMP_LTZ(3)) as `timestamp`,
        CAST(`value` as DOUBLE) as `value`,
        ROW_NUMBER() OVER (PARTITION BY `timestamp`,device_id,name,tag
        ORDER BY `timestamp` asc) AS rownum
    FROM csv_table)
WHERE rownum=1;

CREATE VIEW IF NOT EXISTS csv_table_view
  AS
SELECT device_id,
       name,
       tag,
       `timestamp`,
       `value` 
FROM(SELECT 
        device_id,
        name,
        tag,
        `timestamp`,
        CAST(`value` as DOUBLE) as `value`
    FROM csv_table);

create temporary function cumulativetotalflowhourly as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlowHourly';

create temporary function cumulativetotalflow as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlow';

CREATE VIEW IF NOT EXISTS csv_sink_hour_view
    AS
SELECT 
    name,device_id,tag,agg_ts,cumulativetotalflowhourly(rm,`value`) as `value`
FROM
    csv_table_view
GROUP BY GROUPING SETS ((name,device_id,tag,agg_ts));

CREATE TABLE csv_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `value` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'sink.partition-commit.policy.kind' = 'success-file',
            'path' = '/home/karthik/work/flink/csv/output/totalizer-hourly-new.csv'
        );

CREATE VIEW IF NOT EXISTS TView
  AS
SELECT 
    device_id,
    name,
    tag,
    `timestamp`,
    cumulativetotalflow(`value`) OVER (
    PARTITION BY device_id,name,tag,`timestamp`
    ORDER BY `timestamp`
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS `value`
FROM csv_table_view;

SET 'execution.savepoint.path'='file:/home/karthik/work/flink/savepoints/savepoint-dd2f64-863dcccb7ae0';

INSERT INTO csv_sink SELECT
    device_id,
    name,
    tag,
    TUMBLE_END(`timestamp`, INTERVAL '60' MINUTE) AS `agg_ts`,
    LAST_VALUE(`value`) AS cumulative_total
FROM TView
GROUP BY
    TUMBLE(`timestamp`, INTERVAL '60' MINUTE),
    device_id,
    name,
    tag;

INSERT INTO sinkKafkaCum
 SELECT 
     device_id,
     name,
     tag,
     agg_ts,
     `value` 
 FROM
     csv_sink_hour_view;

CREATE TABLE print_table (
    device_id STRING,
    name STRING,
    tag STRING,
    `agg_ts` TIMESTAMP(3),
    `value` DOUBLE
) WITH (
  'connector' = 'print'
);

CREATE TABLE blackhole_table (
    device_id STRING,
    name STRING,
    tag STRING,
    `agg_ts` TIMESTAMP(3),
    `value` DOUBLE
) WITH (
  'connector' = 'blackhole'
);

CREATE TABLE sinkKafkaCum (
            device_id STRING,
            name STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `value` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_test_topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'json'
        );


INSERT INTO csv_sink
 SELECT 
     device_id,
     name,
     tag,
     CAST(`timestamp` as TIMESTAMP_LTZ(3)) as agg_ts,
     CAST(`value` as DOUBLE) as cumulative_total 
 FROM
     csv_table;
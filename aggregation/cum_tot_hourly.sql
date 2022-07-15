SET 'execution.runtime-mode' = 'batch';
CREATE TABLE csv_table (
            tag STRING,
            `timestamp` STRING,
            device_id STRING,
            `name` STRING,
            `value` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/totalizer.csv'
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
        `value`,
        ROW_NUMBER() OVER (PARTITION BY device_id,name,tag,`timestamp`,
        ORDER BY `timestamp` asc) AS rownum
    FROM csv_table)
WHERE rownum=1;


create temporary function cumulativetotalflowhourly as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlowHourly';

CREATE TABLE csv_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `cumulative_total` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/output/totalizer-hourly.csv'
        );

CREATE VIEW IF NOT EXISTS csv_sink_hour_view
    AS
SELECT 
    name,device_id,tag,agg_ts,cumulativetotalflowhourly(rm,`value`) as cumulative_total
FROM
    csv_table_view
GROUP BY GROUPING SETS ((name,device_id,tag,agg_ts));

INSERT INTO csv_sink
SELECT 
    device_id,
    name,
    tag,
    agg_ts,
    cumulative_total 
FROM

    csv_sink_hour_view;

CREATE VIEW IF NOT EXISTS v2
    AS
SELECT 
    name,device_id,tag,CEIL(agg_ts TO DAY) as agg_ts,cumulative_total
FROM 
    csv_sink_hour_view;

CREATE TABLE csv_sink_daily (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `cumulative_total` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/output/totalizer-daily.csv');

INSERT INTO csv_sink_daily SELECT 
    name,device_id,tag,agg_ts,sum(cumulative_total) as cumulative_total
FROM
    v2
GROUP BY GROUPING SETS ((name,device_id,tag,agg_ts));

CREATE VIEW IF NOT EXISTS v3
    AS
SELECT 
    name,device_id,tag,CEIL(agg_ts TO MONTH) as agg_ts,cumulative_total
FROM 
    csv_sink_hour_view;

CREATE TABLE csv_sink_monthly (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `cumulative_total` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/output/totalizer-monthly.csv');

INSERT INTO csv_sink_monthly SELECT 
    name,device_id,tag,agg_ts,sum(cumulative_total) as cumulative_total
FROM
    v3
GROUP BY GROUPING SETS ((name,device_id,tag,agg_ts));

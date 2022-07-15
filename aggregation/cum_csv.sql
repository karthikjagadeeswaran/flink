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
            'path' = '/home/karthik/work/flink/csv/csv/NWLAL002PUM003.RUN_FB-update.csv'
        );

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
        CAST(`timestamp` as TIMESTAMP_LTZ(3)) as `timestamp`,
        `value`,
        ROW_NUMBER() OVER (PARTITION BY `timestamp`,device_id,name,tag
        ORDER BY `timestamp` asc) AS rownum
    FROM csv_table)
WHERE rownum=1;


create temporary function cumulativetotalflow as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlow';

CREATE TABLE csv_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `timestamp` TIMESTAMP(3) NOT NULL,
            `cumulative_total` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/output/nifi2.csv'
        );

INSERT INTO csv_sink SELECT
    device_id,
    name,
    tag,
    `timestamp`,
    cumulativetotalflow(`value`) OVER (
    PARTITION BY device_id,name,tag
    ORDER BY `timestamp`
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS `cumulative_total`
FROM csv_table_view;


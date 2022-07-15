-- execution mode for batch job --
SET 'execution.runtime-mode' = 'batch';

-- read from csv file --
CREATE TABLE csv_table (
            `timestamp` STRING,
            device_id STRING,
            `name` STRING,
            tag STRING,
            `value` DOUBLE
        ) WITH (
            'connector' = 'custom',
            'path' = '/home/karthik/work/flink/csv/hourlycsv'
        );

--remove duplicate values --
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
        `value`, -- CAST(`value` as DOUBLE) as `value`--
        ROW_NUMBER() OVER (PARTITION BY `timestamp`,device_id,name,tag
        ORDER BY `timestamp` asc) AS rownum
    FROM csv_table)
WHERE rownum=1;

-- create function for cumulative total --
create temporary function cumulativetotalflow as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlow';

-- create output table --
CREATE TABLE csv_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `timestamp` TIMESTAMP(3) NOT NULL,
            `cumulative_total` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/combined-new-output.csv'
        );

-- write to csv file --
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
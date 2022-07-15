-- execution mode for batch job --
SET 'execution.runtime-mode' = 'batch';

-- read from parquet file --
CREATE TABLE parquet_table (
            `name` STRING,
            device_id STRING,
            tag STRING,
            `timestamp` TIMESTAMP_LTZ(3),
            `value` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'parquet',
            'path' = '/home/karthik/work/flink/flow.parquet'
        );

--remove duplicate values --
CREATE VIEW IF NOT EXISTS parquet_table_view
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
        `value`,
        ROW_NUMBER() OVER (PARTITION BY `timestamp`,device_id,name,tag
        ORDER BY `timestamp` asc) AS rownum
    FROM parquet_table)
WHERE rownum=1;

create temporary function cumulativetotalflow as 'ai.plantsense.cumulativeTotalizer.CumulativeTotalFlow';

CREATE TABLE parquet_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `timestamp` TIMESTAMP(3) NOT NULL,
            `cumulative_total` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'parquet',
            'path' = '/home/karthik/work/flink/csv/output/flow-output.parquet'
        );

INSERT INTO parquet_sink SELECT
    device_id,
    name,
    tag,
    `timestamp`,
    cumulativetotalflow(`value`) OVER (
    PARTITION BY device_id,name,tag
    ORDER BY `timestamp`
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS `cumulative_total`
FROM parquet_table_view;

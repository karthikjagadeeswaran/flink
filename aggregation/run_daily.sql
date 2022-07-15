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

CREATE VIEW IF NOT EXISTS t_view
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
         ROW_NUMBER() OVER (PARTITION BY `timestamp`,device_id,name,tag
         ORDER BY `timestamp` asc) AS rownum
     FROM csv_table)
 WHERE rownum=1;

create temporary function hourlyrm as 'ai.plantsense.runminutehourly2.RunTimeHourly';

CREATE TABLE csv_sink (
            device_id STRING,
            `name` STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3) NOT NULL,
            `run_time` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/csv/output/run_fb-daily.csv');


CREATE VIEW IF NOT EXISTS v2
    AS
SELECT 
    name,device_id,tag,CEIL(agg_ts TO DAY) as agg_ts,run_time
FROM 
    (SELECT 
        name,device_id,tag,agg_ts,hourlyrm(rm, `value`) as run_time
    FROM
        t_view
    GROUP BY GROUPING SETS ((name,device_id,tag,agg_ts)));

INSERT INTO csv_sink SELECT 
    name,device_id,tag,agg_ts,sum(run_time) as run_time
FROM
    v2
GROUP BY GROUPING SETS ((name,device_id,tag,agg_ts));
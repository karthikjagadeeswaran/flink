CREATE TABLE random_source (
            id BIGINT, 
            name STRING,
            country STRING,
            gender STRING
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '/home/karthik/work/flink/input.csv'
        );

create temporary function udf_compute as 'org.myorg.quickstart.SampleUdf';

select udf_compute(country, 1, 3) from random_source;
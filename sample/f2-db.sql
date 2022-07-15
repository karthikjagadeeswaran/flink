CREATE TABLE KafkaTable (
        `asset_id` STRING,
        `name` STRING,
        `tag` STRING,
        `timestamp` BIGINT,
        `value` DOUBLE,
        proctime AS PROCTIME()
        ) WITH (
        'connector' = 'kafka',
        'topic' = 'test_topic',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'earliest-offset',
        'value.format' = 'json'
        );

CREATE TABLE sinkDB (
            asset_id STRING,
            name STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `max_value` DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/postgres',
            'table-name' = 't1',
            'username' = 'postgres',
            'password' = 'postgres'
        );

INSERT INTO sinkDB SELECT
          asset_id,
          name,
          tag,
          TUMBLE_END(`proctime`, INTERVAL '20' SECONDS) AS `agg_ts`,
          MAX(`value`) AS max_value
        FROM KafkaTable
        GROUP BY
          TUMBLE(`proctime`, INTERVAL '20' SECONDS),
          asset_id,
          name,
          tag;
-- create source table with kafka connector avro format --
CREATE TABLE user_behavior3 (
    a STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'source_topic3',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup3',
    'scan.startup.mode' = 'latest-offset',
    'properties.auto.offset.reset' = 'latest',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://localhost:8081',
    'value.fields-include' = 'EXCEPT_KEY'
);

-- create sink table with kafka connector json format --
CREATE TABLE sink_table(
    a VARCHAR
) WITH (
    'connector' = 'kafka',
    'topic' = 'sink_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- transfer data from source to sink --
INSERT INTO SELECT * FROM user_behavior3;
from pyflink.table import TableEnvironment, EnvironmentSettings

def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-sql-connector-kafka-1.15.0.jar;file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-sql-avro-confluent-registry-1.15.0.jar;file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-sql-avro-1.15.0.jar")
    
    source_ddl = """
           CREATE TABLE user_behavior3 (
            a STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'source_topic',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'testGroup3',
                'scan.startup.mode' = 'latest-offset',
                'properties.auto.offset.reset' = 'latest',
                'value.format' = 'avro-confluent',
                'value.avro-confluent.schema-registry.url' = 'http://localhost:8081',
                'value.fields-include' = 'EXCEPT_KEY'
            )

            """

    sink_ddl = """
            CREATE TABLE sink_table(
                kafka_key_id STRING,
                a STRING NOT NULL
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'sink_topic',
              'properties.bootstrap.servers' = 'localhost:9092',
              'key.format' = 'avro-confluent',
              'key.avro-confluent.url' = 'http://localhost:8081',
              'key.fields' = 'kafka_key_id',
              'value.format' = 'avro-confluent',
              'value.avro-confluent.schema-registry.url' = 'http://localhost:8081',
              'value.fields-include' = 'EXCEPT_KEY',
              'key.avro-confluent.subject' = 'sink_topic-key3',
              'value.avro-confluent.subject' = 'sink_topic-value3'
            )
            """

    t_env.execute_sql(source_ddl).print()
    t_env.execute_sql(sink_ddl).print()
    # t_env.execute_sql('select * from user_behavior3').print()

    sink = t_env.sql_query("SELECT a as kafka_key_id,a FROM user_behavior3")
    sink.execute_insert("sink_table").wait()


if __name__ == '__main__':
    log_processing()
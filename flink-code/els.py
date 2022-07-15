from pyflink.table import TableEnvironment, EnvironmentSettings

def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-sql-connector-kafka-1.15.0.jar;file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-sql-connector-elasticsearch7-1.15.0.jar")
    
    source_ddl = """
            CREATE TABLE source_table(
                f1 STRING
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'test_source_topic',
              'properties.bootstrap.servers' = 'localhost:9092',
              'properties.group.id' = 'testGroup',
              'json.ignore-parse-errors' = 'true',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    sink_ddl = """
            CREATE TABLE sink_table (
                f1 STRING
            ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://localhost:9200',
            'index' = 'test123',
            'failure-handler' = 'ignore'
            );
            """

    t_env.execute_sql(source_ddl).print()
    t_env.execute_sql(sink_ddl).print()

    t_env.sql_query("SELECT * FROM source_table") \
        .execute_insert("sink_table").wait()


if __name__ == '__main__':
    log_processing()
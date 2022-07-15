from pyflink.table import StreamTableEnvironment,EnvironmentSettings 
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
import logging,sys,os
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.common.serialization import JsonRowSerializationSchema, JsonRowDeserializationSchema


def func():
    # create a batch TableEnvironment
    # env_settings = EnvironmentSettings.in_batch_mode()
    # table_env = TableEnvironment.create(env_settings)

    # create a streaming Environment
    env_settings = StreamExecutionEnvironment.get_execution_environment()
    env_settings.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    table_env = StreamTableEnvironment.create(env_settings, environment_settings=EnvironmentSettings
                                        .new_instance()
                                        .in_streaming_mode()
                                        .use_blink_planner().build())

    table_env.get_config().get_configuration().set_string("parallelism.default", "1")

    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar'
                            # 'flink-connector-kafka-base_2.12-1.11.6.jar'
                            )

    table_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))

    # serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
    #     type_info=Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.DOUBLE()])).build()

    # deserialization_schema = JsonRowDeserializationSchema.builder() \
    #     .type_info(type_info=Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.DOUBLE()])).build()

    # create sql table from kafka
    source_query = """
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
        )
    """

    table_env.execute_sql(source_query)
    # create and initiate loading of source Table
    tbl = table_env.from_path('KafkaTable')

    print('\nSource Schema')
    tbl.print_schema()

    # kafka_consumer = FlinkKafkaConsumer(
    # topics='input_topic',
    # deserialization_schema=deserialization_schema,
    # properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

    # ds = env_settings.add_source(kafka_consumer)

    # Create the datastream
    # ds = table_env.to_append_stream(table_env.from_path('KafkaTable'),
    #     Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.DOUBLE()]))

    # Define Tumbling Window Aggregate Calculation
    sql = """
        SELECT
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
          tag
    """
    # Create the datastream
    # ds = table_env.to_append_stream(table_env.sql_query(sql)
    #     Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.DOUBLE()]))

    agg = table_env.sql_query(sql)

    print('\nProcess Sink Schema')
    agg.print_schema()

    # Create Kafka Sink Table
    sink_query = """
        CREATE TABLE sinkFile (
            asset_id STRING,
            name STRING,
            tag STRING,
            `agg_ts` TIMESTAMP(3),
            `max_value` DOUBLE
        ) WITH (
            'connector'='filesystem',
            'path'='/home/karthik/work/flink/output.csv',
            'format'='csv'
        )
    """
    table_env.execute_sql(sink_query)
    # write time windowed aggregations to sink table
    agg.execute_insert('sinkFile').wait()

    table_env.execute()
    # kafka_producer = FlinkKafkaProducer(
    # topic='test_sink_topic',
    # serialization_schema=serialization_schema,
    # producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

    # write time windowed aggregations to sink table
    # table = table_env.from_data_stream(ds)
    # ds.add_sink(kafka_producer)
    # table_result = table.execute_insert('sinkKafka')
    # table_env.execute('get')
    # env_settings.execute('testing')
    # table_result.wait()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    func()
from pyflink.table import StreamTableEnvironment,EnvironmentSettings 
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
import logging,sys,os
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

    # create sql table from kafka
    source_query = """
        CREATE TABLE KafkaTable (
        `device_id` STRING,
        `name` STRING,
        `tag` STRING,
        `timestamp` TIMESTAMP(3),
        `value` DOUBLE
        ) WITH (
        'connector' = 'kafka',
        'topic' = 'input_test_topic',
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

    # Create Kafka Sink Table
    sink_query = """
        CREATE TABLE sinkKafka (
            device_id STRING,
            name STRING,
            tag STRING,
            `timestamp` TIMESTAMP(3),
            `max_value` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'json'
        )
    """
    table_env.execute_sql(sink_query)
    print('\nProcess Sink Schema')
    tbl.execute_insert('sinkKafka').wait()

    table_env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    func()
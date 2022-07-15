from pyflink.table import EnvironmentSettings, TableEnvironment, Schema, DataTypes, TableDescriptor, FormatDescriptor 
import logging,sys

# create a streaming TableEnvironment
# env_settings = EnvironmentSettings.in_streaming_mode()
# table_env = TableEnvironment.create(env_settings)

def func():
    # create a batch TableEnvironment
    env_settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().get_configuration().set_string("parallelism.default", "1")

    # create sql table from filesystem
    create_query = """
        CREATE TABLE random_source (
            id BIGINT, 
            name STRING,
            country STRING,
            gender STRING
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{}'
        )
    """.format('/home/karthik/work/flink/input.csv')

    table_env.execute_sql(create_query)

    table = table_env.sql_query("""SELECT country,COUNT(*)
        FROM random_source
        GROUP BY country""")

    # emit the table
    table.to_pandas()

    # export to filesystem
    export_query = """
        create table sink (
            word STRING,
            `count` BIGINT
        ) with (
            'connector' = 'filesystem',
            'format' = 'canal-json',
            'path' = '{}'
        )
    """.format('/home/karthik/work/flink/output.json')

    table_env.execute_sql(export_query)
    table.execute_insert("sink").wait()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    func()

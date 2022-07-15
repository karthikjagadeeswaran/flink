from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.typeinfo import *
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from pyflink.datastream import StreamExecutionEnvironment

from pyflink.table import (
    StreamTableEnvironment,
)

from pathlib import Path

from pathlib import Path
root = Path(__file__).parent.resolve()

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.Builder().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    # specify connector and format jars
    t_env.get_config().get_configuration().set_string("pipeline.jars", "file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-connector-jdbc-1.15.0.jar;file:///home/karthik/work/testdir/flink-1.15.0/lib/mysql-connector-java-8.0.29.jar")
    
    jdbc_ddl="""
      CREATE TABLE table1 (
            PersonID INT,
            Name STRING
      )
      WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test?user=sammy&password=password',
    'table-name' = 'person'
    )
    """

    jdbc_sink_ddl="""
      CREATE TABLE table2 (
            PersonID INT,
            Name STRING
      )
      WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test?user=sammy&password=password',
    'table-name' = 'person2'
    )
    """
   
    t_env.execute_sql(jdbc_ddl).print()

    t_env.execute_sql(jdbc_sink_ddl).print()

    # t_env.execute_sql("Select * from table1").print();

     
    # lq="""
    #     SELECT
    #     device_id,
    #     name,
    #     tag,
    #     TUMBLE_START(`ts`, INTERVAL '10' MINUTES) AS `ts`,
    #     TUMBLE_END(`ts`, INTERVAL '10' MINUTES) AS `agg_ts`,
    #     LAST_VALUE(`value2`) AS last_known_value
    # FROM table1
    # GROUP BY
    #     TUMBLE(`ts`, INTERVAL '10' MINUTES),
    #     device_id,
    #     name,
    #     tag
    # """

    # t_env.execute_sql(lq).print()


    t_env.execute_sql("INSERT into table2 SELECT * FROM table1" ).print()


if __name__ == '__main__':
    log_processing()
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")


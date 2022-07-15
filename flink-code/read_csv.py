from pyflink.table import EnvironmentSettings, StreamTableEnvironment,TableEnvironment
import pandas as pd
import numpy as np
from pyflink.table import DataTypes


env_settings = (
    EnvironmentSettings.new_instance().in_batch_mode().build()
)
table_env = TableEnvironment.create(environment_settings=env_settings)

table_env.execute_sql(
    """
    CREATE TABLE table1 (
          sym STRING,
         dt STRING,
        ts STRING,
        open1 DOUBLE,
       high DOUBLE,
       low DOUBLE,
       close1 DOUBLE,
       oi STRING
    ) WITH (
        'connector.type' = 'filesystem',
        'format.type' = 'csv',
        'connector.path' = 'file:///home/karthik/work/testdir/27MAY/ACC.txt'
    )
"""
)
# Sym,date,time,open,high,low,close,volume,oi
# ACC,20220527,09:08,2218.95,2218.95,2218.95,2218.95,483,0



# table_env.execute_sql(
#     """
#     CREATE TABLE table2 (
#         id2 INT,
#         ts2 TIMESTAMP(3),
#         WATERMARK FOR ts2 AS ts2 - INTERVAL '5' SECOND
#     ) WITH (
#         'connector.type' = 'filesystem',
#         'format.type' = 'csv',
#         'connector.path' = '/home/alex/work/test-flink/data2.csv'
#     )
# """
# )

table1 = table_env.from_path("table1")

pandasDF = table1.to_pandas()
print(pandasDF)
pandasDF['Date'] = pd.to_datetime(pandasDF['dt'], format='%Y%m%d').dt.strftime('%m/%d/%Y')

pandasDF['Date'] = pandasDF['Date'].map(str)+' '+pandasDF['ts']
# pandasDF['TimeStamp'] = pandasDF['Date'].astype(str)
pandasDF['TimeStamp'] = pd.to_datetime(pandasDF['Date'].astype(str)).values.astype(np.int64) // 10 ** 6
# table_env.execute_sql("""select * from table1""").print()
# table2 = table_env.from_path("table2")

print(pandasDF)

table2 = table_env.from_pandas(pandasDF,[DataTypes.STRING(), DataTypes.STRING(),DataTypes.STRING(),DataTypes.DOUBLE(),DataTypes.DOUBLE(),DataTypes.DOUBLE(),DataTypes.DOUBLE(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.TIMESTAMP(3)])

table2.print_schema()
table_env.register_table("table2",table2)

view = """CREATE VIEW IF NOT EXISTS t_view
   AS
select *,
CEIL(`TimeStamp` TO HOUR) as agg_ts
from(
 SELECT sym,
        open1,
        close1,
        high,
        low,
        CAST(`TimeStamp` as TIMESTAMP(3)) as `TimeStamp`,
        ROW_NUMBER() OVER (PARTITION BY `TimeStamp`,sym,open1,close1,high,low
         ORDER BY `TimeStamp` asc) AS rownum
     FROM table2)
 WHERE rownum=1;
"""

table_env.execute_sql(view)

lq="""
        SELECT
       *
    FROM t_view
    """

table_env.execute_sql(lq).print()

lq2="""
        SELECT
       sym,
       LAST_VALUE(`close1`) OVER (PARTITION BY sym,`TimeStamp` ROWS BETWEEN 4 preceding and current row)
    FROM t_view
    """

# lg2 = """
#   SELECT
#        sym,
#        TUMBLE_START(`TimeStamp`, INTERVAL '5' MINUTES) AS ts,
#        LAST_VALUE(`close1`) as close2
#     FROM t_view
#     GROUP BY
#         TUMBLE(`TimeStamp`, INTERVAL '5' MINUTES),
#         sym
# """

table_env.execute_sql(lq2).print()

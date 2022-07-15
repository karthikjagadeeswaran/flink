import psycopg2 as pg
import pandas as pd

conn = pg.connect(database='registry_db', user='postgres', password='postgres', host='138.201.248.228',port='31321')

# df1 = pd.read_sql('select * from phed.mst_agencies', con=conn)
# df2 = pd.read_sql('select * from phed.mst_district', con=conn)
# df3 = pd.read_sql('select * from phed.mst_blocks', con=conn)
# df4 = pd.read_sql('select * from phed.ro_meta_data', con=conn, chunksize=10000)
# df5 = pd.read_sql('select * from phed.historical_ro_data limit 100000', con=conn, chunksize=10000000)
df8 = pd.read_sql('select * from phed.mst_nonoprreason',con=conn)

# df1.to_parquet('mst_agencies.parquet',index=False)
# df2.to_parquet('mst_district.parquet',index=False)
# df3.to_parquet('mst_blocks.parquet',index=False)
df8.to_parquet('mst_nonoprreason.parquet', index=False)

# dflist = []
# for gen in df4:
#     dflist.append(gen)
# df6 = pd.concat(dflist)
# df6.to_parquet('ro_meta_data.parquet',index=False)

# df7 = pd.concat(df5)
# df7.to_parquet('historical_ro_data.parquet',index=False)

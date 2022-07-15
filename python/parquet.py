import pandas as pd
import pyarrow.parquet as pq

data = [("James ","","Smith"),
              ("Michael ","Rose"),
              ("Robert ","","Williams"),
              ("Maria ","Anne","Jones"),
              ("Jen","Mary","Brown")]

columns = ["firstname","middlename","lastname"]

# df = pq.read_table('/home/karthik/work/flink/flow.parquet').to_pandas()

# print('parquet data')
# print(df)
# df = pd.DataFrame(data=data,columns=columns)

# print(df)
# df.to_parquet('sample.parquet',index=False)
df = pq.read_table('/home/karthik/work/flink/csv/output/flow-output.parquet').to_pandas()

print('parquet data')
print(df)
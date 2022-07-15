import pandas as pd
df = pd.read_csv('example.csv')
df.to_parquet('output.parquet')

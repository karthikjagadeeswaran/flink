# import pandas as pd  
  
# # read_csv function which is used to read the required CSV file
# data = pd.read_csv('/home/karthik/work/flink/csv/CHCWP004FLW001.csv')
  
# # display 
# print("Original 'input.csv' CSV Data: \n")
# print(data)

# columns = ['unit','quality','status','str_value','json_value','tag','station0','source','m0','m1','m2','m3','tag_type','device_type','station1','variable_type','station2','project','region0','region1','region2','region3','region4','region5','account_id','site_id','server_ts','origin','solution_id']
# # drop function which is used in removing or deleting rows or columns from the CSV files
# data.drop(columns, inplace=True, axis=1)
  
# # display 
# print("\nCSV Data after deleting the column 'year':\n")
# print(data)

import csv, operator
  
# open input CSV file as source
# open output CSV file as result
with open("/home/karthik/work/flink/csv/NWLAL002PUM007.RUN_FB.csv", "r") as source:
    reader = csv.reader(source)
    reader = sorted(reader,key=operator.itemgetter(3))
    with open("/home/karthik/work/flink/csv/test123.csv", "w") as result:
        writer = csv.writer(result)
        for r in reader:
            
            # Use CSV Index to remove a column from CSV
            #r[3] = r['year']
            writer.writerow((r[3], r[4], r[5], r[9], r[6]))


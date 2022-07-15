# import psycopg2
# import numpy as np
# import pandas as pd
from datetime import datetime 
import json
# from jinja2 import Environment, BaseLoader
from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random

import calendar;
import time;

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                      value_serializer=lambda x: 
                      dumps(x).encode('utf-8'))

value = 100
name_list = ["TOTAL_FLOW"]
device_list=["NU_DEVICE"]
counter = 1
while(1):
    if value > 200:
        value = 100
    for device_id in device_list:
        print(device_id)
        timestamp= int(round(time.time()*1000))
        data_list =[]
        for name in name_list:
            # value=random.randint(5,10)
            data ={"name":name,"asset_id":device_id,"tag":device_id+"."+name,"value":value,"timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            data_list.append(data)
            producer.send('input_topic', value=data)

        # producer.send('input_topic', value=data_list)
        print("sending.....",data_list)
        value = value + 10
        sleep(5)

      

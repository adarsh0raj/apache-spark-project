import csv
import numpy as np
import re
import pandas as pd
from pyspark.sql import SparkSession

def f(s):
    data_arr = []
    match = re.match(r'(.+) - - \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] \"(\w+) .+\" (\d{3}) (\d+) .+', s)
    if match:
        data_arr = [match.group(1), match.group(2), match.group(3), match.group(4), match.group(5)]
        
    return data_arr

# Main Program
spark = SparkSession.builder.appName("Q2").getOrCreate()

# part A
data = spark.sparkContext.textFile("access.log")

# part B
data = data.map(lambda row: f(row))
orig_count = data.count()

# part C
data = data.filter(lambda row: row != [])
print("Number of bad rows: ", orig_count - data.count())

# part D a
d_1 = data.map(lambda x: (x[3], 1))
d_1 = d_1.reduceByKey(lambda x,y: x+y)

print("HTTP status analysis:\nstatus     count")
for x in d_1.collect():
    print(x[0], "     ", x[1])
    
# part D b

# part D c
d_3 = data.map(lambda x: (x[0], 1))
d_3 = d_3.reduceByKey(lambda x,y: x+y)

print("Frequent hosts:\nhost       count")
for x in d_3.collect():
    print(x[0], "     ", x[1])

# part D d
print("Unique hosts:\n", d_3.count())

# part D e


print(data.take(3))

spark.stop()
import csv
import numpy as np
import re
import pandas as pd
from pyspark.sql import SparkSession

def f(s):
    data_arr = []
    match = re.match(r'(.+) - - \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] \"(\w+) .+\" (\d{3}) (\d+) .+', s)
    match2 = re.match(r'(.+) - - \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] \"(\w+) .+\" (\d{3}) (-) .+', s)
    if match:
        data_arr = [match.group(1), match.group(2), match.group(3), match.group(4), match.group(5)]
    if match2:
        data_arr = [match2.group(1), match2.group(2), match2.group(3), match2.group(4), 0]
        
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
d_1 = d_1.reduceByKey(lambda x,y: x+y).sortByKey()

print("HTTP status analysis:\nstatus     count")
for x in d_1.collect():
    print(x[0], "     ", x[1])
    
# part D b

tots = d_1.values().reduce(lambda x,y: x+y)

d2 = d_1.map(lambda x : (x[0], x[1] / tots))

#matplotlib pie-chart of d2
import matplotlib.pyplot as plt
#pie-chart of d2
keys = d2.keys().collect()
values = d2.values().collect()
plt.pie(values, labels=keys, autopct='%1.1f%%', startangle=90)
plt.savefig('pie_chart.png')
plt.show()

# part D c
d_3 = data.map(lambda x: (x[0], 1))
d_3 = d_3.reduceByKey(lambda x,y: x+y)

print("Frequent hosts:\nhost       count")
for x in d_3.collect():
    print(x[0], "     ", x[1])

# part D d
print("Unique hosts:\n", d_3.count())

# part D e
date_dataset = data.map(lambda x: ((x[1][0:11], x[0]), 1))
date_dataset = date_dataset.reduceByKey(lambda x,y: 0)
date_dataset = date_dataset.map(lambda x: (x[0][0], 1))
date_dataset = date_dataset.reduceByKey(lambda x,y: x+y).sortByKey()

print("Unique hosts per day:\nday       hosts")

for x in date_dataset.collect():
    print(x[0], "     ", x[1])


# part D f

x = date_dataset.keys().collect()
y = date_dataset.values().collect()

plt.plot(x, y)
plt.xlabel('Day')
plt.ylabel('Hosts Count')
plt.title('No of unique hosts daily')
plt.savefig('hosts_daily.png')
plt.show()

print(data.take(3))

spark.stop()
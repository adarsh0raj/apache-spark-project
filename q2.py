import numpy as np
import re
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import matplotlib.cbook
import sys
import warnings
warnings.filterwarnings("ignore", category=matplotlib.cbook.mplDeprecation)

def f(s):
    data_arr = []
    match = re.match(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(\d{2}/[a-zA-Z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} [-+]\d{4})\] \"([A-Z]*) [^\"]*\" (\d{3}) (\d*) .+', s)
    if match:
        data_arr = [match.group(1), match.group(2), match.group(3), int(match.group(4)), int(match.group(5))]
        
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
print("Number of bad Rows: ", orig_count - data.count())

# part D a
d_1 = data.map(lambda x: (x[3], 1))
d_1 = d_1.groupByKey().mapValues(len).sortByKey()

sys.stdout = open("a.txt", "w")
print("HTTP status analysis:\nstatus    count")
for x in d_1.collect():
    print(x[0], "     ", x[1])
sys.stdout.close()

# part D b
d_1 = d_1.sortBy(lambda x: x[1], ascending=False)
d_1 = d_1.take(5)
total = sum(x[1] for x in d_1)
d_1 = [(x[0], x[1]/total) for x in d_1]

keys = [x[0] for x in d_1]
values = [x[1] for x in d_1]
plt.figure()
plt.pie(values, labels=keys, autopct='%1.1f%%', startangle=90)
plt.savefig('b.png')

# part D c
d_3 = data.map(lambda x: (x[0], 1))
d_3 = d_3.groupByKey().mapValues(len).sortByKey()

sys.stdout = open("c.txt", "w")
print("Frequent Hosts:\nhost                count")
for x in d_3.collect():
    print(x[0],"               ", x[1])
sys.stdout.close()

# part D d
sys.stdout = open("d.txt", "w")
print("Unique hosts:\n", d_3.count())
sys.stdout.close()

# part D e
date_dataset = data.map(lambda x: (x[1][0:11], x[0]))
date_dataset = date_dataset.groupByKey().map(lambda x: (x[0], len(set(x[1])))).sortByKey()

sys.stdout = open("e.txt", "w")
print("Unique hosts per day:\nday               hosts")

for x in date_dataset.collect():
    print(x[0], "     ", x[1])
sys.stdout.close()

# part D f

x = date_dataset.keys().collect()
y = date_dataset.values().collect()

plt.figure()
plt.plot(x, y)
plt.xlabel('Day')
plt.ylabel('Hosts Count')
plt.title('No of unique hosts daily')
plt.savefig('f.png')

# part D g

failure_data = data.filter(lambda x : 400 <= x[3] and x[3] < 600)
failure_data = failure_data.map(lambda x: (x[0], 1))
failure_data = failure_data.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False)

sys.stdout = open("g.txt", "w")
print("Failed HTTP Clients:")
for x in failure_data.take(5):
    print(x[0])
sys.stdout.close()

# part D h

my_data = data.filter(lambda x : x[1][0:11] == '22/Jan/2019')
total_data = my_data.map(lambda x: (int(x[1][12:14]), 1))
failure_data = my_data.filter(lambda x : 400 <= x[3]).map(lambda x: (int(x[1][12:14]), 1))

total_data = total_data.reduceByKey(lambda x,y: x+y).sortByKey()
failure_data = failure_data.reduceByKey(lambda x,y: x+y).sortByKey()

plt.figure()
plt.plot(total_data.keys().collect(), total_data.values().collect(), label='Total Requests')
plt.plot(failure_data.keys().collect(), failure_data.values().collect(), label='Failed Requests')
plt.legend()
plt.xticks(np.arange(total_data.keys().min(), total_data.keys().max() + 1, 1))
plt.savefig('h.png')

# plt.show()

# part D i

my_data = data.map(lambda x: ((x[1][0:11], x[1][12:14]), 1))
my_data = my_data.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0][0], (x[0][1], x[1])))
my_data = my_data.reduceByKey(lambda x,y: x if x[1] > y[1] else y).sortByKey()

sys.stdout = open("i.txt", "w")
print("Active Hours:")

for x in my_data.collect():
    print(x[0], "     ", x[1][0] + ":00")
sys.stdout.close()

# part D j

col = data.map(lambda x : x[4])

sys.stdout = open("j.txt", "w")
print("Response Length Statistics:")
print("Minimum length", col.min())
print("Maximum length", col.max())
print("Average length", col.mean())
sys.stdout.close()

spark.stop()
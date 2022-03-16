from pyspark.sql import SparkSession

def f(row):
    temp = []
    for i in range(1,len(row)):
        if row[i] != '':
            for j in range(i+1,len(row)):
                if row[j] != '':
                    temp.append((row[i], row[j]))
    return temp

def tocsv(data):
    temp = [data[0][0], data[0][1], data[1]]
    return ','.join(str(d) for d in temp)


# Main Program
spark = SparkSession.builder.appName("Q1").getOrCreate()

data = spark.sparkContext.textFile("groceries - groceries.csv").map(lambda line: line.split(","))
header = data.first() 
data = data.filter(lambda row: row != header)

# Create an RDD of (item, item) pairs for each row
rdd_items = data.flatMap(lambda row: f(row))

#sort the tuples of rdd and initialize count as 1
rdd_items = rdd_items.map(lambda x: (tuple(sorted(x)), 1))

# reduce the tuples and add their counts
rdd_reduced = rdd_items.reduceByKey(lambda x,y: x+y)

# sort the tuples according to their counts
rdd_reduced = rdd_reduced.sortBy(lambda x: x[1], ascending=False)

lines = rdd_reduced.map(tocsv)
val = lines.collect()

with open('count.csv', 'w') as f:
    for item in val:
        f.write("%s\n" % item)
        
for x in rdd_reduced.take(5):
    print(x[0])

spark.stop()


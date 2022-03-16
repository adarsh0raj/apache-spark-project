import csv
import numpy as np
from pyspark.sql import SparkSession

# Main Program
spark = SparkSession.builder.appName("Q2").getOrCreate()

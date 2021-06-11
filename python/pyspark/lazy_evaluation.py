from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.sql.types import *

# create spark
conf = SparkConf().setAppName("lazy_evalution").setMaster("local")
sc = SparkContext(conf=conf)

# create a sample list
my_list = [i for i in range(1,10000000)]

# parallelize the data
rdd_0 = sc.parallelize(my_list, 3)

print("### RDD Obj 0 ###")
print(rdd_0)

# add value 4 to each number
rdd_1 = rdd_0.map(lambda x : x+4)

print("### RDD Obj 1 ###")
print(rdd_1)


# get the RDD Lineage
print("### RDD Obj 1 DebugString() ###")
print(rdd_1.toDebugString())


# add value 20 each number
rdd_2 = rdd_1.map(lambda x : x+20)

print("### RDD Obj 2 ###")
print(rdd_2)
# get the RDD Lineage
print(rdd_2.toDebugString())


sc.stop()
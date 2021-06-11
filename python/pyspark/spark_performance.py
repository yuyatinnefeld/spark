from time import time
import numpy as np
from random import random
from operator import add
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession


def naive_method_time(n):
    inside = 0

    t_0 = time()
    for i in range(n):
        x, y = random(), random()
        if x**2 + y**2 < 1:
            inside += 1
    return(np.round(time()-t_0, 3))


def spark_method_time(n):
    def is_point_inside_unit_circle(p):
        # p is useless here
        x, y = random(), random()
        return 1 if x*x + y*y < 1 else 0
    t_0 = time()
    # parallelize creates a spark Resilient Distributed Dataset (RDD)
    # its values are useless in this case
    # but allows us to distribute our calculation (inside function)
    # we do not need to store the results
    sc.parallelize(range(0, n)) \
        .map(is_point_inside_unit_circle).reduce(add)
    return(np.round(time()-t_0, 3))


spark = SparkSession.builder.appName('CalculatePi').getOrCreate()
sc = spark.sparkContext

N = [1000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 100000000]
T = []
T_spark = []
T_spark_2_executors = []
for n in N:
    T_spark.append(spark_method_time(n))
    T.append(naive_method_time(n))

spark.stop()
spark = SparkSession.builder.appName('CalculatePi').getOrCreate()
sc = spark.sparkContext
for n in N:
    T_spark_2_executors.append(spark_method_time(n))

spark.stop()

plt.plot(N, T, label="naive")
plt.plot(N, T_spark, label="spark - 4 exec")
plt.plot(N, T_spark_2_executors, label="spark - 2 exec")

plt.xscale("log")
plt.xlabel("Total number of points.")
plt.ylabel("Time to estimate pi (en sec.)")
plt.legend()
plt.show()
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('employee').getOrCreate()

#spark.sparkContext.setLogLevel("DEBUG")
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")
emp_df = spark.read.csv("data/employee.txt", header=True)

#emp_df.printSchema()
#print(emp_df.columns)
#print(emp_df.take(5))

print("Total data set:", emp_df.count())
sample_df = emp_df.sample(False, 0.1)
print("Sample data set: ", sample_df.count())
emp_mgrs_df = emp_df.filter("salary >= 100000")
print("Manager data set: ", emp_mgrs_df.count())

emp_mgrs_df.select("salary").show()

spark.stop()
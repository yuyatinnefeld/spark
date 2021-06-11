from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer

spark = SparkSession.builder.appName('bucketizer').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

splits = [-float("inf"), -10.0, 0.0, 10.0, float("inf")]

b_data = [(-800.0,),(-10.5,),(-1.7,),(0.0,),(8.2,),(90.1,)]
b_df = spark.createDataFrame(b_data, ["features"])

b_df.show()

bucketizer = Bucketizer(splits=splits, inputCol="features", outputCol="bfeatures")
bucketed_df = bucketizer.transform(b_df)
bucketed_df.show()

spark.stop()
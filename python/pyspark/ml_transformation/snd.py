from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors

spark = SparkSession.builder.appName('snd').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

records = [
    (1, Vectors.dense([10.0, 10000.00, 1.0]),),
    (2, Vectors.dense([20.0, 30000.00, 2.0]),),
    (3, Vectors.dense([30.0, 40000.00, 3.0]),)
]
columns = ["id", "features"]

features_df = spark.createDataFrame(records, columns)

features_df.show(10, False)

features_stand_scaler = StandardScaler(
    inputCol="features", outputCol="sfeatures",
    withStd=True, withMean = True)

stand_smodel = features_stand_scaler.fit(features_df)
stand_sfeatures_df = stand_smodel.transform(features_df)

#print(stand_sfeatures_df.take(1))

stand_sfeatures_df.select("id","sfeatures").show(10, False)

spark.stop()
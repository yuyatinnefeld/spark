from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

spark = SparkSession.builder.appName('normalization').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

records = [
    (1, Vectors.dense([10.0, 10000.00, 1.0]),),
    (2, Vectors.dense([20.0, 30000.00, 2.0]),),
    (3, Vectors.dense([30.0, 40000.00, 3.0]),)
]
columns = ["id", "features"]

features_df = spark.createDataFrame(records, columns)

#print(features_df.take(1))

feature_scaler = MinMaxScaler(inputCol="features", outputCol="sfeatures")
s_model = feature_scaler.fit(features_df)
s_features_df = s_model.transform(features_df)

#print(s_features_df.take(1))

s_features_df.select("features", "sfeatures").show()

spark.stop()
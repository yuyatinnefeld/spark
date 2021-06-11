from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import BisectingKMeans

spark = SparkSession.builder.appName('BK-Means').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

cluster_df = spark.read.csv("data/clustering_dataset.csv", header=True, inferSchema=True)

cluster_df.show(5)

vector_assembler = VectorAssembler(inputCols=["col1", "col2", "col3"], outputCol="features")

v_cluster_df = vector_assembler.transform(cluster_df)

v_cluster_df.show(5)

bkmenas = BisectingKMeans().setK(3).setSeed(1)

bkmodel = bkmenas.fit(v_cluster_df)

bkcenters = bkmodel.clusterCenters()

print(bkcenters)


spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier

spark = SparkSession.builder.appName('Naive-Bayes').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

iris_df = spark.read.csv("data/iris.txt", header=True, inferSchema=True)

print("### data check ###")
iris_df.show(5)

print("### data rename ###")

#print(iris_df.take(2))

iris_df = iris_df.select(
    col("_c0").alias("sepal_length"),
    col("_c1").alias("sepal_width"),
    col("_c2").alias("petal_length"),
    col("_c3").alias("petal_width"),
    col("_c4").alias("species")
)

iris_df.show(5)

print("### data prep ###")

columns = ["sepal_length","sepal_width","petal_length","petal_width"]

vector_assembler = VectorAssembler(inputCols=columns, outputCol="features")

v_iris_df = vector_assembler.transform(iris_df)

indexer = StringIndexer(inputCol="species", outputCol="label")

iv_iris_df = indexer.fit(v_iris_df).transform(v_iris_df)

iv_iris_df.show(2)

print("### classification ###")

splits = iv_iris_df.randomSplit([0.6, 0.4],1)
train_df = splits[0]
test_df = splits[1]

print("Train dataset: ", train_df.count())
print("Test dataset: ", test_df.count())
print("Total dataset: ", iv_iris_df.count())

#layers = [neural_num=4, neural_num=5, neural_num=5, neural_num=3]
layers = [4, 5, 5, 3]

mlp = MultilayerPerceptronClassifier(layers=layers, seed=1)

mlp_model = mlp.fit(train_df)

mlp_predictions = mlp_model.transform(test_df)

print("prediction model")
mlp_predictions.show(5)

print("model evaluate")
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
mlp_accuarcy = evaluator.evaluate(mlp_predictions)
print("Accuarcy: ", mlp_accuarcy)

spark.stop()
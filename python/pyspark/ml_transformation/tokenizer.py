from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer

spark = SparkSession.builder.appName('Tokenizer').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

records = [
    (1, "This is an introduction to Spark MLlib"),
    (2, "MLlib includes libraries for classification and regression"),
    (3, "It also contains supporting tools for pipelines. how nice!")
]
columns = ["id", "sentence"]

sentences_df = spark.createDataFrame(records, columns)

sentences_df.show()

sent_token = Tokenizer(inputCol="sentence", outputCol="words")
sent_tokenized_df = sent_token.transform(sentences_df)

sent_tokenized_df.show()

spark.stop()
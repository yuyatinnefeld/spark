from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF


spark = SparkSession.builder.appName('TF-IDF').getOrCreate()
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

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)

sent_hfTF_df = hashingTF.transform(sent_tokenized_df)

#print(sent_hfTF_df.take(1))
sent_hfTF_df.show()

idf = IDF(inputCol="rawFeatures", outputCol="idf_features")
idfModel = idf.fit(sent_hfTF_df)
tfidf_df = idfModel.transform(sent_hfTF_df)
#print(tfidf_df.take(1))

tfidf_df.select("id", "idf_features").show(5)

spark.stop()
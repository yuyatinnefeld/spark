import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions  import *
from pyspark.sql.types import *
from pyspark.sql.functions import col


# define the configurations for this Spark program
conf = SparkConf().setMaster("local[*]").setAppName("Books")
conf.set("spark.executor.memory", "6G")
conf.set("spark.driver.memory", "2G")
conf.set("spark.executor.cores", "4")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.default.parallelism", "4")

# create a Spark Session instead of a Spark Context
spark = SparkSession.builder \
    .config(conf = conf) \
    .appName("Book Recommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")
books_df = spark.read.option("delimiter", ";").option("header", "true").csv('data/Books.csv')
user_ratings_df = spark.read.option("delimiter", ";").option("header", "true").csv('data/Ratings.csv')


print("### data check ###")
print("book data")
books_df.show(5)
print("Total dataset: ", books_df.count())
print("Publishers: ", books_df.select('Publisher').distinct().count())
num_cols = ['Author', 'Year']

print("user data")
user_ratings_df.show(5)
print("Total dataset: ", user_ratings_df.count())
print("Users: ", user_ratings_df.select('User-ID').distinct().count())

print("### data prep ###")

ratings_df = user_ratings_df \
    .withColumn("User-ID", user_ratings_df['User-ID'].cast(IntegerType())) \
    .withColumn("ISBN", user_ratings_df['ISBN'].cast(IntegerType())) \
    .withColumn("Rating", user_ratings_df['Rating'].cast(IntegerType())) \
    .na \
    .drop()

print("rating data")
ratings_df.show()



print("### create train & test model ###")
als = ALS(maxIter=5, regParam=0.01, userCol="User-ID", itemCol="ISBN", ratingCol="Rating", coldStartStrategy="drop")
#fit the model to the ratings
dataframemodel = als.fit(ratings_df)

ratings = ratings_df.filter(col('User-ID') == 17)

print("user 17 has rated the following books as shown:: ")
books_df.join(ratings, ratings.ISBN == books_df.ISBN)\
    .select(col('User-ID'),col('Title'),col('Author'),col('Year'),col('Rating'))\
    .show()

print("### recommendation ###")
user_id = [[17]]
# convert this into a dataframe so that it can be passed into the recommendForUserSubset

num_rec = 3
recommendations = dataframemodel.recommendForUserSubset(ratings_df, num_rec)
recommendations.collect()

# pick only the ISBN of the books, ignore other fields
recommended_ISBN = [recommendations.collect()[0]['recommendations'][x]['ISBN'] for x in range(0, num_rec)]

rec_df = spark.createDataFrame(recommended_ISBN, IntegerType())
print('Top ',num_rec,' book recommendations for User-ID ',user_id[0][0], ' are:')
books_df.join(rec_df,rec_df.value==books_df.ISBN).select(col('Title'),col('Author'),col('Year')).show()

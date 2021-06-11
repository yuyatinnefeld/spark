from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType


spark = SparkSession.builder.appName('LR').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("### spark starting ###")

schema = StructType([
    StructField("CRIM", DoubleType(), True),
    StructField("ZN", DoubleType(), True),
    StructField("INDUS", DoubleType(), True),
    StructField("CHAS", IntegerType(), True),
    StructField("NOX", DoubleType(), True),
    StructField("RM", DoubleType(), True),
    StructField("AGE", DoubleType(), True),
    StructField("DIS", DoubleType(), True),
    StructField("RAD", IntegerType(), True),
    StructField("TAX", DoubleType(), True),
    StructField("PTRATIO", DoubleType(), True),
    StructField("B", DoubleType(), True),
    StructField("LSTAT", DoubleType(), True),
    StructField("MEDV", DoubleType(), True)
])


house_df = spark.read\
    .options(header='False', delimiter=' ')\
    .schema(schema)\
    .csv("data/housing.csv")


print("### data check ###")
print("Total dataset: ", house_df.count())
house_df.show(3)
num_cols = ['RM', 'AGE', 'DIS', 'RAD', ]
house_df.select(num_cols).describe().show()



print("### data prep ###")
columns = ['CRIM', 'ZN', 'INDUS', 'CHAS', 'NOX', 'RM', 'AGE', 'DIS', 'RAD', 'TAX', 'PTRATIO', 'B', 'LSTAT', 'MEDV']
vector_assembler = VectorAssembler(inputCols=columns, outputCol="features")
vhouse_df = vector_assembler.transform(house_df).select(['features', 'MEDV'])
vhouse_df.show()

print("### create train & test model ###")

splits = vhouse_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

print("### linear regression ###")
lr = LinearRegression(featuresCol = 'features', labelCol='MEDV', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

print("train data")
train_df.describe().show()

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

print("save model")
lr_model.save("lr1.model")

spark.stop()
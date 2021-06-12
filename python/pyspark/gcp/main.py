from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd

storage_client = storage.Client()

def implicit():
    buckets = list(storage_client.list_buckets())
    print(buckets)

def get_blob(bucket_name, blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    print("Blob: {}".format(blob.name))
    print("Bucket: {}".format(blob.bucket.name))
    print("blob correct!")

    return blob


if __name__ == "__main__":
    spark = SparkSession.builder.appName('cloud_storage').getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    implicit()
    bucket_name = 'spark-cloud-storage'
    bucket_file_name = 'gcp_book.csv'
    blob = get_blob(bucket_name, bucket_file_name)

    downloaded_blob = blob.download_as_string().decode('utf-8')
    print(downloaded_blob)

    df = spark.read\
        .option("header",True)\
        .option("charset", "ISO-8859-1")\
        .csv(downloaded_blob)

    print("show records")
    df.show()

    print("count")
    print(df.count())

    spark.stop()



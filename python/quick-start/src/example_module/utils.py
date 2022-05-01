import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_context(app_name: str) -> SparkSession:
    conf = SparkConf()
    conf.setAll(
        [
            ("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark:7077")),
            ("spark.app.name", app_name)
        ]
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()


    return spark
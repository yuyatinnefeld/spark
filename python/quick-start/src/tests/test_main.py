import pytest
import pyspark.sql.functions as F

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from conf import spark


def test_df_count(spark):
    schema = StructType([
        StructField('Category', StringType(), True),
        StructField('Count', IntegerType(), True),
        StructField('Description', StringType(), True)
    ])

    df = spark.read.json('tests/data.json', schema, multiLine=True)
    print(df.schema)
    df.show()
    count = df.count()
    assert count == 3
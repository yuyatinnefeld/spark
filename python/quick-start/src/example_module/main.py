from examples import run_spark_example
from utils import get_spark_context


if __name__ == '__main__':
    print("### create cluster ###")
    spark = get_spark_context("hello_app")
    spark.sparkContext.setLogLevel("WARN")
    print("### create df ###")
    run_spark_example(spark)
    print("### stop cluster ###")
    spark.stop()

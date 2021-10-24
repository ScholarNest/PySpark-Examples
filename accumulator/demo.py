from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def handle_bad_rec(shipments: str) -> int:
    s = None
    try:
        s = int(shipments)
    except ValueError:
        bad_rec.add(1)
    return s


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[3]") \
        .getOrCreate()

    data_list = [("india", "india", '5'),
                 ("india", "china", '7'),
                 ("china", "india", 'three'),
                 ("china", "china", '6'),
                 ("japan", "china", 'Five')]

    df = spark.createDataFrame(data_list) \
        .toDF("source", "destination", "shipments")

    bad_rec = spark.sparkContext.accumulator(0)
    spark.udf.register("udf_handle_bad_rec", handle_bad_rec, IntegerType())
    df.withColumn("shipments_int", expr("udf_handle_bad_rec(shipments)")) \
        .show()

    print("Bad Record Count:" + str(bad_rec.value))

    

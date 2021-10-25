from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[3]") \
        .getOrCreate()

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    # join_df = flight_time_df1.join(flight_time_df2.hint("broadcast"), "id", "inner")
    
    join_df = flight_time_df1.join(broadcast(flight_time_df2), "id", "inner") \
        .hint("COALESCE", 5)

    join_df.show()
    print("Number of Output Partitions:" + str(join_df.rdd.getNumPartitions()))

    

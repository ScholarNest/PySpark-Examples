from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[3]") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50B") \
        .getOrCreate()

    df1 = spark.read.json("data/d1")
    df2 = spark.read.json("data/d2")
    print(df1.join(df2, "id", "inner").count())

    df3 = spark.read.json("data/d3")
    df4 = spark.read.json("data/d4")
    print(df3.join(df4, "id", "inner").count())

    input()

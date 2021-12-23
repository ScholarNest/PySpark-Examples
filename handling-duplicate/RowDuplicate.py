from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("DuplicateDemo") \
        .master("local[3]") \
        .getOrCreate()

    data_list = [(101, "Mumbai", "Goa"),
                 (102, "Mumbai", "Bangalore"),
                 (102, "Mumbai", "Bangalore"),
                 (103, "Delhi", "Chennai"),
                 (104, "Bangalore", "Kolkata")]

    df = spark.createDataFrame(data_list) \
        .toDF("id", "source", "destination")

    # How to check if you have duplicates
    dups = df.count() - df.distinct().count()
    if dups > 0:
        print("You have {} duplicates".format(dups))

    # How to find the duplicate row
    df.groupBy(df.columns) \
        .count() \
        .filter('count > 1') \
        .show()

    # How to remove duplicates
    df.dropDuplicates() \
        .show()

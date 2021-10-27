from pyspark.sql import SparkSession
import threading


def do_job(f1, f2):
    df1 = spark.read.json(f1)
    df2 = spark.read.json(f2)
    outputs.append(df1.join(df2, "id", "inner").count())


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[3]") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50B") \
        .config("spark.scheduler.mode", "FAIR") \
        .getOrCreate()

    file_prefix = "data/d"
    jobs = []
    outputs = []

    for i in range(0, 2):
        file1 = file_prefix + str(i + 1)
        file2 = file_prefix + str(i + 2)
        thread = threading.Thread(target=do_job, args=(file1, file2))
        jobs.append(thread)

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

    print(outputs)
    input()

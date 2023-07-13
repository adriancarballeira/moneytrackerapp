import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, abs, col, round, expr 

if __name__ == "__main__":
    """
        Usage:  Reads in .csv of transactions downloaded from Barclays (credit card purchases).
                Analyzes how many dollars are spent where.
                Returns summary to user.

                To execute:
./bin/spark-submit \
../Git/moneytrackerapp/Barclays.py

    """

    spark = SparkSession\
        .builder\
        .appName("Barclays")\
        .getOrCreate()

    ccStatement = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv("./data/Barclays/CreditCard_20230101_20230711.csv")

    ccStatement.createOrReplaceTempView("ccStatement")

    ccStCount = ccStatement.count()

    ccTotals = ccStatement\
        .groupBy("Description")\
        .sum("amount")\
        .withColumnRenamed("sum(amount)", "amount_total")\
        .orderBy("amount_total")

    ccTotals = ccTotals\
        .withColumn("amount_total", round(ccTotals.amount_total, 2))

    ccTotals.show()

    print("CC statement transaction count: %f" % ccStCount)

    spark.stop()